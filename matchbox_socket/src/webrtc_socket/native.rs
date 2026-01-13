use super::{HandshakeResult, PacketSendError, PeerDataSender, SignallerBuilder, messages::{PeerEvent, PeerRequest, merge_ice_configs}, PeerBuffered, BufferedChannel, StatsProvider};
use webrtc::stats::StatsReportType;
use crate::{
    RtcIceServerConfig,
    webrtc_socket::{
        ChannelConfig, Messenger, Packet, Signaller, error::SignalingError, messages::PeerSignal,
        signal_peer::SignalPeer, socket::create_data_channels_ready_fut,
    },
};
use async_compat::CompatExt;
use async_trait::async_trait;
use async_tungstenite::{
    WebSocketStream,
    async_std::{ConnectStream, connect_async},
    tungstenite::Message,
};
use bytes::Bytes;
use futures::{
    Future, FutureExt, StreamExt,
    future::{Fuse, FusedFuture},
    stream::FuturesUnordered,
};
use futures_channel::{mpsc::{Receiver, Sender, TrySendError, UnboundedReceiver, UnboundedSender}, oneshot};
use futures_timer::Delay;
use futures_util::{lock::Mutex, select};
use log::{debug, error, info, trace, warn};
use matchbox_protocol::PeerId;
use std::{pin::Pin, sync::Arc, time::Duration};
use std::ops::{Deref, DerefMut};
use std::sync::Weak;
use futures::executor::block_on;
use webrtc::{api::APIBuilder, data_channel::{RTCDataChannel, data_channel_init::RTCDataChannelInit}, ice_transport::{
    ice_candidate::{RTCIceCandidate, RTCIceCandidateInit},
    ice_gatherer_state::RTCIceGathererState,
    ice_server::RTCIceServer,
}, peer_connection::{
    RTCPeerConnection, configuration::RTCConfiguration,
    sdp::session_description::RTCSessionDescription,
}, Error};
use webrtc::data_channel::data_channel_state::RTCDataChannelState;
use crate::webrtc_socket::batch::Batch;
use crate::webrtc_socket::error::PeerError;

impl From<webrtc::Error> for SignalingError {
    fn from(value: Error) -> Self {
        Self::UserImplementationError(format!("{value:?}"))
    }
}

pub(crate) struct NativeSignaller {
    websocket_stream: WebSocketStream<ConnectStream>,
}

#[derive(Debug, Default)]
pub(crate) struct NativeSignallerBuilder;

#[derive(Clone)]
pub(crate) struct RtcDataChannelWrapper(Arc<RTCDataChannel>);

impl RtcDataChannelWrapper {
    fn new(data_channel: Arc<RTCDataChannel>) -> Arc<Self> {
        Arc::new(Self(data_channel))
    }
}

impl Deref for RtcDataChannelWrapper {
    type Target = Arc<RTCDataChannel>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[async_trait]
impl StatsProvider for RTCPeerConnection {
    async fn channel_bytes_sent_received(&self, stats_id: &str) -> Option<(usize, usize)> {
        self.get_stats().await.reports.values().find_map(|it| {
            let StatsReportType::DataChannel(channel_report) = it else {
                return None
            };

            channel_report.label.eq(stats_id).then(|| (channel_report.bytes_sent, channel_report.bytes_received))
        })
    }

    async fn rtt(&self) -> Option<f64> {
        self.get_stats().await.reports.values().find_map(|it| {
            let StatsReportType::CandidatePair(pair_report) = it else {
                return None;
            };

            // Return RTT in milliseconds, converting from seconds
            // current_round_trip_time is in seconds, convert to milliseconds
            Some(pair_report.current_round_trip_time * 1000.0)
        })
    }
}

impl Drop for RtcDataChannelWrapper {
    fn drop(&mut self) {
        let state = self.0.ready_state();
        if state == RTCDataChannelState::Closed || state == RTCDataChannelState::Closing {
            return;
        }

        // This should not happens, we just want to make sure the channel always gets closed
        info!("closing data channel on drop");
        if let Err(e) = block_on(self.0.close()) {
            warn!("failed to close data channel: {e:?}");
        }

        info!("data channel closed");
    }
}

#[async_trait]
impl SignallerBuilder for NativeSignallerBuilder {
    async fn new_signaller(
        &self,
        mut attempts: Option<u16>,
        room_url: String,
    ) -> Result<Box<dyn Signaller>, SignalingError> {
        let websocket_stream = 'signaling: loop {
            match connect_async(&room_url).await.map_err(SignalingError::from) {
                Ok((wss, _)) => break wss,
                Err(e) => {
                    if let Some(attempts) = attempts.as_mut() {
                        if *attempts <= 1 {
                            return Err(SignalingError::NegotiationFailed(Box::new(e)));
                        } else {
                            *attempts -= 1;
                            warn!(
                                "connection to signaling server failed, {attempts} attempt(s) remain"
                            );
                            warn!("waiting 3 seconds to re-try connection...");
                            Delay::new(Duration::from_secs(3)).await;
                            info!("retrying connection...");
                            continue 'signaling;
                        }
                    } else {
                        continue 'signaling;
                    }
                }
            };
        };
        Ok(Box::new(NativeSignaller { websocket_stream }))
    }
}

#[async_trait]
impl Signaller for NativeSignaller {
    async fn send(&mut self, request: PeerRequest) -> Result<(), SignalingError> {
        let request = serde_json::to_string(&request).expect("serializing request");
        self.websocket_stream
            .send(Message::Text(request.into()))
            .await
            .map_err(SignalingError::from)
    }

    async fn next_message(&mut self) -> Result<PeerEvent, SignalingError> {
        let message = match self.websocket_stream.next().await {
            Some(Ok(Message::Text(message))) => Ok(message),
            Some(Ok(_)) => Err(SignalingError::UnknownFormat),
            Some(Err(err)) => Err(SignalingError::from(err)),
            None => Err(SignalingError::StreamExhausted),
        }?;
        let message = serde_json::from_str(&message).map_err(|e| {
            error!("failed to deserialize message: {e:?}");
            SignalingError::UnknownFormat
        })?;
        Ok(message)
    }
}

pub(crate) struct NativeMessenger;

pub(crate) struct PeerDataSenderWrapper<T> {
    sender: UnboundedSender<T>,
    is_reliable: bool,
}

impl<T> Deref for PeerDataSenderWrapper<T> where T: Send + 'static {
    type Target = UnboundedSender<T>;
    fn deref(&self) -> &Self::Target {
        &self.sender
    }
}

impl<T> DerefMut for PeerDataSenderWrapper<T> where T: Send + 'static {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.sender
    }
}

#[async_trait]
impl PeerDataSender for PeerDataSenderWrapper<Packet> {
    fn send(&mut self, packet: Packet) -> Result<(), PacketSendError> {
        self.unbounded_send(packet)
            .map_err(|source| PacketSendError {
                source: TrySendError::into_send_error(source),
            })
    }

    fn is_reliable(&self) -> bool {
        self.is_reliable
    }

    async fn close(&mut self) -> Result<(), PacketSendError> {
        self.close_channel();
        Ok(())
    }
}

#[async_trait]
impl BufferedChannel for Arc<RtcDataChannelWrapper> {
    async fn buffered_amount(&self) -> usize {
        RTCDataChannel::buffered_amount(&self).await
    }

    fn stats_id(&self) -> Option<String> {
        Some(self.label().to_owned())
    }
}

#[async_trait]
impl Messenger for NativeMessenger {
    type DataChannel = PeerDataSenderWrapper<Packet>;
    type HandshakeMeta = (
        Vec<UnboundedReceiver<Packet>>,
        Vec<Arc<RtcDataChannelWrapper>>,
        Pin<Box<dyn FusedFuture<Output = Result<(), webrtc::Error>> + Send>>,
        Receiver<()>,
        Arc<RTCPeerConnection>
    );

    async fn offer_handshake(
        signal_peer: SignalPeer,
        mut peer_signal_rx: UnboundedReceiver<PeerSignal>,
        messages_from_peers_tx: Vec<UnboundedSender<(PeerId, Packet)>>,
        ice_server_config: RtcIceServerConfig,
        channel_configs: &[ChannelConfig],
        timeout: Duration,
    ) -> Result<HandshakeResult<Self::DataChannel, Self::HandshakeMeta>, PeerError> {
        async {
            let (to_peer_message_tx, to_peer_message_rx) =
                new_senders_and_receivers(channel_configs);
            let (peer_disconnected_tx, peer_disconnected_rx) = futures_channel::mpsc::channel(1);

            debug!("making offer");
            let (connection, trickle) =
                create_rtc_peer_connection(signal_peer.clone(), &ice_server_config)
                    .await
                    .map_err(|e| PeerError(signal_peer.id, SignalingError::HandshakeFailed))?;

            let (data_channel_ready_txs, data_channels_ready_fut) =
                create_data_channels_ready_fut(channel_configs);

            let (data_channels, buffer_low_rxs) = create_data_channels(
                &connection,
                data_channel_ready_txs,
                signal_peer.id,
                peer_disconnected_tx,
                messages_from_peers_tx,
                channel_configs,
            )
            .await?;

            let stats = Arc::downgrade(&connection);
            let peer_buffered = PeerBuffered::new(
                data_channels.iter().map(|it| {
                    let channel: Box<dyn BufferedChannel> = Box::new(it.clone());
                    channel
                }).collect::<Vec<_>>(),
                stats,
                buffer_low_rxs,
            );

            // TODO: maybe pass in options? ice restart etc.?
            let offer = connection.create_offer(None).await.map_err(|e| PeerError(signal_peer.id, SignalingError::HandshakeFailed))?;
            connection.set_local_description(offer).await.map_err(|e| PeerError(signal_peer.id, SignalingError::HandshakeFailed))?;
            wait_for_ice_gathering_complete(signal_peer.id, &connection, timeout).await?;
            let offer_sdp = connection.local_description().await.unwrap().sdp;
            info!("offer sdp: {}", offer_sdp);
            signal_peer.send(PeerSignal::Offer {
                offer: offer_sdp,
                config: None
            });

            let answer = loop {
                let signal = match peer_signal_rx.next().await {
                    Some(signal) => signal,
                    None => {
                        warn!("Signal server connection lost in the middle of a handshake");
                        return Err(PeerError(signal_peer.id, SignalingError::HandshakeFailed));
                    }
                };

                match signal {
                    PeerSignal::Answer(answer) => {
                        break answer;
                    }
                    PeerSignal::Offer { .. } => {
                        warn!("Got an unexpected Offer, while waiting for Answer. Ignoring.")
                    }
                    PeerSignal::IceCandidate(_) => {
                        warn!("Got an unexpected IceCandidate, while waiting for Answer. Ignoring.")
                    }
                };
            };

            let remote_description = RTCSessionDescription::answer(answer).map_err(|e| PeerError(signal_peer.id, SignalingError::HandshakeFailed))?;
            if let Err(e) = connection
                .set_remote_description(remote_description)
                .await {
                warn!("failed to set remote description: {e:?}");
                return Err(PeerError(signal_peer.id, SignalingError::HandshakeFailed));
            }

            let trickle_fut = complete_handshake(
                signal_peer.id.clone(),
                trickle,
                &connection,
                peer_signal_rx,
                data_channels_ready_fut,
                timeout
            )
            .await?;

            Ok(HandshakeResult::<Self::DataChannel, Self::HandshakeMeta> {
                peer_id: signal_peer.id,
                data_channels: to_peer_message_tx,
                peer_buffered,
                metadata: (
                    to_peer_message_rx,
                    data_channels,
                    trickle_fut,
                    peer_disconnected_rx,
                    connection
                ),
            })
        }
        .compat() // Required to run tokio futures with other async executors
        .await
    }

    async fn accept_handshake(
        signal_peer: SignalPeer,
        mut peer_signal_rx: UnboundedReceiver<PeerSignal>,
        messages_from_peers_tx: Vec<UnboundedSender<(PeerId, Packet)>>,
        ice_server_config: &RtcIceServerConfig,
        channel_configs: &[ChannelConfig],
        timeout: Duration,
    ) -> Result<HandshakeResult<Self::DataChannel, Self::HandshakeMeta>, PeerError> {
        async {
            debug!("handshake_accept");

            // Wait for offer first
            let (offer_sdp, offer_ice_config) = loop {
                let signal = match peer_signal_rx.next().await {
                    Some(signal) => signal,
                    None => {
                        warn!("Signal server connection lost in the middle of a handshake");
                        return Err(PeerError(signal_peer.id, SignalingError::HandshakeFailed));
                    }
                };
                match signal {
                    PeerSignal::Offer { offer, config } => {
                        break (offer, config);
                    }
                    _ => {
                        warn!("ignoring other signal!!!");
                    }
                }
            };
            debug!("received offer");

            // Merge ice configs if offer contains config
            let merged_ice_config = offer_ice_config.unwrap_or(ice_server_config.clone());

            let (to_peer_message_tx, to_peer_message_rx) =
                new_senders_and_receivers(channel_configs);
            let (peer_disconnected_tx, peer_disconnected_rx) = futures_channel::mpsc::channel(1);

            // Create connection with merged config
            let (connection, trickle) =
                create_rtc_peer_connection(signal_peer.clone(), &merged_ice_config)
                    .await.map_err(|e| PeerError(signal_peer.id, SignalingError::HandshakeFailed))?;

            let (data_channel_ready_txs, data_channels_ready_fut) =
                create_data_channels_ready_fut(channel_configs);

            let (data_channels, buffer_low_rxs) = create_data_channels(
                &connection,
                data_channel_ready_txs,
                signal_peer.id,
                peer_disconnected_tx.clone(),
                messages_from_peers_tx,
                channel_configs,
            )
            .await?;

            let stats_provider = Arc::downgrade(&connection);
            let peer_buffered = PeerBuffered::new(
                data_channels.iter().map(|it| {
                    let channel: Box<dyn BufferedChannel> = Box::new(it.clone());
                    channel
                }).collect::<Vec<_>>(),
                stats_provider,
                buffer_low_rxs,
            );

            let remote_description = RTCSessionDescription::offer(offer_sdp).map_err(|e| PeerError(signal_peer.id, SignalingError::HandshakeFailed))?;
            connection
                .set_remote_description(remote_description)
                .await.map_err(|e| PeerError(signal_peer.id, SignalingError::HandshakeFailed))?;

            let answer = connection.create_answer(None).await.map_err(|e| PeerError(signal_peer.id, SignalingError::HandshakeFailed))?;
            connection.set_local_description(answer).await.map_err(|e| PeerError(signal_peer.id, SignalingError::HandshakeFailed))?;
            wait_for_ice_gathering_complete(signal_peer.id, &connection, timeout).await?;
            let answer_sdp = connection.local_description().await.unwrap().sdp;
            info!("answer sdp: {}", answer_sdp);
            signal_peer.send(PeerSignal::Answer(answer_sdp));

            let trickle_fut = complete_handshake(
                signal_peer.id.clone(),
                trickle,
                &connection,
                peer_signal_rx,
                data_channels_ready_fut,
                timeout.clone()
            )
            .await?;

            Ok(HandshakeResult::<Self::DataChannel, Self::HandshakeMeta> {
                peer_id: signal_peer.id,
                data_channels: to_peer_message_tx,
                peer_buffered,
                metadata: (
                    to_peer_message_rx,
                    data_channels,
                    trickle_fut,
                    peer_disconnected_rx,
                    connection
                ),
            })
        }
        .compat() // Required to run tokio futures with other async executors
        .await
    }

    async fn peer_loop(peer_uuid: PeerId, handshake_meta: Self::HandshakeMeta) -> PeerId {
        async {
            let (mut to_peer_message_rx, data_channels, mut trickle_fut, mut peer_disconnected, conn) =
                handshake_meta;

            assert_eq!(
                data_channels.len(),
                to_peer_message_rx.len(),
                "amount of data channels and receivers differ"
            );

            let mut message_loop_futs: FuturesUnordered<_> = data_channels
                .clone()
                .into_iter()
                .zip(to_peer_message_rx.iter_mut())
                .map(|(data_channel, rx)| {
                    async move {
                        while let Some(message) = rx.next().await {
                            trace!("sending packet {message:?}");
                            let message = message.clone();
                            let message = Bytes::from(message);
                            if let Err(e) = data_channel.send(&message).await {
                                error!("error sending to data channel: {e:?}")
                            }
                        }
                    }
                })
                .collect();

            loop {
                select! {
                    _ = peer_disconnected.next() => break,

                    _ = message_loop_futs.next() => break,
                    // TODO: this means that the signaling is down, should return an
                    // error
                    _ = trickle_fut => continue,
                }
            }

            for data_channel in data_channels {
                if let Err(e) = data_channel.close().await {
                    error!("failed to close data channel: {e:?}");
                }
            }

            info!("peer disconnected: {}", peer_uuid);
            if let Err(e) = conn.close().await {
                error!("failed to close peer connection: {e:?}");
            }

            peer_uuid
        }
        .compat() // Required to run tokio futures with other async executors
        .await
    }
}

fn new_senders_and_receivers<T>(
    channel_configs: &[ChannelConfig],
) -> (Vec<PeerDataSenderWrapper<T>>, Vec<UnboundedReceiver<T>>) {
    let mut tx = vec![];
    let mut rx = vec![];
    for channel_config in channel_configs {
        let (s, r) = futures_channel::mpsc::unbounded();
        tx.push(PeerDataSenderWrapper {
            sender: s,
            is_reliable: channel_config.ordered && channel_config.max_retransmits.is_none()
        });
        rx.push(r);
    }

    (tx, rx)
}

async fn complete_handshake<T: Future<Output = ()>>(
    peer_id: PeerId,
    trickle: Arc<CandidateTrickle>,
    connection: &Arc<RTCPeerConnection>,
    peer_signal_rx: UnboundedReceiver<PeerSignal>,
    mut wait_for_channels: Pin<Box<Fuse<T>>>,
    timeout: Duration,
) -> Result<Pin<Box<Fuse<impl Future<Output = Result<(), webrtc::Error>> + use<T>>>>, PeerError> {
    trickle.send_pending_candidates().await;
    let mut trickle_fut = Box::pin(
        CandidateTrickle::listen_for_remote_candidates(Arc::clone(connection), peer_signal_rx)
            .fuse(),
    );

    let mut timeout = Delay::new(timeout).fuse();

    loop {
        select! {
            _ = wait_for_channels => {
                break;
            },
            _ = timeout => {
                warn!("Peer {peer_id} handshake timeout");
                return Err(PeerError(peer_id, SignalingError::HandshakeFailed));
            },
            result = trickle_fut => {
                let _ = result.map_err(|it| PeerError(peer_id, SignalingError::HandshakeFailed))?;
                break;
            },
        };
    }

    Ok(trickle_fut)
}

async fn wait_for_ice_gathering_complete(
    peer_id: PeerId,
    connection: &Arc<RTCPeerConnection>,
    timeout: Duration,
) -> Result<(), PeerError> {
    let (tx, rx) = oneshot::channel();
    let tx = Arc::new(Mutex::new(Some(tx)));

    let tx_clone = tx.clone();
    connection.on_ice_gathering_state_change(Box::new(move |state: RTCIceGathererState| {
        if state == RTCIceGathererState::Complete {
            let tx_clone = tx_clone.clone();
            Box::pin(async move {
                if let Some(sender) = tx_clone.lock().await.take() {
                    let _ = sender.send(());
                }
            })
        } else {
            Box::pin(async {})
        }
    }));

    let mut delay = Delay::new(timeout).fuse();
    let mut rx = rx.fuse();

    select! {
        _ = delay => {
            warn!("timeout waiting for ice gathering to complete");
            return Err(PeerError(peer_id, SignalingError::HandshakeFailed));
        },
        _ = rx => {
            info!("Ice gathering completed");
        }
    }

    Ok(())
}

struct CandidateTrickle {
    signal_peer: SignalPeer,
    pending: Mutex<Vec<String>>,
}

impl CandidateTrickle {
    fn new(signal_peer: SignalPeer) -> Self {
        Self {
            signal_peer,
            pending: Default::default(),
        }
    }

    async fn on_local_candidate(
        &self,
        peer_connection: &RTCPeerConnection,
        candidate: RTCIceCandidate,
    ) {
        let candidate_init = match candidate.to_json() {
            Ok(candidate_init) => candidate_init,
            Err(err) => {
                error!("failed to convert ice candidate to candidate init, ignoring: {err}");
                return;
            }
        };

        let candidate_json =
            serde_json::to_string(&candidate_init).expect("failed to serialize candidate to json");

        // Local candidates can only be sent after the remote description
        if peer_connection.remote_description().await.is_some() {
            // Can send local candidate already
            debug!("sending IceCandidate signal: {candidate:?}");
            self.signal_peer
                .send(PeerSignal::IceCandidate(candidate_json));
        } else {
            // Can't send yet, store in pending
            debug!("storing pending IceCandidate signal: {candidate_json:?}");
            self.pending.lock().await.push(candidate_json);
        }
    }

    async fn send_pending_candidates(&self) {
        let mut pending = self.pending.lock().await;
        for candidate in std::mem::take(&mut *pending) {
            self.signal_peer.send(PeerSignal::IceCandidate(candidate));
        }
    }

    async fn listen_for_remote_candidates(
        peer_connection: Arc<RTCPeerConnection>,
        mut peer_signal_rx: UnboundedReceiver<PeerSignal>,
    ) -> Result<(), webrtc::Error> {
        while let Some(signal) = peer_signal_rx.next().await {
            match signal {
                PeerSignal::IceCandidate(candidate_json) => {
                    debug!("received ice candidate: {candidate_json:?}");
                    match serde_json::from_str::<RTCIceCandidateInit>(&candidate_json) {
                        Ok(candidate_init) => {
                            debug!("ice candidate received: {}", candidate_init.candidate);
                            peer_connection.add_ice_candidate(candidate_init).await?;
                        }
                        Err(err) => {
                            if *candidate_json == *"null" {
                                debug!(
                                    "Received null ice candidate, this means there are no further ice candidates"
                                );
                            } else {
                                warn!("failed to parse ice candidate json, ignoring: {err:?}");
                            }
                        }
                    }
                }
                PeerSignal::Offer { .. } => {
                    warn!("Got an unexpected Offer, while waiting for IceCandidate. Ignoring.")
                }
                PeerSignal::Answer(_) => {
                    warn!("Got an unexpected Answer, while waiting for IceCandidate. Ignoring.")
                }
            }
        }

        Ok(())
    }
}

async fn create_rtc_peer_connection(
    signal_peer: SignalPeer,
    ice_server_config: &RtcIceServerConfig,
) -> Result<(Arc<RTCPeerConnection>, Arc<CandidateTrickle>), Box<dyn std::error::Error>> {
    let api = APIBuilder::new().build();

    let config = RTCConfiguration {
        ice_servers: vec![RTCIceServer {
            urls: ice_server_config.urls.clone(),
            username: ice_server_config.username.clone().unwrap_or_default(),
            credential: ice_server_config.credential.clone().unwrap_or_default(),
        }],
        ..Default::default()
    };

    let connection = api.new_peer_connection(config).await?;
    let connection = Arc::new(connection);

    let trickle = Arc::new(CandidateTrickle::new(signal_peer));

    let connection2 = Arc::downgrade(&connection);
    let trickle2 = trickle.clone();
    connection.on_ice_candidate(Box::new(move |c| {
        let connection2 = connection2.clone();
        let trickle2 = trickle2.clone();
        Box::pin(async move {
            if let Some(c) = c {
                if let Some(connection2) = connection2.upgrade() {
                    trickle2.on_local_candidate(&connection2, c).await;
                } else {
                    warn!("missing peer_connection?");
                }
            }
        })
    }));

    Ok((connection, trickle))
}

async fn create_data_channels(
    connection: &RTCPeerConnection,
    mut data_channel_ready_txs: Vec<futures_channel::mpsc::Sender<()>>,
    peer_id: PeerId,
    peer_disconnected_tx: Sender<()>,
    from_peer_message_tx: Vec<UnboundedSender<(PeerId, Packet)>>,
    channel_configs: &[ChannelConfig],
) -> Result<(Vec<Arc<RtcDataChannelWrapper>>, Vec<futures_channel::mpsc::UnboundedReceiver<()>>), PeerError> {
    let mut channels = vec![];
    let mut buffer_low_rxs = Vec::new();
    for (i, channel_config) in channel_configs.iter().enumerate() {
        let (buffer_low_tx, buffer_low_rx) = futures_channel::mpsc::unbounded();
        buffer_low_rxs.push(buffer_low_rx);
        let channel = create_data_channel(
            connection,
            data_channel_ready_txs.pop().unwrap(),
            peer_id,
            peer_disconnected_tx.clone(),
            from_peer_message_tx.get(i).unwrap().clone(),
            buffer_low_tx,
            channel_config,
            i,
        )
        .await?;

        channels.push(channel);
    }

    Ok((channels, buffer_low_rxs))
}

async fn create_data_channel(
    connection: &RTCPeerConnection,
    mut channel_ready: futures_channel::mpsc::Sender<()>,
    peer_id: PeerId,
    peer_disconnected_tx: Sender<()>,
    from_peer_message_tx: UnboundedSender<(PeerId, Packet)>,
    buffer_low_tx: futures_channel::mpsc::UnboundedSender<()>,
    channel_config: &ChannelConfig,
    channel_index: usize,
) -> Result<Arc<RtcDataChannelWrapper>, PeerError> {
    let config = RTCDataChannelInit {
        ordered: Some(channel_config.ordered),
        negotiated: Some(channel_index as u16),
        max_retransmits: channel_config.max_retransmits,
        ..Default::default()
    };

    let ch = connection
        .create_data_channel(&format!("matchbox_socket_{channel_index}"), Some(config))
        .await.map_err(|it| PeerError(peer_id.clone(), it.into()))?;
    let channel = RtcDataChannelWrapper::new(ch);

    // Set buffer low threshold if configured
    if let Some(threshold) = channel_config.buffer_low_threshold {
        channel.set_buffered_amount_low_threshold(threshold).await;
    }

    channel.on_open(Box::new(move || {
        debug!("Data channel ready");
        Box::pin(async move {
            channel_ready.try_send(()).unwrap();
        })
    }));

    {
        let mut peer_disconnected_tx = peer_disconnected_tx.clone();
        channel.on_close(Box::new(move || {
            debug!("Data channel closed");
            if let Err(err) = peer_disconnected_tx.try_send(()) {
                // should only happen if the socket is dropped, or we are out of memory
                warn!("failed to notify about data channel closing: {err:?}");
            }
            Box::pin(async move {})
        }));
    }

    {
        let mut peer_disconnected_tx = peer_disconnected_tx.clone();
        channel.on_error(Box::new(move |e| {
            warn!("data channel error {e:?}");
            if let Err(err) = peer_disconnected_tx.try_send(()) {
                warn!("failed to notify about data channel error: {err:?}");
            }

            Box::pin(async move {})
        }));
    }

    let mut current_batch = None::<Batch>;
    let is_reliable = channel_config.ordered &&
        channel_config.max_retransmits.is_none();
    channel.on_message(Box::new(move |message| {
        let mut packet: Packet = (*message.data).into();
        if let Some(batch) = if !is_reliable { None } else {
            Batch::from_bytes(&packet, &peer_id)
        } {
            current_batch.replace(batch);
            return Box::pin(async move {});
        }

        if let Some(batch) = current_batch.as_mut() {
            if batch.full_fill(&packet) {
                packet = current_batch.take().map(|it| it.into_packet()).unwrap_or_default();
            }
            else {
                return Box::pin(async move {});
            }
        };

        if let Err(e) = from_peer_message_tx.unbounded_send((peer_id, packet)) {
            // should only happen if the socket is dropped, or we are out of memory
            warn!("failed to notify about data channel message: {e:?}");
        }

        Box::pin(async move {})
    }));

    channel.on_buffered_amount_low(Box::new(move || {
        trace!("Data channel buffer low");
        let _ = buffer_low_tx.unbounded_send(());
        Box::pin(async move {})
    })).await;

    Ok(channel)
}
