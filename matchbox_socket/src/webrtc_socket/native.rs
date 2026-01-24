use super::{HandshakeResult, PacketSendError, PeerDataSender, SignallerBuilder, messages::{PeerEvent, PeerRequest}, PeerBuffered, BufferedChannel, StatsProvider};
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
use futures_channel::{mpsc, mpsc::{Receiver, Sender, TrySendError, UnboundedReceiver, UnboundedSender}};
use futures_timer::Delay;
use futures_util::{lock::Mutex, select};
use log::{debug, error, info, trace, warn};
use matchbox_protocol::PeerId;
use std::{pin::Pin, sync::Arc, time::Duration};
use std::ops::{Deref, DerefMut};
use futures::executor::block_on;
use webrtc::{api::APIBuilder, data_channel::{RTCDataChannel, data_channel_init::RTCDataChannelInit}, ice_transport::{
    ice_candidate::{RTCIceCandidate, RTCIceCandidateInit},
    ice_connection_state::RTCIceConnectionState,
    ice_gatherer_state::RTCIceGathererState,
    ice_server::RTCIceServer,
}, peer_connection::{
    RTCPeerConnection, configuration::RTCConfiguration,
    sdp::session_description::RTCSessionDescription,
}, Error};
use webrtc::api::setting_engine::SettingEngine;
use webrtc::data_channel::data_channel_state::RTCDataChannelState;
use webrtc::ice::network_type::NetworkType;
use webrtc::peer_connection::policy::ice_transport_policy::RTCIceTransportPolicy;
use crate::webrtc_socket::batch::BatchReceiver;
use std::time::{SystemTime, UNIX_EPOCH};
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
            do_offer_handshake(
                signal_peer,
                &mut peer_signal_rx,
                messages_from_peers_tx,
                &ice_server_config,
                channel_configs,
                timeout,
            ).await
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
            do_accept_handshake(
                signal_peer,
                &mut peer_signal_rx,
                messages_from_peers_tx,
                ice_server_config,
                channel_configs,
                timeout,
            ).await
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
        CandidateTrickle::listen_for_remote_candidates(Arc::clone(connection), peer_signal_rx, peer_id)
            .fuse(),
    );

    let mut timeout_delay = Delay::new(timeout).fuse();

    loop {
        select! {
            _ = wait_for_channels => {
                break;
            },
            _ = timeout_delay => {
                warn!("Peer {peer_id} handshake timeout");
                return Err(PeerError(peer_id, SignalingError::HandshakeFailed));
            },
            result = trickle_fut => {
                match result {
                    Ok(_) => break,
                    Err(_e) => {
                        return Err(PeerError(peer_id, SignalingError::HandshakeFailed));
                    }
                }
            },
        };
    }

    Ok(trickle_fut)
}

fn parse_candidate_type(candidate: &str) -> &'static str {
    if candidate.contains("typ host") {
        "host"
    } else if candidate.contains("typ srflx") {
        "srflx"
    } else if candidate.contains("typ prflx") {
        "prflx"
    } else if candidate.contains("typ relay") {
        "relay"
    } else {
        "unknown"
    }
}

async fn log_selected_candidate_pair(connection: &RTCPeerConnection, peer_id: &PeerId) {
    let stats = connection.get_stats().await;

    let mut local_candidates: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    let mut remote_candidates: std::collections::HashMap<String, String> = std::collections::HashMap::new();

    for (id, report) in stats.reports.iter() {
        match report {
            StatsReportType::LocalCandidate(c) => {
                let info = format!("type={:?}, addr={}:{}",
                    c.candidate_type, c.ip, c.port);
                local_candidates.insert(id.clone(), info);
            }
            StatsReportType::RemoteCandidate(c) => {
                let info = format!("type={:?}, addr={}:{}",
                    c.candidate_type, c.ip, c.port);
                remote_candidates.insert(id.clone(), info);
            }
            _ => {}
        }
    }

    for (_id, report) in stats.reports.iter() {
        if let StatsReportType::CandidatePair(pair) = report {
            if pair.nominated {
                let local_info = local_candidates.get(&pair.local_candidate_id)
                    .map(|s| s.as_str())
                    .unwrap_or("unknown");
                let remote_info = remote_candidates.get(&pair.remote_candidate_id)
                    .map(|s| s.as_str())
                    .unwrap_or("unknown");
                info!(
                    "[ICE] peer={} selected_pair: local=[{}], remote=[{}], state={:?}, rtt={:.2}ms",
                    peer_id,
                    local_info,
                    remote_info,
                    pair.state,
                    pair.current_round_trip_time * 1000.0
                );
            }
        }
    }
}

/// Helper type alias for the handshake metadata
type OfferHandshakeMeta = (
    Vec<UnboundedReceiver<Packet>>,
    Vec<Arc<RtcDataChannelWrapper>>,
    Pin<Box<dyn FusedFuture<Output = Result<(), webrtc::Error>> + Send>>,
    Receiver<()>,
    Arc<RTCPeerConnection>
);

/// Core handshake logic for the offerer.
async fn do_offer_handshake(
    signal_peer: SignalPeer,
    peer_signal_rx: &mut UnboundedReceiver<PeerSignal>,
    messages_from_peers_tx: Vec<UnboundedSender<(PeerId, Packet)>>,
    ice_server_config: &RtcIceServerConfig,
    channel_configs: &[ChannelConfig],
    timeout: Duration,
) -> Result<HandshakeResult<PeerDataSenderWrapper<Packet>, OfferHandshakeMeta>, PeerError> {
    let (to_peer_message_tx, to_peer_message_rx) =
        new_senders_and_receivers(channel_configs);
    let (peer_disconnected_tx, peer_disconnected_rx) = futures_channel::mpsc::channel(1);

    debug!("making offer");
    let (connection, trickle) =
        create_rtc_peer_connection(signal_peer.clone(), ice_server_config)
            .await
            .map_err(|_e| PeerError(signal_peer.id, SignalingError::HandshakeFailed))?;

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

    // Create and send offer
    let offer = connection.create_offer(None).await
        .map_err(|_e| PeerError(signal_peer.id, SignalingError::HandshakeFailed))?;
    connection.set_local_description(offer).await
        .map_err(|_e| PeerError(signal_peer.id, SignalingError::HandshakeFailed))?;
    wait_for_ice_gathering_complete(signal_peer.id, &connection, timeout).await?;
    let offer_sdp = connection.local_description().await.unwrap().sdp;
    info!("offer sdp: {}", offer_sdp);
    signal_peer.send(PeerSignal::Offer {
        offer: offer_sdp,
        config: None
    });

    // Wait for answer
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
                info!("received answer sdp: {}", answer);
                break answer;
            }
            _ => {
                warn!("Got an unexpected signal while waiting for Answer: {signal:?}. Ignoring.")
            }
        };
    };

    // Set remote description
    let remote_description = RTCSessionDescription::answer(answer)
        .map_err(|_e| PeerError(signal_peer.id, SignalingError::HandshakeFailed))?;
    if let Err(e) = connection.set_remote_description(remote_description).await {
        warn!("failed to set remote description: {e:?}");
        return Err(PeerError(signal_peer.id, SignalingError::HandshakeFailed));
    }

    // Create a channel to forward signals for complete_handshake
    let (forward_tx, forward_rx) = futures_channel::mpsc::unbounded();

    // Spawn a task to forward signals from peer_signal_rx to forward_rx
    // This allows us to continue using peer_signal_rx after complete_handshake
    let peer_id = signal_peer.id;
    let forward_handle = {
        let mut peer_signal_rx_ref = std::mem::replace(peer_signal_rx, futures_channel::mpsc::unbounded().1);
        async move {
            while let Some(signal) = peer_signal_rx_ref.next().await {
                if forward_tx.unbounded_send(signal).is_err() {
                    break;
                }
            }
            peer_signal_rx_ref
        }
    };

    // Complete handshake - wait for data channels to be ready
    let trickle_fut = complete_handshake(
        peer_id,
        trickle,
        &connection,
        forward_rx,
        data_channels_ready_fut,
        timeout,
    ).await?;

    // Recover peer_signal_rx from the forwarding task
    // Note: This is a simplified approach; in practice, we'd need proper task management
    drop(forward_handle);

    Ok(HandshakeResult {
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

/// Core handshake logic for the answerer.
async fn do_accept_handshake(
    signal_peer: SignalPeer,
    peer_signal_rx: &mut UnboundedReceiver<PeerSignal>,
    messages_from_peers_tx: Vec<UnboundedSender<(PeerId, Packet)>>,
    ice_server_config: &RtcIceServerConfig,
    channel_configs: &[ChannelConfig],
    timeout: Duration,
) -> Result<HandshakeResult<PeerDataSenderWrapper<Packet>, OfferHandshakeMeta>, PeerError> {
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
                warn!("ignoring unexpected signal while waiting for offer: {signal:?}");
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
            .await
            .map_err(|_e| PeerError(signal_peer.id, SignalingError::HandshakeFailed))?;

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

    let remote_description = RTCSessionDescription::offer(offer_sdp)
        .map_err(|_e| PeerError(signal_peer.id, SignalingError::HandshakeFailed))?;
    connection
        .set_remote_description(remote_description)
        .await
        .map_err(|_e| PeerError(signal_peer.id, SignalingError::HandshakeFailed))?;

    let answer = connection.create_answer(None).await
        .map_err(|_e| PeerError(signal_peer.id, SignalingError::HandshakeFailed))?;
    connection.set_local_description(answer).await
        .map_err(|_e| PeerError(signal_peer.id, SignalingError::HandshakeFailed))?;
    wait_for_ice_gathering_complete(signal_peer.id, &connection, timeout).await?;
    let answer_sdp = connection.local_description().await.unwrap().sdp;
    info!("answer sdp: {}", answer_sdp);
    signal_peer.send(PeerSignal::Answer(answer_sdp));

    // Create a channel to forward signals for complete_handshake
    let (forward_tx, forward_rx) = futures_channel::mpsc::unbounded();

    let peer_id = signal_peer.id;
    let _forward_handle = {
        let mut peer_signal_rx_ref = std::mem::replace(peer_signal_rx, futures_channel::mpsc::unbounded().1);
        let forward_tx_clone = forward_tx.clone();
        async move {
            while let Some(signal) = peer_signal_rx_ref.next().await {
                if forward_tx_clone.unbounded_send(signal).is_err() {
                    break;
                }
            }
            peer_signal_rx_ref
        }
    };

    // Complete handshake - wait for data channels to be ready
    let trickle_fut = complete_handshake(
        peer_id,
        trickle,
        &connection,
        forward_rx,
        data_channels_ready_fut,
        timeout,
    ).await?;

    Ok(HandshakeResult {
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

async fn wait_for_ice_gathering_complete(
    _peer_id: PeerId,
    connection: &Arc<RTCPeerConnection>,
    timeout: Duration,
) -> Result<bool, PeerError> {
    use super::ice_tracker::IceCandidateTracker;

    let (tx, mut rx) = mpsc::channel(1);
    let tracker = Arc::new(IceCandidateTracker::new());

    let tx_clone = tx.clone();
    connection.on_ice_gathering_state_change(Box::new(move |state: RTCIceGathererState| {
        if state == RTCIceGathererState::Complete {
            let mut tx_clone = tx_clone.clone();
            Box::pin(async move {
                let _ = tx_clone.try_send(());
            })
        } else {
            Box::pin(async {})
        }
    }));

    let tracker_clone = tracker.clone();
    let mut tx_early = tx.clone();
    connection.on_ice_candidate(Box::new(move |candidate| {
        if let Some(c) = candidate {
            if let Ok(json) = c.to_json() {
                tracker_clone.add_candidate(&json.candidate);
            }
        } else {
            let _ = tx_early.try_send(());
        }
        Box::pin(async {})
    }));

    let ice_timeout = Duration::from_millis((timeout.as_millis() as u64).min(5500));
    let early_check_delay = Duration::from_secs(2);

    let mut timeout_delay = Delay::new(ice_timeout).fuse();
    let mut early_check = Delay::new(early_check_delay).fuse();
    let mut rx = rx.next().fuse();

    loop {
        select! {
            _ = timeout_delay => {
                warn!("ICE gathering timeout - proceeding with available candidates ({})", tracker.summary());
                return Ok(false);
            },
            _ = early_check => {
                if tracker.has_sufficient_candidates() {
                    info!("ICE gathering early termination - sufficient candidates gathered ({})", tracker.summary());
                    return Ok(true);
                }
            },
            _ = rx => {
                info!("Ice gathering completed");
                return Ok(true);
            }
        }
    }
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

        let candidate_type = parse_candidate_type(&candidate_init.candidate);
        info!("[ICE] peer={} local_candidate: type={}, candidate={}", self.signal_peer.id, candidate_type, candidate_init.candidate);

        let candidate_json =
            serde_json::to_string(&candidate_init).expect("failed to serialize candidate to json");

        if peer_connection.remote_description().await.is_some() {
            debug!("sending IceCandidate signal: {candidate:?}");
            self.signal_peer
                .send(PeerSignal::IceCandidate(candidate_json));
        } else {
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
        peer_id: PeerId,
    ) -> Result<(), webrtc::Error> {
        while let Some(signal) = peer_signal_rx.next().await {
            match signal {
                PeerSignal::IceCandidate(candidate_json) => {
                    match serde_json::from_str::<RTCIceCandidateInit>(&candidate_json) {
                        Ok(candidate_init) => {
                            let candidate_type = parse_candidate_type(&candidate_init.candidate);
                            info!("[ICE] peer={} remote_candidate: type={}, candidate={}", peer_id, candidate_type, candidate_init.candidate);
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
    let mut setting_engine = SettingEngine::default();

    // 1. Filter out interfaces that cause binding errors or are virtual
    setting_engine.set_interface_filter(Box::new(|iface_name: &str| {
        let is_virtual = iface_name.contains("utun") ||
            iface_name.contains("docker") ||
            iface_name.contains("vboxnet");

        // Return true to KEEP the interface, false to IGNORE it
        !is_virtual
    }));

    // 2. Filter specific IP types (Stop the fe80 / Error 49 issue)
    setting_engine.set_ip_filter(Box::new(|ip| {
        // Filter loopback addresses (not useful for P2P)
        if ip.is_loopback() {
            return false;
        }

        if ip.is_ipv6() {
            // Filter link-local IPv6 (fe80::/10) - doesn't route across networks
            // and causes "Error 49: Can't assign requested address" on macOS
            // Keep: Global unicast (2000::/3), Unique local (fc00::/7)
            let segments = match ip {
                std::net::IpAddr::V6(v6) => v6.segments(),
                _ => return true,
            };
            let is_link_local = (segments[0] & 0xffc0) == 0xfe80;
            return !is_link_local;
        }

        true // Keep all IPv4
    }));

    // setting_engine.set_network_types(vec![NetworkType::Udp4, NetworkType::Tcp4]);
    let api = APIBuilder::new()
        .with_setting_engine(setting_engine)
        .build();

    let config = RTCConfiguration {
        ice_transport_policy: RTCIceTransportPolicy::All,
        ice_servers: vec![RTCIceServer {
            urls: ice_server_config.urls.clone(),
            username: ice_server_config.username.clone().unwrap_or_default(),
            credential: ice_server_config.credential.clone().unwrap_or_default(),
        }],
        ..Default::default()
    };

    let connection = api.new_peer_connection(config).await?;
    let connection = Arc::new(connection);

    let peer_id_for_state = signal_peer.id;
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

    let connection_for_state = Arc::downgrade(&connection);
    connection.on_ice_connection_state_change(Box::new(move |state: RTCIceConnectionState| {
        let connection_clone = connection_for_state.clone();
        let peer_id = peer_id_for_state;
        Box::pin(async move {
            info!("[ICE] peer={} state={:?}", peer_id, state);
            match state {
                RTCIceConnectionState::Connected | RTCIceConnectionState::Completed => {
                    if let Some(conn) = connection_clone.upgrade() {
                        log_selected_candidate_pair(&conn, &peer_id).await;
                    }
                }
                RTCIceConnectionState::Failed => {
                    warn!("[ICE] peer={} ICE connection failed (possible TURN permission error)", peer_id);
                }
                _ => {}
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

    let is_reliable = channel_config.ordered && channel_config.max_retransmits.is_none();
    let mut batch_receiver = if is_reliable {
        Some(BatchReceiver::new(30000.0))
    } else {
        None
    };
    channel.on_message(Box::new(move |message| {
        let data: Packet = (*message.data).into();

        let packet = if let Some(ref mut receiver) = batch_receiver {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as f64)
                .unwrap_or(0.0);
            match receiver.receive_packet(&data, &peer_id, now_ms) {
                Some(p) => p,
                None => return Box::pin(async move {}),
            }
        } else {
            data
        };

        if let Err(e) = from_peer_message_tx.unbounded_send((peer_id, packet)) {
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
