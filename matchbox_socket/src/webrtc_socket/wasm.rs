use super::{HandshakeResult, PacketSendError, PeerDataSender, SignallerBuilder, error::JsErrorExt, messages::{PeerEvent, PeerRequest}, BufferedChannel, StatsProvider};
use crate::webrtc_socket::{
    ChannelConfig, Messenger, Packet, RtcIceServerConfig, Signaller, error::SignalingError,
    messages::PeerSignal, signal_peer::SignalPeer, socket::create_data_channels_ready_fut,
    PeerBuffered,
};
use async_trait::async_trait;
use futures::{Future, SinkExt, StreamExt};
use futures_channel::mpsc::{Receiver, UnboundedReceiver, UnboundedSender};
use futures_timer::Delay;
use futures_util::{select, FutureExt};
use js_sys::{Function, Object, Reflect};
use log::{debug, error, info, trace, warn};
use matchbox_protocol::PeerId;
use serde::Serialize;
use std::{pin::Pin, time::Duration};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use wasm_bindgen::{JsCast, JsValue, convert::FromWasmAbi, prelude::*};
use wasm_bindgen_futures::JsFuture;
use web_sys::{Event, MessageEvent, RtcConfiguration, RtcDataChannel, RtcDataChannelInit, RtcDataChannelType, RtcIceCandidateInit, RtcIceConnectionState, RtcIceGatheringState, RtcPeerConnection, RtcPeerConnectionIceEvent, RtcSdpType, RtcSessionDescriptionInit, RtcStatsReport, RtcStatsReportInternal};
use ws_stream_wasm::{WsMessage, WsMeta, WsStream};
use crate::webrtc_socket::batch::BatchReceiver;
use crate::webrtc_socket::error::PeerError;

pub(crate) struct WasmSignaller {
    websocket_stream: futures::stream::Fuse<WsStream>,
}

pub struct RtcDataChannelWrapper(pub(crate) RtcDataChannel);

pub struct RtcConnectionWrapper(pub(crate) RtcPeerConnection);

impl RtcDataChannelWrapper {
    fn new(data_channel: RtcDataChannel) -> Arc<Self> {
        Arc::new(Self(data_channel))
    }
}

unsafe impl Sync for RtcConnectionWrapper {}

impl Deref for RtcConnectionWrapper {
    type Target = RtcPeerConnection;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl RtcConnectionWrapper {
    fn new(conn: RtcPeerConnection) -> Arc<Self> {
        Arc::new(Self(conn))
    }
}

#[async_trait(?Send)]
impl StatsProvider for RtcConnectionWrapper {
    async fn channel_bytes_sent_received(&self, stat_id: &str) -> Option<(usize, usize)> {
        let stats_js = JsFuture::from(self.get_stats()).await.ok()?;
        let stats: RtcStatsReport = stats_js.into();

        for val in stats.values() {
            let Ok(report) = val else {
                continue;
            };

            // Get type field - continue to next entry if missing
            let Ok(report_type) = Reflect::get(&report, &JsValue::from_str("type")) else {
                continue;
            };

            // Check if this is a data-channel stat
            let Some(type_str) = report_type.as_string() else {
                continue;
            };

            if type_str != "data-channel" {
                continue;
            }

            // Get label field - continue to next entry if missing
            let Ok(label) = Reflect::get(&report, &JsValue::from_str("label")) else {
                continue;
            };

            let Some(label_str) = label.as_string() else {
                continue;
            };

            if label_str != stat_id {
                continue;
            }

            // Found the matching data channel, get the stats
            let Ok(sent) = Reflect::get(&report, &JsValue::from_str("bytesSent")) else {
                continue;
            };
            let Ok(recv) = Reflect::get(&report, &JsValue::from_str("bytesReceived")) else {
                continue;
            };

            let Some(bytes_sent) = sent.as_f64() else {
                continue;
            };
            let Some(bytes_received) = recv.as_f64() else {
                continue;
            };

            return Some((bytes_sent as usize, bytes_received as usize));
        }

        None
    }

    async fn rtt(&self) -> Option<f64> {
        let stats_js = JsFuture::from(self.get_stats()).await.ok()?;
        let stats: RtcStatsReport = stats_js.into();

        for val in stats.values() {
            let Ok(report) = val else {
                continue;
            };

            // Get type field - continue to next entry if missing
            let Ok(report_type) = Reflect::get(&report, &JsValue::from_str("type")) else {
                continue;
            };

            // Check if this is a candidate-pair stat
            let Some(type_str) = report_type.as_string() else {
                continue;
            };

            if type_str != "candidate-pair" {
                continue;
            }

            // Check if this is the active/selected candidate pair
            let Ok(state) = Reflect::get(&report, &JsValue::from_str("state")) else {
                continue;
            };

            let Some(state_str) = state.as_string() else {
                continue;
            };

            if state_str != "succeeded" {
                continue;
            }

            // Get currentRoundTripTime which is in seconds, convert to milliseconds
            let Ok(rtt) = Reflect::get(&report, &JsValue::from_str("currentRoundTripTime")) else {
                continue;
            };

            let Some(rtt_seconds) = rtt.as_f64() else {
                continue;
            };

            return Some(rtt_seconds * 1000.0);
        }

        None
    }
}

unsafe impl Send for RtcConnectionWrapper {}

unsafe impl Send for RtcDataChannelWrapper {}

unsafe impl Sync for RtcDataChannelWrapper {}

impl Deref for RtcDataChannelWrapper {
    type Target = RtcDataChannel;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for RtcDataChannelWrapper {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Drop for RtcDataChannelWrapper {
    fn drop(&mut self) {
        log::info!("closing data channel on drop");
        self.0.close()
    }
}

impl Drop for RtcConnectionWrapper {
    fn drop(&mut self) {
        log::info!("closing peer connection on drop");
        self.0.close();
    }
}

#[derive(Debug, Default)]
pub(crate) struct WasmSignallerBuilder;

#[async_trait(?Send)]
impl SignallerBuilder for WasmSignallerBuilder {
    async fn new_signaller(
        &self,
        mut attempts: Option<u16>,
        room_url: String,
    ) -> Result<Box<dyn Signaller>, SignalingError> {
        let websocket_stream = 'signaling: loop {
            match WsMeta::connect(&room_url, None)
                .await
                .map_err(SignalingError::from)
            {
                Ok((_, wss)) => break wss.fuse(),
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
        Ok(Box::new(WasmSignaller { websocket_stream }))
    }
}

#[async_trait(?Send)]
impl Signaller for WasmSignaller {
    async fn send(&mut self, request: PeerRequest) -> Result<(), SignalingError> {
        let request = serde_json::to_string(&request).expect("serializing request");
        self.websocket_stream
            .send(WsMessage::Text(request))
            .await
            .map_err(SignalingError::from)
    }

    async fn next_message(&mut self) -> Result<PeerEvent, SignalingError> {
        let message = match self.websocket_stream.next().await {
            Some(WsMessage::Text(message)) => Ok(message),
            Some(_) => Err(SignalingError::UnknownFormat),
            None => Err(SignalingError::StreamExhausted),
        }?;
        let message = serde_json::from_str(&message).map_err(|e| {
            error!("failed to deserialize message: {e:?}");
            SignalingError::UnknownFormat
        })?;
        Ok(message)
    }
}

#[async_trait(?Send)]
impl PeerDataSender for Arc<RtcDataChannelWrapper> {
    fn send(&mut self, packet: Packet) -> Result<(), PacketSendError> {
        self.send_with_u8_array(&packet)
            .efix()
            .map_err(|source| PacketSendError { source })
    }

    fn is_reliable(&self) -> bool {
        self.0.reliable()
    }

    async fn close(&mut self) -> Result<(), PacketSendError> {
        info!("closing data channel");
        self.0.close();

        Ok(())
    }
}

#[async_trait(?Send)]
impl BufferedChannel for Arc<RtcDataChannelWrapper> {
    async fn buffered_amount(&self) -> usize {
        self.0.buffered_amount() as usize
    }

    fn stats_id(&self) -> Option<String> {
        Some(self.0.label())
    }
}

pub(crate) struct WasmMessenger;

#[async_trait(?Send)]
impl Messenger for WasmMessenger {
    type DataChannel = Arc<RtcDataChannelWrapper>;
    type HandshakeMeta = (Receiver<()>, Arc<RtcConnectionWrapper>);

    async fn offer_handshake(
        signal_peer: SignalPeer,
        mut peer_signal_rx: UnboundedReceiver<PeerSignal>,
        messages_from_peers_tx: Vec<UnboundedSender<(PeerId, Packet)>>,
        ice_server_config: RtcIceServerConfig,
        channel_configs: &[ChannelConfig],
        timeout: Duration,
    ) -> Result<HandshakeResult<Self::DataChannel, Self::HandshakeMeta>, PeerError> {
        debug!("making offer");
        log::info!("Using ice config {:?}", ice_server_config);

        let conn = create_rtc_peer_connection(&signal_peer.id, &ice_server_config)?;

        let (data_channel_ready_txs, data_channels_ready_fut) =
            create_data_channels_ready_fut(channel_configs);

        let (peer_disconnected_tx, peer_disconnected_rx) = futures_channel::mpsc::channel(1);

        let (data_channels, buffer_low_rxs) = create_data_channels(
            conn.clone(),
            messages_from_peers_tx,
            signal_peer.id,
            peer_disconnected_tx,
            data_channel_ready_txs,
            channel_configs,
        );

        let stats_provider = Arc::downgrade(&conn);
        let peer_buffered = PeerBuffered::new(
            data_channels.clone().iter().map(|it| {
                let channel: Box<dyn BufferedChannel> = Box::new(it.clone());
                return channel
            }).collect::<Vec<_>>(),
            stats_provider,
            buffer_low_rxs,
        );

        // Create offer
        let offer = JsFuture::from(conn.create_offer()).await.efix().unwrap();
        let offer_sdp = Reflect::get(&offer, &JsValue::from_str("sdp"))
            .efix()
            .unwrap()
            .as_string()
            .expect("");
        let rtc_session_desc_init_dict = RtcSessionDescriptionInit::new(RtcSdpType::Offer);
        rtc_session_desc_init_dict.set_sdp(&offer_sdp);
        JsFuture::from(conn.set_local_description(&rtc_session_desc_init_dict))
            .await
            .efix()
            .unwrap();
        debug!("created offer for new peer");

        // After some investigation, the full trickle on web could causing
        // issues of overloading, we should implement the half-trickle instead.
        // todo: implement the half trickle
        wait_for_ice_gathering_complete(signal_peer.id.clone(), conn.clone(), timeout.clone()).await?;

        log::info!("sending offer to peer {}", conn.local_description().unwrap().sdp());
        let offer_sdp = conn.local_description().unwrap().sdp();
        info!("offer sdp: {}", offer_sdp);
        signal_peer.send(PeerSignal::Offer {
            offer: offer_sdp,
            config: None
        });

        let mut received_candidates = vec![];

        // Wait for answer
        let sdp = loop {
            let signal = match peer_signal_rx.next().await {
                Some(signal) => signal,
                None => {
                    warn!("Signal server connection lost in the middle of a handshake");
                    return Err(PeerError(signal_peer.id, SignalingError::HandshakeFailed));
                }
            };

            match signal {
                PeerSignal::Answer(answer) => break answer,
                PeerSignal::IceCandidate(candidate) => {
                    debug!(
                        "offerer: received an ice candidate while waiting for answer: {candidate:?}"
                    );
                    received_candidates.push(candidate);
                }
                _ => {
                    warn!("ignoring unexpected signal: {signal:?}");
                }
            };
        };

        // Set remote description
        let remote_description = RtcSessionDescriptionInit::new(RtcSdpType::Answer);
        remote_description.set_sdp(&sdp);
        debug!("setting remote description");
        JsFuture::from(conn.set_remote_description(&remote_description))
            .await
            .efix()
            .unwrap();

        complete_handshake(
            signal_peer.clone(),
            conn.clone(),
            received_candidates,
            data_channels_ready_fut,
            peer_signal_rx,
            timeout
        )
        .await?;

        Ok(HandshakeResult {
            peer_id: signal_peer.id,
            data_channels,
            metadata: (peer_disconnected_rx, conn),
            peer_buffered,
        })
    }

    async fn accept_handshake(
        signal_peer: SignalPeer,
        mut peer_signal_rx: UnboundedReceiver<PeerSignal>,
        messages_from_peers_tx: Vec<UnboundedSender<(PeerId, Packet)>>,
        ice_server_config: &RtcIceServerConfig,
        channel_configs: &[ChannelConfig],
        timeout: Duration,
    ) -> Result<HandshakeResult<Self::DataChannel, Self::HandshakeMeta>, PeerError> {
        debug!("handshake_accept");

        let mut received_candidates = vec![];

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
                PeerSignal::IceCandidate(candidate) => {
                    debug!("received IceCandidate signal: {candidate:?}");
                    received_candidates.push(candidate);
                }
                _ => {
                    warn!("ignoring unexpected signal: {signal:?}");
                }
            }
        };

        debug!("received offer");

        // Merge ice configs if offer contains config
        let merged_ice_config = offer_ice_config.unwrap_or(ice_server_config.clone());

        // Create connection with merged config
        let conn = create_rtc_peer_connection(&signal_peer.id, &merged_ice_config)?;

        let (data_channel_ready_txs, data_channels_ready_fut) =
            create_data_channels_ready_fut(channel_configs);

        let (peer_disconnected_tx, peer_disconnected_rx) = futures_channel::mpsc::channel(1);

        let (data_channels, buffer_low_rxs) = create_data_channels(
            conn.clone(),
            messages_from_peers_tx,
            signal_peer.id,
            peer_disconnected_tx,
            data_channel_ready_txs,
            channel_configs,
        );

        let stats_provider = Arc::downgrade(&conn);
        let peer_buffered = PeerBuffered::new(
            data_channels.iter().map(|it| {
                let channel: Box<dyn BufferedChannel> = Box::new(it.clone());
                return channel
            }).collect::<Vec<_>>(),
            stats_provider,
            buffer_low_rxs,
        );

        // Set remote description
        {
            let remote_description = RtcSessionDescriptionInit::new(RtcSdpType::Offer);
            remote_description.set_sdp(&offer_sdp);
            JsFuture::from(conn.set_remote_description(&remote_description))
                .await
                .expect("failed to set remote description");
            debug!("set remote_description from offer");
        }

        let answer = JsFuture::from(conn.create_answer())
            .await
            .expect("error creating answer");

        debug!("created answer");

        let session_desc_init = RtcSessionDescriptionInit::new(RtcSdpType::Answer);

        let answer_sdp = Reflect::get(&answer, &JsValue::from_str("sdp"))
            .efix()
            .unwrap()
            .as_string()
            .expect("");

        session_desc_init.set_sdp(&answer_sdp);

        JsFuture::from(conn.set_local_description(&session_desc_init))
            .await
            .efix()
            .unwrap();

        // todo: implement the full trickle on accept handshake side
        // the offer side will be gathering all ice before sent, and answer side will use full trickle
        wait_for_ice_gathering_complete(signal_peer.id.clone(), conn.clone(), timeout.clone()).await?;

        let sdp_answer = conn.local_description().unwrap().sdp();
        info!("answer sdp {sdp_answer}");
        let answer = PeerSignal::Answer(sdp_answer);
        signal_peer.send(answer);

        complete_handshake(
            signal_peer.clone(),
            conn.clone(),
            received_candidates,
            data_channels_ready_fut,
            peer_signal_rx,
            timeout
        )
        .await?;

        Ok(HandshakeResult {
            peer_id: signal_peer.id,
            data_channels,
            metadata: (peer_disconnected_rx, conn),
            peer_buffered,
        })
    }

    async fn peer_loop(peer_uuid: PeerId, handshake_meta: Self::HandshakeMeta) -> PeerId {
        let (mut peer_loop_finished_rx, conn) = handshake_meta;
        peer_loop_finished_rx.next().await;
        log::info!("peer loop finished for peer {}", peer_uuid);
        conn.close();
        peer_uuid
    }
}

async fn complete_handshake(
    signal_peer: SignalPeer,
    conn: Arc<RtcConnectionWrapper>,
    received_candidates: Vec<String>,
    mut data_channels_ready_fut: Pin<Box<futures::future::Fuse<impl Future<Output = ()>>>>,
    mut peer_signal_rx: UnboundedReceiver<PeerSignal>,
    timeout: Duration,
) -> Result<(), PeerError> {
    let peer_id = signal_peer.id.clone();
    let peer_id_for_ice = signal_peer.id;
    let onicecandidate: Box<dyn FnMut(RtcPeerConnectionIceEvent)> = Box::new(
        move |event: RtcPeerConnectionIceEvent| {
            let candidate_json = match event.candidate() {
                Some(candidate) => {
                    let candidate_str = candidate.candidate();
                    let candidate_type = parse_candidate_type(&candidate_str);
                    info!("[ICE] peer={} local_candidate: type={}, candidate={}", peer_id_for_ice, candidate_type, candidate_str);
                    js_sys::JSON::stringify(&candidate.to_json())
                        .expect("failed to serialize candidate")
                        .as_string()
                        .unwrap()
                },
                None => {
                    debug!(
                        "Received RtcPeerConnectionIceEvent with no candidate. This means there are no further ice candidates for this session"
                    );
                    "null".to_string()
                }
            };

            debug!("sending IceCandidate signal: {candidate_json:?}");
            signal_peer.send(PeerSignal::IceCandidate(candidate_json));
        },
    );
    let onicecandidate = Closure::wrap(onicecandidate);
    conn.set_onicecandidate(Some(onicecandidate.as_ref().unchecked_ref()));
    // note: we can let rust keep ownership of this closure, since we replace
    // the event handler later in this method when ice is finished

    // handle pending ICE candidates
    for candidate in received_candidates {
        debug!("adding ice candidate {candidate:?}");
        try_add_rtc_ice_candidate(&conn, &candidate, &peer_id).await;
    }

    // select for data channels ready or ice candidates
    debug!("waiting for data channels to open");
    let mut delay = Delay::new(timeout).fuse();
    let mut err = None;
    loop {
        select! {
            _ = data_channels_ready_fut => {
                debug!("data channels ready");
                break;
            }
            msg = peer_signal_rx.next() => {
                if let Some(PeerSignal::IceCandidate(candidate)) = msg {
                    try_add_rtc_ice_candidate(&conn, &candidate, &peer_id).await;
                }
            }
            _ = delay => {
                warn!("timeout waiting for data channels to open");
                err.replace(Err(PeerError(peer_id.clone(), SignalingError::HandshakeFailed)));
            }
        };
    }

    // stop listening for ICE candidates
    // TODO: we should support getting new ICE candidates even after connecting,
    //       since it's possible to return to the ice gathering state
    // See: <https://developer.mozilla.org/en-US/docs/Web/API/RTCPeerConnection/iceGatheringState>
    let onicecandidate: Box<dyn FnMut(RtcPeerConnectionIceEvent)> =
        Box::new(move |_event: RtcPeerConnectionIceEvent| {
            warn!("received ice candidate event after handshake completed, ignoring");
        });
    let onicecandidate = Closure::wrap(onicecandidate);
    conn.set_onicecandidate(Some(onicecandidate.as_ref().unchecked_ref()));
    onicecandidate.forget();

    debug!(
        "handshake completed, ice gathering state: {:?}",
        conn.ice_gathering_state()
    );

    err.unwrap_or(Ok(()))
}

async fn try_add_rtc_ice_candidate(connection: &RtcPeerConnection, candidate_string: &str, peer_id: &PeerId) {
    let parsed_candidate = match js_sys::JSON::parse(candidate_string) {
        Ok(c) => c,
        Err(err) => {
            warn!("failed to parse ice candidate json, ignoring: {err:?}");
            return;
        }
    };

    let candidate_init = if parsed_candidate.is_null() {
        debug!("Received null ice candidate, this means there are no further ice candidates");
        None
    } else {
        let candidate = RtcIceCandidateInit::from(parsed_candidate);
        let candidate_str = candidate.get_candidate();
        let candidate_type = parse_candidate_type(&candidate_str);
        info!("[ICE] peer={} remote_candidate: type={}, candidate={}", peer_id, candidate_type, candidate_str);
        Some(candidate)
    };

    JsFuture::from(
        connection.add_ice_candidate_with_opt_rtc_ice_candidate_init(candidate_init.as_ref()),
    )
    .await
    .expect("failed to add ice candidate");
}

fn create_rtc_peer_connection(peer_id: &PeerId, ice_server_config: &RtcIceServerConfig) -> Result<Arc<RtcConnectionWrapper>, PeerError> {
    #[derive(Serialize)]
    struct IceServerConfig {
        urls: Vec<String>,
        username: String,
        credential: String,
    }

    let peer_config = RtcConfiguration::new();
    let ice_server_config = IceServerConfig {
        urls: ice_server_config.urls.clone(),
        username: ice_server_config.username.clone().unwrap_or_default(),
        credential: ice_server_config.credential.clone().unwrap_or_default(),
    };
    let ice_server_config_list = [ice_server_config];
    peer_config.set_ice_servers(&serde_wasm_bindgen::to_value(&ice_server_config_list).unwrap());
    let connection = RtcConnectionWrapper::new(RtcPeerConnection::new_with_configuration(&peer_config).map_err(|it| PeerError(peer_id.clone(), SignalingError::UserImplementationError(format!("{it:?}"))))?);

    let connection_1 = connection.clone();
    let peer_id_for_state = peer_id.clone();
    let oniceconnectionstatechange: Box<dyn FnMut(_)> = Box::new(move |_event: JsValue| {
        let state = connection_1.ice_connection_state();
        info!("[ICE] peer={} state={:?}", peer_id_for_state, state);
        if matches!(state, RtcIceConnectionState::Connected | RtcIceConnectionState::Completed) {
            let conn_for_stats = connection_1.0.clone();
            let peer_id_for_stats = peer_id_for_state;
            wasm_bindgen_futures::spawn_local(async move {
                log_selected_candidate_pair(&conn_for_stats, &peer_id_for_stats).await;
            });
        }
    });
    let oniceconnectionstatechange = Closure::wrap(oniceconnectionstatechange);
    connection
        .set_oniceconnectionstatechange(Some(oniceconnectionstatechange.as_ref().unchecked_ref()));
    oniceconnectionstatechange.forget();

    Ok(connection)
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

async fn log_selected_candidate_pair(conn: &RtcPeerConnection, peer_id: &PeerId) {
    let Ok(stats_js) = JsFuture::from(conn.get_stats()).await else {
        return;
    };
    let stats: RtcStatsReport = stats_js.into();

    let mut local_candidates: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    let mut remote_candidates: std::collections::HashMap<String, String> = std::collections::HashMap::new();

    for val in stats.values() {
        let Ok(report) = val else { continue };
        let Ok(report_type) = Reflect::get(&report, &JsValue::from_str("type")) else { continue };
        let Some(type_str) = report_type.as_string() else { continue };
        let Ok(id) = Reflect::get(&report, &JsValue::from_str("id")) else { continue };
        let Some(id_str) = id.as_string() else { continue };

        if type_str == "local-candidate" || type_str == "remote-candidate" {
            let candidate_type = Reflect::get(&report, &JsValue::from_str("candidateType"))
                .ok().and_then(|v| v.as_string()).unwrap_or_default();
            let address = Reflect::get(&report, &JsValue::from_str("address"))
                .ok().and_then(|v| v.as_string()).unwrap_or_default();
            let port = Reflect::get(&report, &JsValue::from_str("port"))
                .ok().and_then(|v| v.as_f64()).unwrap_or(0.0) as u16;
            let protocol = Reflect::get(&report, &JsValue::from_str("protocol"))
                .ok().and_then(|v| v.as_string()).unwrap_or_default();

            let info = format!("type={}, addr={}:{}, protocol={}", candidate_type, address, port, protocol);
            if type_str == "local-candidate" {
                local_candidates.insert(id_str, info);
            } else {
                remote_candidates.insert(id_str, info);
            }
        }
    }

    for val in stats.values() {
        let Ok(report) = val else { continue };
        let Ok(report_type) = Reflect::get(&report, &JsValue::from_str("type")) else { continue };
        let Some(type_str) = report_type.as_string() else { continue };

        if type_str != "candidate-pair" {
            continue;
        }

        let Ok(state) = Reflect::get(&report, &JsValue::from_str("state")) else { continue };
        let Some(state_str) = state.as_string() else { continue };

        if state_str != "succeeded" {
            continue;
        }

        let local_id = Reflect::get(&report, &JsValue::from_str("localCandidateId"))
            .ok().and_then(|v| v.as_string()).unwrap_or_default();
        let remote_id = Reflect::get(&report, &JsValue::from_str("remoteCandidateId"))
            .ok().and_then(|v| v.as_string()).unwrap_or_default();
        let rtt = Reflect::get(&report, &JsValue::from_str("currentRoundTripTime"))
            .ok().and_then(|v| v.as_f64()).unwrap_or(0.0);

        let local_info = local_candidates.get(&local_id).map(|s| s.as_str()).unwrap_or("unknown");
        let remote_info = remote_candidates.get(&remote_id).map(|s| s.as_str()).unwrap_or("unknown");

        info!(
            "[ICE] peer={} selected_pair: local=[{}], remote=[{}], state={}, rtt={:.2}ms",
            peer_id, local_info, remote_info, state_str, rtt * 1000.0
        );
    }
}

async fn wait_for_ice_gathering_complete(_peer_id: PeerId, conn: Arc<RtcConnectionWrapper>, timeout: Duration) -> Result<bool, PeerError> {
    use super::ice_tracker::IceCandidateTracker;

    if conn.ice_gathering_state() == RtcIceGatheringState::Complete {
        debug!("Ice gathering already completed");
        return Ok(true);
    }

    let (mut tx, mut rx) = futures_channel::mpsc::channel(1);
    let tracker = std::rc::Rc::new(std::cell::RefCell::new(IceCandidateTracker::new()));

    let conn_clone = conn.clone();
    let mut tx1 = tx.clone();
    let onstatechange: Box<dyn FnMut(JsValue)> = Box::new(move |_| {
        if conn_clone.ice_gathering_state() == RtcIceGatheringState::Complete {
            let _ = tx1.try_send(());
        }
    });

    let onstatechange = Closure::wrap(onstatechange);
    conn.set_onicegatheringstatechange(Some(onstatechange.as_ref().unchecked_ref()));

    let tracker_clone = tracker.clone();
    let onicecandidate: Box<dyn FnMut(RtcPeerConnectionIceEvent)> = Box::new(
        move |event: RtcPeerConnectionIceEvent| {
            if let Some(candidate) = event.candidate() {
                let candidate_str = candidate.candidate();
                tracker_clone.borrow_mut().add_candidate(&candidate_str);
            } else {
                let _ = tx.try_send(());
            }
        },
    );
    let onicecandidate_closure = Closure::wrap(onicecandidate);
    conn.set_onicecandidate(Some(onicecandidate_closure.as_ref().unchecked_ref()));

    let ice_timeout = Duration::from_millis((timeout.as_millis() as u64).min(5500));
    let early_check_delay = Duration::from_secs(2);

    let mut timeout_delay = Delay::new(ice_timeout).fuse();
    let mut early_check = Delay::new(early_check_delay).fuse();
    let mut rx = rx.next().fuse();

    let result = loop {
        select! {
            _ = timeout_delay => {
                let summary = tracker.borrow().summary();
                warn!("ICE gathering timeout - proceeding with available candidates ({})", summary);
                break Ok(false);
            },
            _ = early_check => {
                let sufficient = tracker.borrow().has_sufficient_candidates();
                if sufficient {
                    let summary = tracker.borrow().summary();
                    info!("ICE gathering early termination - sufficient candidates gathered ({})", summary);
                    break Ok(true);
                }
            },
            _ = rx => {
                info!("Ice gathering completed");
                break Ok(true);
            }
        }
    };

    conn.set_onicegatheringstatechange(None);

    result
}

fn create_data_channels(
    connection: Arc<RtcConnectionWrapper>,
    mut incoming_tx: Vec<futures_channel::mpsc::UnboundedSender<(PeerId, Packet)>>,
    peer_id: PeerId,
    peer_disconnected_tx: futures_channel::mpsc::Sender<()>,
    mut data_channel_ready_txs: Vec<futures_channel::mpsc::Sender<()>>,
    channel_config: &[ChannelConfig],
) -> (Vec<Arc<RtcDataChannelWrapper>>, Vec<futures_channel::mpsc::UnboundedReceiver<()>>) {
    let mut buffer_low_rxs = Vec::new();
    let channels = channel_config
        .iter()
        .enumerate()
        .map(|(i, channel)| {
            let (buffer_low_tx, buffer_low_rx) = futures_channel::mpsc::unbounded();
            buffer_low_rxs.push(buffer_low_rx);
            create_data_channel(
                connection.clone(),
                incoming_tx.get_mut(i).unwrap().clone(),
                peer_id,
                peer_disconnected_tx.clone(),
                data_channel_ready_txs.pop().unwrap(),
                buffer_low_tx,
                channel,
                i,
            )
        })
        .collect();
    (channels, buffer_low_rxs)
}

fn create_data_channel(
    connection: Arc<RtcConnectionWrapper>,
    incoming_tx: futures_channel::mpsc::UnboundedSender<(PeerId, Packet)>,
    peer_id: PeerId,
    peer_disconnected_tx: futures_channel::mpsc::Sender<()>,
    mut channel_open: futures_channel::mpsc::Sender<()>,
    buffer_low_tx: futures_channel::mpsc::UnboundedSender<()>,
    channel_config: &ChannelConfig,
    channel_id: usize,
) -> Arc<RtcDataChannelWrapper> {
    let data_channel_config = data_channel_config(channel_config);
    data_channel_config.set_id(channel_id as u16);

    let channel = RtcDataChannelWrapper::new(connection.create_data_channel_with_data_channel_dict(
        &format!("matchbox_socket_{channel_id}"),
        &data_channel_config,
    ));

    channel.set_binary_type(RtcDataChannelType::Arraybuffer);

    // Set buffer low threshold if configured
    if let Some(threshold) = channel_config.buffer_low_threshold {
        channel.set_buffered_amount_low_threshold(threshold as u32);
    }

    leaking_channel_event_handler(
        |f| channel.set_onopen(f),
        move |_: JsValue| {
            debug!("data channel open: {channel_id}");
            channel_open
                .try_send(())
                .expect("failed to notify about open connection");
        },
    );

    let is_reliable = channel_config.ordered && channel_config.max_retransmits.is_none();
    log::info!("Data channel {channel_id} is_reliable={is_reliable} (ordered={}, max_retransmits={:?})",
        channel_config.ordered, channel_config.max_retransmits);
    let mut batch_receiver = if is_reliable {
        Some(BatchReceiver::new(30000.0))
    } else {
        None
    };
    leaking_channel_event_handler(
        |f| channel.set_onmessage(f),
        move |event: MessageEvent| {
            trace!("data channel message received {event:?}");
            if let Ok(arraybuf) = event.data().dyn_into::<js_sys::ArrayBuffer>() {
                let uarray = js_sys::Uint8Array::new(&arraybuf);
                let body = uarray.to_vec();

                let packet = if let Some(ref mut receiver) = batch_receiver {
                    let now_ms = js_sys::Date::now();
                    match receiver.receive_packet(&body, &peer_id, now_ms) {
                        Some(p) => p,
                        None => return,
                    }
                } else {
                    body.into_boxed_slice()
                };

                if let Err(e) = incoming_tx.unbounded_send((peer_id, packet)) {
                    warn!("failed to notify about data channel message: {e:?}");
                }
            }
        },
    );

    leaking_channel_event_handler(
        |f| channel.set_onerror(f),
        move |event: Event| {
            // Convert Event into a JsValue
            let js_val: JsValue = event.into();

            // Try to get the `error` property
            match Reflect::get(&js_val, &JsValue::from_str("error")) {
                Ok(err) => {
                    error!("DataChannel error: {:?}", err);
                }
                Err(_) => {
                    error!("DataChannel error: (no error field found)");
                }
            }
        },
    );

    leaking_channel_event_handler(
        |f| channel.set_onclose(f),
        move |event: Event| {
            warn!("data channel closed: {event:?}");
            if let Err(err) = peer_disconnected_tx.clone().try_send(()) {
                // should only happen if the socket is dropped, or we are out of memory
                warn!("failed to notify about data channel closing: {err:?}");
            }
        },
    );

    leaking_channel_event_handler(
        |f| channel.set_onbufferedamountlow(f),
        move |_event: Event| {
            trace!("data channel buffer low: {channel_id}");
            let _ = buffer_low_tx.unbounded_send(());
        },
    );

    channel
}

/// Note that this function leaks some memory because the rust closure is dropped but still needs to
/// be accessed by javascript of the browser
///
/// See also: https://rustwasm.github.io/wasm-bindgen/api/wasm_bindgen/closure/struct.Closure.html#method.into_js_value
fn leaking_channel_event_handler<T: FromWasmAbi + 'static>(
    mut setter: impl FnMut(Option<&Function>),
    handler: impl FnMut(T) + 'static,
) {
    let closure: Closure<dyn FnMut(T)> = Closure::wrap(Box::new(handler));

    setter(Some(closure.as_ref().unchecked_ref()));

    closure.forget();
}

fn data_channel_config(channel_config: &ChannelConfig) -> RtcDataChannelInit {
    let data_channel_config = RtcDataChannelInit::new();

    data_channel_config.set_ordered(channel_config.ordered);
    data_channel_config.set_negotiated(true);

    if let Some(n) = channel_config.max_retransmits {
        data_channel_config.set_max_retransmits(n);
    }

    data_channel_config
}
