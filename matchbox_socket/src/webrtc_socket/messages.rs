use std::fmt::Debug;
use std::sync::{Arc, Weak};
use n0_future::time::{Duration, Instant};
use async_trait::async_trait;
use futures_timer::Delay;
use futures_util::lock::Mutex;
use serde::{Deserialize, Serialize};
use crate::RtcIceServerConfig;

/// Events go from signaling server to peer
pub type PeerEvent = matchbox_protocol::PeerEvent<PeerSignal>;

/// Requests go from peer to signaling server
pub type PeerRequest = matchbox_protocol::PeerRequest<PeerSignal>;

/// Signals go from peer to peer via the signaling server
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum PeerSignal {
    /// Ice Candidate
    IceCandidate(String),
    /// Offer
    Offer {
        /// The SDP offer
        offer: String,
        /// Optional ICE server configuration to merge with the accepter's config
        config: Option<RtcIceServerConfig>
    },
    /// Answer
    Answer(String),
}

#[cfg(not(target_family = "wasm"))]
pub trait MaybeSend: Send + Sync {}
#[cfg(not(target_family = "wasm"))]
impl<T: Send + Sync> MaybeSend for T {}

#[cfg(target_family = "wasm")]
pub trait MaybeSend: Send {}

#[cfg(target_family = "wasm")]
impl<T> MaybeSend for T where T: Send {}

/// Trait representing a channel with a buffer, allowing querying of the buffered amount.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait BufferedChannel: MaybeSend + Sync {
    /// Returns the current buffered amount in the channel.
    async fn buffered_amount(&self) -> usize;
    fn stats_id(&self) -> Option<String>;
}

/// Manages multiple buffered channels, providing utilities to query and flush them.
#[derive(Clone)]
pub struct PeerBuffered {
    channel_refs: Arc<Vec<Box<dyn BufferedChannel>>>,
    stats_provider: Weak<dyn StatsProvider>,
    rtt_cache: Arc<Mutex<Option<(f64, Instant)>>>,
    buffer_low_rx: Vec<Arc<Mutex<futures_channel::mpsc::UnboundedReceiver<()>>>>,
}

impl Debug for PeerBuffered {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerBuffered")
            .field("channel_refs", &self.channel_refs.len())
            .finish()
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait StatsProvider: MaybeSend + Sync {
    async fn channel_bytes_sent_received(&self, stat_id: &str) -> Option<(usize, usize)>;
    async fn rtt(&self) -> Option<f64>;
}

impl PeerBuffered {
    /// Creates a new PeerBuffered with the specified number of channels.
    pub(crate) fn new(
        channels: Vec<Box<dyn BufferedChannel>>,
        stats_provider: Weak<dyn StatsProvider>,
        buffer_low_rx: Vec<futures_channel::mpsc::UnboundedReceiver<()>>,
    ) -> Self {
        Self {
            channel_refs: Arc::new(channels),
            stats_provider,
            rtt_cache: Arc::new(Mutex::new(None)),
            buffer_low_rx: buffer_low_rx
                .into_iter()
                .map(|rx| Arc::new(Mutex::new(rx)))
                .collect(),
        }
    }

    /// Returns the number of bytes sent and received for the channel at the given index.
    /// Returns None if the channel doesn't exist or stats are unavailable.
    pub async fn channel_bytes_sent_received(&self, index: usize) -> Option<(usize, usize)> {
        let Some(stat_id) = self.channel_refs.get(index)?.stats_id() else {
            return None;
        };

        if let Some(provider) = self.stats_provider.upgrade() {
            return provider.channel_bytes_sent_received(&stat_id).await
        };

        None
    }

    /// Return the number of channels.
    pub fn len(&self) -> usize {
        self.channel_refs.len()
    }

    /// Returns the buffered amount for the channel at the given index.
    pub async fn buffered_amount(&self, index: usize) -> usize {
        if let Some(channel) = self.channel_refs.get(index) {
            return channel.buffered_amount().await
        }

        log::error!("Channel not found");

        0
    }

    /// Flushes all channels, waiting until their buffers are empty.
    pub async fn flush_all(&self) {
        for i in 0..self.channel_refs.len() {
            self.flush(i).await;
        }
    }

    /// Returns the sum of buffered amounts across all channels.
    pub async fn sum_buffered_amount(&self) -> usize {
        let mut sum = 0;
        for i in 0..self.channel_refs.len() {
            sum += self.buffered_amount(i).await;
        }

        sum
    }
    
    /// Waits until the buffer for the channel at the given index is empty.
    pub async fn flush(&self, index: usize) {
        loop {
            let buffered_amount = self.buffered_amount(index).await;
            if buffered_amount == 0 {
                break;
            }

            Delay::new(Duration::from_millis(1)).await;
        }
    }

    /// Returns the current Round Trip Time (RTT) in milliseconds.
    /// Returns None if the stats provider is unavailable or RTT cannot be determined.
    ///
    /// The RTT value is cached and only refreshed if the cached value is older than 1 second.
    /// This prevents excessive stats queries when rtt() is called frequently.
    pub async fn rtt(&self) -> Option<f64> {
        let now = Instant::now();

        // Check if we have a valid cached value
        {
            let cache = self.rtt_cache.lock().await;
            if let Some((cached_rtt, cached_time)) = *cache {
                if now.duration_since(cached_time) < Duration::from_secs(1) {
                    return Some(cached_rtt);
                }
            }
        }

        // Cache is expired or empty, fetch new RTT
        if let Some(provider) = self.stats_provider.upgrade() {
            if let Some(new_rtt) = provider.rtt().await {
                // Update cache with new value
                let mut cache = self.rtt_cache.lock().await;
                *cache = Some((new_rtt, now));
                return Some(new_rtt);
            }
        }

        None
    }

    /// Waits until the buffered amount for the channel at the given index falls below the configured threshold.
    ///
    /// This method listens to the bufferedamountlow event from the WebRTC data channel with a timeout fallback.
    /// If the timeout expires before receiving the event, it manually checks the buffered amount.
    /// If the buffer is still high, it continues listening for the event.
    ///
    /// The threshold is configured via `ChannelConfig::buffer_low_threshold`.
    ///
    /// # Arguments
    /// * `index` - The channel index to monitor
    /// * `threshold` - The threshold in bytes. If buffer amount is below this, the function returns.
    /// * `timeout` - Maximum duration to wait for the buffer low event before checking manually
    ///
    /// # Returns
    /// Returns when the buffer low event is triggered or buffer is below threshold, or None if the channel doesn't exist or the sender is dropped.
    pub async fn wait_buffer_low(&self, index: usize, threshold: usize, timeout: Duration) -> Option<()> {
        use futures::StreamExt;
        use futures_util::{select, FutureExt};

        let receiver = self.buffer_low_rx.get(index)?.clone();

        loop {
            let mut delay = Delay::new(timeout).fuse();
            let mut rx_lock = receiver.lock().await;
            let buffered = self.buffered_amount(index).await;
            if buffered < threshold {
                return Some(());
            }

            select! {
                event = rx_lock.next() => {
                    return event;
                }
                _ = delay => {
                    drop(rx_lock);
                    break None
                }
            }
        }
    }
}
