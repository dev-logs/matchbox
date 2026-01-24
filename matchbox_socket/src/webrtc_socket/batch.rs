use bincode::config::{standard, Configuration};
use bincode::{Decode, Encode};
use matchbox_protocol::PeerId;
use crate::Packet;
use crate::webrtc_socket::{PeerDataSender, MAX_PACKET_SIZE};

#[derive(Debug)]
pub enum BatchSendResult {
    Success,
    HeaderFailure { error: String },
    PartialFailure { sent: usize, total: usize, error: String },
}

impl BatchSendResult {
    pub fn is_success(&self) -> bool {
        matches!(self, BatchSendResult::Success)
    }
}

#[derive(Encode, Decode, Clone, PartialEq)]
pub struct Batch {
    #[bincode(with_serde)]
    peer_id: PeerId,
    size: usize,
    buffer: Vec<u8>,
}

impl Batch {
    pub fn new(peer_id: PeerId, size: usize) -> Self {
        Self { peer_id, size, buffer: Vec::new() }
    }

    pub fn from_bytes(data: &[u8], _peer_id: &PeerId) -> Option<Self> {
        if data.len() != 1024 {
            return None
        }

        let len_bytes = &data[0..2];
        let len = u16::from_le_bytes([
            len_bytes[0],
            len_bytes[1]
        ]) as usize;

        if len > 1022 {
            log::info!("Batch header parse failed: length field {} exceeds max 1022", len);
            return None
        }

        let serialized_data = &data[2..2 + len];

        let cfg = standard();
        let result = bincode::decode_from_slice::<Self, Configuration>(serialized_data, cfg);
        match result {
            Ok((mut batch, _)) => {
                batch.buffer = Vec::with_capacity(batch.size);
                Some(batch)
            }
            Err(e) => {
                log::info!("Batch header parse failed: bincode decode error: {e:?}");
                None
            }
        }
    }

    pub fn as_bytes(&self) -> Packet {
        let config = standard();
        let mut this = self.clone();
        this.buffer = vec![];
        let bytes: Vec<u8> = bincode::encode_to_vec(this, config).unwrap();
        let mut buffer = vec![0u8; 1024];

        let len = bytes.len();
        if len + 2 > 1022 {
            panic!("Batch size too large, it will break all things");
        }

        buffer[0..2].copy_from_slice(&(len as u16).to_le_bytes());

        buffer[2..2 + len].copy_from_slice(&bytes);

        buffer.into_boxed_slice()
    }

    pub fn full_fill(&mut self, data: &[u8]) -> bool {
        self.buffer.extend_from_slice(data);
        self.buffer.len() >= self.size
    }

    pub fn into_packet(self) -> Packet {
        self.buffer.into_boxed_slice()
    }

    pub fn send_chunked<S: PeerDataSender>(
        data: &[u8],
        peer_id: PeerId,
        sender: &mut S,
    ) -> BatchSendResult {
        let batch = Batch::new(peer_id, data.len());

        if let Err(e) = sender.send(batch.as_bytes()) {
            return BatchSendResult::HeaderFailure {
                error: format!("{:?}", e),
            };
        }

        let chunks: Vec<_> = data.chunks(MAX_PACKET_SIZE).collect();
        let total = chunks.len();

        for (idx, chunk) in chunks.into_iter().enumerate() {
            if let Err(e) = sender.send(chunk.into()) {
                return BatchSendResult::PartialFailure {
                    sent: idx,
                    total,
                    error: format!("{:?}", e),
                };
            }
        }

        BatchSendResult::Success
    }
}

pub struct BatchReceiver {
    current_batch: Option<Batch>,
    batch_started_ms: Option<f64>,
    timeout_ms: f64,
}

impl BatchReceiver {
    pub fn new(timeout_ms: f64) -> Self {
        Self {
            current_batch: None,
            batch_started_ms: None,
            timeout_ms,
        }
    }

    pub fn receive_packet(&mut self, data: &[u8], peer_id: &PeerId, now_ms: f64) -> Option<Packet> {
        self.check_timeout(now_ms);

        if data.len() == 1024 {
            log::info!("Received 1024-byte packet from peer {peer_id}, checking if batch header");
        }

        if let Some(batch) = Batch::from_bytes(data, peer_id) {
            log::info!("Batch header received from peer {peer_id}, expecting {} bytes", batch.size);
            self.current_batch = Some(batch);
            self.batch_started_ms = Some(now_ms);
            return None;
        }

        if let Some(batch) = self.current_batch.as_mut() {
            if batch.full_fill(data) {
                let size = batch.buffer.len();
                log::info!("Batch finalized from peer {peer_id}, received {size} bytes");
                self.batch_started_ms = None;
                return self.current_batch.take().map(|b| b.into_packet());
            }
            return None;
        }

        Some(data.to_vec().into_boxed_slice())
    }

    pub fn check_timeout(&mut self, now_ms: f64) -> bool {
        if let Some(started) = self.batch_started_ms {
            if now_ms - started > self.timeout_ms {
                log::warn!("Batch receive timeout after {}ms, clearing incomplete batch", self.timeout_ms);
                self.current_batch = None;
                self.batch_started_ms = None;
                return true;
            }
        }
        false
    }
}
