use bincode::config::{standard, Configuration};
use bincode::{Decode, Encode};
use matchbox_protocol::PeerId;
use crate::Packet;

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

    pub fn from_bytes(data: &[u8], peer_id: &PeerId) -> Option<Self> {
        if data.len() != 1024 {
            return None
        }

        let len_bytes = &data[0..2];
        let len = u16::from_le_bytes([
            len_bytes[0],
            len_bytes[1]
        ]) as usize;

        if len > 1022 {
            return None
        }

        let serialized_data = &data[2..2 + len];

        let cfg = standard();
        let (mut result, _) = bincode::decode_from_slice::<Self, Configuration>(serialized_data, cfg).ok()?;
        if result.peer_id != *peer_id {
            return None
        }

        result.buffer = Vec::with_capacity(result.size);
        Some(result)
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
}
