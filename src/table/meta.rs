use super::*;

#[derive(Default, Clone, Eq, PartialEq, Debug)]
pub struct Meta {
    pub len: u64,
}

impl Meta {
    pub fn encode(&self) -> Vec<u8> {
        let capacity = 8;
        let mut out = Vec::with_capacity(capacity);

        out.extend_from_slice(&self.len.to_be_bytes());

        out
    }

    pub fn decode(input: Segment) -> Option<Self> {
        let mut buf = [0u8; 8];
        buf.copy_from_slice(input.get(0..8)?);
        let len = u64::from_be_bytes(buf);

        Some(Meta { len })
    }

    pub fn len(&self) -> u64 {
        self.len
    }
}
