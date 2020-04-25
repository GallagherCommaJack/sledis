use super::*;

// list metadata type
#[derive(Default, Copy, Clone, Eq, PartialEq, Debug)]
pub struct Meta {
    pub head: ListIndex,
    pub len: u64,
}

pub const META_SIZE: usize = INDEX_BYTES + 8;

impl Meta {
    pub fn encode(self) -> [u8; META_SIZE] {
        let mut out = [0u8; META_SIZE];
        out[..INDEX_BYTES].copy_from_slice(&self.head.to_be_bytes());
        out[INDEX_BYTES..].copy_from_slice(&self.len.to_be_bytes());
        out
    }

    pub fn decode(inp: &[u8]) -> Option<Self> {
        if inp.len() < META_SIZE {
            None
        } else {
            let mut head_buf = [0u8; INDEX_BYTES];
            head_buf.copy_from_slice(&inp[..INDEX_BYTES]);
            let mut len_buf = [0u8; 8];
            len_buf.copy_from_slice(&inp[INDEX_BYTES..]);
            Some(Self {
                head: ListIndex::from_be_bytes(head_buf),
                len: u64::from_be_bytes(len_buf),
            })
        }
    }

    pub fn mk_key(&self, ix: u64) -> Option<ListIndex> {
        if ix < self.len {
            Some(self.head + ix as ListIndex)
        } else {
            None
        }
    }

    pub fn head_ix(&self) -> Option<ListIndex> {
        if self.len != 0 {
            Some(self.head)
        } else {
            None
        }
    }

    pub fn tail_ix(&self) -> Option<ListIndex> {
        if self.len != 0 {
            Some(self.head + self.len as ListIndex - 1)
        } else {
            None
        }
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(super) fn push_front(&mut self) -> ListIndex {
        self.head -= 1;
        self.len += 1;

        self.head
    }

    pub(super) fn pop_front(&mut self) -> Option<ListIndex> {
        let res = self.head_ix()?;
        self.head += 1;
        self.len -= 1;
        Some(res)
    }

    pub(super) fn push_back(&mut self) -> ListIndex {
        self.len += 1;
        self.head + self.len as ListIndex - 1
    }

    pub(super) fn pop_back(&mut self) -> Option<ListIndex> {
        let res = self.tail_ix()?;
        self.len -= 1;
        Some(res)
    }
}
