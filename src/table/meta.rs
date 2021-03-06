use super::*;

#[derive(Default, Clone, Eq, PartialEq, Debug)]
pub struct Meta {
    pub len: u64,
}

impl Meta {
    pub fn encode(&self) -> Record {
        let capacity = 8;
        let mut out = Vec::with_capacity(capacity);

        out.extend_from_slice(&self.len.to_be_bytes());
        Record::FromData(Tag::Table, out.into())
    }

    pub fn decode(input: &Record) -> Result<Self, Error> {
        if input.tag() != Tag::Table {
            Err(Error::BadType(Tag::Table, input.tag()))?
        } else if input.len() != 8 {
            Err(TableError::InvalidMeta(input.data()))?
        }

        let mut buf = [0u8; 8];
        buf.copy_from_slice(&input[0..8]);
        let len = u64::from_be_bytes(buf);

        Ok(Meta { len })
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}
