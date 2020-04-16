use bytes::*;

#[derive(Eq, PartialEq, Hash, Clone)]
pub enum Key {
    Blob(Bytes),
    List(Bytes, i64),
    SubMap(Bytes, Bytes),
}

#[derive(Eq, PartialEq, Hash, Clone, Copy)]
pub enum KeyRef<'a> {
    Blob(&'a [u8]),
    List(&'a [u8], i64),
    ListMeta(&'a [u8]),
    SubMap(&'a [u8], &'a [u8]),
    MapMeta(&'a [u8]),
}

impl Key {
    pub fn to_ref(&self) -> KeyRef {
        match self {
            Key::Blob(b) => KeyRef::Blob(b.as_ref()),
            Key::List(b, i) => KeyRef::List(b.as_ref(), *i),
            Key::SubMap(b1, b2) => KeyRef::SubMap(b1.as_ref(), b2.as_ref()),
        }
    }
}

const BLOB_TAG: u8 = 0;
const LIST_TAG: u8 = 1;
const LIST_META_TAG: u8 = 2;
const MAP_TAG: u8 = 3;
const MAP_META_TAG: u8 = 4;

fn encode_i64(i: i64) -> [u8; 8] {
    (i ^ i64::min_value()).to_be_bytes()
}

impl<'a> KeyRef<'a> {
    pub fn encode(&self) -> Vec<u8> {
        let mut out;
        match self {
            KeyRef::Blob(b) => {
                out = Vec::with_capacity(1 + 8 + b.len());
                out.push(BLOB_TAG);
                let len_bytes = (b.len() as u64).to_be_bytes();
                out.extend_from_slice(&len_bytes);
                out.extend_from_slice(b);
            }
            KeyRef::List(k, ix) => {
                out = Vec::with_capacity(1 + 8 + k.len() + 8);
                out.push(LIST_TAG);
                let len_bytes = (k.len() as u64).to_be_bytes();
                out.extend_from_slice(&len_bytes);
                out.extend_from_slice(k);
                let ix_bytes = encode_i64(*ix);
                out.extend_from_slice(&ix_bytes);
            }
            KeyRef::ListMeta(k) => {
                out = Vec::with_capacity(1 + 8 + k.len());
                out.push(LIST_META_TAG);
                out.extend_from_slice(k.as_ref());
            }
            KeyRef::SubMap(k1, k2) => {
                out = Vec::with_capacity(1 + 8 + k1.len() + 8 + k2.len());
                out.push(MAP_TAG);
                {
                    let len_bytes = (k1.len() as u64).to_be_bytes();
                    out.extend_from_slice(&len_bytes);
                    out.extend_from_slice(k1);
                }
                {
                    let len_bytes = (k2.len() as u64).to_be_bytes();
                    out.extend_from_slice(&len_bytes);
                    out.extend_from_slice(k2);
                }
            }
            KeyRef::MapMeta(k) => {
                out = Vec::with_capacity(1 + 8 + k.len());
                out.push(MAP_META_TAG);
                out.extend_from_slice(k.as_ref());
            }
        }
        out
    }
}
