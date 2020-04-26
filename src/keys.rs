use super::*;

pub type ListIndex = i128;
pub const INDEX_BYTES: usize = 16;

#[derive(Eq, PartialEq, Hash, Clone, Copy)]
pub enum Key<'a> {
    Blob(&'a [u8]),
    List {
        name: &'a [u8],
        ix: Option<ListIndex>,
    },
    Table {
        name: &'a [u8],
        key: Option<&'a [u8]>,
    },
}

#[derive(Eq, PartialEq, Hash, Clone, Copy)]
#[repr(u8)]
enum Tag {
    Blob = 0,
    List = 1,
    Table = 2,
}

pub fn encode_list_index(i: ListIndex) -> [u8; INDEX_BYTES] {
    (i ^ ListIndex::min_value()).to_be_bytes()
}

pub fn decode_list_index(inp: &[u8]) -> Option<ListIndex> {
    if inp.len() != INDEX_BYTES {
        return None;
    }

    let mut buf = [0u8; INDEX_BYTES];
    buf.copy_from_slice(inp);

    Some(ListIndex::min_value() ^ ListIndex::from_be_bytes(buf))
}

impl<'a> Key<'a> {
    pub fn encode(&self) -> Vec<u8> {
        let mut out;
        match self {
            Key::Blob(name) => {
                out = Vec::with_capacity(1 + name.len() + 2);
                out.push(Tag::Blob as u8);
                out.extend_from_slice(escape(name).as_ref());
            }
            Key::List { name, ix } => {
                let escaped_name = escape(name);
                out = Vec::with_capacity(
                    1 // tag
                    + escaped_name.len() // null terminated name
                    + 1 // meta or item
                    + ix.map_or_else(|| 0, |_| INDEX_BYTES), // index
                );
                out.push(Tag::List as u8);
                out.extend_from_slice(&escaped_name);
                match ix {
                    None => {
                        out.push(0);
                    }
                    Some(ix) => {
                        out.push(1);
                        let ix_bytes = encode_list_index(*ix);
                        out.extend_from_slice(&ix_bytes);
                    }
                }
            }
            Key::Table { name, key } => {
                let escaped_name = escape(name);
                let escaped_key = key.map(escape);
                out = Vec::with_capacity(
                    1 // tag
                    + escaped_name.len() // null terminated name
                    + 1 // meta or item
                    + escaped_key.as_ref().map_or_else(|| 0, |k| k.len()), // null terminated key
                );
                out.push(Tag::Table as u8);
                out.extend_from_slice(&escaped_name);
                match escaped_key {
                    None => {
                        out.push(0);
                    }
                    Some(key) => {
                        out.push(1);
                        out.extend_from_slice(&key);
                    }
                }
            }
        }
        out
    }
}
