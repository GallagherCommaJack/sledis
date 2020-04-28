use super::*;

pub type ListIndex = i128;
pub const INDEX_BYTES: usize = 16;

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

pub fn blob(name: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + name.len() + 2);
    out.push(Tag::Blob as u8);
    out.extend_from_slice(escape(name).as_ref());
    out
}

#[inline(always)]
fn list_inner(name: &[u8], ix: Option<ListIndex>) -> Vec<u8> {
    let escaped_name = escape(name);
    let mut out = Vec::with_capacity(
        1 // tag
        + escaped_name.len() + 2 // null terminated name
        + 1 // meta or item
        + ix.map_or_else(|| 0, |_| INDEX_BYTES), // index
    );

    out.push(Tag::List as u8);
    out.extend_from_slice(&escaped_name);
    out.extend_from_slice(&TERMINATOR);

    match ix {
        Some(ix) => {
            out.push(1);
            let ix_bytes = encode_list_index(ix);
            out.extend_from_slice(&ix_bytes);
        }
        None => {
            out.push(0);
        }
    }

    out
}

pub fn list(name: &[u8], ix: ListIndex) -> Vec<u8> {
    list_inner(name, Some(ix))
}

pub fn list_meta(name: &[u8]) -> Vec<u8> {
    list_inner(name, None)
}

#[inline(always)]
fn table_inner(name: &[u8], key: Option<&[u8]>) -> Vec<u8> {
    let escaped_name = escape(name);
    let escaped_key = key.map(escape);
    let mut out = Vec::with_capacity(
        1 // tag
        + escaped_name.len() + 2 // null terminated name
        + 1 // meta or item
        + escaped_key.as_ref().map_or_else(|| 0, |k| k.len()), // null terminated key
    );
    out.push(Tag::Table as u8);
    out.extend_from_slice(&escaped_name);
    out.extend_from_slice(&TERMINATOR);
    match escaped_key {
        None => {
            out.push(0);
        }
        Some(key) => {
            out.push(1);
            out.extend_from_slice(&key);
        }
    }
    out
}

pub fn table(name: &[u8], key: &[u8]) -> Vec<u8> {
    table_inner(name, Some(key))
}

pub fn table_meta(name: &[u8]) -> Vec<u8> {
    table_inner(name, None)
}
