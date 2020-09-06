use super::*;

pub type ListIndex = i128;
pub const INDEX_BYTES: usize = 16;

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

pub fn bare(name: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(name.len() + 2);
    escape_into(name, &mut out);
    out.extend_from_slice(&TERMINATOR);
    out
}

pub fn blob(name: &[u8]) -> Vec<u8> {
    bare(name)
}

#[inline(always)]
fn list_inner(name: &[u8], ix: Option<ListIndex>) -> Vec<u8> {
    let mut out = Vec::with_capacity(
        name.len() + 2 // null terminated name, optimistic
        + ix.map_or_else(|| 0, |_| INDEX_BYTES), // index
    );

    escape_into(name, &mut out);
    out.extend_from_slice(&TERMINATOR);

    if let Some(ix) = ix {
        let ix_bytes = encode_list_index(ix);
        out.extend_from_slice(&ix_bytes);
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
    let mut out = Vec::with_capacity(
        name.len() + 2 // null terminated name, optimistic
        + key.map_or_else(|| 0, |k| k.len() + 2), // null terminated key, optimistic
    );

    escape_into(name, &mut out);
    out.extend_from_slice(&TERMINATOR);

    if let Some(key) = key {
        escape_into(key, &mut out);
        out.extend_from_slice(&TERMINATOR);
    }

    out
}

pub fn table(name: &[u8], key: &[u8]) -> Vec<u8> {
    table_inner(name, Some(key))
}

pub fn table_meta(name: &[u8]) -> Vec<u8> {
    table_inner(name, None)
}
