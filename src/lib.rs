use bytes::*;
use std::convert::TryFrom;

pub const NULL: u8 = 0;
pub const ESCAPED_NULL: [u8; 2] = [0, 1];
pub const TERMINATOR: [u8; 2] = [0, 0];

#[derive(Eq, PartialEq, Hash, Clone)]
pub struct ContainsNull;

#[derive(Eq, PartialEq, Hash, Clone)]
pub struct EscapedVec(Bytes);

impl TryFrom<Bytes> for EscapedVec {
    type Error = ContainsNull;
    fn try_from(input: Bytes) -> Result<EscapedVec, ContainsNull> {
        if input.contains(&NULL) {
            Err(ContainsNull)
        } else {
            Ok(EscapedVec(input))
        }
    }
}

impl EscapedVec {
    pub unsafe fn from_bytes_unchecked(bs: Bytes) -> Self {
        Self(bs)
    }

    pub fn escape(input: &[u8]) -> Self {
        Self::escape_with_size_hint(input, input.len())
    }

    pub fn escape_with_size_hint(input: &[u8], hint: usize) -> Self {
        let mut out: Vec<u8> = Vec::with_capacity(hint);
        for chr in input {
            if *chr != NULL {
                out.push(*chr);
            } else {
                out.extend_from_slice(&ESCAPED_NULL);
            }
        }
        EscapedVec(out.into())
    }

    pub fn as_arr(&self) -> EscapedArr {
        EscapedArr(self.0.as_ref())
    }
}

#[derive(Eq, PartialEq, Hash, Clone, Copy)]
pub struct EscapedArr<'a>(&'a [u8]);

impl<'a> TryFrom<&'a [u8]> for EscapedArr<'a> {
    type Error = ContainsNull;

    fn try_from(input: &'a [u8]) -> Result<EscapedArr<'a>, ContainsNull> {
        if input.contains(&NULL) {
            Err(ContainsNull)
        } else {
            Ok(EscapedArr(input))
        }
    }
}

#[derive(Eq, PartialEq, Hash, Clone)]
pub enum Key {
    Blob(EscapedVec),
    List(EscapedVec, i64),
    SubMap(EscapedVec, EscapedVec),
}

#[derive(Eq, PartialEq, Hash, Clone, Copy)]
pub enum KeyRef<'a> {
    Blob(EscapedArr<'a>),
    List(EscapedArr<'a>, i64),
    ListMeta(EscapedArr<'a>),
    SubMap(EscapedArr<'a>, EscapedArr<'a>),
    MapMeta(EscapedArr<'a>),
}

impl Key {
    pub fn to_ref(&self) -> KeyRef {
        match self {
            Key::Blob(b) => KeyRef::Blob(b.as_arr()),
            Key::List(b, i) => KeyRef::List(b.as_arr(), *i),
            Key::SubMap(b1, b2) => KeyRef::SubMap(b1.as_arr(), b2.as_arr()),
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
            KeyRef::Blob(EscapedArr(b)) => {
                out = Vec::with_capacity(1 + b.len() + 2);
                out.push(BLOB_TAG);
                out.extend_from_slice(b);
                out.extend_from_slice(&TERMINATOR);
            }
            KeyRef::List(EscapedArr(k), ix) => {
                out = Vec::with_capacity(1 + k.len() + 2 + 8);
                out.push(LIST_TAG);
                out.extend_from_slice(k);
                out.extend_from_slice(&TERMINATOR);
                let ix_bytes = encode_i64(*ix);
                out.extend_from_slice(&ix_bytes);
            }
            KeyRef::ListMeta(EscapedArr(k)) => {
                out = Vec::with_capacity(1 + k.len() + 2);
                out.push(LIST_META_TAG);
                out.extend_from_slice(k.as_ref());
                out.extend_from_slice(&TERMINATOR);
            }
            KeyRef::SubMap(EscapedArr(k1), EscapedArr(k2)) => {
                out = Vec::with_capacity(1 + k1.len() + 2 + k2.len() + 2);
                out.push(MAP_TAG);
                out.extend_from_slice(k1);
                out.extend_from_slice(&TERMINATOR);
                out.extend_from_slice(k2);
                out.extend_from_slice(&TERMINATOR);
            }
            KeyRef::MapMeta(EscapedArr(k)) => {
                out = Vec::with_capacity(1 + k.len() + 2);
                out.push(MAP_META_TAG);
                out.extend_from_slice(k);
                out.extend_from_slice(&TERMINATOR);
            }
        }
        out
    }
}
