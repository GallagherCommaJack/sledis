use super::*;
use std::{convert::TryFrom, ops::Deref};

pub const NULL: u8 = 0;
pub const ESCAPED_NULL: [u8; 2] = [0, 1];
pub const TERMINATOR: [u8; 2] = [0, 0];

#[derive(Eq, PartialEq, Hash, Clone)]
pub struct ContainsNull;

#[derive(Eq, PartialEq, Hash, Clone)]
pub struct EscapedVec(pub(crate) Bytes);

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

impl Deref for EscapedVec {
    type Target = Bytes;
    fn deref(&self) -> &Bytes {
        &self.0
    }
}

impl EscapedVec {
    pub unsafe fn from_bytes_unchecked(bs: Bytes) -> Self {
        Self(bs)
    }
}

pub fn escape(input: &[u8]) -> EscapedVec {
    escape_with_size_hint(input, input.len() + 2)
}

pub fn escape_with_size_hint(input: &[u8], hint: usize) -> EscapedVec {
    let mut out: Vec<u8> = Vec::with_capacity(hint);
    for chr in input {
        if *chr != NULL {
            out.push(*chr);
        } else {
            out.extend_from_slice(&ESCAPED_NULL);
        }
    }
    out.extend_from_slice(&TERMINATOR);
    EscapedVec(out.into())
}
