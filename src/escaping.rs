use super::*;
use std::{convert::TryFrom, ops::Deref};

pub const NULL: u8 = 0;
pub const ESCAPED_NULL: [u8; 2] = [0, 1];
pub const TERMINATOR: [u8; 2] = [0, 255];

#[derive(Eq, PartialEq, Hash, Clone)]
pub struct ContainsUnescapedNull;

#[derive(Eq, PartialEq, Hash, Clone)]
pub struct EscapedVec(pub(crate) Bytes);

impl TryFrom<Bytes> for EscapedVec {
    type Error = ContainsUnescapedNull;
    fn try_from(input: Bytes) -> Result<EscapedVec, ContainsUnescapedNull> {
        if input.as_ref() == &[NULL] {
            Err(ContainsUnescapedNull)
        } else if input
            .windows(2)
            .any(|chrs| chrs[0] == NULL && chrs != ESCAPED_NULL)
        {
            Err(ContainsUnescapedNull)
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
    /// Creates an `EscapedVec` without checking or copying.
    ///
    /// # Safety
    ///
    /// `bs` must not contain any unescaped `[NULL]` bytes.
    pub unsafe fn from_bytes_unchecked(bs: Bytes) -> Self {
        Self(bs)
    }

    pub fn unescape(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.len());
        let mut was_escape = false;

        for byt in self.0.iter() {
            if *byt == NULL {
                out.push(NULL);
                was_escape = true;
            } else if was_escape {
                was_escape = false;
            } else {
                out.push(*byt)
            }
        }

        out
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
