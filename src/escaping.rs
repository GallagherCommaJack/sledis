use super::*;
use std::{convert::TryFrom, ops::Deref};

pub const NULL: u8 = 0;
pub const ESCAPE_CHAR: u8 = 1;
pub const ESCAPED_NULL: [u8; 2] = [NULL, ESCAPE_CHAR];
pub const TERMINATOR: [u8; 2] = [0, 255];

#[derive(Eq, PartialEq, Hash, Clone)]
pub enum NotEscaped {
    ContainsUnescapedNull,
    MissingTerminator,
}

#[derive(Eq, PartialEq, Hash, Clone)]
pub struct EscapedVec(pub(crate) Bytes);

impl TryFrom<Bytes> for EscapedVec {
    type Error = NotEscaped;
    fn try_from(input: Bytes) -> Result<EscapedVec, NotEscaped> {
        if input.len() < 2 {
            return Err(NotEscaped::MissingTerminator);
        }

        let (content, terminator) = input.split_at(input.len() - 2);
        if terminator != &TERMINATOR {
            return Err(NotEscaped::MissingTerminator);
        }

        let mut nulls_escaped = true;

        for byte in content {
            if !nulls_escaped {
                if *byte == ESCAPE_CHAR {
                    nulls_escaped = true
                } else {
                    return Err(NotEscaped::ContainsUnescapedNull);
                }
            } else if *byte == NULL {
                nulls_escaped = false
            }
        }

        if !nulls_escaped {
            return Err(NotEscaped::ContainsUnescapedNull);
        }

        Ok(EscapedVec(input))
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
        let mut out = Vec::with_capacity(self.len() - 2);
        let mut was_escape = false;

        // subtract 2 from len to remove terminator
        let bytes = self[..self.len() - 2].iter();

        for byt in bytes {
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
        if *chr == NULL {
            out.extend_from_slice(&ESCAPED_NULL);
        } else {
            out.push(*chr);
        }
    }
    out.extend_from_slice(&TERMINATOR);
    EscapedVec(out.into())
}
