use super::*;
use std::{convert::TryFrom, ops::Deref};

pub const NULL: u8 = 0;
pub const ESCAPE_CHAR: u8 = 1;
pub const ESCAPED_NULL: [u8; 2] = [NULL, ESCAPE_CHAR];
pub const TERMINATOR: [u8; 2] = [0, 255];

#[derive(Eq, PartialEq, Hash, Clone)]
pub struct NotEscaped;

pub type EscapedVecInner = Segment;

#[derive(Eq, PartialEq, Hash, Clone)]
pub struct EscapedVec(pub(crate) EscapedVecInner);

impl TryFrom<EscapedVecInner> for EscapedVec {
    type Error = NotEscaped;
    fn try_from(input: EscapedVecInner) -> Result<EscapedVec, NotEscaped> {
        if is_escaped(input.as_ref()) {
            Ok(EscapedVec(input))
        } else {
            Err(NotEscaped)
        }
    }
}

impl Deref for EscapedVec {
    type Target = EscapedVecInner;
    fn deref(&self) -> &EscapedVecInner {
        &self.0
    }
}

impl EscapedVec {
    /// Creates an `EscapedVec` without checking or copying.
    ///
    /// # Safety
    ///
    /// `bs` must not contain any unescaped `[NULL]` bytes.
    pub unsafe fn from_bytes_unchecked(bs: EscapedVecInner) -> Self {
        Self(bs)
    }

    pub fn unescape(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.len());
        let mut was_escape = false;

        for byt in self.0.as_ref() {
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

pub fn is_escaped(input: &[u8]) -> bool {
    let mut nulls_escaped = true;

    for byte in input.as_ref() {
        if !nulls_escaped {
            if *byte == ESCAPE_CHAR {
                nulls_escaped = true
            } else {
                return false;
            }
        } else if *byte == NULL {
            nulls_escaped = false
        }
    }

    nulls_escaped
}

pub fn escape(input: &[u8]) -> EscapedVec {
    escape_with_size_hint(input, input.len())
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

    EscapedVec(Segment::new(out.into()))
}
