use super::*;
use std::{convert::TryFrom, ops::Deref};

pub const NULL: u8 = 0;
pub const ESCAPE_CHAR: u8 = 1;
pub const ESCAPED_NULL: [u8; 2] = [NULL, ESCAPE_CHAR];
pub const TERMINATE_CHAR: u8 = 255;
pub const TERMINATOR: [u8; 2] = [NULL, TERMINATE_CHAR];

#[derive(Eq, PartialEq, Hash, Clone, Debug, Copy)]
pub struct NotEscaped;

pub type EscapedVecInner = IVec;

#[derive(Eq, PartialEq, Hash, Clone, Debug)]
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

    pub fn as_arr(&self) -> EscapedArr {
        EscapedArr(self.0.as_ref())
    }

    pub fn into_segment(self) -> IVec {
        self.0
    }
}

pub fn is_escaped(input: &[u8]) -> bool {
    let mut nulls_escaped = true;

    for byte in input {
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

pub fn escape_optimistic(input: &[u8]) -> EscapedVec {
    escape_with_size_hint(input, input.len())
}

pub fn escape_with_size_hint(input: &[u8], hint: usize) -> EscapedVec {
    let mut out: Vec<u8> = Vec::with_capacity(hint);

    escape_into(input, &mut out);

    EscapedVec(out.into())
}

pub fn escape_into(input: &[u8], out: &mut Vec<u8>) {
    for chr in input {
        if *chr == NULL {
            out.extend_from_slice(&ESCAPED_NULL);
        } else {
            out.push(*chr);
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct EscapedArr<'a>(&'a [u8]);

impl<'a> TryFrom<&'a [u8]> for EscapedArr<'a> {
    type Error = NotEscaped;
    fn try_from(input: &'a [u8]) -> Result<Self, NotEscaped> {
        if is_escaped(input) {
            Ok(EscapedArr(input))
        } else {
            Err(NotEscaped)
        }
    }
}

impl<'a> EscapedArr<'a> {
    pub fn to_vec(self) -> EscapedVec {
        EscapedVec(self.0.into())
    }
}

#[derive(Eq, PartialEq, Hash, Clone, Debug, Copy)]
pub enum InvalidStrings {
    UnescapedNull(usize),
    NoTerminator,
}

pub fn find_terminator(input: &[u8]) -> Result<usize, InvalidStrings> {
    let mut i = 0;
    let mut last_was_null = false;

    for byte in input {
        if last_was_null {
            match *byte {
                ESCAPE_CHAR => {
                    last_was_null = false;
                    i += 2;
                }
                TERMINATE_CHAR => {
                    return Ok(i);
                }
                _ => {
                    return Err(InvalidStrings::UnescapedNull(i));
                }
            }
        } else if *byte == NULL {
            last_was_null = true;
        } else {
            i += 1;
        }
    }

    Err(InvalidStrings::NoTerminator)
}

pub fn take_until_terminator(input: &[u8]) -> Result<(EscapedArr, &[u8]), InvalidStrings> {
    let found_ix = find_terminator(input)?;

    let first = &input[..found_ix];
    let rest = &input[found_ix + 2..];

    Ok((EscapedArr(first), rest))
}
