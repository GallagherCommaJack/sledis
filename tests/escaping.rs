use quickcheck_macros::*;
use sledis::escaping::*;
use std::{convert::TryFrom, ops::Deref};

#[quickcheck]
fn escape_unescape(input: Vec<u8>) -> bool {
    let escaped = escape(&input);
    let unescaped = escaped.unescape();
    input == unescaped
}

#[quickcheck]
fn escape_ends_with_terminator(input: Vec<u8>) -> bool {
    let escaped = escape(&input);
    &escaped[escaped.len() - 2..] == &TERMINATOR
}

#[quickcheck]
fn escape_escaped(input: Vec<u8>) -> bool {
    let escaped = escape(&input);

    let raw = escaped.deref().clone();

    EscapedVec::try_from(raw).is_ok()
}
