use quickcheck_macros::*;
use sledis::escaping::*;

#[quickcheck]
fn escape_unescape(input: Vec<u8>) -> bool {
    let escaped = escape_optimistic(&input);
    let unescaped = escaped.unescape();
    input == unescaped
}

#[quickcheck]
fn escape_is_escaped(input: Vec<u8>) -> bool {
    is_escaped(&escape_optimistic(&input))
}
