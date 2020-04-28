use quickcheck::{Arbitrary, Gen};
use quickcheck_macros::*;
use sledis::*;

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum OwnedKey {
    Blob(Vec<u8>),
    List {
        name: Vec<u8>,
        ix: Option<ListIndex>,
    },
    Table {
        name: Vec<u8>,
        key: Option<Vec<u8>>,
    },
}

use OwnedKey::*;

impl Arbitrary for OwnedKey {
    fn arbitrary<G: Gen>(g: &mut G) -> Self {
        let tag = u8::arbitrary(g);
        match tag % 3 {
            0 => OwnedKey::Blob(Vec::arbitrary(g)),
            1 => OwnedKey::List {
                name: Vec::arbitrary(g),
                ix: Option::arbitrary(g),
            },
            2 => OwnedKey::Table {
                name: Vec::arbitrary(g),
                key: Option::arbitrary(g),
            },
            _ => unreachable!(),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        match self {
            Blob(bs) => Box::new(bs.shrink().map(Blob)),
            List { name, ix } => Box::new(
                (name.clone(), ix.clone())
                    .shrink()
                    .map(|(name, ix)| List { name, ix }),
            ),
            Table { name, key } => Box::new(
                (name.clone(), key.clone())
                    .shrink()
                    .map(|(name, key)| Table { name, key }),
            ),
        }
    }
}

impl OwnedKey {
    fn as_key(&self) -> Key {
        match self {
            Blob(name) => Key::Blob(&name),
            List { name, ix } => Key::List {
                name: &name,
                ix: *ix,
            },
            Table { name, key } => Key::Table {
                name: &name,
                key: key.as_ref().map(AsRef::as_ref),
            },
        }
    }
}

#[quickcheck]
fn encode_inj((k1, k2): (OwnedKey, OwnedKey)) -> bool {
    (k1 == k2) == (k1.as_key().encode() == k2.as_key().encode())
}
