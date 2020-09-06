use quickcheck::{Arbitrary, Gen};
use quickcheck_macros::*;
use sledis::*;

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum OwnedKey {
    Blob(Vec<u8>),
    List(Vec<u8>, ListIndex),
    ListMeta(Vec<u8>),
    Table(Vec<u8>, Vec<u8>),
    TableMeta(Vec<u8>),
}

use OwnedKey::*;

impl Arbitrary for OwnedKey {
    fn arbitrary<G: Gen>(g: &mut G) -> Self {
        let tag = u8::arbitrary(g);
        match tag % 5 {
            0 => Blob(Vec::arbitrary(g)),
            1 => List(Vec::arbitrary(g), ListIndex::arbitrary(g)),
            2 => ListMeta(Vec::arbitrary(g)),
            3 => Table(Vec::arbitrary(g), Vec::arbitrary(g)),
            4 => TableMeta(Vec::arbitrary(g)),
            _ => unreachable!(),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        match self {
            Blob(name) => Box::new(name.shrink().map(Blob)),
            List(name, ix) => Box::new(
                (name.clone(), *ix)
                    .shrink()
                    .map(|(name, ix)| List(name, ix)),
            ),
            ListMeta(name) => Box::new(name.shrink().map(ListMeta)),
            Table(name, key) => Box::new(
                (name.clone(), key.clone())
                    .shrink()
                    .map(|(name, key)| Table(name, key)),
            ),
            TableMeta(name) => Box::new(name.shrink().map(TableMeta)),
        }
    }
}

impl OwnedKey {
    fn encode(&self) -> Vec<u8> {
        match self {
            Blob(name) => keys::blob(name),
            List(name, ix) => keys::list(name, *ix),
            ListMeta(name) => keys::list_meta(name),
            Table(name, key) => keys::table(name, key),
            TableMeta(name) => keys::table_meta(name),
        }
    }
}

#[quickcheck]
fn encode_inj((k1, k2): (OwnedKey, OwnedKey)) -> bool {
    (k1 != k2) || (k1.encode() == k2.encode())
}
