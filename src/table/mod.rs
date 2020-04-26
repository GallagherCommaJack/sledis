use super::*;
use thiserror::*;

mod meta;
pub use self::meta::*;

pub trait TableReadStore: ReadStore {
    fn get_meta(&self, name: &[u8]) -> Result<Option<Meta>, Self::Error>;

    fn table_get(&self, name: &[u8], key: &[u8]) -> Result<Option<IVec>, Self::Error>;
}

pub trait TableWriteStore: WriteStore {
    fn table_set<V>(&self, name: &[u8], key: &[u8], val: V) -> Result<Option<IVec>, Self::Error>
    where
        IVec: From<V>;
}

impl<S> TableReadStore for S
where
    S: ReadStore,
    S::Error: From<Error>,
{
    fn get_meta(&self, name: &[u8]) -> Result<Option<Meta>, Self::Error> {
        let key = Key::Table { name, key: None }.encode();

        if let Some(bs) = self.get(&key)? {
            if let Some(got) = Meta::decode(Segment::new(bs)) {
                Ok(Some(got))
            } else {
                Err(InvalidMeta(name.to_vec()).into())
            }
        } else {
            Ok(None)
        }
    }

    fn table_get(&self, name: &[u8], key: &[u8]) -> Result<Option<IVec>, Self::Error> {
        let key = Key::Table {
            name,
            key: Some(key),
        }
        .encode();

        Ok(self.get(&key)?)
    }
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("invalid table metadata, key was: {0:#?}")]
    InvalidMeta(Vec<u8>),
}

use self::Error::*;
