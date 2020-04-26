use super::*;

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
