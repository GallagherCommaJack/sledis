use super::*;
use sled::IVec;
use std::error::Error as StdError;

pub trait ListStore {
    type Error: StdError + From<sled::Error>;

    fn lpush_front<V>(&self, name: &[u8], val: V) -> Result<(), Self::Error>
    where
        IVec: From<V>;

    fn lpush_back<V>(&self, name: &[u8], val: V) -> Result<(), Self::Error>
    where
        IVec: From<V>;

    fn lset<V>(&self, name: &[u8], ix: ListIndex, val: V) -> Result<(), Self::Error>
    where
        IVec: From<V>;

    fn lget(&self, name: &[u8], ix: ListIndex) -> Result<Option<IVec>, Self::Error>;

    fn lpop_front(&self, name: &[u8]) -> Result<Option<IVec>, Self::Error>;
    fn lpop_back(&self, name: &[u8]) -> Result<Option<IVec>, Self::Error>;
}
