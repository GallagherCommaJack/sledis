#![allow(warnings)]

use sled::IVec;
use std::ops::RangeBounds;

pub mod escaping;
use escaping::*;

pub mod keys;
pub use keys::*;

pub mod list;
pub use list::{ListReadStore, ListWriteStore};

pub mod table;
pub use table::{TableReadStore, TableWriteStore};

mod segment;
pub use segment::*;

mod error;
pub use error::*;

mod lock_table;

pub struct Conn {
    items: sled::Tree,
    ttl: sled::Tree,
    locks: lock_table::Table,
}

pub trait ReadStore {
    type Error: std::error::Error + 'static;

    fn get(&self, key: &[u8]) -> Result<Option<IVec>, Self::Error>;
}

pub trait WriteStore: ReadStore {
    fn insert<V>(&self, key: &[u8], val: V) -> Result<Option<IVec>, Self::Error>
    where
        IVec: From<V>;

    fn remove(&self, key: &[u8]) -> Result<Option<IVec>, Self::Error>;

    fn fetch_update<V, F>(&self, key: &[u8], mut f: F) -> Result<Option<IVec>, Self::Error>
    where
        IVec: From<V>,
        F: FnMut(Option<&[u8]>) -> Option<V>,
    {
        let got = self.get(key)?;
        let res = f(got.as_ref().map(IVec::as_ref));
        match res {
            Some(new) => self.insert(key, new),
            None => self.remove(key),
        }
    }
}

pub trait TransactionalStore: WriteStore {}

pub trait RangeStore: ReadStore {
    type Iter: DoubleEndedIterator<Item = Result<(IVec, IVec), Self::Error>> + 'static;

    fn range<'a, R>(&self, range: R) -> Self::Iter
    where
        R: RangeBounds<&'a [u8]>;
}

impl ReadStore for sled::Tree {
    type Error = Error<sled::Error>;

    fn get(&self, key: &[u8]) -> Result<Option<IVec>, Self::Error> {
        self.get(key).map_err(Error::Store)
    }
}

impl WriteStore for sled::Tree {
    fn insert<V>(&self, key: &[u8], val: V) -> Result<Option<IVec>, Self::Error>
    where
        IVec: From<V>,
    {
        self.insert(key, val).map_err(Error::Store)
    }

    fn remove(&self, key: &[u8]) -> Result<Option<IVec>, Self::Error> {
        self.remove(key).map_err(Error::Store)
    }

    fn fetch_update<V, F>(&self, key: &[u8], f: F) -> Result<Option<IVec>, Self::Error>
    where
        IVec: From<V>,
        F: FnMut(Option<&[u8]>) -> Option<V>,
    {
        self.fetch_and_update(key, f).map_err(Error::Store)
    }
}

impl RangeStore for sled::Tree {
    type Iter = std::iter::Map<
        sled::Iter,
        fn(Result<(IVec, IVec), sled::Error>) -> Result<(IVec, IVec), Error<sled::Error>>,
    >;

    fn range<'a, R>(&self, range: R) -> Self::Iter
    where
        R: RangeBounds<&'a [u8]>,
    {
        fn transform_sled_iter(
            input: Result<(IVec, IVec), sled::Error>,
        ) -> Result<(IVec, IVec), Error<sled::Error>> {
            input.map_err(Error::Store)
        }

        self.range::<&'a [u8], R>(range).map(transform_sled_iter)
    }
}

impl ReadStore for sled::TransactionalTree {
    type Error = Error<sled::ConflictableTransactionError>;

    fn get(&self, key: &[u8]) -> Result<Option<IVec>, Self::Error> {
        self.get(key)
            .map_err(sled::ConflictableTransactionError::from)
            .map_err(Error::Store)
    }
}

impl WriteStore for sled::TransactionalTree {
    fn insert<V>(&self, key: &[u8], val: V) -> Result<Option<IVec>, Self::Error>
    where
        IVec: From<V>,
    {
        self.insert::<&[u8], _>(key, val)
            .map_err(sled::ConflictableTransactionError::from)
            .map_err(Error::Store)
    }

    fn remove(&self, key: &[u8]) -> Result<Option<IVec>, Self::Error> {
        self.remove(key)
            .map_err(sled::ConflictableTransactionError::from)
            .map_err(Error::Store)
    }
}

impl TransactionalStore for sled::TransactionalTree {}
