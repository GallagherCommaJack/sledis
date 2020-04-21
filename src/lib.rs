use bytes::*;
use sled::IVec;

mod escaping;
pub use escaping::*;

mod keys;
pub use keys::*;

pub mod lists;

mod error;
pub use error::*;

pub trait Store {
    type Error;

    fn get<K>(&self, key: K) -> Result<Option<IVec>, Self::Error>
    where
        IVec: From<K>,
        K: AsRef<[u8]>;

    fn insert<K, V>(&self, key: K, val: V) -> Result<Option<IVec>, Self::Error>
    where
        IVec: From<K> + From<V>,
        K: AsRef<[u8]>;

    fn remove<K>(&self, key: K) -> Result<Option<IVec>, Self::Error>
    where
        IVec: From<K>,
        K: AsRef<[u8]>;

    fn fetch_update<K, V, F>(&self, key: K, mut f: F) -> Result<Option<IVec>, Self::Error>
    where
        IVec: From<K> + From<V>,
        K: AsRef<[u8]>,
        F: FnMut(Option<&[u8]>) -> Option<V>,
    {
        let got = self.get(key.as_ref())?;
        let res = f(got.as_ref().map(IVec::as_ref));
        match res {
            Some(new) => self.insert(key, new),
            None => self.remove(key),
        }
    }
}

impl Store for sled::Tree {
    type Error = Error<sled::Error>;

    fn get<K>(&self, key: K) -> Result<Option<IVec>, Self::Error>
    where
        IVec: From<K>,
        K: AsRef<[u8]>,
    {
        self.get(key).map_err(Error::Store)
    }

    fn insert<K, V>(&self, key: K, val: V) -> Result<Option<IVec>, Self::Error>
    where
        IVec: From<K> + From<V>,
        K: AsRef<[u8]>,
    {
        self.insert(key, val).map_err(Error::Store)
    }

    fn remove<K>(&self, key: K) -> Result<Option<IVec>, Self::Error>
    where
        IVec: From<K>,
        K: AsRef<[u8]>,
    {
        self.remove(key).map_err(Error::Store)
    }

    fn fetch_update<K, V, F>(&self, key: K, f: F) -> Result<Option<IVec>, Self::Error>
    where
        IVec: From<K> + From<V>,
        K: AsRef<[u8]>,
        F: FnMut(Option<&[u8]>) -> Option<V>,
    {
        self.fetch_and_update(key, f).map_err(Error::Store)
    }
}

impl Store for sled::TransactionalTree {
    type Error = Error<sled::ConflictableTransactionError>;

    fn get<K>(&self, key: K) -> Result<Option<IVec>, Self::Error>
    where
        IVec: From<K>,
        K: AsRef<[u8]>,
    {
        self.get(key)
            .map_err(sled::ConflictableTransactionError::from)
            .map_err(Error::Store)
    }

    fn insert<K, V>(&self, key: K, val: V) -> Result<Option<IVec>, Self::Error>
    where
        IVec: From<K> + From<V>,
        K: AsRef<[u8]>,
    {
        self.insert(key, val)
            .map_err(sled::ConflictableTransactionError::from)
            .map_err(Error::Store)
    }

    fn remove<K>(&self, key: K) -> Result<Option<IVec>, Self::Error>
    where
        IVec: From<K>,
        K: AsRef<[u8]>,
    {
        self.remove(key)
            .map_err(sled::ConflictableTransactionError::from)
            .map_err(Error::Store)
    }
}
