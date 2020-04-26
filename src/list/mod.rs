use super::*;
use sled::IVec;
use thiserror::*;

mod meta;
pub use self::meta::*;

/// Types that implement this trait provide a byte-slice-indexed table of arrays.
/// `ListReadStore` is implemented for arbitrary `ReadStore`s.
pub trait ListReadStore: ReadStore {
    fn list_get_meta(&self, name: &[u8]) -> Result<Option<Meta>, Self::Error>;

    fn list_len(&self, name: &[u8]) -> Result<Option<u64>, Self::Error>;

    fn list_get(&self, name: &[u8], ix: u64) -> Result<Option<IVec>, Self::Error>;
}

/// This trait provides deque semantics for the lists in `ListReadStore`.
/// # Example Initialization
/// ```
/// use sledis::{ListReadStore, ListWriteStore};
/// use sled::Config;
///
/// let tree = Config::new().temporary(true).open().unwrap();
///
/// let list_meta_data = tree.list_create(b"my_list").unwrap();
/// // A new empty list
/// assert_eq!(list_meta_data.len(), 0);
/// assert_eq!(tree.list_len(b"my_list").unwrap().unwrap(), 0);
///
/// // pushing and popping from the front
/// tree.list_push_front(b"my_list", b"oof").unwrap();
/// assert_eq!(tree.list_len(b"my_list").unwrap().unwrap(), 1);
/// assert_eq!(tree.list_pop_front(b"my_list").unwrap().unwrap(), b"oof");
/// assert_eq!(tree.list_len(b"my_list").unwrap().unwrap(), 0);
///
/// // and the back
/// tree.list_push_back(b"my_list", b"oof").unwrap();
/// assert_eq!(tree.list_len(b"my_list").unwrap().unwrap(), 1);
/// assert_eq!(tree.list_pop_back(b"my_list").unwrap().unwrap(), b"oof");
/// assert_eq!(tree.list_len(b"my_list").unwrap().unwrap(), 0);
/// ```
pub trait ListWriteStore: ListReadStore + WriteStore {
    fn list_create(&self, name: &[u8]) -> Result<Meta, Self::Error>;

    fn list_push_front<V>(&self, name: &[u8], val: V) -> Result<(), Self::Error>
    where
        IVec: From<V>;

    fn list_push_back<V>(&self, name: &[u8], val: V) -> Result<(), Self::Error>
    where
        IVec: From<V>;

    fn list_pop_front(&self, name: &[u8]) -> Result<Option<IVec>, Self::Error>;

    fn list_pop_back(&self, name: &[u8]) -> Result<Option<IVec>, Self::Error>;

    fn list_set<V>(&self, name: &[u8], ix: u64, val: V) -> Result<Option<IVec>, Self::Error>
    where
        IVec: From<V>;
}

/// Fetches previous metadata at `name` if it exists, returning an error if data exists but fails to parse.
/// Applies `f`, and overwrites the metadata with the resulting list if it doesn't return an error.
/// Returns the written `meta`.
fn update_list_meta<S, F>(store: &S, name: &[u8], mut f: F) -> Result<Option<Meta>, S::Error>
where
    S: WriteStore,
    S::Error: From<Error>,
    F: FnMut(Option<Meta>) -> Result<Option<Meta>, S::Error>,
{
    let key = Key::ListMeta(name).encode();
    let mut err: Option<S::Error> = None;
    let mut meta: Option<Meta> = None;

    store.fetch_update::<IVec, _>(&key, |iv| {
        let got = if let Some(bs) = iv {
            if let Some(got) = Meta::decode(bs) {
                Some(got)
            } else {
                err = Some(InvalidMeta(name.to_vec()).into());
                return Some(bs.into());
            }
        } else {
            None
        };
        match f(got) {
            Ok(m) => {
                err = None;
                meta = m;
                m.map(Meta::encode).as_ref().map(IVec::from)
            }
            Err(e) => {
                err = Some(e);
                iv.map(IVec::from)
            }
        }
    })?;

    if let Some(e) = err {
        Err(e)
    } else {
        Ok(meta)
    }
}

impl<S> ListReadStore for S
where
    S: ReadStore,
    S::Error: From<Error>,
{
    fn list_get_meta(&self, name: &[u8]) -> Result<Option<Meta>, Self::Error> {
        let key = Key::ListMeta(name).encode();

        if let Some(bs) = self.get(&key)? {
            if let Some(got) = Meta::decode(&bs) {
                Ok(Some(got))
            } else {
                Err(InvalidMeta(name.to_vec()).into())
            }
        } else {
            Ok(None)
        }
    }

    fn list_len(&self, name: &[u8]) -> Result<Option<u64>, Self::Error> {
        Ok(self.list_get_meta(name)?.as_ref().map(Meta::len))
    }

    fn list_get(&self, name: &[u8], ix: u64) -> Result<Option<IVec>, Self::Error> {
        let key = Key::ListMeta(name).encode();

        if let Some(meta) = self
            .get(&key)?
            .map(|v| Meta::decode(&v).ok_or_else(|| InvalidMeta(name.to_vec())))
            .transpose()?
        {
            if let Some(ix) = meta.mk_key(ix) {
                let key = Key::List(name, ix).encode();
                return self.get(&key);
            }
        }

        Ok(None)
    }
}

impl<S> ListWriteStore for S
where
    S: WriteStore + ListReadStore,
    S::Error: From<Error>,
{
    fn list_create(&self, name: &[u8]) -> Result<Meta, Self::Error> {
        update_list_meta(self, name, |om| Ok(Some(om.unwrap_or_default())))
            .transpose()
            .unwrap()
    }

    fn list_push_front<V>(&self, name: &[u8], val: V) -> Result<(), Self::Error>
    where
        IVec: From<V>,
    {
        // dummy value - will overwrite before use
        let mut ix = 0;

        update_list_meta(self, name, |om| {
            let mut meta = om.unwrap_or_default();
            ix = meta.push_front();
            Ok(Some(meta))
        })?
        .unwrap();

        self.insert(&Key::List(name, ix).encode(), val)?;

        Ok(())
    }

    fn list_push_back<V>(&self, name: &[u8], val: V) -> Result<(), Self::Error>
    where
        IVec: From<V>,
    {
        // dummy value - will overwrite before use
        let mut ix = 0;

        update_list_meta(self, name, |om| {
            let mut meta = om.unwrap_or_default();
            ix = meta.push_back();
            Ok(Some(meta))
        })?
        .unwrap();

        self.insert(&Key::List(name, ix).encode(), val)?;

        Ok(())
    }

    fn list_pop_front(&self, name: &[u8]) -> Result<Option<IVec>, Self::Error> {
        let mut ix: Option<ListIndex> = None;

        update_list_meta(self, name, |om| {
            let mut meta = om.unwrap_or_default();
            ix = meta.pop_front();
            Ok(Some(meta))
        })?
        .unwrap();

        Ok(if let Some(ix) = ix {
            let key = Key::List(name, ix);
            let res = self.remove(&key.encode())?;

            if res.is_none() {
                return Err(MissingVal(name.to_vec(), ix).into());
            }

            res
        } else {
            None
        })
    }

    fn list_pop_back(&self, name: &[u8]) -> Result<Option<IVec>, Self::Error> {
        let mut ix: Option<ListIndex> = None;

        update_list_meta(self, name, |om| {
            let mut meta = om.unwrap_or_default();
            ix = meta.pop_back();
            Ok(Some(meta))
        })?
        .unwrap();

        Ok(if let Some(ix) = ix {
            let key = Key::List(name, ix);
            let res = self.remove(&key.encode())?;

            if res.is_none() {
                return Err(MissingVal(name.to_vec(), ix).into());
            }

            res
        } else {
            None
        })
    }

    fn list_set<V>(&self, name: &[u8], ix: u64, val: V) -> Result<Option<IVec>, Self::Error>
    where
        IVec: From<V>,
    {
        let key = Key::ListMeta(name).encode();

        if let Some(meta) = self
            .get(&key)?
            .map(|v| Meta::decode(&v).ok_or_else(|| InvalidMeta(name.to_vec())))
            .transpose()?
        {
            if let Some(ix) = meta.mk_key(ix) {
                let key = Key::List(name, ix).encode();
                return self.insert(&key, val);
            }
        }

        Ok(None)
    }
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("invalid list metadata, key was: {0:#?}")]
    InvalidMeta(Vec<u8>),
    #[error("missing value in list {0:#?} at index {1}")]
    MissingVal(Vec<u8>, ListIndex),
}

use self::Error::*;