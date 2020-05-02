use super::*;
use sled::IVec;
use thiserror::*;

mod meta;
pub use self::meta::*;

/// Types that implement this trait provide a byte-slice-indexed table of arrays.
/// `ListReadStore` is implemented for arbitrary `ReadStore`s.
pub trait ListReadStore: ReadStore {
    fn list_get_meta(&self, name: &[u8]) -> Result<Meta, Self::Error>;

    fn list_len(&self, name: &[u8]) -> Result<u64, Self::Error>;

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
/// // A new empty list
/// let list_meta_data = tree.list_get_meta(b"my_list").unwrap();
/// assert_eq!(list_meta_data.len(), 0);
/// assert_eq!(tree.list_len(b"my_list").unwrap(), 0);
///
/// // pushing and popping from the front
/// tree.list_push_front(b"my_list", b"oof").unwrap();
/// assert_eq!(tree.list_len(b"my_list").unwrap(), 1);
/// assert_eq!(tree.list_pop_front(b"my_list").unwrap().unwrap(), b"oof");
/// assert_eq!(tree.list_len(b"my_list").unwrap(), 0);
///
/// // and the back
/// tree.list_push_back(b"my_list", b"oof").unwrap();
/// assert_eq!(tree.list_len(b"my_list").unwrap(), 1);
/// assert_eq!(tree.list_pop_back(b"my_list").unwrap().unwrap(), b"oof");
/// assert_eq!(tree.list_len(b"my_list").unwrap(), 0);
/// ```
pub trait ListWriteStore: ListReadStore + WriteStore {
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

pub trait ListRangeStore: ListReadStore + RangeStore {
    type ListRangeIter: DoubleEndedIterator<Item = Result<IVec, Self::Error>>;

    fn list_range<R: RangeBounds<u64>>(
        &self,
        name: &[u8],
        range: R,
    ) -> Result<Option<Self::ListRangeIter>, Self::Error>;
}

/// Fetches previous metadata at `name` if it exists, returning an error if data exists but fails to parse.
/// Applies `f`, and overwrites the metadata with the resulting list if it doesn't return an error.
/// Returns the written `meta`.
fn update_list_meta<S, F>(store: &S, name: &[u8], mut f: F) -> Result<Option<Meta>, S::Error>
where
    S: WriteStore,
    S::Error: From<Error>,
    F: FnMut(Meta) -> Option<Meta>,
{
    let key = keys::list_meta(name);
    let mut err: Option<S::Error> = None;
    let mut meta: Option<Meta> = None;

    store.fetch_update::<IVec, _>(&key, |iv| {
        let got = if let Some(bs) = iv {
            if let Some(got) = Meta::decode(bs) {
                got
            } else {
                err = Some(InvalidMeta(name.to_vec()).into());
                return Some(bs.into());
            }
        } else {
            Meta::default()
        };

        let m = f(got);
        err = None;
        meta = m;
        m.map(Meta::encode).as_ref().map(IVec::from)
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
    fn list_get_meta(&self, name: &[u8]) -> Result<Meta, Self::Error> {
        let key = keys::list_meta(name);

        if let Some(bs) = self.get(&key)? {
            Meta::decode(&bs).ok_or_else(|| InvalidMeta(name.to_vec()).into())
        } else {
            Ok(Meta::default())
        }
    }

    fn list_len(&self, name: &[u8]) -> Result<u64, Self::Error> {
        Ok(self.list_get_meta(name)?.len())
    }

    fn list_get(&self, name: &[u8], ix: u64) -> Result<Option<IVec>, Self::Error> {
        let key = keys::list_meta(name);

        if let Some(meta) = self
            .get(&key)?
            .map(|v| Meta::decode(&v).ok_or_else(|| InvalidMeta(name.to_vec())))
            .transpose()?
        {
            if let Some(ix) = meta.mk_key(ix) {
                let key = keys::list(name, ix);
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
    fn list_push_front<V>(&self, name: &[u8], val: V) -> Result<(), Self::Error>
    where
        IVec: From<V>,
    {
        // dummy value - will overwrite before use
        let mut ix = 0;

        let meta = update_list_meta(self, name, |mut meta| {
            ix = meta.push_front();
            Some(meta)
        })?;
        debug_assert!(meta.unwrap().len() > 0);

        self.insert(&keys::list(name, ix), val)?;

        Ok(())
    }

    fn list_push_back<V>(&self, name: &[u8], val: V) -> Result<(), Self::Error>
    where
        IVec: From<V>,
    {
        // dummy value - will overwrite before use
        let mut ix = 0;

        let meta = update_list_meta(self, name, |mut meta| {
            ix = meta.push_back();
            Some(meta)
        })?;
        debug_assert!(meta.unwrap().len() > 0);

        self.insert(&keys::list(name, ix), val)?;

        Ok(())
    }

    fn list_pop_front(&self, name: &[u8]) -> Result<Option<IVec>, Self::Error> {
        let mut ix: Option<ListIndex> = None;

        let meta = update_list_meta(self, name, |mut meta| {
            ix = meta.pop_front();
            if meta.is_empty() {
                None
            } else {
                Some(meta)
            }
        })?;
        debug_assert!(meta.is_none() || meta.unwrap().len() > 0);

        Ok(if let Some(ix) = ix {
            let key = keys::list(name, ix);
            let res = self.remove(&key)?;

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

        let meta = update_list_meta(self, name, |mut meta| {
            ix = meta.pop_back();
            if meta.is_empty() {
                None
            } else {
                Some(meta)
            }
        })?;
        debug_assert!(meta.is_none() || meta.unwrap().len() > 0);

        Ok(if let Some(ix) = ix {
            let key = keys::list(name, ix);
            let res = self.remove(&key)?;

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
        let key = keys::list_meta(name);

        if let Some(meta) = self
            .get(&key)?
            .map(|v| Meta::decode(&v).ok_or_else(|| InvalidMeta(name.to_vec())))
            .transpose()?
        {
            if let Some(ix) = meta.mk_key(ix) {
                let key = keys::list(name, ix);
                return self.insert(&key, val);
            }
        }

        Ok(None)
    }
}

impl<S> ListRangeStore for S
where
    S: RangeStore,
    S::Error: From<Error>,
{
    type ListRangeIter = Box<dyn DoubleEndedIterator<Item = Result<IVec, Self::Error>> + 'static>;

    fn list_range<R: RangeBounds<u64>>(
        &self,
        name: &[u8],
        range: R,
    ) -> Result<Option<Self::ListRangeIter>, Self::Error> {
        use std::ops::Bound;

        let meta = self.list_get_meta(name)?;

        let start_key;
        let start = match range.start_bound() {
            Bound::Included(u) => {
                if let Some(k) = meta.mk_key(*u) {
                    start_key = keys::list(name, k);
                    Bound::Included(start_key.as_slice())
                } else {
                    return Ok(None);
                }
            }
            Bound::Excluded(u) => {
                if let Some(k) = meta.mk_key(*u) {
                    start_key = keys::list(name, k);
                    Bound::Excluded(start_key.as_slice())
                } else {
                    return Ok(None);
                }
            }
            Bound::Unbounded => {
                if let Some(k) = meta.mk_key(0) {
                    start_key = keys::list(name, k);
                    Bound::Included(start_key.as_slice())
                } else {
                    return Ok(None);
                }
            }
        };

        let end_key;
        let end = match range.end_bound() {
            Bound::Included(u) => {
                if let Some(k) = meta.mk_key(*u) {
                    end_key = keys::list(name, k);
                    Bound::Included(end_key.as_slice())
                } else {
                    return Ok(None);
                }
            }
            Bound::Excluded(u) => {
                if let Some(k) = meta.mk_key(*u) {
                    end_key = keys::list(name, k);
                    Bound::Excluded(end_key.as_slice())
                } else {
                    return Ok(None);
                }
            }
            Bound::Unbounded => {
                if let Some(k) = meta.mk_key(meta.len() - 1) {
                    end_key = keys::list(name, k);
                    Bound::Included(end_key.as_slice())
                } else {
                    return Ok(None);
                }
            }
        };

        let iter = self.range((start, end)).map(|res| res.map(|t| t.1));
        let biter = Box::new(iter);

        Ok(Some(biter))
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
