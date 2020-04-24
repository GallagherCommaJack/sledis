use super::*;
use sled::IVec;
use thiserror::*;

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct Meta {
    pub head: ListIndex,
    pub len: u64,
}

impl Default for Meta {
    fn default() -> Self {
        Meta { head: 0, len: 0 }
    }
}

pub const META_SIZE: usize = INDEX_BYTES + 8;

impl Meta {
    pub fn encode(self) -> [u8; META_SIZE] {
        let mut out = [0u8; META_SIZE];
        out[..INDEX_BYTES].copy_from_slice(&self.head.to_be_bytes());
        out[INDEX_BYTES..].copy_from_slice(&self.len.to_be_bytes());
        out
    }

    pub fn decode(inp: &[u8]) -> Option<Self> {
        if inp.len() < META_SIZE {
            None
        } else {
            let mut head_buf = [0u8; INDEX_BYTES];
            head_buf.copy_from_slice(&inp[..INDEX_BYTES]);
            let mut len_buf = [0u8; 8];
            len_buf.copy_from_slice(&inp[INDEX_BYTES..]);
            Some(Self {
                head: ListIndex::from_be_bytes(head_buf),
                len: u64::from_be_bytes(len_buf),
            })
        }
    }

    pub fn mk_key(&self, ix: u64) -> Option<ListIndex> {
        if ix < self.len {
            Some(self.head + ix as ListIndex)
        } else {
            None
        }
    }

    pub fn head_ix(&self) -> Option<ListIndex> {
        if self.len != 0 {
            Some(self.head)
        } else {
            None
        }
    }

    pub fn tail_ix(&self) -> Option<ListIndex> {
        if self.len != 0 {
            Some(self.head + self.len as ListIndex - 1)
        } else {
            None
        }
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn push_front(&mut self) -> ListIndex {
        self.head -= 1;
        self.len += 1;

        self.head
    }

    pub fn pop_front(&mut self) -> Option<ListIndex> {
        let res = self.head_ix()?;
        self.head += 1;
        self.len -= 1;
        Some(res)
    }

    pub fn push_back(&mut self) -> ListIndex {
        self.len += 1;
        self.head + self.len as ListIndex - 1
    }

    pub fn pop_back(&mut self) -> Option<ListIndex> {
        let res = self.tail_ix()?;
        self.len -= 1;
        Some(res)
    }
}

pub trait ListStore: Store {
    fn list_create(&self, name: &[u8]) -> Result<Meta, Self::Error>;

    fn list_get_meta(&self, name: &[u8]) -> Result<Option<Meta>, Self::Error>;
    fn list_len(&self, name: &[u8]) -> Result<Option<u64>, Self::Error>;

    fn list_push_front<V>(&self, name: &[u8], val: V) -> Result<(), Self::Error>
    where
        IVec: From<V>;

    fn list_push_back<V>(&self, name: &[u8], val: V) -> Result<(), Self::Error>
    where
        IVec: From<V>;

    fn list_pop_front(&self, name: &[u8]) -> Result<Option<IVec>, Self::Error>;
    fn list_pop_back(&self, name: &[u8]) -> Result<Option<IVec>, Self::Error>;

    fn list_get(&self, name: &[u8], ix: u64) -> Result<Option<IVec>, Self::Error>;

    fn list_set<V>(&self, name: &[u8], ix: u64, val: V) -> Result<Option<IVec>, Self::Error>
    where
        IVec: From<V>;
}

/// Fetches previous metadata at `name` if it exists, returning an error if data exists but fails to parse.
/// Applies `f`, and overwrites the metadata with the resulting list if it doesn't return an error.
/// Returns the written `meta`.
fn update_list_meta<S, F>(store: &S, name: &[u8], mut f: F) -> Result<Option<Meta>, S::Error>
where
    S: Store,
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

impl<S> ListStore for S
where
    S: Store,
    S::Error: From<Error>,
{
    fn list_create(&self, name: &[u8]) -> Result<Meta, Self::Error> {
        update_list_meta(self, name, |om| Ok(Some(om.unwrap_or_default())))
            .transpose()
            .unwrap()
    }

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
