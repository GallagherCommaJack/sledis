use super::*;
use sled::IVec;
use thiserror::*;

// list metadata type
#[derive(Default, Copy, Clone, Eq, PartialEq, Debug)]
pub struct Meta {
    pub head: ListIndex,
    pub len: u64,
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
    fn list_create<K>(&self, name: K) -> Result<Meta, Self::Error>
    where
        IVec: From<K>,
        K: AsRef<[u8]>;

    fn lpush_front<K, V>(&self, name: K, val: V) -> Result<(), Self::Error>
    where
        IVec: From<K> + From<V>,
        K: AsRef<[u8]>;

    fn lpush_back<K, V>(&self, name: K, val: V) -> Result<(), Self::Error>
    where
        IVec: From<K> + From<V>,
        K: AsRef<[u8]>;

    fn lpop_front<K>(&self, name: K) -> Result<Option<IVec>, Self::Error>
    where
        IVec: From<K>,
        K: AsRef<[u8]>;

    fn lpop_back<K>(&self, name: K) -> Result<Option<IVec>, Self::Error>
    where
        IVec: From<K>,
        K: AsRef<[u8]>;

    fn lget<K>(&self, name: K, ix: u64) -> Result<Option<IVec>, Self::Error>
    where
        IVec: From<K>,
        K: AsRef<[u8]>;

    fn lset<K, V>(&self, name: K, ix: u64, val: V) -> Result<Option<IVec>, Self::Error>
    where
        IVec: From<K> + From<V>,
        K: AsRef<[u8]>;
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

    store.fetch_update::<&[u8], IVec, _>(key.as_ref(), |iv| {
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
    fn list_create<K>(&self, name: K) -> Result<Meta, Self::Error>
    where
        IVec: From<K>,
        K: AsRef<[u8]>,
    {
        update_list_meta(self, name.as_ref(), |om| Ok(Some(om.unwrap_or_default())))
            .transpose()
            .unwrap()
    }

    fn lpush_front<K, V>(&self, name: K, val: V) -> Result<(), Self::Error>
    where
        IVec: From<K> + From<V>,
        K: AsRef<[u8]>,
    {
        // dummy value - will overwrite before use
        let mut ix = 0;

        update_list_meta(self, name.as_ref(), |om| {
            let mut meta = om.unwrap_or_default();
            ix = meta.push_front();
            Ok(Some(meta))
        })?
        .unwrap();

        self.insert(Key::List(name.as_ref(), ix).encode(), val)?;

        Ok(())
    }

    fn lpush_back<K, V>(&self, name: K, val: V) -> Result<(), Self::Error>
    where
        IVec: From<K> + From<V>,
        K: AsRef<[u8]>,
    {
        // dummy value - will overwrite before use
        let mut ix = 0;

        update_list_meta(self, name.as_ref(), |om| {
            let mut meta = om.unwrap_or_default();
            ix = meta.push_back();
            Ok(Some(meta))
        })?
        .unwrap();

        self.insert(Key::List(name.as_ref(), ix).encode(), val)?;

        Ok(())
    }

    fn lpop_front<K>(&self, name: K) -> Result<Option<IVec>, Self::Error>
    where
        IVec: From<K>,
        K: AsRef<[u8]>,
    {
        let mut ix: Option<ListIndex> = None;

        update_list_meta(self, name.as_ref(), |om| {
            let mut meta = om.unwrap_or_default();
            ix = meta.pop_front();
            Ok(Some(meta))
        })?
        .unwrap();

        Ok(if let Some(ix) = ix {
            let key = Key::List(name.as_ref(), ix);
            let res = self.remove::<&[u8]>(key.encode().as_slice())?;

            if res.is_none() {
                return Err(MissingVal(name.as_ref().to_vec(), ix).into());
            }

            res
        } else {
            None
        })
    }

    fn lpop_back<K>(&self, name: K) -> Result<Option<IVec>, Self::Error>
    where
        IVec: From<K>,
        K: AsRef<[u8]>,
    {
        let mut ix: Option<ListIndex> = None;

        update_list_meta(self, name.as_ref(), |om| {
            let mut meta = om.unwrap_or_default();
            ix = meta.pop_back();
            Ok(Some(meta))
        })?
        .unwrap();

        Ok(if let Some(ix) = ix {
            let key = Key::List(name.as_ref(), ix);
            let res = self.remove::<&[u8]>(key.encode().as_slice())?;

            if res.is_none() {
                return Err(MissingVal(name.as_ref().to_vec(), ix).into());
            }

            res
        } else {
            None
        })
    }

    fn lget<K>(&self, name: K, ix: u64) -> Result<Option<IVec>, Self::Error>
    where
        IVec: From<K>,
        K: AsRef<[u8]>,
    {
        let key = Key::ListMeta(name.as_ref()).encode();

        if let Some(meta) = self
            .get::<&[u8]>(&key)?
            .map(|v| Meta::decode(&v).ok_or_else(|| InvalidMeta(name.as_ref().to_vec())))
            .transpose()?
        {
            if let Some(ix) = meta.mk_key(ix) {
                let key = Key::List(name.as_ref(), ix).encode();
                return self.get::<&[u8]>(&key);
            }
        }

        Ok(None)
    }

    fn lset<K, V>(&self, name: K, ix: u64, val: V) -> Result<Option<IVec>, Self::Error>
    where
        IVec: From<K> + From<V>,
        K: AsRef<[u8]>,
    {
        let key = Key::ListMeta(name.as_ref()).encode();

        if let Some(meta) = self
            .get::<&[u8]>(&key)?
            .map(|v| Meta::decode(&v).ok_or_else(|| InvalidMeta(name.as_ref().to_vec())))
            .transpose()?
        {
            if let Some(ix) = meta.mk_key(ix) {
                let key = Key::List(name.as_ref(), ix).encode();
                return self.insert::<&[u8], _>(&key, val);
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
