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

impl<'a> Conn<'a> {
    pub fn list_create(&self, name: &[u8]) -> Res<Option<Meta>> {
        let res = self.list_get_meta(name)?;
        if res.is_none() {
            self.list_put_meta(name, &Meta::default())?;
        }

        Ok(res)
    }

    pub fn list_get_meta(&self, name: &[u8]) -> Res<Option<Meta>> {
        let key = Key::ListMeta(name).encode();
        self.0
            .get(&key)?
            .map(|v| Meta::decode(&v).ok_or_else(|| InvalidMeta(name.to_vec())))
            .transpose()
            .map_err(abort_err)
    }

    pub fn list_put_meta(&self, name: &[u8], meta: &Meta) -> Res<()> {
        let key = Key::ListMeta(name).encode();
        self.0.insert(key, &meta.encode())?;
        Ok(())
    }

    pub fn lpush_front<V>(&self, name: &[u8], val: V) -> Res<()>
    where
        IVec: From<V>,
    {
        let mut meta = self.list_get_meta(name)?.unwrap_or_default();
        let ix = meta.push_front();
        self.list_put_meta(name, &meta)?;

        let key = Key::List(name, ix).encode();
        self.0.insert::<Vec<u8>, V>(key, val)?;

        Ok(())
    }

    pub fn lpush_back<V>(&self, name: &[u8], val: V) -> Res<()>
    where
        IVec: From<V>,
    {
        let mut meta = self.list_get_meta(name)?.unwrap_or_default();
        let ix = meta.push_back();
        self.list_put_meta(name, &meta)?;

        let key = Key::List(name, ix).encode();
        self.0.insert::<Vec<u8>, V>(key, val)?;

        Ok(())
    }

    pub fn lpop_front(&self, name: &[u8]) -> Res<Option<IVec>> {
        if let Some(mut meta) = self.list_get_meta(name)? {
            if let Some(ix) = meta.pop_front() {
                self.list_put_meta(name, &meta)?;

                let val = self
                    .0
                    .remove(Key::List(name, ix).encode())?
                    .ok_or_else(|| MissingVal(name.to_vec(), ix))
                    .map_err(abort_err)?;

                return Ok(Some(val));
            }
        }
        Ok(None)
    }

    pub fn lpop_back(&self, name: &[u8]) -> Res<Option<IVec>> {
        if let Some(mut meta) = self.list_get_meta(name)? {
            if let Some(ix) = meta.pop_back() {
                self.list_put_meta(name, &meta)?;

                let val = self
                    .0
                    .remove(Key::List(name, ix).encode())?
                    .ok_or_else(|| MissingVal(name.to_vec(), ix))
                    .map_err(abort_err)?;

                return Ok(Some(val));
            }
        }
        Ok(None)
    }

    pub fn lget(&self, name: &[u8], ix: u64) -> Res<Option<IVec>> {
        if let Some(meta) = self.list_get_meta(name)? {
            if let Some(key) = meta.mk_key(ix) {
                return Ok(Some(
                    self.0
                        .get(Key::List(name, key).encode())?
                        .ok_or_else(|| MissingVal(name.to_vec(), key))
                        .map_err(abort_err)?,
                ));
            }
        }
        Ok(None)
    }

    pub fn lset<V>(&self, name: &[u8], ix: u64, v: V) -> Res<Option<IVec>>
    where
        IVec: From<V>,
    {
        if let Some(meta) = self.list_get_meta(name)? {
            if let Some(key) = meta.mk_key(ix) {
                return Ok(Some(
                    self.0
                        .insert::<Vec<_>, V>(Key::List(name, key).encode(), v)?
                        .ok_or_else(|| MissingVal(name.to_vec(), key))
                        .map_err(abort_err)?,
                ));
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

type Res<T> = sled::ConflictableTransactionResult<T, Error>;

fn abort_err<E>(e: E) -> sled::ConflictableTransactionError<E> {
    sled::ConflictableTransactionError::Abort(e)
}

use Error::*;
