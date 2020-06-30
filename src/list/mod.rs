use super::*;
use sled::IVec;
use thiserror::*;

mod meta;
pub use self::meta::*;

impl Conn {
    pub fn list_get_meta(&self, name: &[u8]) -> Result<Meta, Error> {
        let key = keys::list_meta(name);

        if let Some(bs) = self.items.get(&key)? {
            Meta::decode(&bs).ok_or_else(|| InvalidMeta(name.to_vec()).into())
        } else {
            Ok(Meta::default())
        }
    }

    pub fn list_len(&self, name: &[u8]) -> Result<u64, Error> {
        Ok(self.list_get_meta(name)?.len())
    }

    pub fn list_get(&self, name: &[u8], ix: i64) -> Result<Option<IVec>, Error> {
        let meta_key = IVec::from(keys::list_meta(name));

        let mutex = self.locks.lock(&meta_key);
        let guard = mutex.read();

        let meta = self.list_get_meta(name)?;

        if let Some(ix) = meta.mk_key(ix) {
            Ok(self.items.get(&keys::list(name, ix))?)
        } else {
            Ok(None)
        }
    }

    pub fn list_push_front(&self, name: &[u8], val: IVec) -> Result<(), Error> {
        let meta_key = IVec::from(keys::list_meta(name));

        let mutex = self.locks.lock(&meta_key);
        let guard = mutex.write();

        let mut meta = self.list_get_meta(name)?;
        let ix = meta.push_front();
        let item_key = IVec::from(keys::list(name, ix));

        let mut batch = sled::Batch::default();
        batch.insert(&item_key, val);
        batch.insert(&meta_key, &meta.encode());
        self.items.apply_batch(batch)?;

        Ok(())
    }

    pub fn list_push_back(&self, name: &[u8], val: IVec) -> Result<(), Error> {
        let meta_key = IVec::from(keys::list_meta(name));

        let mutex = self.locks.lock(&meta_key);
        let guard = mutex.write();

        let mut meta = self.list_get_meta(name)?;
        let ix = meta.push_back();
        let item_key = IVec::from(keys::list(name, ix));

        let mut batch = sled::Batch::default();
        batch.insert(&item_key, val);
        batch.insert(&meta_key, &meta.encode());
        self.items.apply_batch(batch)?;

        Ok(())
    }

    pub fn list_pop_front(&self, name: &[u8]) -> Result<Option<IVec>, Error> {
        let meta_key = IVec::from(keys::list_meta(name));

        let mutex = self.locks.lock(&meta_key);
        let guard = mutex.write();

        let mut meta = self.list_get_meta(name)?;
        if let Some(ix) = meta.pop_front() {
            let item_key = keys::list(name, ix);
            let old = self.items.get(&item_key)?;

            let mut batch = sled::Batch::default();
            batch.insert(&meta_key, &meta.encode());
            batch.remove(item_key);

            self.items.apply_batch(batch)?;

            Ok(old)
        } else {
            Ok(None)
        }
    }

    pub fn list_pop_back(&self, name: &[u8]) -> Result<Option<IVec>, Error> {
        let meta_key = IVec::from(keys::list_meta(name));

        let mutex = self.locks.lock(&meta_key);
        let guard = mutex.write();

        let mut meta = self.list_get_meta(name)?;
        if let Some(ix) = meta.pop_back() {
            let item_key = keys::list(name, ix);
            let old = self.items.get(&item_key)?;

            let mut batch = sled::Batch::default();
            batch.insert(&meta_key, &meta.encode());
            batch.remove(item_key);

            self.items.apply_batch(batch)?;

            Ok(old)
        } else {
            Ok(None)
        }
    }

    pub fn list_set(&self, name: &[u8], ix: i64, val: IVec) -> Result<Option<IVec>, Error> {
        let meta_key = IVec::from(keys::list_meta(name));

        let mutex = self.locks.lock(&meta_key);
        let guard = mutex.read();

        let meta = self.list_get_meta(name)?;

        if let Some(ix) = meta.mk_key(ix) {
            Ok(self
                .items
                .fetch_and_update(keys::list(name, ix), move |_| Some(val.clone()))?)
        } else {
            Ok(None)
        }
    }
}

// impl<S> ListRangeStore for S
// where
//     S: RangeStore,
//     S::Error: From<Error>,
// {
//     type ListRangeIter = Box<dyn DoubleEndedIterator<Item = Result<IVec, Self::Error>> + 'static>;

//     fn list_range<R: RangeBounds<u64>>(
//         &self,
//         name: &[u8],
//         range: R,
//     ) -> Result<Option<Self::ListRangeIter>, Self::Error> {
//         use std::ops::Bound;

//         let meta = self.list_get_meta(name)?;

//         let start_key;
//         let start = match range.start_bound() {
//             Bound::Included(u) => {
//                 if let Some(k) = meta.mk_key(*u) {
//                     start_key = keys::list(name, k);
//                     Bound::Included(start_key.as_slice())
//                 } else {
//                     return Ok(None);
//                 }
//             }
//             Bound::Excluded(u) => {
//                 if let Some(k) = meta.mk_key(*u) {
//                     start_key = keys::list(name, k);
//                     Bound::Excluded(start_key.as_slice())
//                 } else {
//                     return Ok(None);
//                 }
//             }
//             Bound::Unbounded => {
//                 if let Some(k) = meta.mk_key(0) {
//                     start_key = keys::list(name, k);
//                     Bound::Included(start_key.as_slice())
//                 } else {
//                     return Ok(None);
//                 }
//             }
//         };

//         let end_key;
//         let end = match range.end_bound() {
//             Bound::Included(u) => {
//                 if let Some(k) = meta.mk_key(*u) {
//                     end_key = keys::list(name, k);
//                     Bound::Included(end_key.as_slice())
//                 } else {
//                     return Ok(None);
//                 }
//             }
//             Bound::Excluded(u) => {
//                 if let Some(k) = meta.mk_key(*u) {
//                     end_key = keys::list(name, k);
//                     Bound::Excluded(end_key.as_slice())
//                 } else {
//                     return Ok(None);
//                 }
//             }
//             Bound::Unbounded => {
//                 if let Some(k) = meta.mk_key(meta.len() - 1) {
//                     end_key = keys::list(name, k);
//                     Bound::Included(end_key.as_slice())
//                 } else {
//                     return Ok(None);
//                 }
//             }
//         };

//         let iter = self.range((start, end)).map(|res| res.map(|t| t.1));
//         let biter = Box::new(iter);

//         Ok(Some(biter))
//     }
// }

#[derive(Error, Debug)]
pub enum ListError {
    #[error("invalid list metadata, key was: {0:#?}")]
    InvalidMeta(Vec<u8>),
    #[error("missing value in list {0:#?} at index {1}")]
    MissingVal(Vec<u8>, ListIndex),
}

use ListError::*;
