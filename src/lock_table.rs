use dashmap::DashMap;
use parking_lot::RwLock;

use std::{
    ops::{Deref, DerefMut},
    sync::{atomic::*, Arc},
};

pub struct Table {
    inner: DashMap<sled::IVec, LockEntryInner>,
}

struct LockEntryInner {
    refcount: AtomicUsize,
    lock: RwLock<()>,
}

impl Default for LockEntryInner {
    fn default() -> Self {
        Self {
            refcount: AtomicUsize::new(0),
            lock: RwLock::new(()),
        }
    }
}

type TableRef<'a> = dashmap::mapref::one::Ref<'a, sled::IVec, LockEntryInner>;

pub struct LockEntry<'a> {
    inner: Option<TableRef<'a>>,
    key: &'a sled::IVec,
    table: &'a Table,
}

impl<'a> Deref for LockEntry<'a> {
    type Target = RwLock<()>;

    fn deref(&self) -> &RwLock<()> {
        &self.inner.as_ref().unwrap().lock
    }
}

impl<'a> Drop for LockEntry<'a> {
    fn drop(&mut self) {
        let item = self.inner.take().unwrap();
        if item.refcount.fetch_sub(1, Ordering::AcqRel) == 1 {
            drop(item);
            self.table.inner.remove_if(self.key, |_, item| {
                item.refcount.load(Ordering::Acquire) == 0
            });
        }
    }
}

impl Table {
    pub fn lock<'a>(self: &'a Self, key: &'a sled::IVec) -> LockEntry<'a> {
        let inner = {
            // first we try a shared get, to not contend the map
            if let Some(r) = self.inner.get(key) {
                r.refcount.fetch_add(1, Ordering::AcqRel);
                r
            } else {
                // if that fails, we'll get the entry with an exclusive lock
                let mut entry = self.inner.entry(key.clone()).or_default();
                *entry.refcount.get_mut() += 1;
                entry.downgrade()
            }
        };

        LockEntry {
            inner: Some(inner),
            key,
            table: self,
        }
    }
}
