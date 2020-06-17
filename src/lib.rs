#![allow(warnings)]

use sled::IVec;
use std::{
    ops::RangeBounds,
    path::{Path, PathBuf},
};

pub mod escaping;
use escaping::*;

pub mod keys;
pub use keys::*;

pub mod list;

pub mod table;

mod segment;
pub use segment::*;

mod error;
pub use error::*;

mod lock_table;

pub struct Conn {
    db: sled::Db,
    items: sled::Tree,
    ttl: sled::Tree,
    locks: lock_table::Table,
}

impl Conn {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, sled::Error> {
        let db = sled::open(path)?;
        let items = db.open_tree("items")?;
        let ttl = db.open_tree("ttl")?;
        let locks = lock_table::Table::default();
        Ok(Conn {
            db,
            items,
            ttl,
            locks,
        })
    }

    pub fn clear(&self) -> Result<(), sled::Error> {
        self.items.clear()?;
        self.ttl.clear()?;
        Ok(())
    }

    pub fn blob_get(&self, name: &[u8]) -> Result<Option<IVec>, sled::Error> {
        self.items.get(&keys::blob(name))
    }

    pub fn blob_set(&self, name: &[u8], val: IVec) -> Result<Option<IVec>, sled::Error> {
        self.items.insert(&keys::blob(name), val)
    }
}
