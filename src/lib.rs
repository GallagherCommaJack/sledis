use sled::IVec;
use std::path::Path;

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

pub trait ConfigExt {
    fn open_sledis(&self) -> Result<Conn, sled::Error>;
}

impl ConfigExt for sled::Config {
    fn open_sledis(&self) -> Result<Conn, sled::Error> {
        Conn::with_config(self)
    }
}

pub struct Conn {
    pub db: sled::Db,
    pub items: sled::Tree,
    pub ttl: sled::Tree,
    pub locks: lock_table::Table,
}

impl Conn {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, sled::Error> {
        sled::Config::default().path(path).open_sledis()
    }

    pub fn with_config(c: &sled::Config) -> Result<Self, sled::Error> {
        let db = c.open()?;
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

    pub fn blob_remove(&self, name: &[u8]) -> Result<Option<IVec>, sled::Error> {
        self.items.get(&keys::blob(name))
    }

    pub fn flush(&self) -> Result<(), sled::Error> {
        self.items.flush()?;
        self.ttl.flush()?;
        self.db.flush()?;
        Ok(())
    }
}
