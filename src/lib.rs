use sled::IVec;
use std::path::Path;

pub mod escaping;
use escaping::*;

pub mod blob;
pub mod keys;
pub mod list;
pub mod table;

mod error;
mod lock_table;
pub mod record;

pub use error::*;
pub use keys::*;
use record::*;

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

    pub fn flush(&self) -> Result<(), sled::Error> {
        self.items.flush()?;
        self.ttl.flush()?;
        self.db.flush()?;
        Ok(())
    }

    pub(crate) fn get_record(&self, key: &[u8]) -> Result<Option<Record>, Error> {
        let res = self.items.get(key)?;
        Ok(res.map(Record::decode).transpose()?)
    }

    pub(crate) fn raw_remove_item(
        &self,
        raw_key: &[u8],
        batch: &mut sled::Batch,
    ) -> Result<Option<Record>, Error> {
        let old_rec = self.get_record(raw_key)?;

        match old_rec.as_ref().map(Record::tag) {
            None | Some(Tag::Blob) => {}
            Some(Tag::List) | Some(Tag::Table) => {
                for entry in self.items.scan_prefix(raw_key) {
                    let (key, _) = entry?;
                    batch.remove(key)
                }
            }
        }

        Ok(old_rec)
    }

    pub fn remove_item(&self, key: &[u8]) -> Result<Option<Record>, Error> {
        let key = keys::bare(key).into();
        let lock = self.locks.lock(&key);
        let _guard = lock.write();
        let mut batch = sled::Batch::default();
        let old_rec = self.raw_remove_item(&key, &mut batch)?;
        self.items.apply_batch(batch.clone())?;
        // note: this isn't atomic bc sled transactions aren't very concurrent
        // shouldn't be /too/ bad though, since the ttl tree will never be that large,
        // so potentially leaking here isn't too bad
        self.ttl.apply_batch(batch)?;

        Ok(old_rec)
    }
}
