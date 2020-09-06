use super::*;

impl Conn {
    pub fn blob_get(&self, name: &[u8]) -> Result<Option<IVec>, Error> {
        self.get_record(&keys::blob(name))?
            .map(|rec| {
                if rec.tag() != Tag::Blob {
                    Err(Error::BadType(Tag::Blob, rec.tag()))?
                }

                Ok(rec.data())
            })
            .transpose()
    }

    pub fn blob_insert(&self, name: &[u8], val: IVec) -> Result<Option<Record>, Error> {
        let key = keys::blob(name).into();
        let lock = self.locks.lock(&key);
        let _guard = lock.write();

        let old_record = if cfg!(feature = "safe") {
            let mut batch = sled::Batch::default();
            let old_record = self.raw_remove_item(&key, &mut batch)?;

            let ttl_batch = batch.clone();
            batch.insert(&key, Record::FromData(Tag::Blob, val).into_raw());

            self.items.apply_batch(batch)?;
            self.ttl.apply_batch(ttl_batch)?;
            old_record
        } else {
            let old_rec = self.get_record(&key)?;

            match old_rec.as_ref().map(Record::tag) {
                None | Some(Tag::Blob) => {}
                Some(Tag::List) | Some(Tag::Table) => {
                    for entry in self.items.scan_prefix(&key) {
                        let (key, _) = entry?;
                        self.items.remove(&key)?;
                        self.ttl.remove(&key)?;
                    }
                }
            }

            old_rec
        };

        Ok(old_record)
    }
}
