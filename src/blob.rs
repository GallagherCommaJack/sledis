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

        let mut batch = sled::Batch::default();
        let old_record = self.raw_remove_item(&key, &mut batch)?;

        let ttl_batch = batch.clone();
        batch.insert(&key, Record::FromData(Tag::Blob, val).into_raw());

        self.items.apply_batch(batch)?;
        self.ttl.apply_batch(ttl_batch)?;

        Ok(old_record)
    }

    pub fn blob_remove(&self, name: &[u8]) -> Result<Option<IVec>, Error> {
        let key = keys::blob(name);

        let old_ivec = self.items.fetch_and_update(key, |val| {
            let iv = IVec::from(val?);
            if Record::decode(iv.clone())
                .iter()
                .any(|rec| rec.tag() == Tag::Blob)
            {
                None
            } else {
                Some(iv)
            }
        })?;

        let old_record = old_ivec.map(Record::decode).transpose()?;

        let old_data = old_record
            .iter()
            .filter(|rec| rec.tag() == Tag::Blob)
            .map(Record::data)
            .next();

        Ok(old_data)
    }
}
