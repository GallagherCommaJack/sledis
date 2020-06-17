pub struct TempDb {
    conn: sledis::Conn,
    _dir: tempfile::TempDir,
}

impl TempDb {
    pub fn new() -> Result<Self, sled::Error> {
        let _dir = tempfile::tempdir()?;
        let conn = sledis::Conn::open(_dir.path())?;
        Ok(TempDb { _dir, conn })
    }
}

impl std::ops::Deref for TempDb {
    type Target = sledis::Conn;

    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

impl std::ops::DerefMut for TempDb {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.conn
    }
}
