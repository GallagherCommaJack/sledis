use sledis::*;

pub struct TempDb {
    conn: sledis::Conn,
    _dir: tempfile::TempDir,
}

impl TempDb {
    pub fn new() -> Self {
        let _dir = tempfile::tempdir().expect("failed to create temporary dir");
        let conn = sled::Config::default()
            .path(_dir.path())
            .open_sledis()
            .expect("failed to create temp db");
        TempDb { conn, _dir }
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
