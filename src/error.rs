use crate::Tag;
use thiserror::*;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    List(#[from] crate::list::ListError),
    #[error(transparent)]
    Table(#[from] crate::table::TableError),
    #[error(transparent)]
    Store(#[from] sled::Error),
    #[error(transparent)]
    Record(#[from] crate::record::RecordError),
    #[error("bad type: expected {0:?}, found {1:?}")]
    BadType(Tag, Tag),
}
