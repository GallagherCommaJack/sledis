use thiserror::*;

#[derive(Error, Debug)]
pub enum Error<E: std::error::Error> {
    #[error(transparent)]
    List(#[from] crate::list::Error),
    #[error(transparent)]
    Table(#[from] crate::table::Error),
    #[error("store error: {0}")]
    Store(E),
}
