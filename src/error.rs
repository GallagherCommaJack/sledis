use thiserror::*;

#[derive(Error, Debug)]
pub enum Error<E: std::error::Error> {
    #[error(transparent)]
    List(#[from] crate::lists::Error),
    #[error("store error: {0}")]
    Store(E),
}
