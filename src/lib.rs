use bytes::*;

mod escaping;
pub use escaping::*;

mod keys;
pub use keys::*;

pub mod lists;

mod error;
pub use error::*;

// TODO: make this a normal connection, have alternate "normal" and "transactional" methods
// for now everything is transactional
pub struct Conn<'a>(pub &'a sled::TransactionalTree);
