mod errors;
pub mod engines;
pub use errors::{KvsErr, Result};
pub use crate::engines::KvStore;
