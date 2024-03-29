use crate::Result;
pub trait KvEngine {
    /**
     * Set the value of a string key to a string.
     * Return an error if the value is not written successfully.
     */
    fn set(&mut self, key: String, value: String) -> Result<()>;

    /**  
     * Set the value of a string key to a string.
     * Return an error if the value is not written successfully.
     */

    fn get(&mut self, key: String) -> Result<Option<String>>;

    /**
     * Remove a given string key.
     * Return an error if the key does not exit or value is not read successfully.
     */
    fn remove(&mut self, key: String) -> Result<()>;
}
mod kvs;
mod sled;
pub use self::kvs::KvStore;
pub use self::sled::SledKvsEngine;
