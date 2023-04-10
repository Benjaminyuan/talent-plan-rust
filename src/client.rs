use crate::Result;

pub trait KvClient {
    fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self>;
    fn get(&mut self, key: String) -> Result<String>;
    fn set(&mut self, key: String, value: String) -> Result<()>;
    fn remove(&mut self, key: String) -> ResultK<()>;
}

struct kvClient {}

impl kvClient {
    fn new() -> Result<KvClient> {
        kvClient {}
    }
}
impl KvClient for kvClient {

}
