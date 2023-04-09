use crate::KvEngine;
use crate::Result;
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
pub struct KvServer<E: KvEngine> {
    engine: E,
}
impl<E: KvEngine> KvServer<E> {
    pub fn new(engine: E) -> Self {
        KvServer { engine }
    }
    pub fn run<A: ToSocketAddrs>(mut self, addr: A) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    if let Err(e) = self.serve(stream){
                        error!("Error on serving client: {}", e);
                    }
                }
                Err(e) => error!("connection failed: {}", e),
            }
        }
        Ok(())
    }
    fn serve(&mut self, tcp: TcpStream) -> Result<()> {
        todo!();
    }
}

