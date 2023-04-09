use crate::common::{GetResponse, RemoveResponse, Request, SetResponse};
use crate::engines::KvEngine;

use crate::Result;
use log::{debug, error};
use serde_json::Deserializer;
use std::io::{self, BufReader, BufWriter};
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
                    if let Err(e) = self.serve(stream) {
                        error!("Error on serving client: {}", e);
                    }
                }
                Err(e) => error!("connection failed: {}", e),
            }
        }
        Ok(())
    }
    fn serve(&mut self, tcp: TcpStream) -> Result<()> {
        let peer = tcp.peer_addr()?;
        let reader = BufReader::new(&tcp);
        let mut writer = BufWriter::new(&tcp);
        let req_reader = Deserializer::from_reader(reader).into_iter::<Request>();

        for req in req_reader {
            let req = req?;
            debug!("Receive request from {} : {:?}", peer, req);
            match req {
                Request::Get { key } => send_resp(
                    &mut writer,
                    match self.engine.get(key) {
                        Ok(value) => GetResponse::Ok(value),
                        Err(e) => GetResponse::Err(format!("{}", e)),
                    },
                ),
                Request::Set { key, value } => send_resp(
                    &mut writer,
                    match self.engine.set(key, value) {
                        Ok(value) => SetResponse::Ok(value),
                        Err(e) => SetResponse::Err(format!("{}", e)),
                    },
                ),
                Request::Remove { key } => send_resp(
                    &mut writer,
                    match self.engine.remove(key) {
                        Ok(value) => RemoveResponse::Ok(value),
                        Err(e) => RemoveResponse::Err(format!("{}", e)),
                    },
                ),
            }?;
        }
        Ok(())
    }
}

fn send_resp<W: io::Write, T: serde::Serialize>(mut writer: W, resp: T) -> Result<()> {
    serde_json::to_writer(&mut writer, &resp)?;
    writer.flush()?;
    Ok(())
}
