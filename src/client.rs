use serde::Deserialize;
use serde_json::de::IoRead;
use serde_json::Deserializer;

use crate::common::GetResponse;
use crate::common::Request;
use crate::common::SetResponse;
use crate::Result;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::net::TcpStream;
use std::net::ToSocketAddrs;
pub struct KvClient {
    // reader: Deserializer<IoRead<BufReader<TcpStream>>>,
    // writer: BufWriter<TcpStream>,
    reader: Deserializer<IoRead<BufReader<TcpStream>>>,
    writer: BufWriter<TcpStream>,
}

impl KvClient {
    pub fn connect<A: ToSocketAddrs>(_addr: A) -> Result<Self> {
        let tcp_reader = TcpStream::connect(_addr)?;
        let tcp_writer = tcp_reader.try_clone()?;

        Ok(KvClient {
            reader: Deserializer::from_reader(BufReader::new(tcp_reader)),
            writer: (BufWriter::new(tcp_writer)),
        })
    }

    pub fn get(&mut self, _key: String) -> Result<Option<String>> {
        serde_json::to_writer(&mut self.writer, &Request::Get { key: _key })?;
        self.writer.flush()?;
        let resp = GetResponse::deserialize(&mut self.reader)?;
        match resp {
            GetResponse::Ok(value) => Ok(value),
            GetResponse::Err(msg) => Err(crate::KvsErr::StringErr(msg)),
        }
    }

    pub fn set(&mut self, _key: String, _value: String) -> Result<()> {
        serde_json::to_writer(
            &mut self.writer,
            &Request::Set {
                key: _key,
                value: _value,
            },
        )?;
        self.writer.flush()?;
        match SetResponse::deserialize(&mut self.reader)? {
            SetResponse::Ok(()) => Ok(()),
            SetResponse::Err(msg) => Err(crate::KvsErr::StringErr(msg)),
        }
    }

    pub fn remove(&mut self, _key: String) -> Result<()> {
        serde_json::to_writer(
            &mut self.writer,
            &Request::Remove {
                key: _key
            },
        )?;
        self.writer.flush()?;
        match SetResponse::deserialize(&mut self.reader)? {
            SetResponse::Ok(()) => Ok(()),
            SetResponse::Err(msg) => Err(crate::KvsErr::StringErr(msg)),
        }
    }
}
