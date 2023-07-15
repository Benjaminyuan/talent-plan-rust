use failure::Fail;
use std::{io, string::FromUtf8Error};
#[derive(Fail, Debug)]
pub enum KvsErr {
    #[fail(display = "Key not found")]
    KeyNotFound,
    #[fail(display = "Unexpected command type")]
    UnexpectedCommandType,
    /// IO error.
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),

    #[fail(display = "{}", _0)]
    Serde(#[cause] serde_json::Error),
    /// Key or value is invalid UTF-8 sequence
    #[fail(display = "UTF-8 error: {}", _0)]
    Utf8(#[cause] FromUtf8Error),
    /// Sled error
    #[fail(display = "sled error: {}", _0)]
    Sled(#[cause] sled::Error),

    #[fail(display = "{}", _0)]
    StringErr(String),
}

pub type Result<T> = std::result::Result<T, KvsErr>;

impl From<std::io::Error> for KvsErr {
    fn from(err: std::io::Error) -> Self {
        KvsErr::Io(err)
    }
}
impl From<serde_json::Error> for KvsErr {
    fn from(err: serde_json::Error) -> Self {
        KvsErr::Serde(err)
    }
}
impl From<sled::Error> for KvsErr {
    fn from(err: sled::Error) -> Self {
        KvsErr::Sled(err)
    }
}
impl From<FromUtf8Error> for KvsErr {
    fn from(err: FromUtf8Error) -> Self {
        KvsErr::Utf8(err)
    }
}

impl From<String> for KvsErr {
    fn from(value: String) -> Self {
        KvsErr::StringErr(value)
    }
}
