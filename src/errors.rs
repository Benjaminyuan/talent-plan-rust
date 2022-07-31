use failure::Fail;
use std::io;
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
