use std::{sync::mpsc::SendError, time};

use redis_protocol::types::RedisProtocolError;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("IO Error {0}")]
    IO(#[from] std::io::Error),
    #[error("Time Error {0}")]
    TimeError(time::SystemTimeError),
    #[error("Channel died")]
    SendError,
    #[error("Redis {0}")]
    Redis(RedisProtocolError),
    #[error("Other {0}")]
    Other(String),
}
