use std::{time, sync::mpsc::SendError};

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
    #[error("Other {0}")]
    Other(String),
}
