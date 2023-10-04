use crate::error::*;
use bytes::{Buf, BufMut, BytesMut};
use parking_lot::{Mutex, Condvar};
use std::{
    cmp::Ordering,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, atomic::{AtomicUsize, AtomicBool}},
};

use std::sync::atomic::{Ordering as AOrd};

//pub struct Recv

macro_rules! load {
    () => {

    };
    ($($to_load:tt)*) => {
        {
            ($($to_load)*).load(AOrd::SeqCst)
        }
    };
}

#[derive(Clone, Default)]
struct Blocker(Arc<(Mutex<()>, Condvar )>);

#[derive(Clone, Default)]
struct LogState {
    writer_pos: Arc<AtomicUsize>, // max position of the writer, reader can not pass this.
    reader_pos: Arc<AtomicUsize>, // position
    writer_is_behind_reader: Arc<AtomicBool>, // writer can circle back if the reader is far enough along
    max_size: usize, // our size limit that writer can't pass
    blocker: Blocker,
}


impl LogState {
    fn new(max_size: usize) -> Self {
        Self { 
            max_size,
            ..Default::default()
        }
    }
    ///
    /// Returns the position to start writing at
    fn reserve_write(&self, size: usize) {
        if load!(self.writer_is_behind_reader) {
            // writer has wrapped behind reader
            //let new_writer_pos = self.writer_pos.load(A)
        }

    }
}

pub struct LogQueue {
    path: PathBuf,
    state: LogState
}

impl LogQueue {
    pub fn new(path: PathBuf, max_size: usize) -> Self {
        Self {
            path,
            state: LogState::default(),
        }
    }
}

pub struct LogSender {
    inner: LogQueue,
    state: LogState
}

impl LogSender {
    pub fn send(&self, data: &[u8]) -> Result<()> {
        Ok(())

    }
    
}