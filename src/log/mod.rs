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

use std::sync::atomic::Ordering as AOrd;

macro_rules! load {
    // () => {

    // };
    ($($to_load:tt)*) => {
        {
            ($($to_load)*).load(AOrd::SeqCst)
        }
    };
}

#[derive(Clone, Default)]
struct Blocker(Arc<(Mutex<()>, Condvar )>);


#[derive(Default)]
struct LogState {
    writer_reserve_pos: AtomicUsize, // max position of the writer, reader can not pass this.
    writer_end_position: AtomicUsize, // max position of the writer, reader can not pass this.
    max_reader_pos: AtomicUsize, // max position of the writer, reader can not pass this.
    min_reader_pos: AtomicUsize, // max position of the writer, reader can not pass this.
    writer_is_behind_reader: AtomicBool, // writer can circle back if the reader is far enough along
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
    fn reserve_write(&self, size: usize) -> Option<usize> {
        // reserve space before hand

        let current_writer_end_position = load!(self.writer_end_position);

        let current_writer_position = self.writer_reserve_pos.fetch_add(size, AOrd::SeqCst);
        let next_writer_position = current_writer_position + size;

        let writer_is_behind = load!(self.writer_is_behind_reader);
        let min_reader_pos = load!(self.min_reader_pos);

        if next_writer_position >= self.max_size {
            //wrap around
            self.writer_is_behind_reader.store(true, AOrd::SeqCst);
            self.writer_reserve_pos.fetch_sub(size, AOrd::SeqCst); //Rollback our previous reservation
            self.writer_end_position.fetch_max(current_writer_end_position, AOrd::SeqCst);
            return self.reserve_write(size)
        }

        if writer_is_behind && next_writer_position >= min_reader_pos {
            //need to block here
            
        }

        if load!(self.writer_is_behind_reader) {

            // writer has wrapped behind reader
        } else {


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