use std::fmt::DebugList;

use bytes::Bytes;

use super::*;

pub struct TimeCompressor {
    last_value: i64,
    last_delta: i64,
    bits_writer: BitWriter,
}

impl Compressor<u64, Bytes> for TimeCompressor {
    fn compress(&mut self, i: u64) -> Result<()> {
        let delta = (i as i64) - self.last_value;
        self.last_value = i as i64;
        let d_of_d = delta - self.last_delta;
        self.last_delta = delta;
        todo!()
    }

    fn finish(self) -> Result<Bytes> {
        Ok(self.bits_writer.finish().freeze())
    }
}
