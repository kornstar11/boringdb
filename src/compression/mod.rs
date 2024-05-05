// Compressors for various datatypes
mod bits;
mod time;
use crate::error::*;

pub use bits::{BitReader, BitWriter};

trait Compressor<I: Sized, O> {
    fn compress(&mut self, i: I) -> Result<()>;
    fn finish(self) -> Result<O>;
}

trait Decompressor<O> {
    fn decompress(&mut self) -> Result<O>;
}
