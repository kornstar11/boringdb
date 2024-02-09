// Compressors for various datatypes
mod time;
use crate::error::*;

trait Compressor<I: Sized, O> {
    fn compress(&mut self, i: I) -> Result<()>;
    fn finish(self) -> Result<O>;
}

//trait Decompressor<O> {
//    fn decompress(&mut self) -> Option<O>;
//}