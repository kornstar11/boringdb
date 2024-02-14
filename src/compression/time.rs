use bytes::{BytesMut, BufMut, Buf, Bytes};

// Time compressor uses the "delta of delta" ideas from Facebooks Gorrila paper
// https://www.vldb.org/pvldb/vol8/p1816-teller.pdf
use super::Compressor;
use crate::error::*;

// Range masks (4.1.1)
// (value, bits used)
const MAX_BITS_USED: u32 = 4;
const R0: (u64, u32, u32) = (0b0 << u64::BITS -1, 1, 0);
const R1: (u64, u32, u32) = (0b10 << u64::BITS -2, 2, 7);
const R2: (u64, u32, u32) = (0b110 << u64::BITS -3, 3, 9);
const R3: (u64, u32, u32) = (0b1110 << u64::BITS -MAX_BITS_USED, MAX_BITS_USED, 12);
const R4: (u64, u32, u32) = (0b1111 << u64::BITS -MAX_BITS_USED, MAX_BITS_USED, 32);

const MASKS: [(u64, u32, u32); 5] = [R0, R1, R2, R3, R4];

const U64_BITS: usize = 63 as usize;

#[derive(Default)]
struct BitWriter {
    inner: BytesMut,
    scratch: u64,
    offset: usize,
}

impl BitWriter {
    pub fn on(&mut self) {
        self.write(1, 1)
    }

    pub fn off(&mut self) {
        self.write(0, 1)
    }

    pub fn write(&mut self, mut to_write: u64, mut bits_to_write: usize) {
        let new_offset = self.offset + bits_to_write;
        // mask off to_write
        //println!("to_write1 {to_write:b}");
        let mask = !(u64::MAX << bits_to_write);
        to_write = mask & to_write;
        if new_offset > U64_BITS as _ {
            let remaining = U64_BITS - self.offset; // 64 - 62 = 2
            let shift_right_pos = bits_to_write - remaining; //7 - 2 = 5
            let shifted = to_write.rotate_right(shift_right_pos as u32); // 1, 1, 1,  0, 0, 0,  0
                                                                       //                 1,  1
            
            self.scratch |= (shifted & (u64::MAX >> shift_right_pos));
            self.flush();

            to_write = shifted & (u64::MAX << remaining);
            self.offset += shift_right_pos;
            return;
        }
        self.scratch |= to_write << (U64_BITS - self.offset);
        self.offset += bits_to_write;
    }

    pub fn finish(mut self) -> BytesMut {
        self.flush();
        self.inner
    }

    fn flush(&mut self) {
        self.offset = 0;
        self.inner.put_u64(self.scratch);
        self.scratch = 0;
    }


}

struct BitReader {
    inner: Bytes,
    scratch: u64,
    offset: usize,
}

impl From<Bytes> for BitReader {
    fn from(mut value: Bytes) -> Self {
        let scratch = value.get_u64();
        Self {
            inner: value,
            scratch: scratch,
            offset: 0,
        }
    }
}

impl BitReader {
    pub fn read_bit(&mut self) -> bool {
        if self.read(1) == 1 {
            true
        } else {
            false
        }
    }
    pub fn read(&mut self, mut bits_to_read: usize) -> u64 {
        let mut acc = 0;
        let remaining = U64_BITS - self.offset;
        if bits_to_read > remaining {
            acc |= self.scratch & (u64::MAX >> self.offset);
            acc <<= bits_to_read - remaining;
            self.rotate_scratch();
            bits_to_read -= remaining;
        }
        let mask = !(u64::MAX >> bits_to_read);
        acc |= (self.scratch & mask) >> U64_BITS - bits_to_read + 1;
        self.scratch <<= bits_to_read;
        self.offset += 1;
        return acc;
    }

    fn rotate_scratch(&mut self) {
        self.offset = 0;
        self.scratch = self.inner.get_u64();
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn round_trip_single_bit() {
        let mut writer = BitWriter::default();
        writer.on();
        writer.off();
        writer.on();
        writer.on();

        let mut buf = writer.finish().freeze();
        let v = buf.clone().get_u64();
        println!("v: {v:b}");


        let mut reader = BitReader::from(buf);
        assert_eq!(reader.read_bit(), true);
        assert_eq!(reader.read_bit(), false);
        assert_eq!(reader.read_bit(), true);
    }
}

// pub struct TimeCompressor {
//     prev_value: i64,
//     prev_delta: i64,
//     scratch: u64,
//     bits_used: u32,
//     values_written: u64,
//     buf: BytesMut,
// }



// impl TimeCompressor {
//     pub fn new() -> Self {
//         let mut me = Self {
//             prev_value: 0,
//             prev_delta: 0,
//             scratch: 0,
//             bits_used: 0,
//             values_written: 0,
//             buf: BytesMut::new(),
//         };
//         // we write a u64 0 so that when we finish we can encode the amount of values used
//         me.buf.put_u64(0);
//         me
//     }
//     fn encode_value(&mut self, i: i64) {
//         //let leading = i.leading_zeros();
        
//         let (mask, mask_size, bits_to_write) = match i {
//             0 => R0,
//             i if -63 < i && i < 64 => R1,
//             i if -255 < i && i < 256 => R2,
//             i if -2047 < i && i < 2048 => R3,
//             _ => R4,
//         };
//         let mut value = i as u64;
//         let leading = value.leading_zeros();
//         let value_shifts = leading - mask_size;
//         //let value_bits_used = mask_size + (u64::BITS - leading);
//         value <<= value_shifts;
//         value |= mask;

//         //write what we have ignoring the overflow
//         self.scratch |= value >> self.bits_used;
//         self.bits_used += bits_to_write;
//         self.values_written += 1;

//         // rotate the "scratch"
//         if self.bits_used > u64::BITS {
//             self.buf.put_u64(self.scratch);
//             self.scratch = 0;
//             //split
//             let overflow = self.bits_used - u64::BITS;
//             self.scratch |= value << overflow;
//             self.bits_used = overflow;
//         }
//     }

// }

// impl Compressor<i64, Vec<u8>> for TimeCompressor {
//     fn compress(&mut self, i: i64) -> Result<()> {
//         let delta = self.prev_value - i;
//         let dod = self.prev_delta - delta;
//         self.prev_value = i;
//         self.prev_delta = delta;
//         self.encode_value(dod);
//         Ok(())
//     }

//     fn finish(mut self) -> Result<Vec<u8>> {
//         {
//             let (mut first, _) = self.buf.split_at_mut(8);
//             first.put_u64(self.values_written);
//         }
//         Ok(self.buf.freeze().into())
//     }
// }

// ///
// /// TimeDecompressor
// pub struct TimeDecompressor {
//     buf: BytesMut,
//     scratch: u64,
//     values_to_read: u64,
//     bit_offset: i32,
// }

// impl From<BytesMut> for TimeDecompressor {
//     fn from(mut buf: BytesMut) -> Self {
//         let values_written = buf.get_u64();
//         let scratch = buf.get_u64();
//         Self {
//             buf,
//             scratch,
//             values_to_read: values_written,
//             bit_offset: u64::BITS as _,
//         }
//     }
// }

// impl Iterator for TimeDecompressor {
//     type Item = i64;

//     fn next(&mut self) -> Option<Self::Item> {
//         self.values_to_read -= 1;
//         if self.values_to_read == 0 {
//             return None;
//         }
//         for (mask, mask_size, bits_to_read) in MASKS {
//             let new_bit_pos = self.bit_offset - ((bits_to_read + mask_size) as i32);
//             if new_bit_pos < 0 {
//                 // get remainder from scratch
//                 let shift_over = u64::BITS - self.bit_offset as u32;
//                 let mut value = self.scratch << shift_over;
//                 self.scratch = self.buf.get_u64();

//                 let new_shift_over = new_bit_pos.abs() as u32;
//                 value |= (self.scratch >> (u64::BITS - new_shift_over)) << u64::BITS - (shift_over + new_shift_over);
//                 //discard the mask
//             }
//             let read_mask = self.scratch >> u64::BITS - mask_size;
//             if read_mask != mask {
//                 continue;
//             }
//         }
//         todo!()
//     }
// }

