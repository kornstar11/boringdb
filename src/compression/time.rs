use bytes::{BytesMut, BufMut};

// Time compressor uses the "delta of delta" ideas from Facebooks Gorrila paper
// https://www.vldb.org/pvldb/vol8/p1816-teller.pdf
use super::Compressor;
use crate::error::*;

// Range masks (4.1.1)
// (value, bits used)
const R0: (u64, u32) = (0b0 << u64::BITS -1, 1);
const R1: (u64, u32) = (0b10 << u64::BITS -2, 2);
const R2: (u64, u32) = (0b110 << u64::BITS -3, 3);
const R3: (u64, u32) = (0b1110 << u64::BITS -4, 4);

pub struct TimeCompressor {
    prev_value: i64,
    prev_delta: i64,
    scratch: u64,
    bits_used: u32,
    values_written: u64,
    buf: BytesMut,
}



impl TimeCompressor {
    pub fn new() -> Self {
        let mut me = Self {
            prev_value: 0,
            prev_delta: 0,
            scratch: 0,
            bits_used: 0,
            values_written: 0,
            buf: BytesMut::new(),
        };
        // we write a u64 0 so that when we finish we can encode the amount of values used
        me.buf.put_u64(0);
        me
    }
    fn encode_value(&mut self, i: i32) {
        //let leading = i.leading_zeros();
        
        let (mask, mask_size) = match i {
            0 => R0,
            i if -63 < i && i < 64 => R1,
            i if -255 < i && i < 256 => R1,
            i if -2047 < i && i < 2048 => R2,
            _ => R3,
        };
        let mut value = i as u64;
        let leading = value.leading_zeros();
        let value_shifts = leading - mask_size;
        let value_bits_used = mask_size + (u64::BITS - leading);
        value <<= value_shifts;
        value |= mask;

        //write what we have ignoring the overflow
        self.scratch |= value >> self.bits_used;
        self.bits_used += value_bits_used;
        self.values_written += 1;

        // rotate the "scratch"
        if self.bits_used > u64::BITS {
            self.buf.put_u64(self.scratch);
            self.scratch = 0;
            //split
            let overflow = self.bits_used - u64::BITS;
            self.scratch |= value << overflow;
            self.bits_used = overflow;
        }
    }

}

impl Compressor<i64, Vec<u8>> for TimeCompressor {
    fn compress(&mut self, i: i64) -> Result<()> {
        let delta = self.prev_value - i;
        let dod = self.prev_delta - delta;
        self.prev_value = i;
        self.prev_delta = delta;
        self.encode_value(dod as _);
        Ok(())
    }

    fn finish(mut self) -> Result<Vec<u8>> {
        {
            let (mut first, _) = self.buf.split_at_mut(8);
            first.put_u64(self.values_written);
        }
        Ok(self.buf.freeze().into())
    }
}

