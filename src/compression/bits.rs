use std::ops::Shl;

use bytes::{Buf, BufMut, Bytes, BytesMut};

// Time compressor uses the "delta of delta" ideas from Facebooks Gorrila paper
// https://www.vldb.org/pvldb/vol8/p1816-teller.pdf
// use super::Compressor;
// use crate::error::*;

// // Range masks (4.1.1)
// // (value, bits used)
// const MAX_BITS_USED: u32 = 4;
// const R0: (u64, u32, u32) = (0b0 << u64::BITS -1, 1, 0);
// const R1: (u64, u32, u32) = (0b10 << u64::BITS -2, 2, 7);
// const R2: (u64, u32, u32) = (0b110 << u64::BITS -3, 3, 9);
// const R3: (u64, u32, u32) = (0b1110 << u64::BITS -MAX_BITS_USED, MAX_BITS_USED, 12);
// const R4: (u64, u32, u32) = (0b1111 << u64::BITS -MAX_BITS_USED, MAX_BITS_USED, 32);

// const MASKS: [(u64, u32, u32); 5] = [R0, R1, R2, R3, R4];

// const U64_BITS: usize = 63 as usize;

#[derive(Default)]
pub struct BitWriter {
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
        assert!(bits_to_write <= 64);
        if bits_to_write == 64 {
            //divide the bits up in 32 bits
            let half = 32;
            while bits_to_write > 0 {
                self.inner_write(to_write, bits_to_write);
                to_write >>= half;
                bits_to_write -= half;
            }
        } else {
            self.inner_write(to_write, bits_to_write);
        }
    }

    fn inner_write(&mut self, mut to_write: u64, mut bits_to_write: usize) {
        //assert!(bits_to_write < 64);
        // mask off to_write
        to_write.checked_shl(rhs)

        while bits_to_write > 0 {
            let mask = !(u64::MAX << bits_to_write);
            to_write = mask & to_write;
            let remaining = 64 - self.offset;
            if bits_to_write > remaining {
                self.scratch |= to_write >> (bits_to_write - remaining);
                bits_to_write -= remaining;
                self.flush();
            } else {
                self.scratch |= to_write << remaining - bits_to_write;
                self.offset += bits_to_write;
                bits_to_write -= bits_to_write;
            }
        }
    }

    pub fn finish(mut self) -> BytesMut {
        self.flush();
        self.inner
    }

    fn flush(&mut self) {
        self.offset = 0;
        let s = self.scratch;
        self.inner.put_u64(self.scratch);
        self.scratch = 0;
    }
}

pub struct BitReader {
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

    fn read(&mut self, mut bits_to_read: usize) -> u64 {
        assert!(bits_to_read <= 64);
        if bits_to_read == 64 {
            let mut acc = 0;
            let half = 32;
            while bits_to_read > 0 {
                acc <<= half;
                let read = self.inner_read(half);
                acc |= read;
                bits_to_read -= half;
            }
            return acc;
        } else {
            return self.inner_read(bits_to_read);
        }
    }

    fn inner_read(&mut self, mut bits_to_read: usize) -> u64 {
        let mut acc = 0;
        while bits_to_read > 0 {
            let remaining = 64 - self.offset;
            if remaining >= bits_to_read {
                acc <<= bits_to_read;
                acc |= self.scratch >> 64 - bits_to_read;
                self.scratch <<= bits_to_read;
                self.offset += bits_to_read;
                bits_to_read -= bits_to_read
            } else {
                if remaining > 0 {
                    acc |= self.scratch >> 64 - remaining;
                    bits_to_read -= remaining;
                }
                self.rotate_scratch();
            }
        }
        return acc;
    }

    fn rotate_scratch(&mut self) {
        self.offset = 0;
        self.scratch = self.inner.get_u64();
    }
}

#[cfg(test)]
mod test {
    use rand::random;

    use super::*;

    #[test]
    fn round_trip_single_bit() {
        let mut writer = BitWriter::default();
        writer.on();
        writer.off();
        writer.on();
        writer.on();

        let buf = writer.finish().freeze();
        let mut reader = BitReader::from(buf);
        assert_eq!(reader.read_bit(), true);
        assert_eq!(reader.read_bit(), false);
        assert_eq!(reader.read_bit(), true);
        assert_eq!(reader.read_bit(), true);
    }
    #[test]
    fn writer_all_ones() {
        let mut writer = BitWriter::default();
        for _ in 0..256 {
            writer.on();
        }
        let buf = writer.finish().freeze();
        let mut cloned_buf = buf.clone();
        for _ in 0..(256 / 64) {
            let v = cloned_buf.get_u64();
            assert_eq!(v, u64::MAX);
        }
    }

    #[test]
    fn writer_all_alternate() {
        let mut writer = BitWriter::default();
        for i in 0..128 {
            if i % 2 == 1 {
                writer.on();
            } else {
                writer.off();
            }
        }
        let buf = writer.finish().freeze();
        let mut reader = BitReader::from(buf);
        for i in 0..128 {
            let bit = reader.read_bit();
            if i % 2 == 1 {
                assert!(bit == true);
            } else {
                assert!(bit == false);
            }
        }
    }
    fn do_fixed_bit_test(bits_to_write: usize, expected: u64) {
        let mut writer = BitWriter::default();
        for i in 0..128 {
            if i % bits_to_write == 0 {
                writer.write(expected, bits_to_write);
            } else {
                writer.write(0, bits_to_write);
            }
        }
        let buf = writer.finish().freeze();
        let mut reader = BitReader::from(buf);
        for i in 0..128 {
            let bit = reader.read(bits_to_write);
            if i % bits_to_write == 0 {
                assert!(bit == expected);
            } else {
                assert!(bit == 0);
            }
        }
    }
    #[test]
    fn writer_all_4bits() {
        let bits_to_write = 4;
        let expected = 0x0F;
        do_fixed_bit_test(bits_to_write, expected);
    }

    #[test]
    fn writer_all_8bits() {
        let bits_to_write = 8;
        let expected = 0xFF;
        do_fixed_bit_test(bits_to_write, expected);
    }

    #[test]
    fn writer_all_12bits() {
        let bits_to_write = 12;
        let expected = 0xFFF;
        do_fixed_bit_test(bits_to_write, expected);
    }

    #[test]
    fn writer_all_32bits() {
        let bits_to_write = 32;
        let expected = 0xFFFFFFFF;
        do_fixed_bit_test(bits_to_write, expected);
    }

    #[test]
    fn writer_all_64bits() {
        let bits_to_write = 64;
        let expected = 0xFFFFFFFFFFFFFFFF;
        do_fixed_bit_test(bits_to_write, expected);
    }

    #[test]
    fn writer_all_63bits() {
        let bits_to_write = 63;
        let expected = 0x7FFFFFFFFFFFFFFF;
        do_fixed_bit_test(bits_to_write, expected);
    }

    #[test]
    fn writer_all_3bits() {
        let bits_to_write = 3;
        let expected = 0x07;
        do_fixed_bit_test(bits_to_write, expected);
    }
    #[test]
    fn writer_all_7bits() {
        let bits_to_write = 7;
        let expected = 0x7F;
        do_fixed_bit_test(bits_to_write, expected);
    }

    fn random_read_write(bits_to_write: usize) {
        let mut writer = BitWriter::default();
        let mut expected = vec![];
        for _ in 0..128 {
            let rand: u64 = random();
            let mask = if bits_to_write == 64 {
                u64::MAX
            }
            else {
                !(u64::MAX << bits_to_write)
            };
            let masked = rand & mask;
            writer.write(masked, bits_to_write);
            expected.push(masked);
        }
        let buf = writer.finish().freeze();
        let mut reader = BitReader::from(buf);
        expected.reverse();
        println!("Expectations: {:?}", expected);
        for i in 0..128 {
            let bit = reader.read(bits_to_write);
            let expected = expected.pop().unwrap();
            println!("Iteration: {}", i);
            assert_eq!(expected, bit);
        }
    }

    #[test]
    fn writer_random_3() {
        let bits_to_write = 3;
        random_read_write(bits_to_write);
    }

    #[test]
    fn writer_random_8() {
        let bits_to_write = 8;
        random_read_write(bits_to_write);
    }

    #[test]
    fn writer_random_16() {
        let bits_to_write = 16;
        random_read_write(bits_to_write);
    }

    #[test]
    fn writer_random_18() {
        let bits_to_write = 18;
        random_read_write(bits_to_write);
    }

    #[test]
    fn writer_random_64() {
        let bits_to_write = 64;
        random_read_write(bits_to_write);
    }
}
