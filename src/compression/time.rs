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
       
        // mask off to_write
        while bits_to_write > 0 {
            let mask = !(u64::MAX << bits_to_write);
            println!("to_write1 {to_write:b}");
            to_write = mask & to_write;
            let remaining = 64 - self.offset;
            println!("mask {mask:b} to_write: {to_write:b} rem: {}", remaining);
            if bits_to_write > remaining {
                println!("Flush");
                self.scratch |= to_write >> (bits_to_write - remaining);
                bits_to_write -= remaining;
                self.flush();
            } else {
                self.scratch |= to_write << remaining - bits_to_write;
                let s = self.scratch;
                println!("scr1: {s:b}");
                self.offset += bits_to_write;
                bits_to_write -= bits_to_write;
            }
        }
        // let s = self.scratch;
        // println!("scr1: {s:b}");
        // let mask = !(u64::MAX << bits_to_write);
        // to_write = mask & to_write;
        // if self.offset + bits_to_write > U64_BITS as _ {
        //     let remaining = U64_BITS - self.offset; // 64 - 62 = 2
        //     // write what we can into the scratch
        //     let remaining_after_write = (bits_to_write - remaining) - 1; //7 - 2 = 5
        //     let masked_current = to_write >> remaining;
        //     self.scratch |= masked_current;
        //     let s = self.scratch;
        //     println!("scr2: {s:b}");
        //     self.flush();

        //     bits_to_write = remaining_after_write;
        //     to_write = !(u64::MAX >> (U64_BITS - bits_to_write));
            
        // }
        // self.scratch |= to_write << (U64_BITS - self.offset);
        // self.offset += bits_to_write;
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
        while bits_to_read > 0 {
            let remaining = 64 - self.offset;
            let mask = self.scratch;
            println!("1remain: {}\n1  mask: {mask:b}\n  acc:{acc:b}", remaining);
            if remaining >= bits_to_read {
                acc <<= bits_to_read;
                acc |= self.scratch >> U64_BITS - bits_to_read + 1;
                self.scratch <<= bits_to_read;
                self.offset += bits_to_read;
                bits_to_read -= bits_to_read
            } else {
                acc |= self.scratch >> U64_BITS - remaining;
                bits_to_read -= remaining;
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
        for i in 0..(256/64) {
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
        let mut cloned_buf = buf.clone();
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
        println!("expected: {expected:b}");
        let mut writer = BitWriter::default();
        for i in 0..128 {
            if i % bits_to_write == 0 {
                writer.write(expected, bits_to_write);
            } else {
                writer.write(0, bits_to_write);
            }
        }
        let buf = writer.finish().freeze();
        let mut cloned_buf = buf.clone();
        let v = cloned_buf.get_u64();
        println!("v: {v:b}");
        let mut reader = BitReader::from(buf);
        for i in 0..128 {
            let bit = reader.read(bits_to_write);
            println!("Bit {} at {}", bit, i);
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
    fn writer_all_3bits() {
        let bits_to_write = 3;
        let expected = 0x07;
        do_fixed_bit_test(bits_to_write, expected);
    }
}
