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
        //println!("to_write1 {to_write:b}");
        let mask = !(u64::MAX << bits_to_write);
        to_write = mask & to_write;
        if self.offset + bits_to_write > U64_BITS as _ {
            let remaining = U64_BITS - self.offset; // 64 - 62 = 2
            // write what we can into the scratch
            let remaining_after_write = bits_to_write - remaining; //7 - 2 = 5
            let masked_current = to_write >> remaining;
            self.scratch |= masked_current;
            self.flush();

            bits_to_write = remaining_after_write;
            to_write = !(u64::MAX >> (U64_BITS - bits_to_write));
            
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
        let s = self.scratch;
        println!("1bits_to_read: {}\n1remaining: {}\n1offset: {}\nscr: {s:b}\n=======", bits_to_read, remaining, self.offset);
        if bits_to_read > remaining {
            let s = self.scratch;
            let mask = (u64::MAX << self.offset);
            println!("Flipping:\n scr: {s:b}\n acc: {acc:b}\n mask: {mask:b}");
            acc |= self.scratch & mask;
            acc <<= remaining; //bits_to_read - remaining;
            self.rotate_scratch();
            bits_to_read -= remaining;
        }
        let mask = !(u64::MAX >> bits_to_read);
        println!("mask: {mask:b}");
        acc |= (self.scratch & mask) >> U64_BITS - bits_to_read;
        let xx = self.scratch;
        println!(" acc: {acc:b}\n scr: {xx:b}\n");
        println!("bits: {}", bits_to_read);
        self.scratch <<= bits_to_read;
        self.offset += bits_to_read;
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
        assert_eq!(reader.read_bit(), true);
    }
    #[test]
    fn round_trip_128bits() {
        let mut writer = BitWriter::default();
        for i in 0..128 {
            writer.on();
            // if i % 2 == 0 {
            //     writer.on();
            // } else {
            //     writer.off();
            // }
        }
        let buf = writer.finish().freeze();
        let v = buf.clone().get_u64();
        println!("v: {v:b}");
        let mut reader = BitReader::from(buf);
        for i in 0..128 {
            let bit = reader.read_bit();
            println!("Bit {} at {}", bit, i);
            // if i % 2 == 0 {
            //     assert!(bit == true);
            // } else {
            //     assert!(bit == false);
            // }

        }
    }
}
