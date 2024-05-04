use std::fmt::DebugList;

use bytes::Bytes;
use once_cell::sync::Lazy;

use super::*;

static BITS_ORDER: Lazy<Vec<BitId>> = Lazy::new(|| {
    let mut ids = vec![
        BitId::NoChange, 
        BitId::Bits7,
        BitId::Bits9,
        BitId::Bits16,
        BitId::Bits32,
        BitId::Bits64,
    ];
    ids.reverse();
    ids

}
);
#[derive(Clone, Debug)]
enum BitId {
    NoChange,
    Bits7,
    Bits9,
    Bits16, 
    Bits32, 
    Bits64, 
}
impl BitId {
    pub fn from_leading(leading: usize) -> Self {
        match leading {
            n if n == 0 => Self::NoChange,
            n if n <= 7 => Self::Bits7,
            n if n <= 9 => Self::Bits9,
            n if n <= 16 => Self::Bits16,
            n if n <= 32 => Self::Bits32,
            _ => Self::Bits64,
        }
    }
    pub fn into_size_and_bit(&self) -> (u8, u8) {
        return match self {
            Self::NoChange => (1,0),
            Self::Bits7 => (2, 0b10),
            Self::Bits9 => (3, 0b110),
            Self::Bits16 => (4, 0b1110),
            Self::Bits32 => (5, 0b11110),
            Self::Bits64 => (6, 0b111110),
        };
    }


    pub fn from_bits(reader: &mut BitReader) -> Result<Self> {
        let bit_ids: &Vec<BitId> = BITS_ORDER.as_ref();
        let mut bits_to_zero= 0;
        while reader.read_bit() != false {
            bits_to_zero += 1;
        }
        bit_ids.get(bits_to_zero)
            .cloned()
            .ok_or(Error::Other("Read too many its".to_string()))
    }
    
}


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
        let leading = d_of_d.leading_zeros() as _;
        let (to_write, bit_id) = BitId::from_leading(leading).into_size_and_bit();
        self.bits_writer.write(to_write as _, bit_id as _);
        Ok(())
    }

    fn finish(self) -> Result<Bytes> {
        Ok(self.bits_writer.finish().freeze())
    }
}
