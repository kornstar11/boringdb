use std::{fmt::DebugList, mem::transmute};

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
    //ids.reverse();
    ids
});
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
            n if n == 1 => Self::NoChange,
            n if n <= 7 => Self::Bits7,
            n if n <= 9 => Self::Bits9,
            n if n <= 16 => Self::Bits16,
            n if n <= 32 => Self::Bits32,
            _ => Self::Bits64,
        }
    }
    pub fn supported_size(&self) -> usize { //macro
        return match self {
            Self::NoChange => 1,
            Self::Bits7 => 7,
            Self::Bits9 => 9,
            Self::Bits16 => 16,
            Self::Bits32 => 32,
            Self::Bits64 => 64,
        }
    }
    pub fn into_size_and_bit(&self) -> (u8, u8) {
        return match self {
            Self::NoChange => (1, 0),
            Self::Bits7 => (2, 0b10),
            Self::Bits9 => (3, 0b110),
            Self::Bits16 => (4, 0b1110),
            Self::Bits32 => (5, 0b11110),
            Self::Bits64 => (6, 0b111110),
        };
    }

    pub fn write_value(value: i64, writer: &mut BitWriter) -> Result<()> {
        if value == 0 { // special case
            let (id_size, id) = Self::NoChange.into_size_and_bit();
            writer.write(id as u64, id_size as _);
            return Ok(());
        }
        let abs: u64 = value.abs() as _;
        let leading = abs.leading_zeros(); 
        let pos_of_one = u64::BITS - leading + 1; // add one for leading
        let bit_ids: &Vec<BitId> = BITS_ORDER.as_ref();
        let mut selected_bit_id = Self::Bits64;

        for bit_id in bit_ids {
            let size = bit_id.supported_size();
            if pos_of_one <= size as _ {
                selected_bit_id = bit_id.clone();
                break;
            }
        }
        // write id to buffer
        let (id_size, id) = selected_bit_id.into_size_and_bit();
        writer.write(id as u64, id_size as _);
        // write signed value

        let size = selected_bit_id.supported_size();
        let sign: u64 = if value < 0 {1} else {0};
        let mask: u64 = sign << (size - 1);
        let value = abs | mask;
        writer.write(value, size);

        Ok(())
    }

    pub fn read_value(reader: &mut BitReader) -> Result<i64> {
        let bit_ids: &Vec<BitId> = BITS_ORDER.as_ref();
        let mut bits_to_zero = 0;
        while reader.read_bit() != false {
            bits_to_zero += 1;
        }
        let selected_bit_id = bit_ids
            .get(bits_to_zero)
            .cloned()
            .ok_or(Error::Other("Read too many its".to_string()))?;
        if let Self::NoChange = selected_bit_id {
            return Ok(0);
        }
        let size = selected_bit_id.supported_size();
        let value = reader.read(size);
        let mask: u64 = 1 << size - 1;
        let sign = (value & mask);
        let unsigned_value = value & !mask;
        if sign != 0 {
            return Ok(unsigned_value as i64 * -1);
        }
        return Ok(unsigned_value as _);
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
        BitId::write_value(d_of_d, &mut self.bits_writer)?;
        Ok(())
    }

    fn finish(self) -> Result<Bytes> {
        Ok(self.bits_writer.finish().freeze())
    }
}

pub struct TimeDecompressor {
    last_value: i64,
    last_delta: i64,
    bits_reader: BitReader,
}

impl From<Bytes> for TimeDecompressor {
    fn from(value: Bytes) -> Self {
        Self {
            last_value: 0,
            last_delta: 0,
            bits_reader: BitReader::from(value),
        }
    }
}

impl Decompressor<i64> for TimeDecompressor {
    fn decompress(&mut self) -> Result<i64> {
        let delta_of_delta = BitId::read_value(&mut self.bits_reader)?;
        let delta = delta_of_delta + self.last_delta;
        self.last_delta = delta;
        let value = delta + self.last_value;
        self.last_value = value;
        return Ok(value);
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn bit_id_test(to_write: i64, expected_bits_written: usize) {
        let mut writer = BitWriter::default();
        BitId::write_value(to_write, &mut writer).unwrap();
        let bits_written = writer.bits_written();
        println!("Bits written: {}", bits_written);

        let bytes = writer.finish().freeze();
        let mut reader = BitReader::from(bytes);
        let value = BitId::read_value(&mut reader).unwrap();
        assert_eq!(to_write, value);
        assert_eq!(expected_bits_written, bits_written);
    }

    #[test]
    fn bit_id_zero() {
        bit_id_test(0, 1);
    }

    #[test]
    fn bit_id_1() {
        bit_id_test(1, 7 + 2);
    }

    #[test]
    fn bit_id_63() {
        bit_id_test(63, 7 + 2);
    }

    #[test]
    fn bit_id_255() {
        bit_id_test(255, 9 + 3);
    }

    #[test]
    fn bit_id_neg_255() {
        bit_id_test(-255, 9 + 3);
    }

    #[test]
    fn bit_id_32() {
        bit_id_test((i32::MIN + 1) as _, 37);
    }
    #[test]
    fn bit_id_64() {
        bit_id_test(i64::MAX as _, 70);
    }
    #[test]
    fn bit_id_neg_64() {
        bit_id_test(i64::MIN as _, 70);
    }
}