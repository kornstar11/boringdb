mod disk;
mod iter;
mod mappers;
mod memory;

use std::fmt::Display;

use crate::error::*;
pub use crate::sstable::disk::{DiskSSTable, Value};
pub use crate::sstable::iter::{
    DiskSSTableIterator, DiskSSTableKeyValueIterator, SortedDiskSSTableKeyValueIterator,
};
pub use crate::sstable::memory::Memtable;
pub use mappers::{KeyMapper, ValueMapper};
#[derive(Debug)]
pub enum ValueRef {
    MemoryRef(Vec<u8>),
    Tombstone,
}

impl Display for ValueRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MemoryRef(v) => {
                write!(f, "{}", String::from_utf8_lossy(v).to_string())
            }
            _ => {
                write!(f, "Tombsome()")
            }
        }
    }
}

impl Into<Vec<u8>> for ValueRef {
    fn into(self) -> Vec<u8> {
        match self {
            ValueRef::MemoryRef(v) => v,
            ValueRef::Tombstone => Vec::new(),
        }
    }
}

impl ValueRef {
    fn len(&self) -> usize {
        match self {
            ValueRef::MemoryRef(v) => v.len(),
            ValueRef::Tombstone => 0,
        }
    }
}

pub trait SSTable<G: AsRef<[u8]>> {
    fn get(&self, k: &[u8]) -> Result<Option<G>>;
    fn size(&self) -> Result<usize>;
}

pub trait MutSSTable {
    fn put(&mut self, k: Vec<u8>, v: Vec<u8>);
    fn delete(&mut self, k: &[u8]) -> bool;
}

#[cfg(test)]
pub mod test {
    use std::time;

    use super::{disk::InternalDiskSSTable, Memtable, MutSSTable};

    pub fn time_ms() -> u128 {
        let time = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap()
            .as_micros();
        time
    }

    pub fn generate_kvs() -> Box<dyn Iterator<Item = (String, String)>> {
        Box::new((0..10).map(|i| (format!("k{}", i), format!("v{}", i))))
    }

    pub fn generate_even_kvs() -> Box<dyn Iterator<Item = (String, String)>> {
        Box::new(
            (0..10)
                .filter(|x| x % 2 == 0)
                .map(|i| (format!("k{}", i), format!("v{}", i))),
        )
    }
    pub fn generate_odd_kvs() -> Box<dyn Iterator<Item = (String, String)>> {
        Box::new(
            (0..10)
                .filter(|x| x % 2 == 1)
                .map(|i| (format!("k{}", i), format!("v{}", i))),
        )
    }

    pub fn generate_memory(it: Box<dyn Iterator<Item = (String, String)>>) -> Memtable {
        let mut memory = Memtable::default();
        for (k, v) in it {
            memory.put(k.as_bytes().to_vec(), v.as_bytes().to_vec());
        }
        memory
    }
    pub fn generate_disk(memory: Memtable) -> InternalDiskSSTable {
        let file = tempfile::tempfile().unwrap();
        println!("file: {:?}", file);
        InternalDiskSSTable::encode_inmemory_sstable(memory, file).unwrap()
    }
}
