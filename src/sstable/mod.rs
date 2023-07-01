mod disk;
mod memory;
mod mappers;
mod iter;

use std::fmt::Display;

use crate::error::*;
pub use mappers::{KeyMapper, ValueMapper};
pub use crate::sstable::disk::{DiskSSTable, Value};
pub use crate::sstable::iter::{DiskSSTableKeyValueIterator, DiskSSTableIterator, SortedDiskSSTableKeyValueIterator};
pub use crate::sstable::memory::Memtable;
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
