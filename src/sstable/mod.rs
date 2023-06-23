mod memory;
mod disk;
use crate::error::*;
pub enum ValueRef {
    MemoryRef(Vec<u8>),
    Tombstone,
}

impl Into<Vec<u8>> for ValueRef {
    fn into(self) -> Vec<u8> {
        match self {
            ValueRef::MemoryRef(v) => v,
            ValueRef::Tombstone => Vec::new()
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
    fn might_contain(&self, k: &[u8]) -> Result<bool>;
    fn size(&self) -> Result<usize>;
}

pub trait MutSSTable {
    fn put(&mut self, k: Vec<u8>, v: Vec<u8>);
    fn delete(&mut self, k: &[u8]) -> bool;
}