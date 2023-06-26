use super::{MutSSTable, SSTable, ValueRef};
use crate::error::*;
///
/// In-memory SSTable implementation
/// Deletes marked as Tombstones
///
pub struct Memtable {
    size: usize,
    keys: Vec<Vec<u8>>,
    values: Vec<ValueRef>,
}

impl Memtable {
    pub fn into_key_values(self) -> (Vec<Vec<u8>>, Vec<ValueRef>) {
        (self.keys, self.values)
    }
}

impl Default for Memtable {
    fn default() -> Self {
        Memtable {
            size: 0,
            keys: Vec::new(),
            values: Vec::new(),
        }
    }
}

impl AsRef<Memtable> for Memtable {
    fn as_ref(&self) -> &Memtable {
        self
    }
}

// impl Deref for InMemorySSTable {
//     type Target = InMemorySSTable;

//     fn deref(&self) -> &Self::Target {
//         self
//     }
// }

impl<'a> SSTable<&'a [u8]> for &'a Memtable {
    fn size(&self) -> Result<usize> {
        Ok(self.size)
    }
    fn get(&self, k: &[u8]) -> Result<Option<&'a [u8]>> {
        Ok(match self.keys.binary_search_by_key(&k, |i| i.as_slice()) {
            Ok(pos) => match &self.values[pos] {
                ValueRef::MemoryRef(v) => Some(v.as_slice()),
                ValueRef::Tombstone => None,
            },
            Err(_) => None,
        })
    }
}

impl MutSSTable for Memtable {
    fn put(&mut self, k: Vec<u8>, v: Vec<u8>) {
        match self.keys.binary_search(&k) {
            Ok(pos) => {
                self.values[pos] = ValueRef::MemoryRef(v);
            }
            Err(pos) => {
                self.size += k.len() + v.len();
                self.keys.insert(pos, k);
                self.values.insert(pos, ValueRef::MemoryRef(v));
            }
        }
    }
    fn delete(&mut self, k: &[u8]) -> bool {
        self.size += 1; // no matter what we add a tombstone, so this should be ok.
        match self.keys.binary_search_by_key(&k, |i| i.as_slice()) {
            Ok(pos) => {
                if let Some(v) = self.values.get_mut(pos) {
                    self.size -= v.len();
                    *v = ValueRef::Tombstone;
                }
                true
            }
            Err(_) => false,
        }
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn in_memory_sstable_can_get_put_delete() {
        use super::*;
        let mut sstable = Memtable::default();
        sstable.put(b"key1".to_vec(), b"value1".to_vec());
        sstable.put(b"key2".to_vec(), b"value2".to_vec());
        sstable.put(b"key3".to_vec(), b"value3".to_vec());
        assert_eq!(sstable.size, 30);
        assert_eq!((&sstable).get(b"key1").unwrap(), Some(b"value1".as_ref()));
        assert_eq!((&sstable).get(b"key2").unwrap(), Some(b"value2".as_ref()));
        assert_eq!(
            sstable.as_ref().get(b"key3").unwrap(),
            Some(b"value3".as_ref())
        );
        sstable.delete(b"key2");
        assert_eq!(sstable.as_ref().get(b"key2").unwrap(), None);
    }
}
