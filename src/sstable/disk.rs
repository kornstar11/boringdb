use crate::error::*;
use bytes::{Buf, BufMut, BytesMut};
use parking_lot::Mutex;
use std::{
    cmp::Ordering,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use super::{memory::Memtable, SSTable, ValueRef, mappers::{KeyValueMapper, KeyIndexMapper}, KeyMapper, ValueMapper, DiskSSTableIterator, DiskSSTableKeyValueIterator};

#[derive(Debug, Copy, Clone)]
pub struct ValueIndex {
    pos: usize,
    len: usize,
}

impl ValueIndex {
    fn new(pos: usize, len: usize) -> Self {
        ValueIndex { pos, len }
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64_le(self.pos as _);
        buf.put_u64_le(self.len as _);
    }

    fn decode(mut buf: &[u8]) -> Self {
        let pos = buf.get_u64_le() as usize;
        let len = buf.get_u64_le() as usize;
        ValueIndex { pos, len }
    }
}

#[derive(Default)]
struct ValuesToPositions {
    values_to_positions: Vec<ValueIndex>,
    position: usize,
}

impl ValuesToPositions {
    fn split(self) -> (Vec<ValueIndex>, usize) {
        (self.values_to_positions, self.position)
    }
}

#[derive(Debug)]
pub struct Value {
    pub value_ref: ValueRef,
}

impl Value {
    fn encode(self, buf: &mut BytesMut) {
        match self.value_ref {
            ValueRef::MemoryRef(v) => {
                buf.put_u8(1);
                buf.put_u64_le(v.len() as u64);
                buf.put_slice(v.as_slice());
            }
            ValueRef::Tombstone => {
                buf.put_u8(0);
            }
        }
    }

    pub fn decode(mut buf: &[u8]) -> Self {
        let is_tombstone = buf.get_u8();
        match is_tombstone {
            0 => Value {
                value_ref: ValueRef::Tombstone,
            },
            1 => {
                let len = buf.get_u64_le() as usize;
                let mut value_buf = vec![0u8; len];
                buf.copy_to_slice(&mut value_buf);
                Value {
                    value_ref: ValueRef::MemoryRef(value_buf),
                }
            }
            _ => panic!("Invalid value"),
        }
    }
}

///
/// Immutable SSTable stored on Disk. The general file layout is as follows:
/// ```text
///   === Values Section: Value | Value | Value
///   === Keys Section: Key, ValueIndex | Key, ValueIndex | Key, ValueIndex
///   === Key Position Section: Number of keys | KeyIndex | KeyIndex | KeyIndex
///   === Footer: Key position section start position (u64)
/// ```
/// Both keys and values retain their sorted order from the MemorySSTable.
/// Tombstones are tracked via a 0 byte proceeding the values.
///
/// As opposed to other impls I have seen, we store the keys and values seperate from each other. The thinking
/// is that this will help compresion since we are keeping like for like. It may also help with compaction, in the sense
/// that we can load the keys quicker when comparing sstables.
pub struct InternalDiskSSTable {
    file: File,
}

impl InternalDiskSSTable {
    pub fn encode_it(
        keys: impl Iterator<Item = Vec<u8>>,
        values: impl Iterator<Item = Result<ValueRef>>,
        mut file: File,
    ) -> Result<InternalDiskSSTable> {
        let mut values_to_position = ValuesToPositions::default();
        Self::encode_values(values, &mut file, &mut values_to_position)?;
        Self::encode_keys(keys, &mut file, values_to_position)?;
        file.sync_all()?;

        Ok(InternalDiskSSTable { file })
    }

    pub fn encode_inmemory_sstable(
        memory_sstable: Memtable,
        file: File,
    ) -> Result<InternalDiskSSTable> {
        let (keys, values) = memory_sstable.into_key_values();
        Self::encode_it(keys.into_iter(), values.into_iter().map(Ok), file)
    }

    fn read_u64(&mut self) -> Result<u64> {
        let mut buf = [0u8; 8];
        self.file.read_exact(&mut buf)?;
        Ok(u64::from_le_bytes(buf))
    }

    fn read_bytes(&mut self, len: usize) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; len];
        self.file.read_exact(&mut buf)?;
        Ok(buf)
    }

    fn read_number_of_keys(&mut self) -> Result<usize> {
        self.file.seek(SeekFrom::End(-8))?; //find the footer
        let keys_start_pos = self.read_u64()?;
        self.file.seek(SeekFrom::Start(keys_start_pos))?;
        let number_of_keys = self.read_u64()? as usize;
        Ok(number_of_keys)
    }
    ///
    /// Returns the positions of the sorted keys and their length
    pub fn read_key_index(&mut self) -> Result<(Vec<ValueIndex>, Vec<ValueIndex>)> {
        let number_of_keys = self.read_number_of_keys()?;
        let mut key_idxs = vec![];
        let mut value_idxs = vec![];
        for _ in 0..number_of_keys {
            //let key_pos = self.read_u64()? as usize;
            key_idxs.push(self.read_value_idx()?);
            value_idxs.push(self.read_value_idx()?);
        }
        Ok((key_idxs, value_idxs))
    }

    pub fn read_by_value_idx(&mut self, idx: &ValueIndex) -> Result<Vec<u8>> {
        self.file.seek(SeekFrom::Start(idx.pos as u64))?;
        let key_buf = self.read_bytes(idx.len)?;
        Ok(key_buf)
    }

    pub fn read_value_by_value_idx(&mut self, idx: &ValueIndex) -> Result<Value> {
        let value_buf = self.read_by_value_idx(&idx)?;
        Ok(Value::decode(&value_buf))
    }

    fn read_value_idx(&mut self) -> Result<ValueIndex> {
        let idx_buf = self.read_bytes(16)?;
        Ok(ValueIndex::decode(&idx_buf))
    }

    fn search_key_positions(&mut self, k: &[u8]) -> Result<Option<ValueIndex>> {
        let (key_idx_to_position, value_idx_to_position) = self.read_key_index()?;
        let mut l = 0;
        let mut r = key_idx_to_position.len() - 1;
        while r >= l {
            let mid = (l + r) / 2;
            let key_idx = &key_idx_to_position[mid];
            let value_idx = value_idx_to_position[mid];
            let key_buf = self.read_by_value_idx(key_idx)?;
            match k.cmp(&key_buf) {
                Ordering::Equal => {
                    return Ok(Some(value_idx));
                }
                Ordering::Less => {
                    if let Some(new_r) = mid.checked_sub(1) {
                        r = new_r;
                    } else {
                        return Ok(None);
                    };
                }
                Ordering::Greater => {
                    if let Some(new_l) = mid.checked_add(1) {
                        l = new_l;
                    } else {
                        return Ok(None);
                    };
                }
            }
        }
        Ok(None)
    }

    fn get_value(&mut self, k: &[u8]) -> Result<Option<Vec<u8>>> {
        let value_idx = self.search_key_positions(k)?;
        match value_idx {
            Some(ref value_idx) => {
                if let Value {
                    value_ref: ValueRef::MemoryRef(value),
                } = self.read_value_by_value_idx(value_idx)?
                {
                    return Ok(Some(value));
                }
            }
            None => {}
        }
        Ok(None)
    }

    fn encode_keys(
        keys: impl Iterator<Item = Vec<u8>>,
        file: &mut File,
        values_to_position: ValuesToPositions,
    ) -> Result<()> {
        let (value_idxs, mut position) = values_to_position.split();
        // tracks the key index to position in the file
        let mut key_idxs = vec![];
        // write the key values to the file as well as the corresponding value index in the file.
        for key in keys {
            key_idxs.push(ValueIndex::new(position, key.len()));
            let mut buf = BytesMut::new();
            buf.put_slice(key.as_slice());
            let len = buf.len();
            file.write_all(&buf)?;
            position += len;
        }
        // write key index to position
        let mut buf = BytesMut::new(); //just use one buffer for these since they should be small
        let keys_start_pos = position;
        // encode number of keys
        buf.put_u64_le(key_idxs.len() as u64);
        for (key_idx, value_idx) in key_idxs.into_iter().zip(value_idxs.into_iter()) {
            key_idx.encode(&mut buf);
            value_idx.encode(&mut buf);
        }
        // write keys start position
        buf.put_u64_le(keys_start_pos as u64);
        file.write_all(&buf)?;
        file.sync_all()?;
        Ok(())
    }

    fn encode_values(
        values: impl Iterator<Item = Result<ValueRef>>,
        file: &mut File,
        values_to_position: &mut ValuesToPositions,
    ) -> Result<()> {
        for value_ref_res in values {
            let value_ref = value_ref_res?;
            let mut buf = BytesMut::new();
            let value = Value { value_ref };
            value.encode(&mut buf);
            let len = buf.len();
            file.write_all(&buf)?;
            values_to_position
                .values_to_positions
                .push(ValueIndex::new(values_to_position.position, len));
            values_to_position.position += len;
        }
        Ok(())
    }
}

///
/// Outward facing interface of the sstable, allows for cloning, and is a central point to control the mutex.
#[derive(Clone)]
pub struct DiskSSTable {
    path: PathBuf,
    inner: Arc<Mutex<InternalDiskSSTable>>,
}

impl DiskSSTable {
    pub fn convert_mem<P: AsRef<Path>>(path: P, memory_sstable: Memtable) -> Result<DiskSSTable> {
        let (keys, values) = memory_sstable.into_key_values();
        Self::convert_from_iter(path, keys.into_iter(), values.into_iter().map(Ok))
    }

    pub fn convert_from_iter<P: AsRef<Path>>(
        path: P, 
        keys: impl Iterator<Item = Vec<u8>>,
        values: impl Iterator<Item = Result<ValueRef>>,) -> Result<DiskSSTable> {
        let file = {
            OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(path.as_ref())?
        };
        let inner = InternalDiskSSTable::encode_it(keys, values, file)?;
        Ok(DiskSSTable {
            path: path.as_ref().to_path_buf(),
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<DiskSSTable> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(path.clone())?;
        let inner = InternalDiskSSTable { file };
        Ok(DiskSSTable {
            path,
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    pub fn drop_and_remove_file(self) -> Result<()> {
        // this is tricky, it would be nice to enforce the lock being dead
        if let Ok(inner) = Arc::try_unwrap(self.inner) {
            let inner = inner.into_inner();
            std::mem::drop(inner);
            std::fs::remove_file(self.path)?;
            Ok(())
        } else {
            Err(Error::Other(String::from(
                "Reference to this SSTable is still held.",
            )))
        }
    }

    pub fn path(&self) -> PathBuf {
        self.path.clone()
    }

    pub fn read_value_by_value_idx(&self, idx: &ValueIndex) -> Result<Value> {
        self.inner.lock().read_value_by_value_idx(idx)
    }

    pub fn iter_key_values(&self) -> DiskSSTableKeyValueIterator {
        DiskSSTableKeyValueIterator::new (
            DiskSSTableIterator::new(Arc::clone(&self.inner), KeyValueMapper {}),
        )
    }

    pub fn iter_key_idxs(&self) -> DiskSSTableIterator<(Vec<u8>, (ValueIndex, ValueIndex)), KeyIndexMapper> {
        DiskSSTableIterator::new(Arc::clone(&self.inner), KeyIndexMapper {})
    }

    pub fn iter_key(&self) -> DiskSSTableIterator<Vec<u8>, KeyMapper> {
        DiskSSTableIterator::new(Arc::clone(&self.inner), KeyMapper {})
    }
    pub fn iter_value(&self) -> DiskSSTableIterator<Value, ValueMapper> {
        DiskSSTableIterator::new(Arc::clone(&self.inner), ValueMapper {})
    }

}

impl SSTable<Vec<u8>> for DiskSSTable {
    fn get(&self, k: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.inner.lock().get_value(k)?)
    }

    fn size(&self) -> Result<usize> {
        self.inner.lock().read_number_of_keys()
    }
}

#[cfg(test)]
mod test {
    use crate::sstable::MutSSTable;
    
    use super::*;
    use crate::sstable::test::*;

    // fn generate_kvs() -> Box<dyn Iterator<Item = (String, String)>> {
    //     Box::new((0..100).map(|i| (format!("k{}", i), format!("v{}", i))))
    // }

    // fn generate_memory() -> Memtable {
    //     let mut memory = Memtable::default();
    //     let it = generate_kvs();
    //     for (k, v) in it {
    //         memory.put(k.as_bytes().to_vec(), v.as_bytes().to_vec());
    //     }
    //     memory
    // }

    // fn generate_disk() -> InternalDiskSSTable {
    //     let memory = generate_memory();
    //     let file = tempfile::tempfile().unwrap();
    //     println!("file: {:?}", file);
    //     InternalDiskSSTable::encode_inmemory_sstable(memory, file).unwrap()
    // }

    fn generate_default_disk() -> InternalDiskSSTable {
        generate_disk(generate_memory(generate_kvs()))
    }

    fn test_key_fresh(k: &str) -> Option<Vec<u8>> {
        let mut sstable = generate_default_disk();
        return sstable.get_value(k.as_bytes()).unwrap();
    }

    #[test]
    fn returns_none_for_missing() {
        assert_eq!(test_key_fresh("k3a"), None);
        assert_eq!(test_key_fresh("a3a"), None);
        assert_eq!(test_key_fresh(""), None);
    }

    #[test]
    fn encodes_an_fresh_sstable_and_finds_value() {
        for (k, v) in generate_kvs() {
            assert_eq!(test_key_fresh(&k), Some(v.as_bytes().to_vec()));
        }
    }
    #[test]
    fn encodes_an_sstable_and_finds_value() {
        let mut ss_table = generate_default_disk();
        for (k, v) in generate_kvs() {
            assert_eq!(
                ss_table.get_value(k.as_bytes()).unwrap(),
                Some(v.as_bytes().to_vec())
            );
        }
    }
    #[test]
    fn is_able_to_iterate() {
        let ss_table = generate_default_disk();
        let result = DiskSSTableIterator::new(Arc::new(Mutex::new(ss_table)), KeyValueMapper {})
            .map(|res| res.unwrap())
            .map(|(k, v)| {
                (
                    String::from_utf8_lossy(&k).to_string(),
                    v.value_ref.to_string(),
                )
            })
            .collect::<Vec<_>>();
        let mut control = generate_kvs().collect::<Vec<_>>();
        control.sort();

        assert_eq!(control, result);
    }
    #[test]
    fn is_able_to_iterate_values() {
        let ss_table = generate_default_disk();
        let result = DiskSSTableKeyValueIterator::new(
            DiskSSTableIterator::new(Arc::new(Mutex::new(ss_table)), KeyValueMapper {}),
        )
        .map(|res| res.unwrap())
        .map(|(k, v)| {
            (
                String::from_utf8_lossy(&k).to_string(),
                String::from_utf8_lossy(&v).to_string(),
            )
        })
        .collect::<Vec<_>>();
        let mut control = generate_kvs().collect::<Vec<_>>();
        control.sort();

        assert_eq!(control, result);
    }
}
