use std::{fs::File, io::{Write, Seek, SeekFrom, Read}, cmp::Ordering, sync::{Arc}, path::{Path, PathBuf}};
use bytes::{BytesMut, BufMut, Buf};
use crate::error::*;
use parking_lot::Mutex;

use super::{ValueRef, SSTable, memory::Memtable};

#[derive(Debug)]
struct ValueIndex {
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


struct Value {
    value_ref: ValueRef,
}

impl Value {
    fn encode(self, buf: &mut BytesMut) {
        match self.value_ref {
            ValueRef::MemoryRef(v) => {
                buf.put_u8(1);
                buf.put_u64_le(v.len() as u64);
                buf.put_slice(v.as_slice());
            },
            ValueRef::Tombstone => {
                buf.put_u8(0);
            }
        }
    }

    fn decode(mut buf: &[u8]) -> Self {
        let is_tombstone = buf.get_u8();
        match is_tombstone {
            0 => Value { value_ref: ValueRef::Tombstone },
            1 => {
                let len = buf.get_u64_le() as usize;
                let mut value_buf = vec![0u8; len];
                buf.copy_to_slice(&mut value_buf);
                Value { value_ref: ValueRef::MemoryRef(value_buf) }
            },
            _ => panic!("Invalid value")
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
struct InternalDiskSSTable {
    file: File,
}

impl InternalDiskSSTable {
    // pub fn new(file: File, memory_sstable: InMemorySSTable) -> Self {
    //     DiskSSTable { file }
    // }

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

    fn read_key_index(&mut self) -> Result<Vec<ValueIndex>> {
        let number_of_keys = self.read_number_of_keys()?;
        let mut result = vec![];
        for _ in 0..number_of_keys {
            //let key_pos = self.read_u64()? as usize;
            let idx_buf = self.read_bytes(16)?;
            let idx = ValueIndex::decode(&idx_buf);
            result.push(idx);
        }
        Ok(result)
    }

    fn search_key_positions(&mut self, k: &[u8]) -> Result<Option<ValueIndex>> {
        let key_idx_to_position = self.read_key_index()?;
        let mut l = 0;
        let mut r = key_idx_to_position.len() - 1;
        while r >= l {
            let mid = (l + r) / 2;
            //println!("mid: {}, l: {}, r: {}", mid, l, r);
            let idx = &key_idx_to_position[mid];
            self.file.seek(SeekFrom::Start(idx.pos as u64))?;
            //read key length
            //let key_len = self.read_u64()? as usize;
            let key_buf = self.read_bytes(idx.len)?;
            //println!("key_buf: {:?}", String::from_utf8_lossy(&key_buf).to_string());
            match k.cmp(&key_buf) {
                Ordering::Equal => {
                    let value_idx_buf = self.read_bytes(16)?;
                    let value_idx = ValueIndex::decode(&value_idx_buf);
                    return Ok(Some(value_idx));
                },
                Ordering::Less => {
                    if let Some(new_r) = mid.checked_sub(1) {
                        r = new_r;
                    } else {
                        return Ok(None);
                    };
                },
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
            Some(value_idx) => {
                self.file.seek(SeekFrom::Start(value_idx.pos as u64))?;
                let value_buf = self.read_bytes(value_idx.len)?;
                if let Value{ value_ref: ValueRef::MemoryRef(value)} = Value::decode(&value_buf) {
                    return Ok(Some(value));
                }
            },
            None => {}
        }
        Ok(None)
    }

    pub fn encode_inmemory_sstable(memory_sstable: Memtable, mut file: File) -> Result<InternalDiskSSTable> {
        let mut values_to_position = ValuesToPositions::default();
        let (keys, values) = memory_sstable.into_key_values();
        Self::encode_values(values, &mut file, &mut values_to_position)?;
        Self::encode_keys(keys, &mut file, values_to_position)?;

        Ok(InternalDiskSSTable { file })
    }

    fn encode_keys(keys: Vec<Vec<u8>>, file: &mut File, values_to_position: ValuesToPositions) -> Result<()> {
        let (key_idxs, mut position) = values_to_position.split();
        // tracks the key index to position in the file
        let mut key_idx_to_pos = vec![];
        // write the key values to the file as well as the corresponding value index in the file.
        for (key, value_idx) in keys.into_iter().zip(key_idxs) {
            key_idx_to_pos.push(ValueIndex::new(position, key.len()));
            let mut buf = BytesMut::new();
            buf.put_slice(key.as_slice());
            value_idx.encode(&mut buf);
            let len = buf.len();
            file.write_all(&buf)?;
            position += len;
        }
        // write key index to position
        let mut buf = BytesMut::new(); //just use one buffer for these since they should be small
        let keys_start_pos = position;
        // encode number of keys
        buf.put_u64_le(key_idx_to_pos.len() as u64);
        for key_idx in key_idx_to_pos {
            key_idx.encode(&mut buf);
        }
        // write keys start position
        buf.put_u64_le(keys_start_pos as u64);
        file.write_all(&buf)?;
        file.sync_all()?;
        Ok(())

    }

    fn encode_values(values: Vec<ValueRef>, file: &mut File, values_to_position: &mut ValuesToPositions) -> Result<()> {
        for value_ref in values.into_iter() {
            let mut buf = BytesMut::new();
            let value = Value { value_ref };
            value.encode(&mut buf);
            let len = buf.len();
            file.write_all(&buf)?;
            values_to_position.values_to_positions.push(ValueIndex::new(values_to_position.position, len));
            values_to_position.position += len;
        }
        Ok(())
    }
}
pub struct DiskSSTable {
    path: PathBuf,
    inner: Arc<Mutex<InternalDiskSSTable>>,
}

impl DiskSSTable {
    pub fn convert_mem<P: AsRef<Path>>(path: P, memory_sstable: Memtable) -> Result<DiskSSTable> {
        let file = File::create(path.as_ref())?;
        let inner = InternalDiskSSTable::encode_inmemory_sstable(memory_sstable, file)?;
        Ok(DiskSSTable { 
            path: path.as_ref().to_path_buf(),
            inner: Arc::new(Mutex::new(inner)) 
        })

    }
    pub fn open<P: AsRef<Path>>(path: P) -> Result<DiskSSTable> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(path.clone())?;
        let inner = InternalDiskSSTable { file };
        Ok(DiskSSTable { 
            path,
            inner: Arc::new(Mutex::new(inner)) 
        })
    }

    pub fn path(&self) -> PathBuf {
        self.path.clone()
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

    fn generate_kvs() -> Box<dyn Iterator<Item = (String, String)>> {
        Box::new((0..100).map(|i| {
            (format!("k{}", i), format!("v{}", i))
        }))
    }

    fn generate_memory() -> Memtable {
        let mut memory = Memtable::default();
        let it = generate_kvs();
        for (k, v) in it {
            memory.put(k.as_bytes().to_vec(), v.as_bytes().to_vec());
        }
        memory
    }

    fn generate_disk() -> InternalDiskSSTable {
        let memory = generate_memory();
        let file = tempfile::tempfile().unwrap();
        println!("file: {:?}", file);
        InternalDiskSSTable::encode_inmemory_sstable(memory, file).unwrap()
    }

    fn test_key_fresh(k: &str) -> Option<Vec<u8>> {
        let mut sstable = generate_disk();
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
        let mut ss_table = generate_disk();
        for (k, v) in generate_kvs() {
            assert_eq!(ss_table.get_value(k.as_bytes()).unwrap(), Some(v.as_bytes().to_vec()));
        }
    }
}