use std::{path::PathBuf, fs::File, time};

use crate::{error::*, sstable::*};

///
/// Engine handle managment of the SSTables. It is responsible for converting a memory SSTable into a disk SSTable.
/// Also handled are management of Bloomfilters for the DiskSSTables.
/// 
/// TODO need a WAL to make memtable safe...
const SSTABLE_FILE_PREFIX: &str = "sstable_";

pub struct EngineConfig {
    base_dir: PathBuf,
    max_memory_bytes: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self { 
            base_dir: PathBuf::from("/tmp"), 
            max_memory_bytes: 1024 * 1000 * 10 // 10MB
        }
    }
}

pub struct Engine {
    config: EngineConfig,
    memtable: Memtable,
    disk_sstables: Vec<DiskSSTable>,
}

impl Engine {
    pub fn new(config: EngineConfig) -> Engine {
        Engine {
            config,
            memtable: Memtable::default(),
            disk_sstables: Vec::new(),
        }
    }

    fn load(base_dir: PathBuf) -> Result<Vec<DiskSSTable>> {
        let mut disk_sstables = Vec::new();
        for entry in std::fs::read_dir(base_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() && path.to_str().unwrap().starts_with(SSTABLE_FILE_PREFIX) {
                disk_sstables.push(DiskSSTable::open(path)?);
            }
        }
        disk_sstables.sort_by_key(|s| s.path().to_string_lossy().to_string());
        Ok(disk_sstables)
    }

    fn flush_to_disk(&mut self) -> Result<()> {
        let time = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH).map_err(Error::TimeError)?
            .as_micros();
        let mut path = self.config.base_dir.clone();
        path.push(format!("{}_{}.data", SSTABLE_FILE_PREFIX, time));
        let memory_sstable = std::mem::take(&mut self.memtable);
        let disk_table = DiskSSTable::convert_mem(path, memory_sstable)?;
        self.disk_sstables.push(disk_table);
        Ok(())
    }

    fn check_flush(&mut self) -> Result<()> {
        if self.memtable.as_ref().size()? > self.config.max_memory_bytes as _{
            self.flush_to_disk()?;
        }
        Ok(())
    }

    fn lookup_disk(&self, k: &[u8]) -> Result<Option<Vec<u8>>> {
        for table in self.disk_sstables.iter() {
            //BF stuff TODO
            if let Some(v) = table.get(k)? {
                return Ok(Some(v));
            }
        }
        Ok(None)
    }

    pub fn get(&self, k: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(v) = self.memtable.as_ref().get(k)? {
            return Ok(Some(v.to_vec()));
        }

        return self.lookup_disk(k);
    }

    pub fn put(&mut self, k: Vec<u8>, v: Vec<u8>) -> Result<()> {
        self.memtable.put(k, v);
        self.check_flush()?;
        Ok(())
    }

    pub fn delete(&mut self, k: &[u8]) -> Result<()> {
        self.memtable.delete(k);
        self.check_flush()?;
        Ok(())
    }
}