use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::spawn;
use std::{fs::File, path::PathBuf, time, collections::BTreeMap};
use std::sync::mpsc::{sync_channel, SyncSender, Receiver};
use parking_lot::Mutex;

use crate::{error::*, sstable::*};

use super::CompactorCommand;
use super::compaction::{CompactorFactory, SimpleCompactorFactory};

///
/// Engine handle managment of the SSTables. It is responsible for converting a memory SSTable into a disk SSTable.
/// Also handled are management of Bloomfilters for the DiskSSTables.
///
/// TODO need a WAL to make memtable safe...
const SSTABLE_FILE_PREFIX: &str = "sstable_";

#[derive(Default)]
pub struct DatabaseMetrics {
    flushed_bytes: AtomicUsize
}

pub struct DatabaseConfig {
    pub base_dir: PathBuf,
    pub max_memory_bytes: u64,
    pub compactor_factory: Box<dyn CompactorFactory>,
    pub metrics: Arc<DatabaseMetrics>,
}

impl Clone for DatabaseConfig {
    fn clone(&self) -> Self {
        Self { 
            base_dir: self.base_dir.clone(), 
            max_memory_bytes: self.max_memory_bytes.clone(), 
            compactor_factory: self.compactor_factory.clone(), 
            metrics: self.metrics.clone() 
        }
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            base_dir: PathBuf::from("/tmp"),
            max_memory_bytes: 1024 * 1000 * 10, // 10MB
            compactor_factory: Box::new(SimpleCompactorFactory::default()),
            metrics: Arc::new(DatabaseMetrics::default()),
        }
    }
}

pub struct Database {
    config: DatabaseConfig,
    memtable: Memtable,
    disk_sstables: BTreeMap<PathBuf, DiskSSTable>,
    compactor_evt_tx: SyncSender<CompactorCommand>,
}

impl Database {

    pub fn start(config: DatabaseConfig) -> Arc<Mutex<Self>> {
        let (compactor_evt_tx, compactor_evt_rx) = sync_channel(1);
        let db = Self::new(config.clone(), compactor_evt_tx);
        let db = Arc::new(Mutex::new(db));
        let event_db = Arc::clone(&db);

        let (compactor_evt_rx, _join) = config.compactor_factory.start(compactor_evt_rx);

        let _evt_handler = spawn(move || {
            while let Ok(evt) = compactor_evt_rx.recv() {
                let mut db = event_db.lock();
                match evt {
                    CompactorCommand::NewSSTable(new) => {
                        db.add_sstable(new);

                    },
                    CompactorCommand::RemoveSSTables(to_drop) => {
                        db.remove_sstable(to_drop);
                    }
                }
            }
        });

        db

    }

    fn new(config: DatabaseConfig, compactor_evt_tx: SyncSender<CompactorCommand>) -> Database {
        Database {
            config,
            compactor_evt_tx,
            memtable: Memtable::default(),
            disk_sstables: BTreeMap::new(),
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

    ///
    /// Flush memtable to disk and add the new disktable to our stack of disktables.
    ///
    fn flush_to_disk(&mut self) -> Result<()> {
        let time = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .map_err(Error::TimeError)?
            .as_micros();
        // make path
        let mut path = self.config.base_dir.clone();
        path.push(format!("{}{}.data", SSTABLE_FILE_PREFIX, time));

        let memory_sstable = std::mem::take(&mut self.memtable);
        let size = memory_sstable.as_ref().size()?;
        let disk_table = DiskSSTable::convert_mem(path.clone(), memory_sstable)?;
        // update compactor
        self
            .compactor_evt_tx
            .send(CompactorCommand::NewSSTable(disk_table.clone()))
            .map_err(|_| Error::SendError)?;
        self.add_sstable(disk_table);
        Arc::clone(&self.config.metrics).flushed_bytes.fetch_add(size, Ordering::Relaxed);


        Ok(())
    }

    fn add_sstable(&mut self, disk_table: DiskSSTable) {
        self.disk_sstables.insert(disk_table.path(), disk_table);
    }

    fn remove_sstable<P: AsRef<Path>>(&mut self, p: P) {
        if let Some((_, table)) = self.disk_sstables.remove_entry(&p.as_ref().to_path_buf()) {
            table.drop_and_remove_file().unwrap(); // making this fatal for now
        }
    }

    fn check_flush(&mut self) -> Result<()> {
        if self.memtable.as_ref().size()? >= self.config.max_memory_bytes as _ {
            self.flush_to_disk()?;
        }
        Ok(())
    }

    fn lookup_disk(&self, k: &[u8]) -> Result<Option<Vec<u8>>> {
        for (_path, table) in self.disk_sstables.iter() {
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

#[cfg(test)]
mod test {
    use std::sync::{
        atomic::{AtomicU64, AtomicUsize},
        Arc,
    };

    use super::*;

    #[test]
    fn flushes_data_to_disk_when_mem_size() {
        let flushed = Arc::new(AtomicUsize::new(0));
        let (compactor_evt_tx, compactor_evt_rx) = sync_channel(1);
        let metrics = Arc::new(DatabaseMetrics::default());
        let config = DatabaseConfig {
            max_memory_bytes: 30,
            metrics: Arc::clone(&metrics),
            ..DatabaseConfig::default()
        };
        let mut engine = Database::new(config, compactor_evt_tx);
        engine.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        engine.put(b"key2".to_vec(), b"value2".to_vec()).unwrap();
        engine.put(b"key3".to_vec(), b"value3".to_vec()).unwrap();
        engine.put(b"key4".to_vec(), b"value4".to_vec()).unwrap();
        assert_eq!(metrics.flushed_bytes.load(std::sync::atomic::Ordering::SeqCst), 30);

        assert_eq!(engine.get(&b"key1".to_vec()).unwrap().unwrap(), b"value1".to_vec());
        assert_eq!(engine.get(&b"key2".to_vec()).unwrap().unwrap(), b"value2".to_vec());
        assert_eq!(engine.get(&b"key3".to_vec()).unwrap().unwrap(), b"value3".to_vec());
        assert_eq!(engine.get(&b"key4".to_vec()).unwrap().unwrap(), b"value4".to_vec());

        let evt = compactor_evt_rx.recv().unwrap();
        let evt_is_new_sstable = if let CompactorCommand::NewSSTable(_) = evt {
            true
        } else {
            false
        };

        assert!(evt_is_new_sstable);

    }
}
