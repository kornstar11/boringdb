use parking_lot::Mutex;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{sync_channel, SyncSender};
use std::sync::Arc;
use std::thread::spawn;
use std::{collections::BTreeMap, path::PathBuf};

use crate::{error::*, sstable::*};

use super::compaction::{CompactorFactory, SimpleCompactorFactory};
use super::{CompactorCommand, SSTableNamer, SSTABLE_FILE_PREFIX};

///
/// Engine handle managment of the SSTables. It is responsible for converting a memory SSTable into a disk SSTable.
/// Also handled are management of Bloomfilters for the DiskSSTables.
///
/// TODO need a WAL to make memtable safe...

#[derive(Default)]
pub struct DatabaseMetrics {
    flushed_bytes: AtomicUsize,
}
pub struct DatabaseContext {
    pub sstable_namer: SSTableNamer,
    pub max_memory_bytes: u64,
    pub compactor_factory: Box<dyn CompactorFactory>,
    pub metrics: Arc<DatabaseMetrics>,
}

impl Clone for DatabaseContext {
    fn clone(&self) -> Self {
        Self {
            sstable_namer: self.sstable_namer.clone(),
            max_memory_bytes: self.max_memory_bytes.clone(),
            compactor_factory: self.compactor_factory.clone(),
            metrics: self.metrics.clone(),
        }
    }
}

impl Default for DatabaseContext {
    fn default() -> Self {
        Self {
            sstable_namer: SSTableNamer::default(),
            max_memory_bytes: 1024 * 1000, // 1MB
            compactor_factory: Box::new(SimpleCompactorFactory::default()),
            metrics: Arc::new(DatabaseMetrics::default()),
        }
    }
}

pub struct Database {
    config: DatabaseContext,
    memtable: Memtable,
    disk_sstables: BTreeMap<PathBuf, DiskSSTable>,
    compactor_evt_tx: SyncSender<CompactorCommand>,
}

impl Database {
    pub fn start(config: DatabaseContext) -> Arc<Mutex<Self>> {
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
                    }
                    CompactorCommand::RemoveSSTables(to_drops) => {
                        for to_drop in to_drops {
                            db.remove_sstable(to_drop);
                        }
                    }
                }
            }
        });

        db
    }

    fn new(config: DatabaseContext, compactor_evt_tx: SyncSender<CompactorCommand>) -> Database {
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
    pub fn flush_to_disk(&mut self) -> Result<()> {
        // make path
        let path = self.config.sstable_namer.sstable_path()?;
        log::debug!("Attempt flush of memtable to: {:?}", path);

        let memory_sstable = std::mem::take(&mut self.memtable);
        let size = memory_sstable.as_ref().size()?;
        if size == 0 {
            return Ok(());
        }
        let disk_table = DiskSSTable::convert_mem(path.clone(), memory_sstable)?;
        // update compactor
        self.compactor_evt_tx
            .send(CompactorCommand::NewSSTable(disk_table.clone()))
            .map_err(|_| Error::SendError)?;
        self.add_sstable(disk_table);
        Arc::clone(&self.config.metrics)
            .flushed_bytes
            .fetch_add(size, Ordering::Relaxed);

        log::info!("Flushed memtable to: {:?}", path);
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
    use std::sync::Arc;

    use super::*;

    #[test]
    fn flushes_data_to_disk_when_mem_size() {
        let (compactor_evt_tx, compactor_evt_rx) = sync_channel(1);
        let metrics = Arc::new(DatabaseMetrics::default());
        let config = DatabaseContext {
            max_memory_bytes: 30,
            metrics: Arc::clone(&metrics),
            ..DatabaseContext::default()
        };
        let mut engine = Database::new(config, compactor_evt_tx);
        engine.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        engine.put(b"key2".to_vec(), b"value2".to_vec()).unwrap();
        engine.put(b"key3".to_vec(), b"value3".to_vec()).unwrap();
        engine.put(b"key4".to_vec(), b"value4".to_vec()).unwrap();
        assert_eq!(
            metrics
                .flushed_bytes
                .load(std::sync::atomic::Ordering::SeqCst),
            30
        );

        assert_eq!(
            engine.get(&b"key1".to_vec()).unwrap().unwrap(),
            b"value1".to_vec()
        );
        assert_eq!(
            engine.get(&b"key2".to_vec()).unwrap().unwrap(),
            b"value2".to_vec()
        );
        assert_eq!(
            engine.get(&b"key3".to_vec()).unwrap().unwrap(),
            b"value3".to_vec()
        );
        assert_eq!(
            engine.get(&b"key4".to_vec()).unwrap().unwrap(),
            b"value4".to_vec()
        );

        let evt = compactor_evt_rx.recv().unwrap();
        let evt_is_new_sstable = if let CompactorCommand::NewSSTable(_) = evt {
            true
        } else {
            false
        };

        assert!(evt_is_new_sstable);
    }
}
