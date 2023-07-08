use std::{path::PathBuf, time};

use crate::sstable::DiskSSTable;
use crate::error::*;
mod compaction;
mod database;

pub use database::{Database, DatabaseContext};
pub use compaction::{SimpleCompactorFactory, SimpleCompactorConfig};

pub const SSTABLE_FILE_PREFIX: &str = "sstable_";
///
/// Commands used between the database and compactor
pub enum CompactorCommand {
    NewSSTable(DiskSSTable),
    RemoveSSTables(Vec<PathBuf>),
}
#[derive(Clone)]
pub struct SSTableNamer {
    base_dir: PathBuf,
}

impl Default for SSTableNamer {
    fn default() -> Self {
        Self{ base_dir: PathBuf::from("/tmp")}
    }
}

impl SSTableNamer {
    pub fn sstable_path(&self) -> Result<PathBuf> {
        let time = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .map_err(Error::TimeError)?
            .as_micros();
        let mut path = self.base_dir.clone();
        path.push(format!("{}{}.data", SSTABLE_FILE_PREFIX, time));

        Ok(path)
    }
}
