use std::path::PathBuf;

use crate::sstable::DiskSSTable;

mod compaction;
mod database;

///
/// Commands used between the database and compactor
pub enum CompactorCommand {
    NewSSTable(DiskSSTable),
    RemoveSSTables(Vec<PathBuf>),
}
