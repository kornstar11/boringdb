use std::path::PathBuf;

use crate::sstable::DiskSSTable;

mod database;
mod compaction;

///
/// Commands used between the database and compactor
enum CompactorCommand {
    NewSSTable(DiskSSTable),
    RemoveSSTables(PathBuf)
}