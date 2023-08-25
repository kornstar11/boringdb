use super::{CompactorCommand, CompactorFactory};
use crate::{
    error::*,
    sstable::{DiskSSTable, SortedDiskSSTableKeyValueIterator}, SSTableNamer,
};
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::mpsc::{sync_channel, Receiver},
    thread::JoinHandle
};

struct Level {
    tracked_sstables: HashMap<PathBuf, DiskSSTable>
}

///
/// Based on Googles LevelDB we aim to provide mostly non overlapping sstables per level.
/// https://github.com/google/leveldb/blob/main/doc/impl.md#compactions

struct LevelCompactorState {
    levels: Vec<Level>
}