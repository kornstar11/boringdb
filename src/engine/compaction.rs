use super::{CompactorCommand, database::DatabaseConfig};
use crate::{
    error::*,
    sstable::{DiskSSTable, SortedDiskSSTableKeyValueIterator},
};
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::mpsc::{sync_channel, Receiver},
    thread::{spawn, JoinHandle},
};

pub trait CompactorFactory: Send {
    fn start(
        &self,
        compactor_evt_rx: Receiver<CompactorCommand>,
    ) -> (Receiver<CompactorCommand>, JoinHandle<Result<()>>);
    fn clone(&self) -> Box<dyn CompactorFactory>;
}


/// Just merges sstables when there are X amount of sstables
///
#[derive(Default)]
struct SimpleCompactorState {
    config: DatabaseConfig,
    tracked_sstables: HashMap<PathBuf, DiskSSTable>,
}

impl SimpleCompactorState {
    ///
    /// Returns a Vec<> of paths to delete as well as a new SSTable
    fn compact(&mut self) -> Result<(Vec<PathBuf>, DiskSSTable)> {
        let mut to_merge = std::mem::take(&mut self.tracked_sstables)
            .into_values()
            .collect::<Vec<_>>();
        let iters = to_merge.iter()
            .map(|table| table.iter_key_idxs())
            .collect::<Vec<_>>();
        let sorted_iter = SortedDiskSSTableKeyValueIterator::new(iters).collect::<Result<Vec<_>>>()?;
        let key_it = sorted_iter
            .iter()
            .map(|(k, _, _, _)| {k.to_vec()});
        let value_it = sorted_iter
            .iter()
            .map(|(_, idx, _, vidx)| {
                if let Some(table) = to_merge.get_mut(*idx) {
                    table.read_value_by_value_idx(&vidx).map(|v| v.value_ref)
                } else {
                    Err(Error::Other(String::from("while compacting, unable to locate indexed table.")))
                }
            });
        let path = self.config.sstable_path()?;
        let new_ss_table = DiskSSTable::convert_from_iter(path, key_it, value_it)?;
        Ok((to_merge.into_iter().map(|table| table.path()).collect(), new_ss_table))
    }
}

#[derive(Clone, Copy)]
struct SimpleCompactorConfig {
    max_ss_tables: usize,
}

impl Default for SimpleCompactorConfig {
    fn default() -> Self {
        Self { max_ss_tables: 2 }
    }
}

#[derive(Default)]
pub struct SimpleCompactorFactory {
    config: SimpleCompactorConfig,
}

impl CompactorFactory for SimpleCompactorFactory {
    fn clone(&self) -> Box<dyn CompactorFactory> {
        Box::new(Self {
            config: self.config,
        })
    }
    fn start(
        &self,
        compactor_evt_rx: Receiver<CompactorCommand>,
    ) -> (Receiver<CompactorCommand>, JoinHandle<Result<()>>) {
        let (tx, rx) = sync_channel(1);
        let mut state = SimpleCompactorState::default();
        let config = self.config;
        (
            rx,
            spawn(move || {
                while let Ok(evt) = compactor_evt_rx.recv() {
                    match evt {
                        CompactorCommand::NewSSTable(table) => {
                            state.tracked_sstables.insert(table.path(), table);
                            if state.tracked_sstables.len() >= config.max_ss_tables {
                                let (to_delete, new_table) = state.compact().unwrap();
                                if let Err(_) = tx.send(CompactorCommand::NewSSTable(new_table)) {
                                    // log::info!("Closing")
                                    break;
                                }
                                if let Err(_) = tx.send(CompactorCommand::RemoveSSTables(to_delete))
                                {
                                    break;
                                }
                            }
                        }
                        CompactorCommand::RemoveSSTables(paths) => {
                            for path in paths {
                                state.tracked_sstables.remove(&path);
                            }
                        }
                    }
                }
                Ok(())
            }),
        )
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn simple_compactor_can_merge_two_tables() {

    }
}
