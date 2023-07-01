use super::CompactorCommand;
use crate::{
    error::*,
    sstable::DiskSSTable,
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


/// Just merges sstables when there are X amount of them
///
#[derive(Default)]
struct SimpleCompactorState {
    tracked_sstables: HashMap<PathBuf, DiskSSTable>,
}

impl SimpleCompactorState {
    ///
    /// Returns a Vec<> of paths to delete as well as a new SSTable
    fn compact(&mut self) -> (Vec<PathBuf>, DiskSSTable) {
        let tracked = std::mem::take(&mut self.tracked_sstables)
            .into_values()
            .map(|table| table.iter_key_values())
            .collect::<Vec<_>>();
        //let sorted_iter = SortedDiskSSTableKeyValueIterator::new(tracked);

        //let mut left_to_iter = tracked.len();

        //TODO need to refactor iterators, to take ownership
        todo!()
    }
}

#[derive(Clone, Copy)]
struct SimpleCompactorConfig {
    max_ss_tables: usize,
}

impl Default for SimpleCompactorConfig {
    fn default() -> Self {
        Self { max_ss_tables: 10 }
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
                                let (to_delete, new_table) = state.compact();
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
