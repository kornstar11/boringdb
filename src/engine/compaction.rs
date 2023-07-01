use super::CompactorCommand;
use crate::{
    error::*,
    sstable::{DiskSSTable, DiskSSTableKeyValueIterator},
};
use std::{
    cmp::Ordering,
    collections::HashMap,
    iter::Peekable,
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

struct SortedDiskSSTableKeyValueIterator {
    iters: Vec<Peekable<DiskSSTableKeyValueIterator>>,
    order: Ordering,
}

impl SortedDiskSSTableKeyValueIterator {
    fn new(iters: Vec<DiskSSTableKeyValueIterator>) -> Self {
        SortedDiskSSTableKeyValueIterator {
            iters: iters.into_iter().map(|it| it.peekable()).collect(),
            order: Ordering::Less,
        }
    }
}

impl Iterator for SortedDiskSSTableKeyValueIterator {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut preferable_key: Option<(usize, &Vec<u8>)> = None;
        for (idx, iter) in self.iters.iter_mut().enumerate() {
            let peeked_key = match iter.peek() {
                Some(Ok((peeked, _))) => Some(peeked),
                Some(Err(e)) => {
                    return Some(Err(Error::Other(e.to_string())));
                }
                None => None,
            };

            let current_prefered_key = preferable_key.map(|(_, k)| k);

            if peeked_key.cmp(&current_prefered_key) == self.order || preferable_key.is_none() {
                preferable_key = peeked_key.map(|k| (idx, k))
            }
        }
        if let Some((idx, _)) = preferable_key {
            if let Some(ref mut it) = self.iters.get_mut(idx) {
                return it.next();
            }
        }

        None
    }
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
        let sorted_iter = SortedDiskSSTableKeyValueIterator::new(tracked);

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
