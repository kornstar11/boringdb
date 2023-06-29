use std::{sync::mpsc::{sync_channel, SyncSender, Receiver}, thread::{JoinHandle, spawn}, collections::HashMap, path::PathBuf};
use crate::{error::*, sstable::DiskSSTable};
use super::CompactorCommand;

pub trait CompactorFactory: Send {
    fn start(&self, compactor_evt_rx: Receiver<CompactorCommand>) -> (Receiver<CompactorCommand>, JoinHandle<Result<()>>);
    fn clone(&self) -> Box<dyn CompactorFactory>;
}

/// Just merges sstables when there are X amount of them
/// 
#[derive(Default)]
struct SimpleCompactorState {
    tracked_sstables: HashMap<PathBuf, DiskSSTable>
}

impl SimpleCompactorState {
    fn compact(&mut self) -> DiskSSTable {
        let mut tracked = std::mem::take(&mut self.tracked_sstables)
            .into_values()
            .collect::<Vec<_>>();
        tracked.sort_by_key(|d| d.path());

        let mut last: Option<DiskSSTable> = None;


    }
    
}


#[derive(Default)]
pub struct SimpleCompactorFactory{max_ss_tables: usize}

impl CompactorFactory for SimpleCompactorFactory {
    fn clone(&self) -> Box<dyn CompactorFactory> {
        Box::new(Self {
            max_ss_tables: self.max_ss_tables
        })
    }
    fn start(&self, compactor_evt_rx: Receiver<CompactorCommand>) -> (Receiver<CompactorCommand>, JoinHandle<Result<()>>) {
        let (tx, rx) = sync_channel(1);
        let mut state = SimpleCompactorState::default();
        (rx, spawn(move || {
            while let Ok(evt) = compactor_evt_rx.recv() {
                match evt {
                    CompactorCommand::NewSSTable(table) => {
                        state.tracked_sstables.insert(table.path(), table);
                        if state.tracked_sstables.len() >= self.max_ss_tables {

                        }
                    },
                    CompactorCommand::RemoveSSTables(path) => {
                        state.tracked_sstables.remove(&path);
                    }
                }

            }
            Ok(())
        }))
        
    }
}

