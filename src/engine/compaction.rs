use std::{sync::mpsc::{sync_channel, SyncSender, Receiver}, thread::{JoinHandle, spawn}};
use crate::error::*;
use super::CompactorCommand;

pub trait CompactorFactory: Send {
    fn start(&self, compactor_evt_rx: Receiver<CompactorCommand>) -> (Receiver<CompactorCommand>, JoinHandle<Result<()>>);
    fn clone(&self) -> Box<dyn CompactorFactory>;
}

///
/// Base on the concepts of leveldb, all level 0 tables are tracked to a certain point, then merged to level 1.
/// level 1 and above are made so that nothing is overlaping.
#[derive(Default)]
pub struct LevelCompactorFactory{}

impl CompactorFactory for LevelCompactorFactory {
    fn clone(&self) -> Box<dyn CompactorFactory> {
        Box::new(Self {})
    }
    fn start(&self, compactor_evt_rx: Receiver<CompactorCommand>) -> (Receiver<CompactorCommand>, JoinHandle<Result<()>>) {
        let (tx, rx) = sync_channel(1);
        (rx, spawn(move || {
            while let Ok(evt) = compactor_evt_rx.recv() {

            }
            Ok(())
        }))
        
    }
}

