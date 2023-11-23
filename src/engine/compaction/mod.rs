use std::{sync::mpsc::Receiver, thread::JoinHandle};

use crate::CompactorCommand;
use crate::error::*;

mod simple;
mod level;
pub use {simple::SimpleCompactorConfig, simple::SimpleCompactorFactory};

// pub struct CompactionResult {
//     pub to_delete: Vec<String>,
//     pub new_table: String,
// }

pub trait CompactorFactory: Send {
    fn start(
        &self,
        compactor_evt_rx: Receiver<CompactorCommand>,
    ) -> (Receiver<CompactorCommand>, JoinHandle<Result<()>>);
    fn clone(&self) -> Box<dyn CompactorFactory>;
}