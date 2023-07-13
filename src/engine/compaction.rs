use futures_util::{StreamExt, FutureExt, TryFutureExt};
use tokio::{task::JoinHandle, sync::mpsc::{channel, Receiver}, spawn};

use super::{CompactorCommand, SSTableNamer};
use crate::{
    error::*,
    sstable::{DiskSSTable, SortedDiskSSTableKeyValueIterator},
};
use std::{
    collections::HashMap,
    path::PathBuf
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
    config: SimpleCompactorConfig,
    tracked_sstables: HashMap<PathBuf, DiskSSTable>,
}

impl SimpleCompactorState {
    ///
    /// Returns a Vec<> of paths to delete as well as a new SSTable
    async fn compact(&mut self) -> Result<(Vec<PathBuf>, DiskSSTable)> {
        let mut to_merge = std::mem::take(&mut self.tracked_sstables)
            .into_values()
            .collect::<Vec<_>>();
        let iters = to_merge
            .iter()
            .map(|table| table.iter_key_idxs())
            .collect::<Vec<_>>();
        let sorted_iter =
            SortedDiskSSTableKeyValueIterator::new(iters)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        // todo 
        let key_it = futures_util::stream::iter(sorted_iter.clone().into_iter().map(|(k, _, _, _)| k.to_vec())).boxed();
        let to_merge_inner = to_merge.clone();
        let value_it = futures_util::stream::iter(sorted_iter.clone().into_iter()).then(move |(_, idx, _, vidx)| {
            let mut to_merge_inner = to_merge_inner.clone();
            async move {
                if let Some(table) = to_merge_inner.get_mut(idx) {
                    table
                        .read_value_by_value_idx(&vidx)
                        .and_then(|v| async move{ Ok(v.value_ref)})
                        .await
                } else {
                    Err(Error::Other(String::from(
                        "while compacting, unable to locate indexed table.",
                    )))
                }
            }
        }).boxed();
        let path = self.config.namer.sstable_path()?;
        let new_ss_table = DiskSSTable::convert_from_iter(path, key_it, value_it)
            .await?;
        Ok((
            to_merge.into_iter().map(|table| table.path()).collect(),
            new_ss_table,
        ))
    }
}

#[derive(Clone)]
pub struct SimpleCompactorConfig {
    max_ss_tables: usize,
    namer: SSTableNamer,
}

impl Default for SimpleCompactorConfig {
    fn default() -> Self {
        Self { max_ss_tables: 2, namer: SSTableNamer::default() }
    }
}

#[derive(Default)]
pub struct SimpleCompactorFactory {
    config: SimpleCompactorConfig,
}

impl CompactorFactory for SimpleCompactorFactory {
    fn clone(&self) -> Box<dyn CompactorFactory> {
        Box::new(Self {
            config: self.config.clone(),
        })
    }
    fn start(
        &self,
        mut compactor_evt_rx: Receiver<CompactorCommand>,
    ) -> (Receiver<CompactorCommand>, JoinHandle<Result<()>>) {
        let (tx, rx) = channel(1);
        let mut state = SimpleCompactorState{
            config: self.config.clone(),
            ..Default::default()
        };
        let config = self.config.clone();
        (
            rx,
            spawn(async move {
                while let Some(evt) = compactor_evt_rx.recv().await {
                    match evt {
                        CompactorCommand::NewSSTable(table) => {
                            state.tracked_sstables.insert(table.path(), table);
                            if state.tracked_sstables.len() >= config.max_ss_tables {
                                let (to_delete, new_table) = state.compact().await.unwrap();
                                if let Err(_) = tx.send(CompactorCommand::NewSSTable(new_table)).await {
                                    break;
                                }
                                if let Err(_) = tx.send(CompactorCommand::RemoveSSTables(to_delete)).await
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
                log::info!("Closing compactor loop");
                Ok(())
            }),
        )
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::sstable::test::*;
    #[test]
    fn simple_compactor_can_merge_two_tables() {
        let ts = time_ms();
        let mut evens_path = PathBuf::from("/tmp");
        let mut odds_path = PathBuf::from("/tmp");
        evens_path.push(format!("evens_{}", ts));
        odds_path.push(format!("odds_{}", ts));
        let evens =
            DiskSSTable::convert_mem(evens_path.clone(), generate_memory(generate_even_kvs()))
                .unwrap();
        let odds = DiskSSTable::convert_mem(odds_path.clone(), generate_memory(generate_odd_kvs()))
            .unwrap();
        let tracked_sstables = vec![(evens.path(), evens), (odds.path(), odds)]
            .into_iter()
            .collect();
        let mut compactor_state = SimpleCompactorState {
            tracked_sstables,
            ..Default::default()
        };
        let (to_delete, new_table) = compactor_state.compact().unwrap();
        assert_eq!(to_delete.len(), 2);
        // check new sstable contains the contents of evens and odds
        let new_contents = new_table
            .iter_key_values()
            .collect::<Result<Vec<_>>>()
            .unwrap()
            .into_iter()
            .map(|(k, v)| (String::from_utf8(k).unwrap(), String::from_utf8(v).unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(
            new_contents,
            vec![
                ("k0", "v0"),
                ("k1", "v1"),
                ("k2", "v2"),
                ("k3", "v3"),
                ("k4", "v4"),
                ("k5", "v5"),
                ("k6", "v6"),
                ("k7", "v7"),
                ("k8", "v8"),
                ("k9", "v9")
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<Vec<_>>()
        );
    }
}
