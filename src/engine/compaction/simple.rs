use super::{CompactorCommand, CompactorFactory};
use crate::{
    error::*,
    sstable::{DiskSSTable, SortedDiskSSTableKeyValueIterator}, SSTableNamer,
};
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::mpsc::{sync_channel, Receiver},
    thread::{JoinHandle}
};

/// Just merges sstables when there are X amount of sstables
///
#[derive(Default)]
struct SimpleCompactorState {
    config: SimpleCompactorConfig,
    tracked_sstables: HashMap<PathBuf, DiskSSTable>,
}

impl SimpleCompactorState {
    ///
    /// Returns a Vec<> of paths to delete as well as a new SSTable, we iterate first over the keys checking if there are duplicates. Duplicates
    /// will not have their enum_idx added to the enumeration_idx Deque. When we iterate the values we check to see if the head of the Deque matches our current
    /// enum_idx if it does we pop it and return the value, otherwise we continue.
    fn compact(&mut self) -> Result<(Vec<PathBuf>, DiskSSTable)> {
        let mut seen: HashSet<Vec<u8>> = HashSet::new();
        let mut to_merge = std::mem::take(&mut self.tracked_sstables)
            .into_values()
            .collect::<Vec<_>>();
        let iters = to_merge
            .iter()
            .map(|table| table.iter_key_idxs())
            .collect::<Vec<_>>();
        let sorted_iter =
            SortedDiskSSTableKeyValueIterator::new(iters).filter_map(|res| {
                match res {
                    Ok((k, k_idx, v, v_idx)) => {
                        if seen.contains(&k) {
                            None
                        } else {
                            seen.insert(k.to_vec());
                            Some(Ok((k, k_idx, v, v_idx)))
                        }
                    },
                    Err(e) => Some(Err(e))
                }
            }).collect::<Result<Vec<_>>>()?;
        //let mut enumeration_idx_inner = Rc::clone(&enumeration_idx);
        let key_it = sorted_iter
            .iter()
            .map(|(k, _, _, _)| {
                k.to_vec()
            });
        let value_it = sorted_iter
            .iter()
            .map(|(_, idx, _, vidx)| {
            if let Some(table) = to_merge.get_mut(*idx) {
                table.read_value_by_value_idx(&vidx).map(|v| v.value_ref)
            } else {
                Err(Error::Other(String::from(
                    "while compacting, unable to locate indexed table.",
                )))
            }
        });
        let path = self.config.namer.sstable_path()?;
        let new_ss_table = DiskSSTable::convert_from_iter(path, key_it, value_it)?;
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
        Self {
            max_ss_tables: 2,
            namer: SSTableNamer::default(),
        }
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
        compactor_evt_rx: Receiver<CompactorCommand>,
    ) -> (Receiver<CompactorCommand>, JoinHandle<Result<()>>) {
        let (tx, rx) = sync_channel(1);
        let mut state = SimpleCompactorState {
            config: self.config.clone(),
            ..Default::default()
        };
        let thread_bldr = std::thread::Builder::new().name("SimpleCompaction".into());
        let config = self.config.clone();
        (
            rx,
            thread_bldr
                .spawn(move || {
                    while let Ok(evt) = compactor_evt_rx.recv() {
                        match evt {
                            CompactorCommand::NewSSTable(table) => {
                                state.tracked_sstables.insert(table.path(), table);
                                if state.tracked_sstables.len() >= config.max_ss_tables {
                                    log::info!("Starting compaction...");
                                    let (to_delete, new_table) = state.compact().unwrap();
                                    log::info!("Compaction done [to_delete={}]", to_delete.len());
                                    if let Err(_) = tx.send(CompactorCommand::NewSSTable(new_table))
                                    {
                                        break;
                                    }
                                    if let Err(_) =
                                        tx.send(CompactorCommand::RemoveSSTables(to_delete))
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
                    log::info!("Closing compaction thread");
                    Ok(())
                })
                .unwrap(),
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
    #[test]
    fn simple_compactor_can_drop_dups() {
        let ts = time_ms();
        let mut evens1_path = PathBuf::from("/tmp");
        let mut evens2_path = PathBuf::from("/tmp");
        evens1_path.push(format!("evens_{}", ts));
        evens2_path.push(format!("odds_{}", ts));
        let evens1 =
            DiskSSTable::convert_mem(evens1_path.clone(), generate_memory(generate_even_kvs()))
                .unwrap();
        let evens2 = DiskSSTable::convert_mem(evens2_path.clone(), generate_memory(generate_even_kvs()))
            .unwrap();
        let tracked_sstables = vec![(evens1.path(), evens1), (evens2.path(), evens2)]
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
                ("k2", "v2"),
                ("k4", "v4"),
                ("k6", "v6"),
                ("k8", "v8"),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<Vec<_>>()
        );
    }
}