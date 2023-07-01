use std::{sync::Arc, marker::PhantomData};

use parking_lot::Mutex;

use super::{mappers::{Mapper, KeyValueMapper}, Value, ValueRef, disk::{InternalDiskSSTable, ValueIndex}};
use crate::error::*;


pub struct DiskSSTableKeyValueIterator {
    inner: DiskSSTableIterator<(Vec<u8>, Value), KeyValueMapper>,
}

impl DiskSSTableKeyValueIterator {
    pub fn new(inner: DiskSSTableIterator<(Vec<u8>, Value), KeyValueMapper>) -> Self {
        Self {inner}
    }
    fn get_next(&mut self) -> Option<Result<Option<(Vec<u8>, Vec<u8>)>>> {
        match self.inner.next() {
            Some(Ok((
                k,
                Value {
                    value_ref: ValueRef::MemoryRef(data),
                },
            ))) => Some(Ok(Some((k, data)))),
            Some(Ok((
                _,
                Value {
                    value_ref: ValueRef::Tombstone,
                },
            ))) => Some(Ok(None)),
            Some(Err(e)) => Some(Err(e)),
            _ => None,
        }
    }
}

impl Iterator for DiskSSTableKeyValueIterator {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(next) = self.get_next() {
            match next {
                Ok(Some((k, v))) => {
                    return Some(Ok((k, v)));
                }
                Err(e) => {
                    return Some(Err(e));
                }
                Ok(None) => { /* loop again */ }
            }
        }
        return None;
    }
}


///
/// Iterator over all keys and optionally values in InternalDiskSSTable.
pub struct DiskSSTableIterator<O, M> {
    table: Arc<Mutex<InternalDiskSSTable>>,
    mapper: M,
    index: Option<Result<(Vec<ValueIndex>, Vec<ValueIndex>)>>,
    pos: usize,
    phant: PhantomData<O>,
}

impl<O: Send, M: Mapper<O>> DiskSSTableIterator<O, M> {
    pub fn new(table: Arc<Mutex<InternalDiskSSTable>>, mapper: M) -> Self {
        Self {
            table,
            mapper,
            index: None,
            pos: 0,
            phant: Default::default(),
        }
    }
    fn get_next(&mut self) -> Result<Option<O>> {
        let table = Arc::clone(&self.table);
        let mut table = table.lock();
        let index = self.index.get_or_insert_with(|| table.read_key_index());

        match index {
            Ok((key_idxs, value_idxs)) => {
                let kv_opt = key_idxs.get(self.pos).and_then(|key_idx| {
                    value_idxs
                        .get(self.pos)
                        .map(move |value_idx| (key_idx, value_idx))
                });
                let (key_idx, value_idx) = if let Some(kv) = kv_opt {
                    kv
                } else {
                    return Ok(None);
                };

                let mapped = self
                    .mapper
                    .map(&mut table, (key_idx, value_idx))?;

                self.pos += 1;
                return Ok(Some(mapped));
            }
            Err(e) => Err(Error::Other(e.to_string())),
        }
    }
}

impl<O: Send, M: Mapper<O>> Iterator for DiskSSTableIterator<O, M> {
    type Item = Result<O>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.get_next().transpose();
        next
    }
}