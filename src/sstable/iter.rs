use std::{cmp::Ordering, iter::Peekable, marker::PhantomData, sync::Arc};

use parking_lot::Mutex;

use super::{
    disk::{InternalDiskSSTable, ValueIndex},
    mappers::{KeyIndexMapper, KeyValueMapper, Mapper},
    Value, ValueRef,
};
use crate::error::*;

pub struct DiskSSTableKeyValueIterator {
    inner: DiskSSTableIterator<(Vec<u8>, Value), KeyValueMapper>,
}

impl DiskSSTableKeyValueIterator {
    pub fn new(inner: DiskSSTableIterator<(Vec<u8>, Value), KeyValueMapper>) -> Self {
        Self { inner }
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
        let index = self
            .index
            .get_or_insert_with(|| table.read_key_index().map(|idx| idx.as_ref().clone()));

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

                let mapped = self.mapper.map(&mut table, (key_idx, value_idx))?;

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

//pub struct

type KeyIdxIt = DiskSSTableIterator<(Vec<u8>, (ValueIndex, ValueIndex)), KeyIndexMapper>;

///
/// Iterator takes multiple Iterators from multiple sstables and returns a tuple:
/// (sstable_index, KeyIndex), (sstable_index, ValueIndex)
/// the `sstable_index`, is the index of the iters vec, that the caller should use to look up their key.
pub struct SortedDiskSSTableKeyValueIterator {
    iters: Vec<Peekable<KeyIdxIt>>,
    order: Ordering,
}

impl SortedDiskSSTableKeyValueIterator {
    pub fn new(iters: Vec<KeyIdxIt>) -> Self {
        SortedDiskSSTableKeyValueIterator {
            iters: iters.into_iter().map(|it| it.peekable()).collect(),
            order: Ordering::Less,
        }
    }
}

impl Iterator for SortedDiskSSTableKeyValueIterator {
    type Item = Result<(Vec<u8>, usize, ValueIndex, ValueIndex)>;

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

            if peeked_key.is_some()
                && (peeked_key.cmp(&current_prefered_key) == self.order || preferable_key.is_none())
            {
                preferable_key = peeked_key.map(|k| (idx, k))
            }
        }
        if let Some((idx, _)) = preferable_key {
            if let Some(ref mut it) = self.iters.get_mut(idx) {
                let result = it.next();
                return result.map(|res| {
                    res.map(|t| {
                        let (k, (ki, vi)) = t;
                        (k, idx, ki, vi)
                    })
                });
            }
        }

        None
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::sstable::test::*;

    #[test]
    fn match_with_one_it() {
        let it = generate_disk(generate_memory(generate_kvs()));
        let it = DiskSSTableIterator::new(Arc::new(Mutex::new(it)), KeyIndexMapper {});
        let sorted = SortedDiskSSTableKeyValueIterator::new(vec![it]);
        let sorted: Result<Vec<_>> = sorted.collect();
        let sorted = sorted
            .unwrap()
            .into_iter()
            .map(|x| String::from_utf8(x.0).unwrap())
            .collect::<Vec<_>>();
        println!("{:?}", sorted);
        assert_eq!(
            sorted,
            vec!["k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7", "k8", "k9"]
        )
    }

    #[test]
    fn sorted_disk_iter_can_sort_multiple() {
        let even = generate_disk(generate_memory(generate_even_kvs()));
        let even = DiskSSTableIterator::new(Arc::new(Mutex::new(even)), KeyIndexMapper {});
        let odd = generate_disk(generate_memory(generate_odd_kvs()));
        let odd = DiskSSTableIterator::new(Arc::new(Mutex::new(odd)), KeyIndexMapper {});

        let sorted = SortedDiskSSTableKeyValueIterator::new(vec![odd, even]);

        let sorted: Result<Vec<_>> = sorted.collect();
        let sorted = sorted
            .unwrap()
            .into_iter()
            .map(|x| String::from_utf8(x.0).unwrap())
            .collect::<Vec<_>>();
        println!("{:?}", sorted);

        assert_eq!(
            sorted,
            vec!["k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7", "k8", "k9"]
        )
    }
}
