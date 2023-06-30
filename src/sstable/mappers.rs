use super::Value;
use super::disk::{InternalDiskSSTable, ValueIndex};
use crate::error::*;

pub trait Mapper<O> {
    fn map(&self, table: &mut InternalDiskSSTable, v: (&ValueIndex, &ValueIndex)) -> Result<O>;
}

///
/// KeyValue Mappers
pub struct KeyValueMapper;

impl Mapper<(Vec<u8>, Value)> for KeyValueMapper {
    fn map(
        &self,
        table: &mut InternalDiskSSTable,
        kv_idxs: (&ValueIndex, &ValueIndex),
    ) -> Result<(Vec<u8>, Value)> {
        let (key_idx, value_idx) = kv_idxs;
        let value_buf = table.read_by_value_idx(&value_idx)?;
        let key_buf = table.read_by_value_idx(key_idx)?;
        Ok((key_buf, Value::decode(&value_buf)))
    }
}

///
/// KeyMapper
pub struct KeyMapper;

impl Mapper<Vec<u8>> for KeyMapper {
    fn map(
        &self,
        table: &mut InternalDiskSSTable,
        kv_idxs: (&ValueIndex, &ValueIndex),
    ) -> Result<Vec<u8>> {
        let (key_idx, _) = kv_idxs;
        let key_buf = table.read_by_value_idx(key_idx)?;
        Ok(key_buf)
    }
}

///
/// ValueMapper
pub struct ValueMapper;

impl Mapper<Value> for ValueMapper {
    fn map(
        &self,
        table: &mut InternalDiskSSTable,
        kv_idxs: (&ValueIndex, &ValueIndex),
    ) -> Result<Value> {
        let (_key_idx, value_idx) = kv_idxs;
        let value_buf = table.read_by_value_idx(&value_idx)?;
        Ok(Value::decode(&value_buf))
    }
}