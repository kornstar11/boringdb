use async_trait::async_trait;

use super::disk::{InternalDiskSSTable, ValueIndex};
use super::Value;
use crate::error::*;

#[async_trait]
pub trait Mapper<O> : Send + Sync {
    async fn map(&self, table: &mut InternalDiskSSTable, v: (&ValueIndex, &ValueIndex)) -> Result<O>;
}

///
/// KeyValue Mappers
pub struct KeyValueMapper;

#[async_trait]
impl Mapper<(Vec<u8>, Value)> for KeyValueMapper {
    async fn map(
        &self,
        table: &mut InternalDiskSSTable,
        kv_idxs: (&ValueIndex, &ValueIndex),
    ) -> Result<(Vec<u8>, Value)> {
        let (key_idx, value_idx) = kv_idxs;
        let value_buf = table.read_by_value_idx(&value_idx).await?;
        let key_buf = table.read_by_value_idx(key_idx).await?;
        Ok((key_buf, Value::decode(&value_buf)))
    }
}

//
/// KeyIndex Mappers
pub struct KeyIndexMapper;

#[async_trait]
impl Mapper<(Vec<u8>, (ValueIndex, ValueIndex))> for KeyIndexMapper {
    async fn map(
        &self,
        table: &mut InternalDiskSSTable,
        kv_idxs: (&ValueIndex, &ValueIndex),
    ) -> Result<(Vec<u8>, (ValueIndex, ValueIndex))> {
        let (key_idx, value_idx) = kv_idxs;
        let key_buf = table.read_by_value_idx(key_idx).await?;
        Ok((key_buf, (key_idx.to_owned(), value_idx.to_owned())))
    }
}

///
/// KeyMapper
pub struct KeyMapper;

#[async_trait]
impl Mapper<Vec<u8>> for KeyMapper {
    async fn map(
        &self,
        table: &mut InternalDiskSSTable,
        kv_idxs: (&ValueIndex, &ValueIndex),
    ) -> Result<Vec<u8>> {
        let (key_idx, _) = kv_idxs;
        let key_buf = table.read_by_value_idx(key_idx).await?;
        Ok(key_buf)
    }
}

///
/// ValueMapper
pub struct ValueMapper;

#[async_trait]
impl Mapper<Value> for ValueMapper {
    async fn map(
        &self,
        table: &mut InternalDiskSSTable,
        kv_idxs: (&ValueIndex, &ValueIndex),
    ) -> Result<Value> {
        let (_key_idx, value_idx) = kv_idxs;
        let value_buf = table.read_by_value_idx(&value_idx).await?;
        Ok(Value::decode(&value_buf))
    }
}
