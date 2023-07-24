use std::collections::HashMap;

use bytes::BytesMut;
use clickhouse::{Row as ClickHouseRow, Client};
use itertools::Itertools;
use risingwave_common::{buffer::Bitmap, util::iter_util::ZipEqFast};
use risingwave_common::row::Row;
use risingwave_common::catalog::Schema;
use anyhow::anyhow;
use risingwave_common::types::{Int256, Serial, Decimal, Interval, Date, Time, Timestamp, Timestamptz, JsonbRef, StructRef, ListRef, JsonbVal, ScalarRefImpl, DataType};
use risingwave_rpc_client::ConnectorClient;
use serde::Serialize;
use risingwave_common::array::{Op, RowRef, StreamChunk};
use serde::ser::SerializeSeq;
use serde_derive::Deserialize;
use serde_with::serde_as;
use crate::common::ClickHouseCommon;
use crate::sink::Result;
use super::{SinkError, SINK_TYPE_APPEND_ONLY, SINK_TYPE_UPSERT, Sink, DummySinkCommitCoordinator, SinkWriterParam, SinkWriter, SINK_TYPE_OPTION};



struct ClickHouseColumn<'a>{
    // row: Row,
    schema: Schema,
    row: Vec<(Option<ClickHouseField<'a>>,bool)>, 
}

impl ClickHouseRow for ClickHouseColumn<'_>{
    const COLUMN_NAMES: &'static [&'static str] = &[];


    fn get_column_names() -> Vec<String> {
        Self::COLUMN_NAMES
            .into_iter()
            .map(|s| s.to_string())
            .collect()
    }
}

enum ClickHouseField<'a> {
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Serial(Serial),
    Float32(f32),
    Float64(f64),
    Utf8(&'a str),
    Bool(bool),
    Date(i32),
    Timestamp(i64),
    List(Vec<Option<ClickHouseField<'a>>>),
    Bytea(&'a[u8]),
    // None,
}

impl<'a> ClickHouseField<'a> {
    pub fn from_scalar_ref(data: Option<ScalarRefImpl<'a>>,accuracy_time: u8, can_null: bool) -> Result<Option<Self>>{
        if data == None{
            if can_null{
                return Ok(None);
            }else{
                return Err(SinkError::ClickHouse(
                    "clickhouse column can not insert null".to_string(),
                ));
            }
        }
        let data = match data.unwrap() {
            ScalarRefImpl::Int16(v) => ClickHouseField::Int16(v),
            ScalarRefImpl::Int32(v) => ClickHouseField::Int32(v),
            ScalarRefImpl::Int64(v) => ClickHouseField::Int64(v),
            ScalarRefImpl::Int256(_) => return Err(SinkError::ClickHouse(
                "clickhouse can not support Int256".to_string(),
            )),
            ScalarRefImpl::Serial(v) => ClickHouseField::Serial(v),
            ScalarRefImpl::Float32(v) => ClickHouseField::Float32(v.into_inner()),
            ScalarRefImpl::Float64(v) => ClickHouseField::Float64(v.into_inner()),
            ScalarRefImpl::Utf8(v) => ClickHouseField::Utf8(v),
            ScalarRefImpl::Bool(v) => ClickHouseField::Bool(v),
            ScalarRefImpl::Decimal(_) => todo!(),
            ScalarRefImpl::Interval(_) => return Err(SinkError::ClickHouse(
                "clickhouse can not support Interval".to_string(),
            )),
            ScalarRefImpl::Date(v) => {
                // let days = v.get_nums_days_unix_epoch();
                ClickHouseField::Date(0)
            },
            ScalarRefImpl::Time(_) => return Err(SinkError::ClickHouse(
                "clickhouse can not support Time".to_string(),
            )),
            ScalarRefImpl::Timestamp(v) => {
                // let time = v.get_timestamp_nanos() / 10_i32.pow((9 - accuracy_time).into()) as i64;
                ClickHouseField::Timestamp(0)
            },
            ScalarRefImpl::Timestamptz(_) => return Err(SinkError::ClickHouse(
                "clickhouse can not support Timestamptz".to_string(),
            )),
            ScalarRefImpl::Jsonb(_) => todo!(),
            ScalarRefImpl::Struct(_) => todo!(),
            ScalarRefImpl::List(v) => {
                let mut vec = vec![];
                for i in v.iter(){
                    vec.push(Self::from_scalar_ref(i,accuracy_time,can_null)?)
                }
                ClickHouseField::List(vec)
            },
            ScalarRefImpl::Bytea(_) => return Err(SinkError::ClickHouse(
                "clickhouse can not support Bytea".to_string(),
            )),
    };
    Ok(Some(data))
    }
}

impl Serialize for ClickHouseField<'_>{
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer {
        match self{
            ClickHouseField::Int16(v) => serializer.serialize_i16(*v),
            ClickHouseField::Int32(v) => serializer.serialize_i32(*v),
            ClickHouseField::Int64(v) => serializer.serialize_i64(*v),
            ClickHouseField::Serial(v) => v.serialize(serializer),
            ClickHouseField::Float32(v) => serializer.serialize_f32(*v),
            ClickHouseField::Float64(v) => serializer.serialize_f64(*v),
            ClickHouseField::Utf8(v) => serializer.serialize_bytes(v.as_bytes()),
            ClickHouseField::Bool(v) => serializer.serialize_bool(*v),
            ClickHouseField::Date(v) => serializer.serialize_i32(*v),
            ClickHouseField::Timestamp(v) => serializer.serialize_i64(*v),
            ClickHouseField::List(v) => {
                let mut s = serializer.serialize_seq(Some(v.len()))?;
                for i in v.iter(){
                    s.serialize_element(i)?;
                }
                s.end()
            },
            ClickHouseField::Bytea(v) => serializer.serialize_bytes(v),
        }
    }
}



#[serde_as]
#[derive(Clone, Debug, Deserialize)]
pub struct ClickHouseConfig {
    #[serde(flatten)]
    pub common: ClickHouseCommon,

    pub r#type: String, // accept "append-only" or "upsert"
}

#[derive(Clone, Debug)]
pub struct ClickHouseSink {
    pub config: ClickHouseConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
}

impl Serialize for ClickHouseColumn<'_>{
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer {
        for (column,can_null) in self.row{
            if can_null{
                match column{
                    Some(v) => v.serialize(serializer)?,
                    None => serializer.serialize_none()?,
                }                
            }else{
                if let Some(v) = column{
                    v.serialize(serializer)?
                }else{
                    return Err(serde::ser::Error::custom("clickhouse column can not insert null"));
                }
            }
        }
        Ok(())
    }
}

impl ClickHouseConfig {
    pub fn from_hashmap(properties: HashMap<String, String>) -> Result<Self> {
        let config =
            serde_json::from_value::<ClickHouseConfig>(serde_json::to_value(properties).unwrap())
                .map_err(|e| SinkError::Config(anyhow!(e)))?;
        if config.r#type != SINK_TYPE_APPEND_ONLY && config.r#type != SINK_TYPE_UPSERT {
            return Err(SinkError::Config(anyhow!(
                "`{}` must be {}, or {}",
                SINK_TYPE_OPTION,
                SINK_TYPE_APPEND_ONLY,
                SINK_TYPE_UPSERT
            )));
        }
        Ok(config)
    }
}

impl ClickHouseSink {
    pub fn new(
        config: ClickHouseConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        Ok(Self {
            config,
            schema,
            pk_indices,
            is_append_only,
        })
    }

    /// Check that the column names and types of risingwave and clickhouse are identical
    fn check_column_name_and_type(&self, clickhouse_column: Vec<SystemColumn>) -> Result<()> {
        let fields = self.schema.fields().clone();
        assert_eq!(
            fields.len(),
            clickhouse_column.len(),
            "Schema len not match"
        );
        fields
            .iter()
            .zip_eq_fast(clickhouse_column)
            .try_for_each(|(key, value)| {
                assert_eq!(
                    key.name, value.name,
                    "Column name is not match, risingwave is {:?} and clickhouse is {:?}",
                    key.name, value.name
                );
                Self::check_and_correct_column_type(&key.data_type, &value)
            })?;
        Ok(())
    }
    /// Check that the column types of risingwave and clickhouse are identical
    fn check_and_correct_column_type(
        fields_type: &DataType,
        ck_column: &SystemColumn,
    ) -> Result<()> {
        let is_match = match fields_type {
            risingwave_common::types::DataType::Boolean => Ok(ck_column.r#type.contains("Bool")),
            risingwave_common::types::DataType::Int16 => {
                Ok(ck_column.r#type.contains("UInt16") | ck_column.r#type.contains("Int16"))
            }
            risingwave_common::types::DataType::Int32 => {
                Ok(ck_column.r#type.contains("UInt32") | ck_column.r#type.contains("Int32"))
            }
            risingwave_common::types::DataType::Int64 => {
                Ok(ck_column.r#type.contains("UInt64") | ck_column.r#type.contains("Int64"))
            }
            risingwave_common::types::DataType::Float32 => Ok(ck_column.r#type.contains("Float32")),
            risingwave_common::types::DataType::Float64 => Ok(ck_column.r#type.contains("Float64")),
            risingwave_common::types::DataType::Decimal => todo!(),
            risingwave_common::types::DataType::Date => Ok(ck_column.r#type.contains("Date32")),
            risingwave_common::types::DataType::Varchar => Ok(ck_column.r#type.contains("String")),
            risingwave_common::types::DataType::Time => Err(SinkError::ClickHouse(
                "clickhouse can not support Time".to_string(),
            )),
            risingwave_common::types::DataType::Timestamp => {
                Ok(ck_column.r#type.contains("DateTime64"))
            }
            risingwave_common::types::DataType::Timestamptz => Err(SinkError::ClickHouse(
                "clickhouse can not support Timestamptz".to_string(),
            )),
            risingwave_common::types::DataType::Interval => Err(SinkError::ClickHouse(
                "clickhouse can not support Interval".to_string(),
            )),
            risingwave_common::types::DataType::Struct(_) => todo!(),
            risingwave_common::types::DataType::List(list) => {
                Self::check_and_correct_column_type(list.as_ref(), ck_column)?;
                Ok(ck_column.r#type.contains("Array"))
            }
            risingwave_common::types::DataType::Bytea => Err(SinkError::ClickHouse(
                "clickhouse can not support Bytea".to_string(),
            )),
            risingwave_common::types::DataType::Jsonb => todo!(),
            risingwave_common::types::DataType::Serial => {
                Ok(ck_column.r#type.contains("UInt64") | ck_column.r#type.contains("Int64"))
            }
            risingwave_common::types::DataType::Int256 => Err(SinkError::ClickHouse(
                "clickhouse can not support Interval".to_string(),
            )),
        };
        if !is_match? {
            return Err(SinkError::ClickHouse(format!(
                "Column type can not match name is {:?}, risingwave is {:?} and clickhouse is {:?}",
                ck_column.name, fields_type, ck_column.r#type
            )));
        }

        Ok(())
    }


}
#[async_trait::async_trait]
impl Sink for ClickHouseSink {
    type Coordinator = DummySinkCommitCoordinator;
    type Writer = ClickHouseSinkWriter;

    async fn validate(&self, _client: Option<ConnectorClient>) -> Result<()> {
        // For upsert clickhouse sink, the primary key must be defined.
        if !self.is_append_only && self.pk_indices.is_empty() {
            return Err(SinkError::Config(anyhow!(
                "Primary key not defined for upsert clickhouse sink (please define in `primary_key` field)")));
        }

        // check reachability
        let client = self.config.common.build_client()?;
        let query_column = "select distinct ?fields from system.columns where database = ? and table = ? order by ?".to_string();
        let clickhouse_column = client
            .query(&query_column)
            .bind(self.config.common.database.clone())
            .bind(self.config.common.table.clone())
            .bind("position")
            .fetch_all::<SystemColumn>()
            .await?;
        assert!(
            !clickhouse_column.is_empty(),
            "table {:?}.{:?} is not find in clickhouse",
            self.config.common.database,
            self.config.common.table
        );
        self.check_column_name_and_type(clickhouse_column)?;
        Ok(())
    }

    async fn new_writer(&self, _writer_env: SinkWriterParam) -> Result<Self::Writer> {
        Ok(ClickHouseSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
            self.is_append_only,
        )
        .await?)
    }
}
pub struct ClickHouseSinkWriter {
    pub config: ClickHouseConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    client: Client,
    is_append_only: bool,
    // Save some features of the clickhouse column type
    column_correct_vec: Vec<(bool, u8)>,
}

impl ClickHouseSinkWriter {
    pub async fn new(
        config: ClickHouseConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        if !is_append_only {
            tracing::warn!("Update and delete are not recommended because of their impact on clickhouse performance.");
        }
        let client = config.common.build_client()?;
        let query_column = "select distinct ?fields from system.columns where database = ? and table = ? order by position".to_string();
        let clickhouse_column = client
            .query(&query_column)
            .bind(config.common.database.clone())
            .bind(config.common.table.clone())
            .fetch_all::<SystemColumn>()
            .await?;
        let column_correct_vec: Result<Vec<(bool, u8)>> = clickhouse_column
            .iter()
            .map(Self::build_column_correct_vec)
            .collect();
        Ok(Self {
            config,
            schema,
            pk_indices,
            client,
            is_append_only,
            column_correct_vec: column_correct_vec?,
        })
    }

    /// Check if clickhouse's column is 'Nullable', valid bits of `DateTime64`. And save it in
    /// `column_correct_vec`
    fn build_column_correct_vec(ck_column: &SystemColumn) -> Result<(bool, u8)> {
        let can_null = ck_column.r#type.contains("Nullable");
        let accuracy_time = if ck_column.r#type.contains("DateTime64(") {
            ck_column
                .r#type
                .split("DateTime64(")
                .last()
                .ok_or(SinkError::ClickHouse("must have last".to_string()))?
                .split(')')
                .next()
                .ok_or(SinkError::ClickHouse("must have next".to_string()))?
                .parse::<u8>()
                .map_err(|e| SinkError::ClickHouse(format!("clickhouse sink error {}", e)))?
        } else {
            0_u8
        };
        Ok((can_null, accuracy_time))
    }

    /// Build clickhouse fields from risingwave row
    async fn append_only(&mut self, chunk: StreamChunk) -> Result<()> {
        let mut insert = self
            .client
            .insert::<ClickHouseColumn<'_>>(&self.config.common.table)?;
        for (index,(op, row)) in chunk.rows().enumerate() {
            if op != Op::Insert {
                continue;
            }
            let mut clickhouse_filed_vec = vec![];
            for data in row.iter() {
                let &(can_null, accuracy_time) = self.column_correct_vec.get(index).unwrap();
                clickhouse_filed_vec.push((ClickHouseField::from_scalar_ref(data, accuracy_time, can_null)?,can_null));
            }
            let clickhouse_column = ClickHouseColumn{
                schema: self.schema.clone(),
                row: clickhouse_filed_vec,            
            };
            insert.write(&clickhouse_column).await?;
        }


        let mut inter = self
            .client
            .insert_with_filed::<Rows>(&self.config.common.table, Some(file_name))?;
        
        inter.end().await?;
        Ok(())
    }

    

    async fn upsert(&mut self, chunk: StreamChunk) -> Result<()> {
        // Get field names from schema.
        let field_names = self
            .schema
            .fields()
            .iter()
            .map(|f| f.name.clone())
            .collect_vec();
        let pk_index = *self.pk_indices.first().unwrap();
        let pk_name_0 = field_names.get(pk_index).unwrap();

        // Get the names of the columns excluding pk, and use them to update.
        let field_names_update = field_names
            .iter()
            .enumerate()
            .filter(|(index, _)| !self.pk_indices.contains(index))
            .map(|(_, value)| value.clone())
            .collect_vec();
        for (index, (op, row)) in chunk.rows().enumerate() {
            let &(_, accuracy_time) = self.column_correct_vec.get(index).unwrap();
            match op {
                Op::Insert => {
                    let mut inter = self.client.insert_with_filed::<Rows>(
                        &self.config.common.table,
                        Some(field_names.clone()),
                    )?;
                    const BUFFER_SIZE: usize = 1;
                    let mut buffer = BytesMut::with_capacity(BUFFER_SIZE);
                    self.build_row_binary(row, &mut buffer)?;
                    inter.write_row_binary(buffer).await?;
                    inter.end().await?;
                }
                Op::Delete => match Self::build_ck_fields(row.datum_at(index), accuracy_time)? {
                    Some(f) => {
                        self.client
                            .delete(&self.config.common.table, pk_name_0, vec![f])
                            .delete()
                            .await?;
                    }
                    None => return Err(SinkError::ClickHouse("pk can not be null".to_string())),
                },
                Op::UpdateDelete => continue,
                Op::UpdateInsert => {
                    let pk = Self::build_ck_fields(row.datum_at(pk_index), accuracy_time)?
                        .ok_or(SinkError::ClickHouse("pk can not be none".to_string()))?;
                    let fields_vec = self.build_update_fields(row, accuracy_time)?;
                    self.client
                        .update(
                            &self.config.common.table,
                            pk_name_0,
                            field_names_update.clone(),
                        )
                        .update_fields(fields_vec, pk)
                        .await?;
                }
            }
        }
        Ok(())
    }

   
}
#[async_trait::async_trait]
impl SinkWriter for ClickHouseSinkWriter {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        if self.is_append_only {
            self.append_only(chunk).await
        } else {
            self.upsert(chunk).await
        }
    }

    async fn begin_epoch(&mut self, _epoch: u64) -> Result<()> {
        // clickhouse no transactional guarantees, so we do nothing here.
        Ok(())
    }

    async fn barrier(&mut self, _is_checkpoint: bool) -> Result<()> {
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }

    async fn update_vnode_bitmap(&mut self, _vnode_bitmap: Bitmap) -> Result<()> {
        Ok(())
    }
}
#[derive(ClickHouseRow)]
struct Rows {}

#[derive(ClickHouseRow, Deserialize)]
struct SystemColumn {
    name: String,
    r#type: String,
}