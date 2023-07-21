use clickhouse::Row as ClickHouseRow;
use risingwave_common::row::Row;
use risingwave_common::catalog::Schema;
use risingwave_common::types::{Int256, Serial, Decimal, Interval, Date, Time, Timestamp, Timestamptz, JsonbRef, StructRef, ListRef, JsonbVal, ScalarRefImpl};
use serde::Serialize;
use crate::sink::Result;
use super::SinkError;



struct ClickHouseColumn{
    // row: Row,
    schema: Schema,
}

impl ClickHouseRow for ClickHouseColumn{
    const COLUMN_NAMES: &'static [&'static str] = &[];

    // TODO: count
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
    List(Vec<ClickHouseField<'a>>),
    Bytea(&'a[u8]),
    None,
}

impl<'a> ClickHouseField<'a> {
    pub fn from_scalar_ref(data: Option<ScalarRefImpl<'a>>,accuracy_time: u8, can_null: bool) -> Result<Self>{
        if data == None{
            if can_null{
                return Ok(ClickHouseField::None);
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
    Ok(data)
    }
}

impl Serialize for ClickHouseField<'_>{
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer {
        match s
    }
}