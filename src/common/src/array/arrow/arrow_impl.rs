// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Converts between arrays and Apache Arrow arrays.
//!
//! This file acts as a template file for conversion code between
//! arrays and different version of Apache Arrow.
//!
//! The conversion logic will be implemented for the arrow version specified in the outer mod by
//! `super::arrow_xxx`, such as `super::arrow_array`.
//!
//! When we want to implement the conversion logic for an arrow version, we first
//! create a new mod file, and rename the corresponding arrow package name to `arrow_xxx`
//! using the `use` clause, and then declare a sub-mod and set its file path with attribute
//! `#[path = "./arrow_impl.rs"]` so that the code in this template file can be embedded to
//! the new mod file, and the conversion logic can be implemented for the corresponding arrow
//! version.
//!
//! Example can be seen in `arrow_default.rs`, which is also as followed:
//! ```ignore
//! use {arrow_array, arrow_buffer, arrow_cast, arrow_schema};
//!
//! #[allow(clippy::duplicate_mod)]
//! #[path = "./arrow_impl.rs"]
//! mod arrow_impl;
//! ```

use std::fmt::Write;
use std::sync::Arc;

use chrono::{NaiveDateTime, NaiveTime};
use itertools::Itertools;

// This is important because we want to use the arrow version specified by the outer mod.
use super::{arrow_array, arrow_buffer, arrow_cast, arrow_schema};
// Other import should always use the absolute path.
use crate::array::*;
use crate::buffer::Bitmap;
use crate::types::*;
use crate::util::iter_util::ZipEqFast;

/// Converts RisingWave array to Arrow array with the schema.
/// This function will try to convert the array if the type is not same with the schema.
pub fn to_record_batch_with_schema(
    schema: arrow_schema::SchemaRef,
    chunk: &DataChunk,
) -> Result<arrow_array::RecordBatch, ArrayError> {
    if !chunk.is_compacted() {
        let c = chunk.clone();
        return to_record_batch_with_schema(schema, &c.compact());
    }
    let columns: Vec<_> = chunk
        .columns()
        .iter()
        .zip_eq_fast(schema.fields().iter())
        .map(|(column, field)| {
            let column: arrow_array::ArrayRef = column.as_ref().try_into()?;
            if column.data_type() == field.data_type() {
                Ok(column)
            } else {
                arrow_cast::cast(&column, field.data_type()).map_err(ArrayError::from_arrow)
            }
        })
        .try_collect::<_, _, ArrayError>()?;

    let opts = arrow_array::RecordBatchOptions::default().with_row_count(Some(chunk.capacity()));
    arrow_array::RecordBatch::try_new_with_options(schema, columns, &opts)
        .map_err(ArrayError::to_arrow)
}

// Implement bi-directional `From` between `DataChunk` and `arrow_array::RecordBatch`.
impl TryFrom<&DataChunk> for arrow_array::RecordBatch {
    type Error = ArrayError;

    fn try_from(chunk: &DataChunk) -> Result<Self, Self::Error> {
        if !chunk.is_compacted() {
            let c = chunk.clone();
            return Self::try_from(&c.compact());
        }
        let columns: Vec<_> = chunk
            .columns()
            .iter()
            .map(|column| column.as_ref().try_into())
            .try_collect::<_, _, Self::Error>()?;

        let fields: Vec<_> = columns
            .iter()
            .map(|array: &Arc<dyn arrow_array::Array>| {
                let nullable = array.null_count() > 0;
                let data_type = array.data_type().clone();
                arrow_schema::Field::new("", data_type, nullable)
            })
            .collect();

        let schema = Arc::new(arrow_schema::Schema::new(fields));
        let opts =
            arrow_array::RecordBatchOptions::default().with_row_count(Some(chunk.capacity()));
        arrow_array::RecordBatch::try_new_with_options(schema, columns, &opts)
            .map_err(ArrayError::to_arrow)
    }
}

impl TryFrom<&arrow_array::RecordBatch> for DataChunk {
    type Error = ArrayError;

    fn try_from(batch: &arrow_array::RecordBatch) -> Result<Self, Self::Error> {
        let mut columns = Vec::with_capacity(batch.num_columns());
        for array in batch.columns() {
            let column = Arc::new(array.try_into()?);
            columns.push(column);
        }
        Ok(DataChunk::new(columns, batch.num_rows()))
    }
}

/// Implement bi-directional `From` between `ArrayImpl` and `arrow_array::ArrayRef`.
macro_rules! converts_generic {
    ($({ $ArrowType:ty, $ArrowPattern:pat, $ArrayImplPattern:path }),*) => {
        // RisingWave array -> Arrow array
        impl TryFrom<&ArrayImpl> for arrow_array::ArrayRef {
            type Error = ArrayError;
            fn try_from(array: &ArrayImpl) -> Result<Self, Self::Error> {
                match array {
                    $($ArrayImplPattern(a) => Ok(Arc::new(<$ArrowType>::try_from(a)?)),)*
                    ArrayImpl::Timestamptz(a) => Ok(Arc::new(arrow_array::TimestampMicrosecondArray::try_from(a)?. with_timezone_utc())),
                    _ => todo!("unsupported array"),
                }
            }
        }
        // Arrow array -> RisingWave array
        impl TryFrom<&arrow_array::ArrayRef> for ArrayImpl {
            type Error = ArrayError;
            fn try_from(array: &arrow_array::ArrayRef) -> Result<Self, Self::Error> {
                use arrow_schema::DataType::*;
                use arrow_schema::IntervalUnit::*;
                use arrow_schema::TimeUnit::*;
                match array.data_type() {
                    $($ArrowPattern => Ok($ArrayImplPattern(
                        array
                            .as_any()
                            .downcast_ref::<$ArrowType>()
                            .unwrap()
                            .try_into()?,
                    )),)*
                    Timestamp(Microsecond, Some(_)) => Ok(ArrayImpl::Timestamptz(
                        array
                            .as_any()
                            .downcast_ref::<arrow_array::TimestampMicrosecondArray>()
                            .unwrap()
                            .try_into()?,
                    )),
                    t => Err(ArrayError::from_arrow(format!("unsupported data type: {t:?}"))),
                }
            }
        }
    };
}
converts_generic! {
    { arrow_array::Int16Array, Int16, ArrayImpl::Int16 },
    { arrow_array::Int32Array, Int32, ArrayImpl::Int32 },
    { arrow_array::Int64Array, Int64, ArrayImpl::Int64 },
    { arrow_array::Float32Array, Float32, ArrayImpl::Float32 },
    { arrow_array::Float64Array, Float64, ArrayImpl::Float64 },
    { arrow_array::StringArray, Utf8, ArrayImpl::Utf8 },
    { arrow_array::BooleanArray, Boolean, ArrayImpl::Bool },
    // Arrow doesn't have a data type to represent unconstrained numeric (`DECIMAL` in RisingWave and
    // Postgres). So we pick a special type `LargeBinary` for it.
    // Values stored in the array are the string representation of the decimal. e.g. b"1.234", b"+inf"
    { arrow_array::LargeBinaryArray, LargeBinary, ArrayImpl::Decimal },
    { arrow_array::Decimal256Array, Decimal256(_, _), ArrayImpl::Int256 },
    { arrow_array::Date32Array, Date32, ArrayImpl::Date },
    { arrow_array::TimestampMicrosecondArray, Timestamp(Microsecond, None), ArrayImpl::Timestamp },
    { arrow_array::Time64MicrosecondArray, Time64(Microsecond), ArrayImpl::Time },
    { arrow_array::IntervalMonthDayNanoArray, Interval(MonthDayNano), ArrayImpl::Interval },
    { arrow_array::StructArray, Struct(_), ArrayImpl::Struct },
    { arrow_array::ListArray, List(_), ArrayImpl::List },
    { arrow_array::BinaryArray, Binary, ArrayImpl::Bytea },
    { arrow_array::LargeStringArray, LargeUtf8, ArrayImpl::Jsonb }    // we use LargeUtf8 to represent Jsonb in arrow
}

// Arrow Datatype -> Risingwave Datatype
impl From<&arrow_schema::DataType> for DataType {
    fn from(value: &arrow_schema::DataType) -> Self {
        use arrow_schema::DataType::*;
        use arrow_schema::IntervalUnit::*;
        use arrow_schema::TimeUnit::*;
        match value {
            Boolean => Self::Boolean,
            Int16 => Self::Int16,
            Int32 => Self::Int32,
            Int64 => Self::Int64,
            Float32 => Self::Float32,
            Float64 => Self::Float64,
            LargeBinary => Self::Decimal,
            Decimal256(_, _) => Self::Int256,
            Date32 => Self::Date,
            Time64(Microsecond) => Self::Time,
            Timestamp(Microsecond, None) => Self::Timestamp,
            Timestamp(Microsecond, Some(_)) => Self::Timestamptz,
            Interval(MonthDayNano) => Self::Interval,
            Binary => Self::Bytea,
            Utf8 => Self::Varchar,
            LargeUtf8 => Self::Jsonb,
            Struct(fields) => Self::Struct(fields.into()),
            List(field) => Self::List(Box::new(field.data_type().into())),
            _ => todo!("Unsupported arrow data type: {value:?}"),
        }
    }
}

impl From<&arrow_schema::Fields> for StructType {
    fn from(fields: &arrow_schema::Fields) -> Self {
        Self::new(
            fields
                .iter()
                .map(|f| (f.name().clone(), f.data_type().into()))
                .collect(),
        )
    }
}

impl TryFrom<&StructType> for arrow_schema::Fields {
    type Error = ArrayError;

    fn try_from(struct_type: &StructType) -> Result<Self, Self::Error> {
        struct_type
            .iter()
            .map(|(name, ty)| Ok(arrow_schema::Field::new(name, ty.try_into()?, true)))
            .try_collect()
    }
}

impl From<arrow_schema::DataType> for DataType {
    fn from(value: arrow_schema::DataType) -> Self {
        (&value).into()
    }
}

impl TryFrom<&DataType> for arrow_schema::DataType {
    type Error = ArrayError;

    fn try_from(value: &DataType) -> Result<Self, Self::Error> {
        match value {
            DataType::Boolean => Ok(Self::Boolean),
            DataType::Int16 => Ok(Self::Int16),
            DataType::Int32 => Ok(Self::Int32),
            DataType::Int64 => Ok(Self::Int64),
            DataType::Int256 => Ok(Self::Decimal256(arrow_schema::DECIMAL256_MAX_PRECISION, 0)),
            DataType::Float32 => Ok(Self::Float32),
            DataType::Float64 => Ok(Self::Float64),
            DataType::Date => Ok(Self::Date32),
            DataType::Timestamp => Ok(Self::Timestamp(arrow_schema::TimeUnit::Microsecond, None)),
            DataType::Timestamptz => Ok(Self::Timestamp(
                arrow_schema::TimeUnit::Microsecond,
                Some("+00:00".into()),
            )),
            DataType::Time => Ok(Self::Time64(arrow_schema::TimeUnit::Microsecond)),
            DataType::Interval => Ok(Self::Interval(arrow_schema::IntervalUnit::MonthDayNano)),
            DataType::Varchar => Ok(Self::Utf8),
            DataType::Jsonb => Ok(Self::LargeUtf8),
            DataType::Bytea => Ok(Self::Binary),
            DataType::Decimal => Ok(Self::LargeBinary),
            DataType::Struct(struct_type) => Ok(Self::Struct(
                struct_type
                    .iter()
                    .map(|(name, ty)| Ok(arrow_schema::Field::new(name, ty.try_into()?, true)))
                    .try_collect::<_, _, ArrayError>()?,
            )),
            DataType::List(datatype) => Ok(Self::List(Arc::new(arrow_schema::Field::new(
                "item",
                datatype.as_ref().try_into()?,
                true,
            )))),
            DataType::Serial => Err(ArrayError::to_arrow(
                "Serial type is not supported to convert to arrow",
            )),
        }
    }
}

impl TryFrom<DataType> for arrow_schema::DataType {
    type Error = ArrayError;

    fn try_from(value: DataType) -> Result<Self, Self::Error> {
        (&value).try_into()
    }
}

impl From<&Bitmap> for arrow_buffer::NullBuffer {
    fn from(bitmap: &Bitmap) -> Self {
        bitmap.iter().collect()
    }
}

/// Implement bi-directional `From` between concrete array types.
macro_rules! converts {
    ($ArrayType:ty, $ArrowType:ty) => {
        impl From<&$ArrayType> for $ArrowType {
            fn from(array: &$ArrayType) -> Self {
                array.iter().collect()
            }
        }
        impl From<&$ArrowType> for $ArrayType {
            fn from(array: &$ArrowType) -> Self {
                array.iter().collect()
            }
        }
        impl From<&[$ArrowType]> for $ArrayType {
            fn from(arrays: &[$ArrowType]) -> Self {
                arrays.iter().flat_map(|a| a.iter()).collect()
            }
        }
    };
    // convert values using FromIntoArrow
    ($ArrayType:ty, $ArrowType:ty, @map) => {
        impl From<&$ArrayType> for $ArrowType {
            fn from(array: &$ArrayType) -> Self {
                array.iter().map(|o| o.map(|v| v.into_arrow())).collect()
            }
        }
        impl From<&$ArrowType> for $ArrayType {
            fn from(array: &$ArrowType) -> Self {
                array
                    .iter()
                    .map(|o| {
                        o.map(|v| {
                            <<$ArrayType as Array>::RefItem<'_> as FromIntoArrow>::from_arrow(v)
                        })
                    })
                    .collect()
            }
        }
        impl From<&[$ArrowType]> for $ArrayType {
            fn from(arrays: &[$ArrowType]) -> Self {
                arrays
                    .iter()
                    .flat_map(|a| a.iter())
                    .map(|o| {
                        o.map(|v| {
                            <<$ArrayType as Array>::RefItem<'_> as FromIntoArrow>::from_arrow(v)
                        })
                    })
                    .collect()
            }
        }
    };
}
converts!(BoolArray, arrow_array::BooleanArray);
converts!(I16Array, arrow_array::Int16Array);
converts!(I32Array, arrow_array::Int32Array);
converts!(I64Array, arrow_array::Int64Array);
converts!(F32Array, arrow_array::Float32Array, @map);
converts!(F64Array, arrow_array::Float64Array, @map);
converts!(BytesArray, arrow_array::BinaryArray);
converts!(Utf8Array, arrow_array::StringArray);
converts!(DateArray, arrow_array::Date32Array, @map);
converts!(TimeArray, arrow_array::Time64MicrosecondArray, @map);
converts!(TimestampArray, arrow_array::TimestampMicrosecondArray, @map);
converts!(TimestamptzArray, arrow_array::TimestampMicrosecondArray, @map);
converts!(IntervalArray, arrow_array::IntervalMonthDayNanoArray, @map);

/// Converts RisingWave value from and into Arrow value.
trait FromIntoArrow {
    /// The corresponding element type in the Arrow array.
    type ArrowType;
    fn from_arrow(value: Self::ArrowType) -> Self;
    fn into_arrow(self) -> Self::ArrowType;
}

impl FromIntoArrow for F32 {
    type ArrowType = f32;

    fn from_arrow(value: Self::ArrowType) -> Self {
        value.into()
    }

    fn into_arrow(self) -> Self::ArrowType {
        self.into()
    }
}

impl FromIntoArrow for F64 {
    type ArrowType = f64;

    fn from_arrow(value: Self::ArrowType) -> Self {
        value.into()
    }

    fn into_arrow(self) -> Self::ArrowType {
        self.into()
    }
}

impl FromIntoArrow for Date {
    type ArrowType = i32;

    fn from_arrow(value: Self::ArrowType) -> Self {
        Date(arrow_array::types::Date32Type::to_naive_date(value))
    }

    fn into_arrow(self) -> Self::ArrowType {
        arrow_array::types::Date32Type::from_naive_date(self.0)
    }
}

impl FromIntoArrow for Time {
    type ArrowType = i64;

    fn from_arrow(value: Self::ArrowType) -> Self {
        Time(
            NaiveTime::from_num_seconds_from_midnight_opt(
                (value / 1_000_000) as _,
                (value % 1_000_000 * 1000) as _,
            )
            .unwrap(),
        )
    }

    fn into_arrow(self) -> Self::ArrowType {
        self.0
            .signed_duration_since(NaiveTime::default())
            .num_microseconds()
            .unwrap()
    }
}

impl FromIntoArrow for Timestamp {
    type ArrowType = i64;

    fn from_arrow(value: Self::ArrowType) -> Self {
        Timestamp(
            NaiveDateTime::from_timestamp_opt(
                (value / 1_000_000) as _,
                (value % 1_000_000 * 1000) as _,
            )
            .unwrap(),
        )
    }

    fn into_arrow(self) -> Self::ArrowType {
        self.0
            .signed_duration_since(NaiveDateTime::default())
            .num_microseconds()
            .unwrap()
    }
}

impl FromIntoArrow for Timestamptz {
    type ArrowType = i64;

    fn from_arrow(value: Self::ArrowType) -> Self {
        Timestamptz::from_micros(value)
    }

    fn into_arrow(self) -> Self::ArrowType {
        self.timestamp_micros()
    }
}

impl FromIntoArrow for Interval {
    type ArrowType = i128;

    fn from_arrow(value: Self::ArrowType) -> Self {
        // XXX: the arrow-rs decoding is incorrect
        // let (months, days, ns) = arrow_array::types::IntervalMonthDayNanoType::to_parts(value);
        let months = value as i32;
        let days = (value >> 32) as i32;
        let ns = (value >> 64) as i64;
        Interval::from_month_day_usec(months, days, ns / 1000)
    }

    fn into_arrow(self) -> Self::ArrowType {
        // XXX: the arrow-rs encoding is incorrect
        // arrow_array::types::IntervalMonthDayNanoType::make_value(
        //     self.months(),
        //     self.days(),
        //     // TODO: this may overflow and we need `try_into`
        //     self.usecs() * 1000,
        // )
        let m = self.months() as u128 & u32::MAX as u128;
        let d = (self.days() as u128 & u32::MAX as u128) << 32;
        let n = ((self.usecs() * 1000) as u128 & u64::MAX as u128) << 64;
        (m | d | n) as i128
    }
}

impl From<&DecimalArray> for arrow_array::LargeBinaryArray {
    fn from(array: &DecimalArray) -> Self {
        let mut builder =
            arrow_array::builder::LargeBinaryBuilder::with_capacity(array.len(), array.len() * 8);
        for value in array.iter() {
            builder.append_option(value.map(|d| d.to_string()));
        }
        builder.finish()
    }
}

impl TryFrom<&arrow_array::LargeBinaryArray> for DecimalArray {
    type Error = ArrayError;

    fn try_from(array: &arrow_array::LargeBinaryArray) -> Result<Self, Self::Error> {
        array
            .iter()
            .map(|o| {
                o.map(|s| {
                    let s = std::str::from_utf8(s)
                        .map_err(|_| ArrayError::from_arrow(format!("invalid decimal: {s:?}")))?;
                    s.parse()
                        .map_err(|_| ArrayError::from_arrow(format!("invalid decimal: {s:?}")))
                })
                .transpose()
            })
            .try_collect()
    }
}

impl From<&JsonbArray> for arrow_array::LargeStringArray {
    fn from(array: &JsonbArray) -> Self {
        let mut builder =
            arrow_array::builder::LargeStringBuilder::with_capacity(array.len(), array.len() * 16);
        for value in array.iter() {
            match value {
                Some(jsonb) => {
                    write!(&mut builder, "{}", jsonb).unwrap();
                    builder.append_value("");
                }
                None => builder.append_null(),
            }
        }
        builder.finish()
    }
}

impl TryFrom<&arrow_array::LargeStringArray> for JsonbArray {
    type Error = ArrayError;

    fn try_from(array: &arrow_array::LargeStringArray) -> Result<Self, Self::Error> {
        array
            .iter()
            .map(|o| {
                o.map(|s| {
                    s.parse()
                        .map_err(|_| ArrayError::from_arrow(format!("invalid json: {s}")))
                })
                .transpose()
            })
            .try_collect()
    }
}

impl From<arrow_buffer::i256> for Int256 {
    fn from(value: arrow_buffer::i256) -> Self {
        let buffer = value.to_be_bytes();
        Int256::from_be_bytes(buffer)
    }
}

impl<'a> From<Int256Ref<'a>> for arrow_buffer::i256 {
    fn from(val: Int256Ref<'a>) -> Self {
        let buffer = val.to_be_bytes();
        arrow_buffer::i256::from_be_bytes(buffer)
    }
}

impl From<&Int256Array> for arrow_array::Decimal256Array {
    fn from(array: &Int256Array) -> Self {
        array
            .iter()
            .map(|o| o.map(arrow_buffer::i256::from))
            .collect()
    }
}

impl From<&arrow_array::Decimal256Array> for Int256Array {
    fn from(array: &arrow_array::Decimal256Array) -> Self {
        let values = array.iter().map(|o| o.map(Int256::from)).collect_vec();

        values
            .iter()
            .map(|i| i.as_ref().map(|v| v.as_scalar_ref()))
            .collect()
    }
}

impl TryFrom<&ListArray> for arrow_array::ListArray {
    type Error = ArrayError;

    fn try_from(array: &ListArray) -> Result<Self, Self::Error> {
        use arrow_array::builder::*;
        fn build<A, B, F>(
            array: &ListArray,
            a: &A,
            builder: B,
            mut append: F,
        ) -> arrow_array::ListArray
        where
            A: Array,
            B: arrow_array::builder::ArrayBuilder,
            F: FnMut(&mut B, Option<A::RefItem<'_>>),
        {
            let mut builder = ListBuilder::with_capacity(builder, a.len());
            for i in 0..array.len() {
                for j in array.offsets[i]..array.offsets[i + 1] {
                    append(builder.values(), a.value_at(j as usize));
                }
                builder.append(!array.is_null(i));
            }
            builder.finish()
        }
        Ok(match &*array.value {
            ArrayImpl::Int16(a) => build(array, a, Int16Builder::with_capacity(a.len()), |b, v| {
                b.append_option(v)
            }),
            ArrayImpl::Int32(a) => build(array, a, Int32Builder::with_capacity(a.len()), |b, v| {
                b.append_option(v)
            }),
            ArrayImpl::Int64(a) => build(array, a, Int64Builder::with_capacity(a.len()), |b, v| {
                b.append_option(v)
            }),

            ArrayImpl::Float32(a) => {
                build(array, a, Float32Builder::with_capacity(a.len()), |b, v| {
                    b.append_option(v.map(|f| f.0))
                })
            }
            ArrayImpl::Float64(a) => {
                build(array, a, Float64Builder::with_capacity(a.len()), |b, v| {
                    b.append_option(v.map(|f| f.0))
                })
            }
            ArrayImpl::Utf8(a) => build(
                array,
                a,
                StringBuilder::with_capacity(a.len(), a.data().len()),
                |b, v| b.append_option(v),
            ),
            ArrayImpl::Int256(a) => build(
                array,
                a,
                Decimal256Builder::with_capacity(a.len()).with_data_type(
                    arrow_schema::DataType::Decimal256(arrow_schema::DECIMAL256_MAX_PRECISION, 0),
                ),
                |b, v| b.append_option(v.map(Into::into)),
            ),
            ArrayImpl::Bool(a) => {
                build(array, a, BooleanBuilder::with_capacity(a.len()), |b, v| {
                    b.append_option(v)
                })
            }
            ArrayImpl::Decimal(a) => build(
                array,
                a,
                LargeBinaryBuilder::with_capacity(a.len(), a.len() * 8),
                |b, v| b.append_option(v.map(|d| d.to_string())),
            ),
            ArrayImpl::Interval(a) => build(
                array,
                a,
                IntervalMonthDayNanoBuilder::with_capacity(a.len()),
                |b, v| b.append_option(v.map(|d| d.into_arrow())),
            ),
            ArrayImpl::Date(a) => build(array, a, Date32Builder::with_capacity(a.len()), |b, v| {
                b.append_option(v.map(|d| d.into_arrow()))
            }),
            ArrayImpl::Timestamp(a) => build(
                array,
                a,
                TimestampMicrosecondBuilder::with_capacity(a.len()),
                |b, v| b.append_option(v.map(|d| d.into_arrow())),
            ),
            ArrayImpl::Timestamptz(a) => build(
                array,
                a,
                TimestampMicrosecondBuilder::with_capacity(a.len()),
                |b, v| b.append_option(v.map(|d| d.into_arrow())),
            ),
            ArrayImpl::Time(a) => build(
                array,
                a,
                Time64MicrosecondBuilder::with_capacity(a.len()),
                |b, v| b.append_option(v.map(|d| d.into_arrow())),
            ),
            ArrayImpl::Jsonb(a) => build(
                array,
                a,
                LargeStringBuilder::with_capacity(a.len(), a.len() * 16),
                |b, v| b.append_option(v.map(|j| j.to_string())),
            ),
            ArrayImpl::Serial(_) => todo!("list of serial"),
            ArrayImpl::Struct(a) => {
                let values = Arc::new(arrow_array::StructArray::try_from(a)?);
                arrow_array::ListArray::new(
                    Arc::new(arrow_schema::Field::new(
                        "item",
                        a.data_type().try_into()?,
                        true,
                    )),
                    arrow_buffer::OffsetBuffer::new(arrow_buffer::ScalarBuffer::from(
                        array
                            .offsets()
                            .iter()
                            .map(|o| *o as i32)
                            .collect::<Vec<i32>>(),
                    )),
                    values,
                    Some(array.null_bitmap().into()),
                )
            }
            ArrayImpl::List(_) => todo!("list of list"),
            ArrayImpl::Bytea(a) => build(
                array,
                a,
                BinaryBuilder::with_capacity(a.len(), a.data().len()),
                |b, v| b.append_option(v),
            ),
        })
    }
}

impl TryFrom<&arrow_array::ListArray> for ListArray {
    type Error = ArrayError;

    fn try_from(array: &arrow_array::ListArray) -> Result<Self, Self::Error> {
        use arrow_array::Array;
        Ok(ListArray {
            value: Box::new(ArrayImpl::try_from(array.values())?),
            bitmap: match array.nulls() {
                Some(nulls) => nulls.iter().collect(),
                None => Bitmap::ones(array.len()),
            },
            offsets: array.offsets().iter().map(|o| *o as u32).collect(),
        })
    }
}

impl TryFrom<&StructArray> for arrow_array::StructArray {
    type Error = ArrayError;

    fn try_from(array: &StructArray) -> Result<Self, Self::Error> {
        Ok(arrow_array::StructArray::new(
            array.data_type().as_struct().try_into()?,
            array
                .fields()
                .map(|arr| arr.as_ref().try_into())
                .try_collect::<_, _, ArrayError>()?,
            Some(array.null_bitmap().into()),
        ))
    }
}

impl TryFrom<&arrow_array::StructArray> for StructArray {
    type Error = ArrayError;

    fn try_from(array: &arrow_array::StructArray) -> Result<Self, Self::Error> {
        use arrow_array::Array;
        let arrow_schema::DataType::Struct(fields) = array.data_type() else {
            panic!("nested field types cannot be determined.");
        };
        Ok(StructArray::new(
            fields.into(),
            array
                .columns()
                .iter()
                .map(|a| ArrayImpl::try_from(a).map(Arc::new))
                .try_collect()?,
            (0..array.len()).map(|i| !array.is_null(i)).collect(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::arrow_array::Array as _;
    use super::*;

    #[test]
    fn bool() {
        let array = BoolArray::from_iter([None, Some(false), Some(true)]);
        let arrow = arrow_array::BooleanArray::from(&array);
        assert_eq!(BoolArray::from(&arrow), array);
    }

    #[test]
    fn i16() {
        let array = I16Array::from_iter([None, Some(-7), Some(25)]);
        let arrow = arrow_array::Int16Array::from(&array);
        assert_eq!(I16Array::from(&arrow), array);
    }

    #[test]
    fn f32() {
        let array = F32Array::from_iter([None, Some(-7.0), Some(25.0)]);
        let arrow = arrow_array::Float32Array::from(&array);
        assert_eq!(F32Array::from(&arrow), array);
    }

    #[test]
    fn date() {
        let array = DateArray::from_iter([
            None,
            Date::with_days(12345).ok(),
            Date::with_days(-12345).ok(),
        ]);
        let arrow = arrow_array::Date32Array::from(&array);
        assert_eq!(DateArray::from(&arrow), array);
    }

    #[test]
    fn time() {
        let array = TimeArray::from_iter([None, Time::with_micro(24 * 3600 * 1_000_000 - 1).ok()]);
        let arrow = arrow_array::Time64MicrosecondArray::from(&array);
        assert_eq!(TimeArray::from(&arrow), array);
    }

    #[test]
    fn timestamp() {
        let array =
            TimestampArray::from_iter([None, Timestamp::with_micros(123456789012345678).ok()]);
        let arrow = arrow_array::TimestampMicrosecondArray::from(&array);
        assert_eq!(TimestampArray::from(&arrow), array);
    }

    #[test]
    fn interval() {
        let array = IntervalArray::from_iter([
            None,
            Some(Interval::from_month_day_usec(
                1_000_000,
                1_000,
                1_000_000_000,
            )),
            Some(Interval::from_month_day_usec(
                -1_000_000,
                -1_000,
                -1_000_000_000,
            )),
        ]);
        let arrow = arrow_array::IntervalMonthDayNanoArray::from(&array);
        assert_eq!(IntervalArray::from(&arrow), array);
    }

    #[test]
    fn string() {
        let array = Utf8Array::from_iter([None, Some("array"), Some("arrow")]);
        let arrow = arrow_array::StringArray::from(&array);
        assert_eq!(Utf8Array::from(&arrow), array);
    }

    #[test]
    fn decimal() {
        let array = DecimalArray::from_iter([
            None,
            Some(Decimal::NaN),
            Some(Decimal::PositiveInf),
            Some(Decimal::NegativeInf),
            Some(Decimal::Normalized("123.4".parse().unwrap())),
            Some(Decimal::Normalized("123.456".parse().unwrap())),
        ]);
        let arrow = arrow_array::LargeBinaryArray::from(&array);
        assert_eq!(DecimalArray::try_from(&arrow).unwrap(), array);
    }

    #[test]
    fn jsonb() {
        let array = JsonbArray::from_iter([
            None,
            Some("null".parse().unwrap()),
            Some("false".parse().unwrap()),
            Some("1".parse().unwrap()),
            Some("[1, 2, 3]".parse().unwrap()),
            Some(r#"{ "a": 1, "b": null }"#.parse().unwrap()),
        ]);
        let arrow = arrow_array::LargeStringArray::from(&array);
        assert_eq!(JsonbArray::try_from(&arrow).unwrap(), array);
    }

    #[test]
    fn int256() {
        let values = vec![
            None,
            Some(Int256::from(1)),
            Some(Int256::from(i64::MAX)),
            Some(Int256::from(i64::MAX) * Int256::from(i64::MAX)),
            Some(Int256::from(i64::MAX) * Int256::from(i64::MAX) * Int256::from(i64::MAX)),
            Some(
                Int256::from(i64::MAX)
                    * Int256::from(i64::MAX)
                    * Int256::from(i64::MAX)
                    * Int256::from(i64::MAX),
            ),
            Some(Int256::min_value()),
            Some(Int256::max_value()),
        ];

        let array =
            Int256Array::from_iter(values.iter().map(|r| r.as_ref().map(|x| x.as_scalar_ref())));
        let arrow = arrow_array::Decimal256Array::from(&array);
        assert_eq!(Int256Array::from(&arrow), array);
    }

    #[test]
    fn struct_array() {
        // Empty array - risingwave to arrow conversion.
        let test_arr = StructArray::new(StructType::empty(), vec![], Bitmap::ones(0));
        assert_eq!(
            arrow_array::StructArray::try_from(&test_arr).unwrap().len(),
            0
        );

        // Empty array - arrow to risingwave conversion.
        let test_arr_2 = arrow_array::StructArray::from(vec![]);
        assert_eq!(StructArray::try_from(&test_arr_2).unwrap().len(), 0);

        // Struct array with primitive types. arrow to risingwave conversion.
        let test_arrow_struct_array = arrow_array::StructArray::try_from(vec![
            (
                "a",
                Arc::new(arrow_array::BooleanArray::from(vec![
                    Some(false),
                    Some(false),
                    Some(true),
                    None,
                ])) as arrow_array::ArrayRef,
            ),
            (
                "b",
                Arc::new(arrow_array::Int32Array::from(vec![
                    Some(42),
                    Some(28),
                    Some(19),
                    None,
                ])) as arrow_array::ArrayRef,
            ),
        ])
        .unwrap();
        let actual_risingwave_struct_array =
            StructArray::try_from(&test_arrow_struct_array).unwrap();
        let expected_risingwave_struct_array = StructArray::new(
            StructType::new(vec![("a", DataType::Boolean), ("b", DataType::Int32)]),
            vec![
                BoolArray::from_iter([Some(false), Some(false), Some(true), None]).into_ref(),
                I32Array::from_iter([Some(42), Some(28), Some(19), None]).into_ref(),
            ],
            [true, true, true, true].into_iter().collect(),
        );
        assert_eq!(
            expected_risingwave_struct_array,
            actual_risingwave_struct_array
        );
    }

    #[test]
    fn list() {
        let array = ListArray::from_iter([None, Some(vec![0, -127, 127, 50]), Some(vec![0; 0])]);
        let arrow = arrow_array::ListArray::try_from(&array).unwrap();
        assert_eq!(ListArray::try_from(&arrow).unwrap(), array);
    }
}
