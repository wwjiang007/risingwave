// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Data types in RisingWave.

// NOTE: When adding or modifying data types, remember to update the type matrix in
// src/expr/macro/src/types.rs

use std::convert::TryFrom;
use std::fmt::Debug;
use std::hash::Hash;
use std::str::{FromStr, Utf8Error};

use bytes::{Buf, BufMut, Bytes};
use chrono::{Datelike, Timelike};
use itertools::Itertools;
use parse_display::{Display, FromStr};
use paste::paste;
use postgres_types::{FromSql, IsNull, ToSql, Type};
use risingwave_pb::data::data_type::PbTypeName;
use risingwave_pb::data::PbDataType;
use serde::{Deserialize, Serialize, Serializer};
use strum_macros::EnumDiscriminants;
use thiserror_ext::AsReport;

use crate::array::{
    ArrayBuilderImpl, ArrayError, ArrayResult, PrimitiveArrayItemType, NULL_VAL_FOR_HASH,
};
pub use crate::array::{ListRef, ListValue, StructRef, StructValue};
use crate::cast::{str_to_bool, str_to_bytea};
use crate::error::BoxedError;
use crate::estimate_size::EstimateSize;
use crate::util::iter_util::ZipEqDebug;
use crate::{
    dispatch_data_types, dispatch_scalar_ref_variants, dispatch_scalar_variants,
    for_all_scalar_variants, for_all_type_pairs,
};

mod datetime;
mod decimal;
mod fields;
mod interval;
mod jsonb;
mod macros;
mod native_type;
mod num256;
mod ops;
mod ordered;
mod ordered_float;
mod postgres_type;
mod scalar_impl;
mod sentinel;
mod serial;
mod struct_type;
mod successor;
mod timestamptz;
mod to_binary;
mod to_sql;
mod to_text;
mod with_data_type;

pub use fields::Fields;
pub use risingwave_fields_derive::Fields;

pub use self::datetime::{Date, Time, Timestamp};
pub use self::decimal::{Decimal, PowError as DecimalPowError};
pub use self::interval::{test_utils, DateTimeField, Interval, IntervalDisplay};
pub use self::jsonb::{JsonbRef, JsonbVal};
pub use self::native_type::*;
pub use self::num256::{Int256, Int256Ref};
pub use self::ops::{CheckedAdd, IsNegative};
pub use self::ordered::*;
pub use self::ordered_float::{FloatExt, IntoOrdered};
pub use self::scalar_impl::*;
pub use self::sentinel::Sentinelled;
pub use self::serial::Serial;
pub use self::struct_type::StructType;
pub use self::successor::Successor;
pub use self::timestamptz::*;
pub use self::to_binary::ToBinary;
pub use self::to_text::ToText;
pub use self::with_data_type::WithDataType;

/// A 32-bit floating point type with total order.
pub type F32 = ordered_float::OrderedFloat<f32>;

/// A 64-bit floating point type with total order.
pub type F64 = ordered_float::OrderedFloat<f64>;

/// The set of datatypes that are supported in RisingWave.
// `EnumDiscriminants` will generate a `DataTypeName` enum with the same variants,
// but without data fields.
#[derive(
    Debug, Display, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, EnumDiscriminants, FromStr,
)]
#[strum_discriminants(derive(Hash, Ord, PartialOrd))]
#[strum_discriminants(name(DataTypeName))]
#[strum_discriminants(vis(pub))]
#[cfg_attr(test, strum_discriminants(derive(strum_macros::EnumIter)))]
pub enum DataType {
    #[display("boolean")]
    #[from_str(regex = "(?i)^bool$|^boolean$")]
    Boolean,
    #[display("smallint")]
    #[from_str(regex = "(?i)^smallint$|^int2$")]
    Int16,
    #[display("integer")]
    #[from_str(regex = "(?i)^integer$|^int$|^int4$")]
    Int32,
    #[display("bigint")]
    #[from_str(regex = "(?i)^bigint$|^int8$")]
    Int64,
    #[display("real")]
    #[from_str(regex = "(?i)^real$|^float4$")]
    Float32,
    #[display("double precision")]
    #[from_str(regex = "(?i)^double precision$|^float8$")]
    Float64,
    #[display("numeric")]
    #[from_str(regex = "(?i)^numeric$|^decimal$")]
    Decimal,
    #[display("date")]
    #[from_str(regex = "(?i)^date$")]
    Date,
    #[display("character varying")]
    #[from_str(regex = "(?i)^character varying$|^varchar$")]
    Varchar,
    #[display("time without time zone")]
    #[from_str(regex = "(?i)^time$|^time without time zone$")]
    Time,
    #[display("timestamp without time zone")]
    #[from_str(regex = "(?i)^timestamp$|^timestamp without time zone$")]
    Timestamp,
    #[display("timestamp with time zone")]
    #[from_str(regex = "(?i)^timestamptz$|^timestamp with time zone$")]
    Timestamptz,
    #[display("interval")]
    #[from_str(regex = "(?i)^interval$")]
    Interval,
    #[display("{0}")]
    #[from_str(regex = "(?i)^(?P<0>.+)$")]
    Struct(StructType),
    #[display("{0}[]")]
    #[from_str(regex = r"(?i)^(?P<0>.+)\[\]$")]
    List(Box<DataType>),
    #[display("bytea")]
    #[from_str(regex = "(?i)^bytea$")]
    Bytea,
    #[display("jsonb")]
    #[from_str(regex = "(?i)^jsonb$")]
    Jsonb,
    #[display("serial")]
    #[from_str(regex = "(?i)^serial$")]
    Serial,
    #[display("rw_int256")]
    #[from_str(regex = "(?i)^rw_int256$")]
    Int256,
}

impl std::str::FromStr for Box<DataType> {
    type Err = BoxedError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Box::new(DataType::from_str(s)?))
    }
}

impl DataTypeName {
    pub fn is_scalar(&self) -> bool {
        match self {
            DataTypeName::Boolean
            | DataTypeName::Int16
            | DataTypeName::Int32
            | DataTypeName::Int64
            | DataTypeName::Int256
            | DataTypeName::Serial
            | DataTypeName::Decimal
            | DataTypeName::Float32
            | DataTypeName::Float64
            | DataTypeName::Varchar
            | DataTypeName::Date
            | DataTypeName::Timestamp
            | DataTypeName::Timestamptz
            | DataTypeName::Time
            | DataTypeName::Bytea
            | DataTypeName::Jsonb
            | DataTypeName::Interval => true,

            DataTypeName::Struct | DataTypeName::List => false,
        }
    }

    pub fn to_type(self) -> Option<DataType> {
        let t = match self {
            DataTypeName::Boolean => DataType::Boolean,
            DataTypeName::Int16 => DataType::Int16,
            DataTypeName::Int32 => DataType::Int32,
            DataTypeName::Int64 => DataType::Int64,
            DataTypeName::Int256 => DataType::Int256,
            DataTypeName::Serial => DataType::Serial,
            DataTypeName::Decimal => DataType::Decimal,
            DataTypeName::Float32 => DataType::Float32,
            DataTypeName::Float64 => DataType::Float64,
            DataTypeName::Varchar => DataType::Varchar,
            DataTypeName::Bytea => DataType::Bytea,
            DataTypeName::Date => DataType::Date,
            DataTypeName::Timestamp => DataType::Timestamp,
            DataTypeName::Timestamptz => DataType::Timestamptz,
            DataTypeName::Time => DataType::Time,
            DataTypeName::Interval => DataType::Interval,
            DataTypeName::Jsonb => DataType::Jsonb,
            DataTypeName::Struct | DataTypeName::List => {
                return None;
            }
        };
        Some(t)
    }
}

impl From<DataTypeName> for DataType {
    fn from(type_name: DataTypeName) -> Self {
        type_name.to_type().unwrap_or_else(|| panic!("Functions returning struct or list can not be inferred. Please use `FunctionCall::new_unchecked`."))
    }
}

impl From<&PbDataType> for DataType {
    fn from(proto: &PbDataType) -> DataType {
        match proto.get_type_name().expect("missing type field") {
            PbTypeName::Int16 => DataType::Int16,
            PbTypeName::Int32 => DataType::Int32,
            PbTypeName::Int64 => DataType::Int64,
            PbTypeName::Serial => DataType::Serial,
            PbTypeName::Float => DataType::Float32,
            PbTypeName::Double => DataType::Float64,
            PbTypeName::Boolean => DataType::Boolean,
            PbTypeName::Varchar => DataType::Varchar,
            PbTypeName::Date => DataType::Date,
            PbTypeName::Time => DataType::Time,
            PbTypeName::Timestamp => DataType::Timestamp,
            PbTypeName::Timestamptz => DataType::Timestamptz,
            PbTypeName::Decimal => DataType::Decimal,
            PbTypeName::Interval => DataType::Interval,
            PbTypeName::Bytea => DataType::Bytea,
            PbTypeName::Jsonb => DataType::Jsonb,
            PbTypeName::Struct => {
                let fields: Vec<DataType> = proto.field_type.iter().map(|f| f.into()).collect_vec();
                let field_names: Vec<String> = proto.field_names.iter().cloned().collect_vec();
                DataType::new_struct(fields, field_names)
            }
            PbTypeName::List => DataType::List(
                // The first (and only) item is the list element type.
                Box::new((&proto.field_type[0]).into()),
            ),
            PbTypeName::TypeUnspecified => unreachable!(),
            PbTypeName::Int256 => DataType::Int256,
        }
    }
}

impl From<DataTypeName> for PbTypeName {
    fn from(type_name: DataTypeName) -> Self {
        match type_name {
            DataTypeName::Boolean => PbTypeName::Boolean,
            DataTypeName::Int16 => PbTypeName::Int16,
            DataTypeName::Int32 => PbTypeName::Int32,
            DataTypeName::Int64 => PbTypeName::Int64,
            DataTypeName::Serial => PbTypeName::Serial,
            DataTypeName::Float32 => PbTypeName::Float,
            DataTypeName::Float64 => PbTypeName::Double,
            DataTypeName::Varchar => PbTypeName::Varchar,
            DataTypeName::Date => PbTypeName::Date,
            DataTypeName::Timestamp => PbTypeName::Timestamp,
            DataTypeName::Timestamptz => PbTypeName::Timestamptz,
            DataTypeName::Time => PbTypeName::Time,
            DataTypeName::Interval => PbTypeName::Interval,
            DataTypeName::Decimal => PbTypeName::Decimal,
            DataTypeName::Bytea => PbTypeName::Bytea,
            DataTypeName::Jsonb => PbTypeName::Jsonb,
            DataTypeName::Struct => PbTypeName::Struct,
            DataTypeName::List => PbTypeName::List,
            DataTypeName::Int256 => PbTypeName::Int256,
        }
    }
}

impl DataType {
    pub fn create_array_builder(&self, capacity: usize) -> ArrayBuilderImpl {
        use crate::array::*;

        dispatch_data_types!(self, [B = ArrayBuilder], {
            B::with_type(capacity, self.clone()).into()
        })
    }

    pub fn prost_type_name(&self) -> PbTypeName {
        match self {
            DataType::Int16 => PbTypeName::Int16,
            DataType::Int32 => PbTypeName::Int32,
            DataType::Int64 => PbTypeName::Int64,
            DataType::Int256 => PbTypeName::Int256,
            DataType::Serial => PbTypeName::Serial,
            DataType::Float32 => PbTypeName::Float,
            DataType::Float64 => PbTypeName::Double,
            DataType::Boolean => PbTypeName::Boolean,
            DataType::Varchar => PbTypeName::Varchar,
            DataType::Date => PbTypeName::Date,
            DataType::Time => PbTypeName::Time,
            DataType::Timestamp => PbTypeName::Timestamp,
            DataType::Timestamptz => PbTypeName::Timestamptz,
            DataType::Decimal => PbTypeName::Decimal,
            DataType::Interval => PbTypeName::Interval,
            DataType::Jsonb => PbTypeName::Jsonb,
            DataType::Struct { .. } => PbTypeName::Struct,
            DataType::List { .. } => PbTypeName::List,
            DataType::Bytea => PbTypeName::Bytea,
        }
    }

    pub fn to_protobuf(&self) -> PbDataType {
        let mut pb = PbDataType {
            type_name: self.prost_type_name() as i32,
            is_nullable: true,
            ..Default::default()
        };
        match self {
            DataType::Struct(t) => {
                pb.field_type = t.types().map(|f| f.to_protobuf()).collect();
                pb.field_names = t.names().map(|s| s.into()).collect();
            }
            DataType::List(datatype) => {
                pb.field_type = vec![datatype.to_protobuf()];
            }
            _ => {}
        }
        pb
    }

    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Serial
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal
        )
    }

    pub fn is_scalar(&self) -> bool {
        DataTypeName::from(self).is_scalar()
    }

    pub fn is_array(&self) -> bool {
        matches!(self, DataType::List(_))
    }

    pub fn is_struct(&self) -> bool {
        matches!(self, DataType::Struct(_))
    }

    pub fn is_int(&self) -> bool {
        matches!(self, DataType::Int16 | DataType::Int32 | DataType::Int64)
    }

    /// Returns the output type of window function on a given input type.
    pub fn window_of(input: &DataType) -> Option<DataType> {
        match input {
            DataType::Timestamptz => Some(DataType::Timestamptz),
            DataType::Timestamp | DataType::Date => Some(DataType::Timestamp),
            _ => None,
        }
    }

    pub fn new_struct(fields: Vec<DataType>, field_names: Vec<String>) -> Self {
        Self::Struct(StructType::from_parts(field_names, fields))
    }

    pub fn as_struct(&self) -> &StructType {
        match self {
            DataType::Struct(t) => t,
            _ => panic!("expect struct type"),
        }
    }

    /// Returns the inner type of a list type.
    ///
    /// # Panics
    ///
    /// Panics if the type is not a list type.
    pub fn as_list(&self) -> &DataType {
        match self {
            DataType::List(t) => t,
            _ => panic!("expect list type"),
        }
    }

    /// WARNING: Currently this should only be used in `WatermarkFilterExecutor`. Please be careful
    /// if you want to use this.
    pub fn min_value(&self) -> ScalarImpl {
        match self {
            DataType::Int16 => ScalarImpl::Int16(i16::MIN),
            DataType::Int32 => ScalarImpl::Int32(i32::MIN),
            DataType::Int64 => ScalarImpl::Int64(i64::MIN),
            DataType::Int256 => ScalarImpl::Int256(Int256::min_value()),
            DataType::Serial => ScalarImpl::Serial(Serial::from(i64::MIN)),
            DataType::Float32 => ScalarImpl::Float32(F32::neg_infinity()),
            DataType::Float64 => ScalarImpl::Float64(F64::neg_infinity()),
            DataType::Boolean => ScalarImpl::Bool(false),
            DataType::Varchar => ScalarImpl::Utf8("".into()),
            DataType::Bytea => ScalarImpl::Bytea("".to_string().into_bytes().into()),
            DataType::Date => ScalarImpl::Date(Date::MIN),
            DataType::Time => ScalarImpl::Time(Time::from_hms_uncheck(0, 0, 0)),
            DataType::Timestamp => ScalarImpl::Timestamp(Timestamp::MIN),
            DataType::Timestamptz => ScalarImpl::Timestamptz(Timestamptz::MIN),
            DataType::Decimal => ScalarImpl::Decimal(Decimal::NegativeInf),
            DataType::Interval => ScalarImpl::Interval(Interval::MIN),
            DataType::Jsonb => ScalarImpl::Jsonb(JsonbVal::null()), // NOT `min` #7981
            DataType::Struct(data_types) => ScalarImpl::Struct(StructValue::new(
                data_types
                    .types()
                    .map(|data_type| Some(data_type.min_value()))
                    .collect_vec(),
            )),
            DataType::List(data_type) => ScalarImpl::List(ListValue::empty(data_type)),
        }
    }

    /// Return a new type that removes the outer list.
    ///
    /// ```
    /// use risingwave_common::types::DataType::*;
    /// assert_eq!(List(Box::new(Int32)).unnest_list(), &Int32);
    /// assert_eq!(List(Box::new(List(Box::new(Int32)))).unnest_list(), &Int32);
    /// ```
    pub fn unnest_list(&self) -> &Self {
        match self {
            DataType::List(inner) => inner.unnest_list(),
            _ => self,
        }
    }

    /// Return the number of dimensions of this array/list type. Return `0` when this type is not an
    /// array/list.
    pub fn array_ndims(&self) -> usize {
        let mut d = 0;
        let mut t = self;
        while let Self::List(inner) = t {
            d += 1;
            t = inner;
        }
        d
    }

    /// Compares the datatype with another, ignoring nested field names and metadata.
    pub fn equals_datatype(&self, other: &DataType) -> bool {
        match (self, other) {
            (Self::Struct(s1), Self::Struct(s2)) => s1.equals_datatype(s2),
            (Self::List(d1), Self::List(d2)) => d1.equals_datatype(d2),
            _ => self == other,
        }
    }
}

impl From<DataType> for PbDataType {
    fn from(data_type: DataType) -> Self {
        data_type.to_protobuf()
    }
}

/// Common trait bounds of scalar and scalar reference types.
///
/// NOTE(rc): `Hash` is not in the trait bound list, it's implemented as [`ScalarRef::hash_scalar`].
pub trait ScalarBounds<Impl> = Debug
    + Send
    + Sync
    + Clone
    + PartialEq
    + Eq
    // in default ascending order
    + PartialOrd
    + Ord
    + TryFrom<Impl, Error = ArrayError>
    // `ScalarImpl`/`ScalarRefImpl`
    + Into<Impl>;

/// `Scalar` is a trait over all possible owned types in the evaluation
/// framework.
///
/// `Scalar` is reciprocal to `ScalarRef`. Use `as_scalar_ref` to get a
/// reference which has the same lifetime as `self`.
pub trait Scalar: ScalarBounds<ScalarImpl> + 'static {
    /// Type for reference of `Scalar`
    type ScalarRefType<'a>: ScalarRef<'a, ScalarType = Self> + 'a
    where
        Self: 'a;

    /// Get a reference to current scalar.
    fn as_scalar_ref(&self) -> Self::ScalarRefType<'_>;

    fn to_scalar_value(self) -> ScalarImpl {
        self.into()
    }
}

/// Convert an `Option<Scalar>` to corresponding `Option<ScalarRef>`.
pub fn option_as_scalar_ref<S: Scalar>(scalar: &Option<S>) -> Option<S::ScalarRefType<'_>> {
    scalar.as_ref().map(|x| x.as_scalar_ref())
}

/// `ScalarRef` is a trait over all possible references in the evaluation
/// framework.
///
/// `ScalarRef` is reciprocal to `Scalar`. Use `to_owned_scalar` to get an
/// owned scalar.
pub trait ScalarRef<'a>: ScalarBounds<ScalarRefImpl<'a>> + 'a + Copy {
    /// `ScalarType` is the owned type of current `ScalarRef`.
    type ScalarType: Scalar<ScalarRefType<'a> = Self>;

    /// Convert `ScalarRef` to an owned scalar.
    fn to_owned_scalar(&self) -> Self::ScalarType;

    /// A wrapped hash function to get the hash value for this scaler.
    fn hash_scalar<H: std::hash::Hasher>(&self, state: &mut H);
}

/// Define `ScalarImpl` and `ScalarRefImpl` with macro.
macro_rules! scalar_impl_enum {
    ($( { $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty } ),*) => {
        /// `ScalarImpl` embeds all possible scalars in the evaluation framework.
        #[derive(Debug, Clone, PartialEq, Eq, EstimateSize)]
        pub enum ScalarImpl {
            $( $variant_name($scalar) ),*
        }

        /// `ScalarRefImpl` embeds all possible scalar references in the evaluation
        /// framework.
        #[derive(Debug, Copy, Clone, PartialEq, Eq)]
        pub enum ScalarRefImpl<'scalar> {
            $( $variant_name($scalar_ref) ),*
        }
    };
}

for_all_scalar_variants! { scalar_impl_enum }

// We MUST NOT implement `Ord` for `ScalarImpl` because that will make `Datum` derive an incorrect
// default `Ord`. To get a default-ordered `ScalarImpl`/`ScalarRefImpl`/`Datum`/`DatumRef`, you can
// use `DefaultOrdered<T>`. If non-default order is needed, please refer to `sort_util`.
impl !PartialOrd for ScalarImpl {}
impl !PartialOrd for ScalarRefImpl<'_> {}

pub type Datum = Option<ScalarImpl>;
pub type DatumRef<'a> = Option<ScalarRefImpl<'a>>;

/// This trait is to implement `to_owned_datum` for `Option<ScalarImpl>`
pub trait ToOwnedDatum {
    /// Convert the datum to an owned [`Datum`].
    fn to_owned_datum(self) -> Datum;
}

impl ToOwnedDatum for DatumRef<'_> {
    #[inline(always)]
    fn to_owned_datum(self) -> Datum {
        self.map(ScalarRefImpl::into_scalar_impl)
    }
}

#[auto_impl::auto_impl(&)]
pub trait ToDatumRef: PartialEq + Eq + Debug {
    /// Convert the datum to [`DatumRef`].
    fn to_datum_ref(&self) -> DatumRef<'_>;
}

impl ToDatumRef for Datum {
    #[inline(always)]
    fn to_datum_ref(&self) -> DatumRef<'_> {
        self.as_ref().map(|d| d.as_scalar_ref_impl())
    }
}
impl ToDatumRef for Option<&ScalarImpl> {
    #[inline(always)]
    fn to_datum_ref(&self) -> DatumRef<'_> {
        self.map(|d| d.as_scalar_ref_impl())
    }
}
impl ToDatumRef for DatumRef<'_> {
    #[inline(always)]
    fn to_datum_ref(&self) -> DatumRef<'_> {
        *self
    }
}

/// To make sure there is `as_scalar_ref` for all scalar ref types.
pub trait SelfAsScalarRef {
    fn as_scalar_ref(&self) -> Self;
}
macro_rules! impl_self_as_scalar_ref {
    ($($t:ty),*) => {
        $(
            impl SelfAsScalarRef for $t {
                fn as_scalar_ref(&self) -> Self {
                    *self
                }
            }
        )*
    };
}
impl_self_as_scalar_ref! { &str, &[u8], Int256Ref<'_>, JsonbRef<'_>, ListRef<'_>, StructRef<'_>, ScalarRefImpl<'_> }

/// `for_all_native_types` includes all native variants of our scalar types.
///
/// Specifically, it doesn't support u8/u16/u32/u64.
#[macro_export]
macro_rules! for_all_native_types {
    ($macro:ident) => {
        $macro! {
            { i16, Int16 },
            { i32, Int32 },
            { i64, Int64 },
            { Serial, Serial },
            { $crate::types::F32, Float32 },
            { $crate::types::F64, Float64 }
        }
    };
}

/// `impl_convert` implements several conversions for `Scalar`.
/// * `Scalar <-> ScalarImpl` with `From` and `TryFrom` trait.
/// * `ScalarRef <-> ScalarRefImpl` with `From` and `TryFrom` trait.
/// * `&ScalarImpl -> &Scalar` with `impl.as_int16()`.
/// * `ScalarImpl -> Scalar` with `impl.into_int16()`.
macro_rules! impl_convert {
    ($( { $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty } ),*) => {
        $(
            impl From<$scalar> for ScalarImpl {
                fn from(val: $scalar) -> Self {
                    ScalarImpl::$variant_name(val)
                }
            }

            impl TryFrom<ScalarImpl> for $scalar {
                type Error = ArrayError;

                fn try_from(val: ScalarImpl) -> ArrayResult<Self> {
                    match val {
                        ScalarImpl::$variant_name(scalar) => Ok(scalar),
                        other_scalar => bail!("cannot convert ScalarImpl::{} to concrete type", other_scalar.get_ident()),
                    }
                }
            }

            impl <'scalar> From<$scalar_ref> for ScalarRefImpl<'scalar> {
                fn from(val: $scalar_ref) -> Self {
                    ScalarRefImpl::$variant_name(val)
                }
            }

            impl <'scalar> TryFrom<ScalarRefImpl<'scalar>> for $scalar_ref {
                type Error = ArrayError;

                fn try_from(val: ScalarRefImpl<'scalar>) -> ArrayResult<Self> {
                    match val {
                        ScalarRefImpl::$variant_name(scalar_ref) => Ok(scalar_ref),
                        other_scalar => bail!("cannot convert ScalarRefImpl::{} to concrete type {}", other_scalar.get_ident(), stringify!($variant_name)),
                    }
                }
            }

            paste! {
                impl ScalarImpl {
                    /// # Panics
                    /// If the scalar is not of the expected type.
                    pub fn [<as_ $suffix_name>](&self) -> &$scalar {
                        match self {
                            Self::$variant_name(ref scalar) => scalar,
                            other_scalar => panic!("cannot convert ScalarImpl::{} to concrete type {}", other_scalar.get_ident(), stringify!($variant_name))
                        }
                    }

                    /// # Panics
                    /// If the scalar is not of the expected type.
                    pub fn [<into_ $suffix_name>](self) -> $scalar {
                        match self {
                            Self::$variant_name(scalar) => scalar,
                            other_scalar =>  panic!("cannot convert ScalarImpl::{} to concrete type {}", other_scalar.get_ident(), stringify!($variant_name))
                        }
                    }
                }

                impl <'scalar> ScalarRefImpl<'scalar> {
                    /// # Panics
                    /// If the scalar is not of the expected type.
                    pub fn [<into_ $suffix_name>](self) -> $scalar_ref {
                        match self {
                            Self::$variant_name(inner) => inner,
                            other_scalar => panic!("cannot convert ScalarRefImpl::{} to concrete type {}", other_scalar.get_ident(), stringify!($variant_name))
                        }
                    }
                }
            }
        )*
    };
}

for_all_scalar_variants! { impl_convert }

// Implement `From<raw float>` for `ScalarImpl::Float` as a sugar.
impl From<f32> for ScalarImpl {
    fn from(f: f32) -> Self {
        Self::Float32(f.into())
    }
}
impl From<f64> for ScalarImpl {
    fn from(f: f64) -> Self {
        Self::Float64(f.into())
    }
}

// Implement `From<string like>` for `ScalarImpl::Utf8` as a sugar.
impl From<String> for ScalarImpl {
    fn from(s: String) -> Self {
        Self::Utf8(s.into_boxed_str())
    }
}
impl From<&str> for ScalarImpl {
    fn from(s: &str) -> Self {
        Self::Utf8(s.into())
    }
}
impl From<&String> for ScalarImpl {
    fn from(s: &String) -> Self {
        Self::Utf8(s.as_str().into())
    }
}
impl TryFrom<ScalarImpl> for String {
    type Error = ArrayError;

    fn try_from(val: ScalarImpl) -> ArrayResult<Self> {
        match val {
            ScalarImpl::Utf8(s) => Ok(s.into()),
            other_scalar => bail!(
                "cannot convert ScalarImpl::{} to concrete type",
                other_scalar.get_ident()
            ),
        }
    }
}

impl From<&[u8]> for ScalarImpl {
    fn from(s: &[u8]) -> Self {
        Self::Bytea(s.into())
    }
}

impl From<JsonbRef<'_>> for ScalarImpl {
    fn from(jsonb: JsonbRef<'_>) -> Self {
        Self::Jsonb(jsonb.to_owned_scalar())
    }
}

/// Error type for [`ScalarImpl::from_binary`] and [`ScalarImpl::from_text`].
#[derive(Debug, thiserror::Error, thiserror_ext::Construct)]
pub enum FromSqlError {
    #[error(transparent)]
    FromBinary(BoxedError),

    #[error("Invalid param: {0}")]
    FromText(String),

    #[error("Unsupported data type: {0}")]
    Unsupported(DataType),
}

impl ScalarImpl {
    pub fn from_binary(bytes: &Bytes, data_type: &DataType) -> Result<Self, FromSqlError> {
        let res = match data_type {
            DataType::Varchar => Self::Utf8(
                String::from_sql(&Type::VARCHAR, bytes)
                    .map_err(FromSqlError::from_binary)?
                    .into(),
            ),
            DataType::Bytea => Self::Bytea(
                Vec::<u8>::from_sql(&Type::BYTEA, bytes)
                    .map_err(FromSqlError::from_binary)?
                    .into(),
            ),
            DataType::Boolean => {
                Self::Bool(bool::from_sql(&Type::BOOL, bytes).map_err(FromSqlError::from_binary)?)
            }
            DataType::Int16 => {
                Self::Int16(i16::from_sql(&Type::INT2, bytes).map_err(FromSqlError::from_binary)?)
            }
            DataType::Int32 => {
                Self::Int32(i32::from_sql(&Type::INT4, bytes).map_err(FromSqlError::from_binary)?)
            }
            DataType::Int64 => {
                Self::Int64(i64::from_sql(&Type::INT8, bytes).map_err(FromSqlError::from_binary)?)
            }

            DataType::Serial => Self::Serial(Serial::from(
                i64::from_sql(&Type::INT8, bytes).map_err(FromSqlError::from_binary)?,
            )),
            DataType::Float32 => Self::Float32(
                f32::from_sql(&Type::FLOAT4, bytes)
                    .map_err(FromSqlError::from_binary)?
                    .into(),
            ),
            DataType::Float64 => Self::Float64(
                f64::from_sql(&Type::FLOAT8, bytes)
                    .map_err(FromSqlError::from_binary)?
                    .into(),
            ),
            DataType::Decimal => Self::Decimal(
                rust_decimal::Decimal::from_sql(&Type::NUMERIC, bytes)
                    .map_err(FromSqlError::from_binary)?
                    .into(),
            ),
            DataType::Date => Self::Date(
                chrono::NaiveDate::from_sql(&Type::DATE, bytes)
                    .map_err(FromSqlError::from_binary)?
                    .into(),
            ),
            DataType::Time => Self::Time(
                chrono::NaiveTime::from_sql(&Type::TIME, bytes)
                    .map_err(FromSqlError::from_binary)?
                    .into(),
            ),
            DataType::Timestamp => Self::Timestamp(
                chrono::NaiveDateTime::from_sql(&Type::TIMESTAMP, bytes)
                    .map_err(FromSqlError::from_binary)?
                    .into(),
            ),
            DataType::Timestamptz => Self::Timestamptz(
                chrono::DateTime::<chrono::Utc>::from_sql(&Type::TIMESTAMPTZ, bytes)
                    .map_err(FromSqlError::from_binary)?
                    .into(),
            ),
            DataType::Interval => Self::Interval(
                Interval::from_sql(&Type::INTERVAL, bytes).map_err(FromSqlError::from_binary)?,
            ),
            DataType::Jsonb => Self::Jsonb(
                JsonbVal::value_deserialize(bytes)
                    .ok_or_else(|| FromSqlError::from_binary("Invalid value of Jsonb"))?,
            ),
            DataType::Int256 => {
                Self::Int256(Int256::from_binary(bytes).map_err(FromSqlError::from_binary)?)
            }
            DataType::Struct(_) | DataType::List { .. } => {
                return Err(FromSqlError::Unsupported(data_type.clone()));
            }
        };
        Ok(res)
    }

    pub fn cstr_to_str(b: &[u8]) -> Result<&str, Utf8Error> {
        let without_null = if b.last() == Some(&0) {
            &b[..b.len() - 1]
        } else {
            b
        };
        std::str::from_utf8(without_null)
    }

    pub fn from_text(bytes: &[u8], data_type: &DataType) -> Result<Self, FromSqlError> {
        let str =
            Self::cstr_to_str(bytes).map_err(|_| FromSqlError::from_text(format!("{bytes:?}")))?;
        let res = match data_type {
            DataType::Varchar => Self::Utf8(str.to_string().into()),
            DataType::Boolean => {
                Self::Bool(str_to_bool(str).map_err(|_| FromSqlError::from_text(str))?)
            }
            DataType::Int16 => {
                Self::Int16(i16::from_str(str).map_err(|_| FromSqlError::from_text(str))?)
            }
            DataType::Int32 => {
                Self::Int32(i32::from_str(str).map_err(|_| FromSqlError::from_text(str))?)
            }
            DataType::Int64 => {
                Self::Int64(i64::from_str(str).map_err(|_| FromSqlError::from_text(str))?)
            }
            DataType::Int256 => {
                Self::Int256(Int256::from_str(str).map_err(|_| FromSqlError::from_text(str))?)
            }
            DataType::Serial => Self::Serial(Serial::from(
                i64::from_str(str).map_err(|_| FromSqlError::from_text(str))?,
            )),
            DataType::Float32 => Self::Float32(
                f32::from_str(str)
                    .map_err(|_| FromSqlError::from_text(str))?
                    .into(),
            ),
            DataType::Float64 => Self::Float64(
                f64::from_str(str)
                    .map_err(|_| FromSqlError::from_text(str))?
                    .into(),
            ),
            DataType::Decimal => Self::Decimal(
                rust_decimal::Decimal::from_str(str)
                    .map_err(|_| FromSqlError::from_text(str))?
                    .into(),
            ),
            DataType::Date => {
                Self::Date(Date::from_str(str).map_err(|_| FromSqlError::from_text(str))?)
            }
            DataType::Time => {
                Self::Time(Time::from_str(str).map_err(|_| FromSqlError::from_text(str))?)
            }
            DataType::Timestamp => {
                Self::Timestamp(Timestamp::from_str(str).map_err(|_| FromSqlError::from_text(str))?)
            }
            DataType::Timestamptz => Self::Timestamptz(
                Timestamptz::from_str(str).map_err(|_| FromSqlError::from_text(str))?,
            ),
            DataType::Interval => {
                Self::Interval(Interval::from_str(str).map_err(|_| FromSqlError::from_text(str))?)
            }
            DataType::Jsonb => {
                Self::Jsonb(JsonbVal::from_str(str).map_err(|_| FromSqlError::from_text(str))?)
            }
            DataType::List(elem_type) => {
                // TODO: support nested list
                if !(str.starts_with('{') && str.ends_with('}')) {
                    return Err(FromSqlError::from_text(str));
                }
                let mut builder = elem_type.create_array_builder(0);
                for s in str[1..str.len() - 1].split(',') {
                    if s.is_empty() {
                        continue;
                    } else if s.eq_ignore_ascii_case("null") {
                        builder.append_null();
                    } else {
                        builder.append(Some(Self::from_text(s.trim().as_bytes(), elem_type)?));
                    }
                }
                Self::List(ListValue::new(builder.finish()))
            }
            DataType::Struct(s) => {
                if !(str.starts_with('(') && str.ends_with(')')) {
                    return Err(FromSqlError::from_text(str));
                }
                let mut fields = Vec::with_capacity(s.len());
                for (s, ty) in str[1..str.len() - 1].split(',').zip_eq_debug(s.types()) {
                    fields.push(Some(Self::from_text(s.trim().as_bytes(), ty)?));
                }
                ScalarImpl::Struct(StructValue::new(fields))
            }
            DataType::Bytea => {
                return Err(FromSqlError::unsupported(data_type.clone()));
            }
        };
        Ok(res)
    }
}

impl From<ScalarRefImpl<'_>> for ScalarImpl {
    fn from(scalar_ref: ScalarRefImpl<'_>) -> Self {
        scalar_ref.into_scalar_impl()
    }
}

impl<'a> From<&'a ScalarImpl> for ScalarRefImpl<'a> {
    fn from(scalar: &'a ScalarImpl) -> Self {
        scalar.as_scalar_ref_impl()
    }
}

impl ScalarImpl {
    /// A lite version of casting from string to target type. Used by frontend to handle types that have
    /// to be created by casting.
    ///
    /// For example, the user can input `1` or `true` directly, but they have to use
    /// `'2022-01-01'::date`.
    pub fn from_literal(s: &str, t: &DataType) -> std::result::Result<Self, BoxedError> {
        Ok(match t {
            DataType::Boolean => str_to_bool(s)?.into(),
            DataType::Int16 => i16::from_str(s)?.into(),
            DataType::Int32 => i32::from_str(s)?.into(),
            DataType::Int64 => i64::from_str(s)?.into(),
            DataType::Int256 => Int256::from_str(s)?.into(),
            DataType::Serial => return Err("not supported".into()),
            DataType::Decimal => Decimal::from_str(s)?.into(),
            DataType::Float32 => F32::from_str(s)?.into(),
            DataType::Float64 => F64::from_str(s)?.into(),
            DataType::Varchar => s.into(),
            DataType::Date => Date::from_str(s)?.into(),
            DataType::Timestamp => Timestamp::from_str(s)?.into(),
            // We only handle the case with timezone here, and leave the implicit session timezone case
            // for later phase.
            DataType::Timestamptz => Timestamptz::from_str(s)?.into(),
            DataType::Time => Time::from_str(s)?.into(),
            DataType::Interval => Interval::from_str(s)?.into(),
            DataType::List { .. } => ListValue::from_str(s, t)?.into(),
            // Not processing struct literal right now. Leave it for later phase (normal backend evaluation).
            DataType::Struct(_) => return Err("not supported".into()),
            DataType::Jsonb => JsonbVal::from_str(s)?.into(),
            DataType::Bytea => str_to_bytea(s)?.into(),
        })
    }

    /// Converts [`ScalarImpl`] to [`ScalarRefImpl`]
    pub fn as_scalar_ref_impl(&self) -> ScalarRefImpl<'_> {
        dispatch_scalar_variants!(self, inner, { inner.as_scalar_ref().into() })
    }
}

impl<'a> ScalarRefImpl<'a> {
    /// Converts [`ScalarRefImpl`] to [`ScalarImpl`]
    pub fn into_scalar_impl(self) -> ScalarImpl {
        dispatch_scalar_ref_variants!(self, inner, { inner.to_owned_scalar().into() })
    }
}

impl Hash for ScalarImpl {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        dispatch_scalar_variants!(self, inner, { inner.as_scalar_ref().hash_scalar(state) })
    }
}

impl Hash for ScalarRefImpl<'_> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        dispatch_scalar_ref_variants!(self, inner, { inner.hash_scalar(state) })
    }
}

/// Feeds the raw scalar reference of `datum` to the given `state`, which should behave the same
/// as [`crate::array::Array::hash_at`], where NULL value will be carefully handled.
///
/// **FIXME**: the result of this function might be different from [`std::hash::Hash`] due to the
/// type alias of `DatumRef = Option<_>`, we should manually implement [`std::hash::Hash`] for
/// [`DatumRef`] in the future when it becomes a newtype. (#477)
#[inline(always)]
pub fn hash_datum(datum: impl ToDatumRef, state: &mut impl std::hash::Hasher) {
    match datum.to_datum_ref() {
        Some(scalar_ref) => scalar_ref.hash(state),
        None => NULL_VAL_FOR_HASH.hash(state),
    }
}

impl ScalarRefImpl<'_> {
    /// Encode the scalar to postgresql binary format.
    /// The encoder implements encoding using <https://docs.rs/postgres-types/0.2.3/postgres_types/trait.ToSql.html>
    pub fn binary_format(&self, data_type: &DataType) -> to_binary::Result<Bytes> {
        self.to_binary_with_type(data_type).transpose().unwrap()
    }

    pub fn text_format(&self, data_type: &DataType) -> String {
        self.to_text_with_type(data_type)
    }

    /// Serialize the scalar.
    pub fn serialize(
        &self,
        ser: &mut memcomparable::Serializer<impl BufMut>,
    ) -> memcomparable::Result<()> {
        match self {
            Self::Int16(v) => v.serialize(ser)?,
            Self::Int32(v) => v.serialize(ser)?,
            Self::Int64(v) => v.serialize(ser)?,
            Self::Serial(v) => v.serialize(ser)?,
            Self::Float32(v) => v.serialize(ser)?,
            Self::Float64(v) => v.serialize(ser)?,
            Self::Utf8(v) => v.serialize(ser)?,
            Self::Bytea(v) => ser.serialize_bytes(v)?,
            Self::Bool(v) => v.serialize(ser)?,
            Self::Decimal(v) => ser.serialize_decimal((*v).into())?,
            Self::Interval(v) => v.serialize(ser)?,
            Self::Date(v) => v.0.num_days_from_ce().serialize(ser)?,
            Self::Timestamp(v) => {
                v.0.timestamp().serialize(&mut *ser)?;
                v.0.timestamp_subsec_nanos().serialize(ser)?;
            }
            Self::Timestamptz(v) => v.serialize(ser)?,
            Self::Time(v) => {
                v.0.num_seconds_from_midnight().serialize(&mut *ser)?;
                v.0.nanosecond().serialize(ser)?;
            }
            Self::Int256(v) => v.memcmp_serialize(ser)?,
            Self::Jsonb(v) => v.memcmp_serialize(ser)?,
            Self::Struct(v) => v.memcmp_serialize(ser)?,
            Self::List(v) => v.memcmp_serialize(ser)?,
        };
        Ok(())
    }
}

impl ScalarImpl {
    /// Serialize the scalar.
    pub fn serialize(
        &self,
        ser: &mut memcomparable::Serializer<impl BufMut>,
    ) -> memcomparable::Result<()> {
        self.as_scalar_ref_impl().serialize(ser)
    }

    /// Deserialize the scalar.
    pub fn deserialize(
        ty: &DataType,
        de: &mut memcomparable::Deserializer<impl Buf>,
    ) -> memcomparable::Result<Self> {
        use DataType as Ty;
        Ok(match ty {
            Ty::Int16 => Self::Int16(i16::deserialize(de)?),
            Ty::Int32 => Self::Int32(i32::deserialize(de)?),
            Ty::Int64 => Self::Int64(i64::deserialize(de)?),
            Ty::Int256 => Self::Int256(Int256::memcmp_deserialize(de)?),
            Ty::Serial => Self::Serial(Serial::from(i64::deserialize(de)?)),
            Ty::Float32 => Self::Float32(f32::deserialize(de)?.into()),
            Ty::Float64 => Self::Float64(f64::deserialize(de)?.into()),
            Ty::Varchar => Self::Utf8(Box::<str>::deserialize(de)?),
            Ty::Bytea => Self::Bytea(serde_bytes::ByteBuf::deserialize(de)?.into_vec().into()),
            Ty::Boolean => Self::Bool(bool::deserialize(de)?),
            Ty::Decimal => Self::Decimal(de.deserialize_decimal()?.into()),
            Ty::Interval => Self::Interval(Interval::deserialize(de)?),
            Ty::Time => Self::Time({
                let secs = u32::deserialize(&mut *de)?;
                let nano = u32::deserialize(de)?;
                Time::with_secs_nano(secs, nano)
                    .map_err(|e| memcomparable::Error::Message(e.to_report_string()))?
            }),
            Ty::Timestamp => Self::Timestamp({
                let secs = i64::deserialize(&mut *de)?;
                let nsecs = u32::deserialize(de)?;
                Timestamp::with_secs_nsecs(secs, nsecs)
                    .map_err(|e| memcomparable::Error::Message(e.to_report_string()))?
            }),
            Ty::Timestamptz => Self::Timestamptz(Timestamptz::deserialize(de)?),
            Ty::Date => Self::Date({
                let days = i32::deserialize(de)?;
                Date::with_days(days)
                    .map_err(|e| memcomparable::Error::Message(e.to_report_string()))?
            }),
            Ty::Jsonb => Self::Jsonb(JsonbVal::memcmp_deserialize(de)?),
            Ty::Struct(t) => StructValue::memcmp_deserialize(t.types(), de)?.to_scalar_value(),
            Ty::List(t) => ListValue::memcmp_deserialize(t, de)?.to_scalar_value(),
        })
    }

    pub fn as_integral(&self) -> i64 {
        match self {
            Self::Int16(v) => *v as i64,
            Self::Int32(v) => *v as i64,
            Self::Int64(v) => *v,
            _ => panic!(
                "Can't convert ScalarImpl::{} to a integral",
                self.get_ident()
            ),
        }
    }
}

/// Returns whether the `literal` matches the `data_type`.
pub fn literal_type_match(data_type: &DataType, literal: Option<&ScalarImpl>) -> bool {
    match literal {
        Some(scalar) => {
            macro_rules! matches {
                ($( { $DataType:ident, $PhysicalType:ident }),*) => {
                    match (data_type, scalar) {
                        $(
                            (DataType::$DataType { .. }, ScalarImpl::$PhysicalType(_)) => true,
                            (DataType::$DataType { .. }, _) => false, // so that we won't forget to match a new logical type
                        )*
                    }
                }
            }
            for_all_type_pairs! { matches }
        }
        None => true,
    }
}

#[cfg(test)]
mod tests {
    use std::hash::{BuildHasher, Hasher};

    use strum::IntoEnumIterator;

    use super::*;
    use crate::util::hash_util::Crc32FastBuilder;

    #[test]
    fn test_size() {
        use static_assertions::const_assert_eq;

        use crate::array::*;

        macro_rules! assert_item_size_eq {
            ($array:ty, $size:literal) => {
                const_assert_eq!(std::mem::size_of::<<$array as Array>::OwnedItem>(), $size);
            };
        }

        assert_item_size_eq!(StructArray, 16); // Box<[Datum]>
        assert_item_size_eq!(ListArray, 8); // Box<ArrayImpl>
        assert_item_size_eq!(Utf8Array, 16); // Box<str>
        assert_item_size_eq!(IntervalArray, 16);
        assert_item_size_eq!(TimestampArray, 12);

        // TODO: try to reduce the memory usage of `Decimal`, `ScalarImpl` and `Datum`.
        assert_item_size_eq!(DecimalArray, 20);

        const_assert_eq!(std::mem::size_of::<ScalarImpl>(), 24);
        const_assert_eq!(std::mem::size_of::<ScalarRefImpl<'_>>(), 24);
        const_assert_eq!(std::mem::size_of::<Datum>(), 24);
        const_assert_eq!(std::mem::size_of::<StructType>(), 8);
        const_assert_eq!(std::mem::size_of::<DataType>(), 16);
    }

    #[test]
    fn test_data_type_display() {
        let d: DataType = DataType::new_struct(
            vec![DataType::Int32, DataType::Varchar],
            vec!["i".to_string(), "j".to_string()],
        );
        assert_eq!(
            format!("{}", d),
            "struct<i integer, j character varying>".to_string()
        );
    }

    #[test]
    fn test_hash_implementation() {
        fn test(datum: Datum, data_type: DataType) {
            assert!(literal_type_match(&data_type, datum.as_ref()));

            let mut builder = data_type.create_array_builder(6);
            for _ in 0..3 {
                builder.append_null();
                builder.append(&datum);
            }
            let array = builder.finish();

            let hash_from_array = {
                let mut state = Crc32FastBuilder.build_hasher();
                array.hash_at(3, &mut state);
                state.finish()
            };

            let hash_from_datum = {
                let mut state = Crc32FastBuilder.build_hasher();
                hash_datum(&datum, &mut state);
                state.finish()
            };

            let hash_from_datum_ref = {
                let mut state = Crc32FastBuilder.build_hasher();
                hash_datum(datum.to_datum_ref(), &mut state);
                state.finish()
            };

            assert_eq!(hash_from_array, hash_from_datum);
            assert_eq!(hash_from_datum, hash_from_datum_ref);
        }

        for name in DataTypeName::iter() {
            let (scalar, data_type) = match name {
                DataTypeName::Boolean => (ScalarImpl::Bool(true), DataType::Boolean),
                DataTypeName::Int16 => (ScalarImpl::Int16(233), DataType::Int16),
                DataTypeName::Int32 => (ScalarImpl::Int32(233333), DataType::Int32),
                DataTypeName::Int64 => (ScalarImpl::Int64(233333333333), DataType::Int64),
                DataTypeName::Int256 => (
                    ScalarImpl::Int256(233333333333_i64.into()),
                    DataType::Int256,
                ),
                DataTypeName::Serial => (ScalarImpl::Serial(233333333333.into()), DataType::Serial),
                DataTypeName::Float32 => (ScalarImpl::Float32(23.33.into()), DataType::Float32),
                DataTypeName::Float64 => (
                    ScalarImpl::Float64(23.333333333333.into()),
                    DataType::Float64,
                ),
                DataTypeName::Decimal => (
                    ScalarImpl::Decimal("233.33".parse().unwrap()),
                    DataType::Decimal,
                ),
                DataTypeName::Date => (
                    ScalarImpl::Date(Date::from_ymd_uncheck(2333, 3, 3)),
                    DataType::Date,
                ),
                DataTypeName::Varchar => (ScalarImpl::Utf8("233".into()), DataType::Varchar),
                DataTypeName::Bytea => (
                    ScalarImpl::Bytea("\\x233".as_bytes().into()),
                    DataType::Bytea,
                ),
                DataTypeName::Time => (
                    ScalarImpl::Time(Time::from_hms_uncheck(2, 3, 3)),
                    DataType::Time,
                ),
                DataTypeName::Timestamp => (
                    ScalarImpl::Timestamp(Timestamp::from_timestamp_uncheck(23333333, 2333)),
                    DataType::Timestamp,
                ),
                DataTypeName::Timestamptz => (
                    ScalarImpl::Timestamptz(Timestamptz::from_micros(233333333)),
                    DataType::Timestamptz,
                ),
                DataTypeName::Interval => (
                    ScalarImpl::Interval(Interval::from_month_day_usec(2, 3, 3333)),
                    DataType::Interval,
                ),
                DataTypeName::Jsonb => (ScalarImpl::Jsonb(JsonbVal::null()), DataType::Jsonb),
                DataTypeName::Struct => (
                    ScalarImpl::Struct(StructValue::new(vec![
                        ScalarImpl::Int64(233).into(),
                        ScalarImpl::Float64(23.33.into()).into(),
                    ])),
                    DataType::Struct(StructType::new(vec![
                        ("a", DataType::Int64),
                        ("b", DataType::Float64),
                    ])),
                ),
                DataTypeName::List => (
                    ScalarImpl::List(ListValue::from_iter([233i64, 2333])),
                    DataType::List(Box::new(DataType::Int64)),
                ),
            };

            test(Some(scalar), data_type.clone());
            test(None, data_type);
        }
    }

    #[test]
    fn test_data_type_from_str() {
        assert_eq!(DataType::from_str("bool").unwrap(), DataType::Boolean);
        assert_eq!(DataType::from_str("boolean").unwrap(), DataType::Boolean);
        assert_eq!(DataType::from_str("BOOL").unwrap(), DataType::Boolean);
        assert_eq!(DataType::from_str("BOOLEAN").unwrap(), DataType::Boolean);

        assert_eq!(DataType::from_str("int2").unwrap(), DataType::Int16);
        assert_eq!(DataType::from_str("smallint").unwrap(), DataType::Int16);
        assert_eq!(DataType::from_str("INT2").unwrap(), DataType::Int16);
        assert_eq!(DataType::from_str("SMALLINT").unwrap(), DataType::Int16);

        assert_eq!(DataType::from_str("int4").unwrap(), DataType::Int32);
        assert_eq!(DataType::from_str("integer").unwrap(), DataType::Int32);
        assert_eq!(DataType::from_str("int4").unwrap(), DataType::Int32);
        assert_eq!(DataType::from_str("INT4").unwrap(), DataType::Int32);
        assert_eq!(DataType::from_str("INTEGER").unwrap(), DataType::Int32);
        assert_eq!(DataType::from_str("INT").unwrap(), DataType::Int32);

        assert_eq!(DataType::from_str("int8").unwrap(), DataType::Int64);
        assert_eq!(DataType::from_str("bigint").unwrap(), DataType::Int64);
        assert_eq!(DataType::from_str("INT8").unwrap(), DataType::Int64);
        assert_eq!(DataType::from_str("BIGINT").unwrap(), DataType::Int64);

        assert_eq!(DataType::from_str("rw_int256").unwrap(), DataType::Int256);
        assert_eq!(DataType::from_str("RW_INT256").unwrap(), DataType::Int256);

        assert_eq!(DataType::from_str("float4").unwrap(), DataType::Float32);
        assert_eq!(DataType::from_str("real").unwrap(), DataType::Float32);
        assert_eq!(DataType::from_str("FLOAT4").unwrap(), DataType::Float32);
        assert_eq!(DataType::from_str("REAL").unwrap(), DataType::Float32);

        assert_eq!(DataType::from_str("float8").unwrap(), DataType::Float64);
        assert_eq!(
            DataType::from_str("double precision").unwrap(),
            DataType::Float64
        );
        assert_eq!(DataType::from_str("FLOAT8").unwrap(), DataType::Float64);
        assert_eq!(
            DataType::from_str("DOUBLE PRECISION").unwrap(),
            DataType::Float64
        );

        assert_eq!(DataType::from_str("decimal").unwrap(), DataType::Decimal);
        assert_eq!(DataType::from_str("DECIMAL").unwrap(), DataType::Decimal);
        assert_eq!(DataType::from_str("numeric").unwrap(), DataType::Decimal);
        assert_eq!(DataType::from_str("NUMERIC").unwrap(), DataType::Decimal);

        assert_eq!(DataType::from_str("date").unwrap(), DataType::Date);
        assert_eq!(DataType::from_str("DATE").unwrap(), DataType::Date);

        assert_eq!(DataType::from_str("varchar").unwrap(), DataType::Varchar);
        assert_eq!(DataType::from_str("VARCHAR").unwrap(), DataType::Varchar);

        assert_eq!(DataType::from_str("time").unwrap(), DataType::Time);
        assert_eq!(
            DataType::from_str("time without time zone").unwrap(),
            DataType::Time
        );
        assert_eq!(DataType::from_str("TIME").unwrap(), DataType::Time);
        assert_eq!(
            DataType::from_str("TIME WITHOUT TIME ZONE").unwrap(),
            DataType::Time
        );

        assert_eq!(
            DataType::from_str("timestamp").unwrap(),
            DataType::Timestamp
        );
        assert_eq!(
            DataType::from_str("timestamp without time zone").unwrap(),
            DataType::Timestamp
        );
        assert_eq!(
            DataType::from_str("TIMESTAMP").unwrap(),
            DataType::Timestamp
        );
        assert_eq!(
            DataType::from_str("TIMESTAMP WITHOUT TIME ZONE").unwrap(),
            DataType::Timestamp
        );

        assert_eq!(
            DataType::from_str("timestamptz").unwrap(),
            DataType::Timestamptz
        );
        assert_eq!(
            DataType::from_str("timestamp with time zone").unwrap(),
            DataType::Timestamptz
        );
        assert_eq!(
            DataType::from_str("TIMESTAMPTZ").unwrap(),
            DataType::Timestamptz
        );
        assert_eq!(
            DataType::from_str("TIMESTAMP WITH TIME ZONE").unwrap(),
            DataType::Timestamptz
        );

        assert_eq!(DataType::from_str("interval").unwrap(), DataType::Interval);
        assert_eq!(DataType::from_str("INTERVAL").unwrap(), DataType::Interval);

        assert_eq!(
            DataType::from_str("int2[]").unwrap(),
            DataType::List(Box::new(DataType::Int16))
        );
        assert_eq!(
            DataType::from_str("int[]").unwrap(),
            DataType::List(Box::new(DataType::Int32))
        );
        assert_eq!(
            DataType::from_str("int8[]").unwrap(),
            DataType::List(Box::new(DataType::Int64))
        );
        assert_eq!(
            DataType::from_str("float4[]").unwrap(),
            DataType::List(Box::new(DataType::Float32))
        );
        assert_eq!(
            DataType::from_str("float8[]").unwrap(),
            DataType::List(Box::new(DataType::Float64))
        );
        assert_eq!(
            DataType::from_str("decimal[]").unwrap(),
            DataType::List(Box::new(DataType::Decimal))
        );
        assert_eq!(
            DataType::from_str("varchar[]").unwrap(),
            DataType::List(Box::new(DataType::Varchar))
        );
        assert_eq!(
            DataType::from_str("date[]").unwrap(),
            DataType::List(Box::new(DataType::Date))
        );
        assert_eq!(
            DataType::from_str("time[]").unwrap(),
            DataType::List(Box::new(DataType::Time))
        );
        assert_eq!(
            DataType::from_str("timestamp[]").unwrap(),
            DataType::List(Box::new(DataType::Timestamp))
        );
        assert_eq!(
            DataType::from_str("timestamptz[]").unwrap(),
            DataType::List(Box::new(DataType::Timestamptz))
        );
        assert_eq!(
            DataType::from_str("interval[]").unwrap(),
            DataType::List(Box::new(DataType::Interval))
        );

        assert_eq!(
            DataType::from_str("record").unwrap(),
            DataType::Struct(StructType::unnamed(vec![]))
        );
        assert_eq!(
            DataType::from_str("struct<a int4, b varchar>").unwrap(),
            DataType::Struct(StructType::new(vec![
                ("a", DataType::Int32),
                ("b", DataType::Varchar)
            ]))
        );
    }
}
