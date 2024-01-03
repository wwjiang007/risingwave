// Copyright 2023 RisingWave Labs
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

use risingwave_common::types::{Decimal, Date, Timestamp, Timestamptz};
use risingwave_expr::function;

// Bucket
trait Hash {
    fn hash(&self) -> i32;
}

impl Hash for i32 {
    fn hash(&self) -> i32 {
        todo!()
    }
}

impl Hash for i64 {
    fn hash(&self) -> i32 {
        todo!()
    }
}

#[function(
    "iceberg_bucket(int4, int4) -> int4",
    prebuild = "i32::clone(&$1)"
)]
#[function(
    "iceberg_bucket(int8, int4) -> int4",
    prebuild = "i32::clone(&$1)"
)]
fn iceberg_bucket_i32<T: Hash>(v1: T, n: &i32) -> i32 {
    todo!()
}


// Truncate
trait Truncate {
    fn truncate(&self, n: &i32) -> Self;
}

impl Truncate for i32 {
    fn truncate(&self, n: &i32) -> Self {
        todo!()
    }
}

impl Truncate for i64 {
    fn truncate(&self, n: &i32) -> Self {
        todo!()
    }
}

#[function(
    "iceberg_truncate(int4, int4) -> int4",
    prebuild = "i32::clone(&$1)"
)]
#[function(
    "iceberg_truncate(int8, int4) -> int4",
    prebuild = "i32::clone(&$1)"
)]
fn iceberg_truncate<T: Truncate>(v1: T, n: &i32) -> i32 {
    todo!()
}

trait Time {
    fn year(&self) -> i32;
    fn month(&self) -> i32;
    fn day(&self) -> i32;
}

impl Time for Date {
    fn year(&self) -> i32 {
        todo!()
    }

    fn month(&self) -> i32 {
        todo!()
    }

    fn day(&self) -> i32 {
        todo!()
    }
}

impl Time for Timestamp {
    fn year(&self) -> i32 {
        todo!()
    }

    fn month(&self) -> i32 {
        todo!()
    }

    fn day(&self) -> i32 {
        todo!()
    }
}

impl Time for Timestamptz {
    fn year(&self) -> i32 {
        todo!()
    }

    fn month(&self) -> i32 {
        todo!()
    }

    fn day(&self) -> i32 {
        todo!()
    }
}

#[function(
    "iceberg_year(date) -> int4",
)]
#[function(
    "iceberg_year(timestamp) -> int4",
)]
#[function(
    "iceberg_year(timestamptz) -> int4",
)]
fn iceberg_year<T: Time>(v1: T) -> i32 {
    v1.year()
}

#[function(
    "iceberg_month(date) -> int4",
)]
#[function(
    "iceberg_month(timestamp) -> int4",
)]
#[function(
    "iceberg_month(timestamptz) -> int4",
)]
fn iceberg_month<T: Time>(v1: T) -> i32 {
    v1.month()
}

#[function(
    "iceberg_day(date) -> int4",
)]
#[function(
    "iceberg_day(timestamp) -> int4",
)]
#[function(
    "iceberg_day(timestamptz) -> int4",
)]
fn iceberg_day<T: Time>(v1: T) -> i32 {
    v1.day()
}

trait Hour {
    fn hour(&self) -> i32;
}

impl Hour for Timestamp {
    fn hour(&self) -> i32 {
        todo!()
    }
}

impl Hour for Timestamptz {
    fn hour(&self) -> i32 {
        todo!()
    }
}

#[function(
    "iceberg_hour(timestamp) -> int4",
)]
#[function(
    "iceberg_hour(timestamptz) -> int4",
)]
fn iceberg_hour<T: Hour>(v1: T) -> i32 {
    v1.hour()
}

