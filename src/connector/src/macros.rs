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

#[macro_export]
macro_rules! for_all_classified_sources {
    ($macro:path $(,$extra_args:tt)*) => {
        $macro! {
            // cdc sources
            {
                { Mysql },
                { Postgres },
                { Citus }
            },
            // other sources
            // todo: file source do not nest with mq source.
            {
                { Kafka, $crate::source::kafka::KafkaProperties, $crate::source::kafka::KafkaSplit },
                { Pulsar, $crate::source::pulsar::PulsarProperties, $crate::source::pulsar::PulsarSplit },
                { Kinesis, $crate::source::kinesis::KinesisProperties, $crate::source::kinesis::split::KinesisSplit },
                { Nexmark, $crate::source::nexmark::NexmarkProperties, $crate::source::nexmark::NexmarkSplit },
                { Datagen, $crate::source::datagen::DatagenProperties, $crate::source::datagen::DatagenSplit },
                { GooglePubsub, $crate::source::google_pubsub::PubsubProperties, $crate::source::google_pubsub::PubsubSplit },
                { Nats, $crate::source::nats::NatsProperties, $crate::source::nats::split::NatsSplit },
                { S3, $crate::source::filesystem::S3Properties, $crate::source::filesystem::FsSplit },
                { Gcs, $crate::source::filesystem::opendal_source::GcsProperties , $crate::source::filesystem::OpendalFsSplit<$crate::source::filesystem::opendal_source::OpendalGcs> },
                { OpendalS3, $crate::source::filesystem::opendal_source::OpendalS3Properties, $crate::source::filesystem::OpendalFsSplit<$crate::source::filesystem::opendal_source::OpendalS3> },
                { PosixFs, $crate::source::filesystem::opendal_source::PosixFsProperties, $crate::source::filesystem::OpendalFsSplit<$crate::source::filesystem::opendal_source::OpendalPosixFs> },
                { Test, $crate::source::test_source::TestSourceProperties, $crate::source::test_source::TestSourceSplit}
            }
            $(
                ,$extra_args
            )*
        }
    };
}

#[macro_export]
macro_rules! for_all_sources_inner {
    (
        {$({ $cdc_source_type:ident }),* },
        { $({ $source_variant:ident, $prop_name:ty, $split:ty }),* },
        $macro:tt $(, $extra_args:tt)*
    ) => {
        $crate::paste! {
            $macro! {
                {
                    $(
                        {
                            [< $cdc_source_type Cdc >],
                            $crate::source::cdc::[< $cdc_source_type CdcProperties >],
                            $crate::source::cdc::DebeziumCdcSplit<$crate::source::cdc::$cdc_source_type>
                        },
                    )*
                    $(
                        { $source_variant, $prop_name, $split }
                    ),*
                }
                $(,$extra_args)*
            }
        }
    };
}

#[macro_export]
macro_rules! for_all_sources {
    ($macro:path $(, $arg:tt )*) => {
        $crate::for_all_classified_sources! {$crate::for_all_sources_inner, $macro $(,$arg)* }
    };
}

#[macro_export]
macro_rules! dispatch_source_enum_inner {
    (
        {$({$source_variant:ident, $prop_name:ty, $split:ty }),*},
        $enum_name:ident,
        $impl:tt,
        {$inner_name:ident, $prop_type_name:ident, $split_type_name:ident},
        $body:expr
    ) => {{
        match $impl {
            $(
                $enum_name::$source_variant($inner_name) => {
                    type $prop_type_name = $prop_name;
                    type $split_type_name = $split;
                    {
                        $body
                    }
                },
            )*
        }
    }}
}

#[macro_export]
macro_rules! dispatch_source_enum {
    ($enum_name:ident, $impl:expr, $inner_name:tt, $body:expr) => {{
        $crate::for_all_sources! {$crate::dispatch_source_enum_inner, $enum_name, { $impl }, $inner_name, $body}
    }};
}

#[macro_export]
macro_rules! match_source_name_str_inner {
    (
        {$({$source_variant:ident, $prop_name:ty, $split:ty }),*},
        $source_name_str:expr,
        $prop_type_name:ident,
        $body:expr,
        $on_other_closure:expr
    ) => {{
        match $source_name_str {
            $(
                <$prop_name>::SOURCE_NAME => {
                    type $prop_type_name = $prop_name;
                    {
                        $body
                    }
                },
            )*
            other => ($on_other_closure)(other),
        }
    }}
}

/// Matches against `SourceProperties::SOURCE_NAME` to dispatch logic.
#[macro_export]
macro_rules! match_source_name_str {
    ($source_name_str:expr, $prop_type_name:ident, $body:expr, $on_other_closure:expr) => {{
        $crate::for_all_sources! {
            $crate::match_source_name_str_inner,
            { $source_name_str },
            $prop_type_name,
            { $body },
            { $on_other_closure }
        }
    }};
}

#[macro_export]
macro_rules! dispatch_split_impl {
    ($impl:expr, $inner_name:ident, $prop_type_name:ident, $body:expr) => {{
        use $crate::source::SplitImpl;
        $crate::dispatch_source_enum! {SplitImpl, { $impl }, {$inner_name, $prop_type_name, IgnoreSplitType}, $body}
    }};
}

#[macro_export]
macro_rules! impl_split {
    ({$({ $variant_name:ident, $prop_name:ty, $split:ty}),*}) => {

        #[derive(Debug, Clone, EnumAsInner, PartialEq)]
        pub enum SplitImpl {
            $(
                $variant_name($split),
            )*
        }

        $(
            impl TryFrom<SplitImpl> for $split {
                type Error = anyhow::Error;

                fn try_from(split: SplitImpl) -> std::result::Result<Self, Self::Error> {
                    match split {
                        SplitImpl::$variant_name(inner) => Ok(inner),
                        other => Err(anyhow::anyhow!("expect {} but get {:?}", stringify!($split), other))
                    }
                }
            }

            impl From<$split> for SplitImpl {
                fn from(split: $split) -> SplitImpl {
                    SplitImpl::$variant_name(split)
                }
            }

        )*
    }
}

#[macro_export]
macro_rules! dispatch_source_prop {
    ($impl:expr, $source_prop:tt, $body:expr) => {{
        use $crate::source::ConnectorProperties;
        $crate::dispatch_source_enum! {ConnectorProperties, { $impl }, {$source_prop, IgnorePropType, IgnoreSplitType}, {$body}}
    }};
}

#[macro_export]
macro_rules! impl_connector_properties {
    ({$({ $variant_name:ident, $prop_name:ty, $split:ty}),*}) => {
        #[derive(Clone, Debug)]
        pub enum ConnectorProperties {
            $(
                $variant_name(Box<$prop_name>),
            )*
        }

        $(
            impl From<$prop_name> for ConnectorProperties {
                fn from(prop: $prop_name) -> ConnectorProperties {
                    ConnectorProperties::$variant_name(Box::new(prop))
                }
            }
        )*
    }
}

#[macro_export]
macro_rules! impl_cdc_source_type {
    (
        {$({$cdc_source_type:tt}),*},
        {$($_ignore:tt),*}
    ) => {
        $(
            $crate::paste!{
                #[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
                pub struct $cdc_source_type;
                impl CdcSourceTypeTrait for $cdc_source_type {
                    const CDC_CONNECTOR_NAME: &'static str = concat!(stringify!([<$cdc_source_type:lower>]), "-cdc");
                    fn source_type() -> CdcSourceType {
                        CdcSourceType::$cdc_source_type
                    }
                }
                pub type [<$cdc_source_type CdcProperties>] = CdcProperties<$cdc_source_type>;
            }
        )*

        pub enum CdcSourceType {
            $(
                $cdc_source_type,
            )*
            Unspecified,
        }

        impl From<PbSourceType> for CdcSourceType {
            fn from(value: PbSourceType) -> Self {
                match value {
                    PbSourceType::Unspecified => CdcSourceType::Unspecified,
                    $(
                        PbSourceType::$cdc_source_type => CdcSourceType::$cdc_source_type,
                    )*
                }
            }
        }

        impl From<CdcSourceType> for PbSourceType {
            fn from(this: CdcSourceType) -> PbSourceType {
                match this {
                    $(
                        CdcSourceType::$cdc_source_type => PbSourceType::$cdc_source_type,
                    )*
                   CdcSourceType::Unspecified => PbSourceType::Unspecified,
                }
            }
        }

    }
}
