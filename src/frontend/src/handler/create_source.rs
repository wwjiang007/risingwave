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

use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::sync::LazyLock;

use either::Either;
use itertools::Itertools;
use maplit::{convert_args, hashmap};
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::catalog::{
    is_column_ids_dedup, ColumnCatalog, ColumnDesc, TableId, INITIAL_SOURCE_VERSION_ID,
    KAFKA_TIMESTAMP_COLUMN_NAME,
};
use risingwave_common::error::ErrorCode::{self, InvalidInputSyntax, ProtocolError};
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::DataType;
use risingwave_connector::parser::{
    schema_to_columns, AvroParserConfig, DebeziumAvroParserConfig, ProtobufParserConfig,
    SpecificParserConfig,
};
use risingwave_connector::schema::schema_registry::{
    name_strategy_from_str, SchemaRegistryAuth, SCHEMA_REGISTRY_PASSWORD, SCHEMA_REGISTRY_USERNAME,
};
use risingwave_connector::source::cdc::external::CdcTableType;
use risingwave_connector::source::cdc::{
    CDC_SHARING_MODE_KEY, CDC_SNAPSHOT_BACKFILL, CDC_SNAPSHOT_MODE_KEY, CITUS_CDC_CONNECTOR,
    MYSQL_CDC_CONNECTOR, POSTGRES_CDC_CONNECTOR,
};
use risingwave_connector::source::datagen::DATAGEN_CONNECTOR;
use risingwave_connector::source::nexmark::source::{get_event_data_types_with_names, EventType};
use risingwave_connector::source::test_source::TEST_CONNECTOR;
use risingwave_connector::source::{
    get_connector_compatible_additional_columns, GCS_CONNECTOR, GOOGLE_PUBSUB_CONNECTOR,
    KAFKA_CONNECTOR, KINESIS_CONNECTOR, NATS_CONNECTOR, NEXMARK_CONNECTOR, OPENDAL_S3_CONNECTOR,
    PULSAR_CONNECTOR, S3_CONNECTOR,
};
use risingwave_pb::catalog::{
    PbSchemaRegistryNameStrategy, PbSource, StreamSourceInfo, WatermarkDesc,
};
use risingwave_pb::plan_common::{AdditionalColumnType, EncodeType, FormatType};
use risingwave_pb::stream_plan::stream_fragment_graph::Parallelism;
use risingwave_sqlparser::ast::{
    get_delimiter, AstString, ColumnDef, ConnectorSchema, CreateSourceStatement, Encode, Format,
    Ident, ProtobufSchema, SourceWatermark,
};

use super::RwPgResponse;
use crate::binder::Binder;
use crate::catalog::source_catalog::SourceCatalog;
use crate::catalog::ColumnId;
use crate::expr::Expr;
use crate::handler::create_table::{
    bind_pk_on_relation, bind_sql_column_constraints, bind_sql_columns, bind_sql_pk_names,
    ensure_table_constraints_supported, ColumnIdGenerator,
};
use crate::handler::util::{
    get_connector, is_cdc_connector, is_kafka_connector, SourceSchemaCompatExt,
};
use crate::handler::HandlerArgs;
use crate::optimizer::plan_node::{LogicalSource, ToStream, ToStreamContext};
use crate::session::SessionImpl;
use crate::utils::resolve_privatelink_in_with_option;
use crate::{bind_data_type, build_graph, OptimizerContext, WithOptions};

pub(crate) const UPSTREAM_SOURCE_KEY: &str = "connector";

/// Map a JSON schema to a relational schema
async fn extract_json_table_schema(
    schema_config: &Option<(AstString, bool)>,
    with_properties: &HashMap<String, String>,
    format_encode_options: &mut BTreeMap<String, String>,
) -> Result<Option<Vec<ColumnCatalog>>> {
    match schema_config {
        None => Ok(None),
        Some((schema_location, use_schema_registry)) => {
            let schema_registry_auth = use_schema_registry.then(|| {
                let auth = SchemaRegistryAuth::from(&*format_encode_options);
                try_consume_string_from_options(format_encode_options, SCHEMA_REGISTRY_USERNAME);
                try_consume_string_from_options(format_encode_options, SCHEMA_REGISTRY_PASSWORD);
                auth
            });
            Ok(Some(
                schema_to_columns(&schema_location.0, schema_registry_auth, with_properties)
                    .await?
                    .into_iter()
                    .map(|col| ColumnCatalog {
                        column_desc: col.into(),
                        is_hidden: false,
                    })
                    .collect_vec(),
            ))
        }
    }
}

pub fn debezium_cdc_source_schema() -> Vec<ColumnCatalog> {
    let columns = vec![
        ColumnCatalog {
            column_desc: ColumnDesc::named("payload", ColumnId::placeholder(), DataType::Jsonb),
            is_hidden: false,
        },
        ColumnCatalog::offset_column(),
        ColumnCatalog::cdc_table_name_column(),
    ];
    columns
}

fn json_schema_infer_use_schema_registry(schema_config: &Option<(AstString, bool)>) -> bool {
    match schema_config {
        None => false,
        Some((_, use_registry)) => *use_registry,
    }
}

/// Map an Avro schema to a relational schema.
async fn extract_avro_table_schema(
    info: &StreamSourceInfo,
    with_properties: &HashMap<String, String>,
    format_encode_options: &mut BTreeMap<String, String>,
    is_debezium: bool,
) -> Result<Vec<ColumnCatalog>> {
    let parser_config = SpecificParserConfig::new(info, with_properties)?;
    try_consume_string_from_options(format_encode_options, SCHEMA_REGISTRY_USERNAME);
    try_consume_string_from_options(format_encode_options, SCHEMA_REGISTRY_PASSWORD);
    consume_aws_config_from_options(format_encode_options);

    let vec_column_desc = if is_debezium {
        let conf = DebeziumAvroParserConfig::new(parser_config.encoding_config).await?;
        conf.map_to_columns()?
    } else {
        let conf = AvroParserConfig::new(parser_config.encoding_config).await?;
        conf.map_to_columns()?
    };
    Ok(vec_column_desc
        .into_iter()
        .map(|col| ColumnCatalog {
            column_desc: col.into(),
            is_hidden: false,
        })
        .collect_vec())
}

async fn extract_debezium_avro_table_pk_columns(
    info: &StreamSourceInfo,
    with_properties: &HashMap<String, String>,
) -> Result<Vec<String>> {
    let parser_config = SpecificParserConfig::new(info, with_properties)?;
    let conf = DebeziumAvroParserConfig::new(parser_config.encoding_config).await?;
    Ok(conf.extract_pks()?.drain(..).map(|c| c.name).collect())
}

/// Map a protobuf schema to a relational schema.
async fn extract_protobuf_table_schema(
    schema: &ProtobufSchema,
    with_properties: &HashMap<String, String>,
    format_encode_options: &mut BTreeMap<String, String>,
) -> Result<Vec<ColumnCatalog>> {
    let info = StreamSourceInfo {
        proto_message_name: schema.message_name.0.clone(),
        row_schema_location: schema.row_schema_location.0.clone(),
        use_schema_registry: schema.use_schema_registry,
        format: FormatType::Plain.into(),
        row_encode: EncodeType::Protobuf.into(),
        format_encode_options: format_encode_options.clone(),
        ..Default::default()
    };
    let parser_config = SpecificParserConfig::new(&info, with_properties)?;
    try_consume_string_from_options(format_encode_options, SCHEMA_REGISTRY_USERNAME);
    try_consume_string_from_options(format_encode_options, SCHEMA_REGISTRY_PASSWORD);
    consume_aws_config_from_options(format_encode_options);

    let conf = ProtobufParserConfig::new(parser_config.encoding_config).await?;

    let column_descs = conf.map_to_columns()?;

    Ok(column_descs
        .into_iter()
        .map(|col| ColumnCatalog {
            column_desc: col.into(),
            is_hidden: false,
        })
        .collect_vec())
}

fn non_generated_sql_columns(columns: &[ColumnDef]) -> Vec<ColumnDef> {
    columns
        .iter()
        .filter(|c| !c.is_generated())
        .cloned()
        .collect()
}

fn try_consume_string_from_options(
    format_encode_options: &mut BTreeMap<String, String>,
    key: &str,
) -> Option<AstString> {
    format_encode_options.remove(key).map(AstString)
}

fn consume_string_from_options(
    format_encode_options: &mut BTreeMap<String, String>,
    key: &str,
) -> Result<AstString> {
    try_consume_string_from_options(format_encode_options, key).ok_or(RwError::from(ProtocolError(
        format!("missing field {} in options", key),
    )))
}

fn consume_aws_config_from_options(format_encode_options: &mut BTreeMap<String, String>) {
    format_encode_options.retain(|key, _| !key.starts_with("aws."))
}

pub fn get_json_schema_location(
    format_encode_options: &mut BTreeMap<String, String>,
) -> Result<Option<(AstString, bool)>> {
    let schema_location = try_consume_string_from_options(format_encode_options, "schema.location");
    let schema_registry = try_consume_string_from_options(format_encode_options, "schema.registry");
    match (schema_location, schema_registry) {
        (None, None) => Ok(None),
        (None, Some(schema_registry)) => Ok(Some((schema_registry, true))),
        (Some(schema_location), None) => Ok(Some((schema_location, false))),
        (Some(_), Some(_)) => Err(RwError::from(ProtocolError(
            "only need either the schema location or the schema registry".to_string(),
        ))),
    }
}

fn get_schema_location(
    format_encode_options: &mut BTreeMap<String, String>,
) -> Result<(AstString, bool)> {
    let schema_location = try_consume_string_from_options(format_encode_options, "schema.location");
    let schema_registry = try_consume_string_from_options(format_encode_options, "schema.registry");
    match (schema_location, schema_registry) {
        (None, None) => Err(RwError::from(ProtocolError(
            "missing either a schema location or a schema registry".to_string(),
        ))),
        (None, Some(schema_registry)) => Ok((schema_registry, true)),
        (Some(schema_location), None) => Ok((schema_location, false)),
        (Some(_), Some(_)) => Err(RwError::from(ProtocolError(
            "only need either the schema location or the schema registry".to_string(),
        ))),
    }
}

#[inline]
fn get_name_strategy_or_default(name_strategy: Option<AstString>) -> Result<Option<i32>> {
    match name_strategy {
        None => Ok(None),
        Some(name) => Ok(Some(name_strategy_from_str(name.0.as_str())
            .ok_or_else(|| RwError::from(ProtocolError(format!("\
            expect strategy name in topic_name_strategy, record_name_strategy and topic_record_name_strategy, but got {}", name))))? as i32)),
    }
}

/// resolve the schema of the source from external schema file, return the relation's columns. see <https://www.risingwave.dev/docs/current/sql-create-source> for more information.
/// return `(columns, source info)`
pub(crate) async fn bind_columns_from_source(
    session: &SessionImpl,
    source_schema: &ConnectorSchema,
    with_properties: &HashMap<String, String>,
) -> Result<(Option<Vec<ColumnCatalog>>, StreamSourceInfo)> {
    const MESSAGE_NAME_KEY: &str = "message";
    const KEY_MESSAGE_NAME_KEY: &str = "key.message";
    const NAME_STRATEGY_KEY: &str = "schema.registry.name.strategy";

    let is_kafka: bool = is_kafka_connector(with_properties);
    let format_encode_options = WithOptions::try_from(source_schema.row_options())?.into_inner();
    let mut format_encode_options_to_consume = format_encode_options.clone();

    fn get_key_message_name(options: &mut BTreeMap<String, String>) -> Option<String> {
        consume_string_from_options(options, KEY_MESSAGE_NAME_KEY)
            .map(|ele| Some(ele.0))
            .unwrap_or(None)
    }
    fn get_sr_name_strategy_check(
        options: &mut BTreeMap<String, String>,
        use_sr: bool,
    ) -> Result<Option<i32>> {
        let name_strategy = get_name_strategy_or_default(try_consume_string_from_options(
            options,
            NAME_STRATEGY_KEY,
        ))?;
        if !use_sr && name_strategy.is_some() {
            return Err(RwError::from(ProtocolError(
                "schema registry name strategy only works with schema registry enabled".to_string(),
            )));
        }
        Ok(name_strategy)
    }

    let mut stream_source_info = StreamSourceInfo {
        format: format_to_prost(&source_schema.format) as i32,
        row_encode: row_encode_to_prost(&source_schema.row_encode) as i32,
        format_encode_options,
        ..Default::default()
    };

    let columns = match (&source_schema.format, &source_schema.row_encode) {
        (Format::Native, Encode::Native)
        | (Format::Plain, Encode::Bytes)
        | (Format::DebeziumMongo, Encode::Json) => None,
        (Format::Plain, Encode::Protobuf) => {
            let (row_schema_location, use_schema_registry) =
                get_schema_location(&mut format_encode_options_to_consume)?;
            let protobuf_schema = ProtobufSchema {
                message_name: consume_string_from_options(
                    &mut format_encode_options_to_consume,
                    MESSAGE_NAME_KEY,
                )?,
                row_schema_location,
                use_schema_registry,
            };
            let name_strategy = get_sr_name_strategy_check(
                &mut format_encode_options_to_consume,
                protobuf_schema.use_schema_registry,
            )?;

            stream_source_info.use_schema_registry = protobuf_schema.use_schema_registry;
            stream_source_info.row_schema_location = protobuf_schema.row_schema_location.0.clone();
            stream_source_info.proto_message_name = protobuf_schema.message_name.0.clone();
            stream_source_info.key_message_name =
                get_key_message_name(&mut format_encode_options_to_consume);
            stream_source_info.name_strategy =
                name_strategy.unwrap_or(PbSchemaRegistryNameStrategy::Unspecified as i32);

            Some(
                extract_protobuf_table_schema(
                    &protobuf_schema,
                    with_properties,
                    &mut format_encode_options_to_consume,
                )
                .await?,
            )
        }
        (format @ (Format::Plain | Format::Upsert | Format::Debezium), Encode::Avro) => {
            let (row_schema_location, use_schema_registry) =
                get_schema_location(&mut format_encode_options_to_consume)?;

            if matches!(format, Format::Debezium) && !use_schema_registry {
                return Err(RwError::from(ProtocolError(
                    "schema location for DEBEZIUM_AVRO row format is not supported".to_string(),
                )));
            }

            let message_name = try_consume_string_from_options(
                &mut format_encode_options_to_consume,
                MESSAGE_NAME_KEY,
            );
            let name_strategy = get_sr_name_strategy_check(
                &mut format_encode_options_to_consume,
                use_schema_registry,
            )?;

            stream_source_info.use_schema_registry = use_schema_registry;
            stream_source_info.row_schema_location = row_schema_location.0.clone();
            stream_source_info.proto_message_name = message_name.unwrap_or(AstString("".into())).0;
            stream_source_info.key_message_name =
                get_key_message_name(&mut format_encode_options_to_consume);
            stream_source_info.name_strategy =
                name_strategy.unwrap_or(PbSchemaRegistryNameStrategy::Unspecified as i32);

            Some(
                extract_avro_table_schema(
                    &stream_source_info,
                    with_properties,
                    &mut format_encode_options_to_consume,
                    matches!(format, Format::Debezium),
                )
                .await?,
            )
        }
        (Format::Plain, Encode::Csv) => {
            let chars =
                consume_string_from_options(&mut format_encode_options_to_consume, "delimiter")?.0;
            let delimiter =
                get_delimiter(chars.as_str()).map_err(|e| RwError::from(e.to_string()))?;
            let has_header = try_consume_string_from_options(
                &mut format_encode_options_to_consume,
                "without_header",
            )
            .map(|s| s.0 == "false")
            .unwrap_or(true);

            if is_kafka && has_header {
                return Err(RwError::from(ProtocolError(
                    "CSV HEADER is not supported when creating table with Kafka connector"
                        .to_owned(),
                )));
            }

            stream_source_info.csv_delimiter = delimiter as i32;
            stream_source_info.csv_has_header = has_header;

            None
        }
        (
            Format::Plain | Format::Upsert | Format::Maxwell | Format::Canal | Format::Debezium,
            Encode::Json,
        ) => {
            let schema_config = get_json_schema_location(&mut format_encode_options_to_consume)?;
            stream_source_info.use_schema_registry =
                json_schema_infer_use_schema_registry(&schema_config);

            extract_json_table_schema(
                &schema_config,
                with_properties,
                &mut format_encode_options_to_consume,
            )
            .await?
        }
        (format, encoding) => {
            return Err(RwError::from(ProtocolError(format!(
                "Unknown combination {:?} {:?}",
                format, encoding
            ))));
        }
    };

    if !format_encode_options_to_consume.is_empty() {
        let err_string = format!(
            "Get unknown format_encode_options for {:?} {:?}: {}",
            source_schema.format,
            source_schema.row_encode,
            format_encode_options_to_consume
                .keys()
                .map(|k| k.to_string())
                .collect::<Vec<String>>()
                .join(","),
        );
        session.notice_to_user(err_string);
    }
    Ok((columns, stream_source_info))
}

fn bind_columns_from_source_for_cdc(
    session: &SessionImpl,
    source_schema: &ConnectorSchema,
    _with_properties: &HashMap<String, String>,
) -> Result<(Option<Vec<ColumnCatalog>>, StreamSourceInfo)> {
    let format_encode_options = WithOptions::try_from(source_schema.row_options())?.into_inner();
    let mut format_encode_options_to_consume = format_encode_options.clone();

    match (&source_schema.format, &source_schema.row_encode) {
        (Format::Plain, Encode::Json) => (),
        (format, encoding) => {
            // Note: parser will also check this. Just be extra safe here
            return Err(RwError::from(ProtocolError(format!(
                "Row format for CDC connectors should be either omitted or set to `FORMAT PLAIN ENCODE JSON`, got: {:?} {:?}",
                format, encoding
            ))));
        }
    };

    let columns = debezium_cdc_source_schema();
    let schema_config = get_json_schema_location(&mut format_encode_options_to_consume)?;

    let stream_source_info = StreamSourceInfo {
        format: format_to_prost(&source_schema.format) as i32,
        row_encode: row_encode_to_prost(&source_schema.row_encode) as i32,
        format_encode_options,
        use_schema_registry: json_schema_infer_use_schema_registry(&schema_config),
        cdc_source_job: true,
        ..Default::default()
    };
    if !format_encode_options_to_consume.is_empty() {
        let err_string = format!(
            "Get unknown format_encode_options for {:?} {:?}: {}",
            source_schema.format,
            source_schema.row_encode,
            format_encode_options_to_consume
                .keys()
                .map(|k| k.to_string())
                .collect::<Vec<String>>()
                .join(","),
        );
        session.notice_to_user(err_string);
    }
    Ok((Some(columns), stream_source_info))
}

/// add connector-spec columns to the end of column catalog
pub fn handle_addition_columns(
    with_properties: &HashMap<String, String>,
    mut additional_columns: Vec<(Ident, Option<Ident>)>,
    columns: &mut Vec<ColumnCatalog>,
) -> Result<()> {
    let connector_name = get_connector(with_properties).unwrap(); // there must be a connector in source

    let addition_col_list =
        match get_connector_compatible_additional_columns(connector_name.as_str()) {
            Some(cols) => cols,
            // early return if there are no accepted additional columns for the connector
            None => {
                return if additional_columns.is_empty() {
                    Ok(())
                } else {
                    Err(RwError::from(ProtocolError(format!(
                        "Connector {} accepts no additional column but got {:?}",
                        connector_name, additional_columns
                    ))))
                }
            }
        };
    let gen_default_column_name = |connector_name: &str, addi_column_name: &str| {
        format!("_rw_{}_{}", connector_name, addi_column_name)
    };

    let latest_col_id: ColumnId = columns
        .iter()
        .map(|col| col.column_desc.column_id)
        .max()
        .unwrap(); // there must be at least one column in the column catalog

    for (col_name, gen_column_catalog_fn) in addition_col_list {
        // always insert in spec order
        if let Some(idx) = additional_columns
            .iter()
            .position(|(col, _)| col.real_value().eq_ignore_ascii_case(col_name))
        {
            let (_, alias) = additional_columns.remove(idx);
            columns.push(gen_column_catalog_fn(
                latest_col_id.next(),
                alias
                    .map(|alias| alias.real_value())
                    .unwrap_or_else(|| gen_default_column_name(connector_name.as_str(), col_name))
                    .as_str(),
            ))
        }
    }
    if !additional_columns.is_empty() {
        return Err(RwError::from(ProtocolError(format!(
            "Unknown additional columns {:?}",
            additional_columns
        ))));
    }

    Ok(())
}

/// Bind columns from both source and sql defined.
pub(crate) fn bind_all_columns(
    source_schema: &ConnectorSchema,
    cols_from_source: Option<Vec<ColumnCatalog>>,
    cols_from_sql: Vec<ColumnCatalog>,
    col_defs_from_sql: &[ColumnDef],
) -> Result<Vec<ColumnCatalog>> {
    if let Some(cols_from_source) = cols_from_source {
        if cols_from_sql.is_empty() {
            Ok(cols_from_source)
        } else {
            // TODO(yuhao): https://github.com/risingwavelabs/risingwave/issues/12209
            Err(RwError::from(ProtocolError(
                format!("User-defined schema from SQL is not allowed with FORMAT {} ENCODE {}. \
                Please refer to https://www.risingwave.dev/docs/current/sql-create-source/ for more information.", source_schema.format, source_schema.row_encode))))
        }
    } else {
        // FIXME(yuhao): cols_from_sql should be None is no `()` is given.
        if cols_from_sql.is_empty() {
            return Err(RwError::from(ProtocolError(
                "Schema definition is required, either from SQL or schema registry.".to_string(),
            )));
        }
        match (&source_schema.format, &source_schema.row_encode) {
            (Format::DebeziumMongo, Encode::Json) => {
                let mut columns = vec![
                    ColumnCatalog {
                        column_desc: ColumnDesc::named("_id", 0.into(), DataType::Varchar),
                        is_hidden: false,
                    },
                    ColumnCatalog {
                        column_desc: ColumnDesc::named("payload", 0.into(), DataType::Jsonb),
                        is_hidden: false,
                    },
                ];
                let non_generated_sql_defined_columns =
                    non_generated_sql_columns(col_defs_from_sql);
                if non_generated_sql_defined_columns.len() != 2
                    || non_generated_sql_defined_columns[0].name.real_value() != columns[0].name()
                    || non_generated_sql_defined_columns[1].name.real_value() != columns[1].name()
                {
                    return Err(RwError::from(ProtocolError(
                        "the not generated columns of the source with row format DebeziumMongoJson
        must be (_id [Jsonb | Varchar | Int32 | Int64], payload jsonb)."
                            .to_string(),
                    )));
                }
                // ok to unwrap since it was checked at `bind_sql_columns`
                let key_data_type = bind_data_type(
                    non_generated_sql_defined_columns[0]
                        .data_type
                        .as_ref()
                        .unwrap(),
                )?;
                match key_data_type {
                    DataType::Jsonb | DataType::Varchar | DataType::Int32 | DataType::Int64 => {
                        columns[0].column_desc.data_type = key_data_type.clone();
                    }
                    _ => {
                        return Err(RwError::from(ProtocolError(
                            "the `_id` column of the source with row format DebeziumMongoJson
        must be [Jsonb | Varchar | Int32 | Int64]"
                                .to_string(),
                        )));
                    }
                }

                // ok to unwrap since it was checked at `bind_sql_columns`
                let value_data_type = bind_data_type(
                    non_generated_sql_defined_columns[1]
                        .data_type
                        .as_ref()
                        .unwrap(),
                )?;
                if !matches!(value_data_type, DataType::Jsonb) {
                    return Err(RwError::from(ProtocolError(
                        "the `payload` column of the source with row format DebeziumMongoJson
        must be Jsonb datatype"
                            .to_string(),
                    )));
                }
                Ok(columns)
            }
            (Format::Plain, Encode::Bytes) => {
                if cols_from_sql.len() != 1 || cols_from_sql[0].data_type() != &DataType::Bytea {
                    return Err(RwError::from(ProtocolError(
                        "ENCODE BYTES only accepts one BYTEA type column".to_string(),
                    )));
                }
                Ok(cols_from_sql)
            }
            (_, _) => Ok(cols_from_sql),
        }
    }
}

/// Bind column from source. Add key column to table columns if necessary.
/// Return (columns, pks)
pub(crate) async fn bind_source_pk(
    source_schema: &ConnectorSchema,
    source_info: &StreamSourceInfo,
    columns: &mut [ColumnCatalog],
    sql_defined_pk_names: Vec<String>,
    with_properties: &HashMap<String, String>,
) -> Result<Vec<String>> {
    let sql_defined_pk = !sql_defined_pk_names.is_empty();
    let key_column_name: Option<String> = {
        // iter columns to check if contains additional columns from key part
        // return the key column names if exists
        columns.iter().find_map(|catalog| {
            if catalog.column_desc.additional_column_type == AdditionalColumnType::Key {
                Some(catalog.name().to_string())
            } else {
                None
            }
        })
    };
    let additional_column_names = columns
        .iter()
        .filter_map(|col| {
            if (col.column_desc.additional_column_type != AdditionalColumnType::Unspecified)
                && (col.column_desc.additional_column_type != AdditionalColumnType::Normal)
            {
                Some(col.name().to_string())
            } else {
                None
            }
        })
        .collect_vec();

    let res = match (&source_schema.format, &source_schema.row_encode) {
        (Format::Native, Encode::Native) | (Format::Plain, _) => sql_defined_pk_names,

        // For all Upsert formats, we only accept one and only key column as primary key.
        // Additional KEY columns must be set in this case and must be primary key.
        (Format::Upsert, encode @ Encode::Json | encode @ Encode::Avro) => {
            if let Some(ref key_column_name) = key_column_name
                && sql_defined_pk
            {
                if sql_defined_pk_names.len() != 1 {
                    return Err(RwError::from(ProtocolError(format!(
                        "upsert {:?} supports only one primary key column ({}).",
                        encode, key_column_name
                    ))));
                }
                // the column name have been converted to real value in `handle_addition_columns`
                // so we don't ignore ascii case here
                if !key_column_name.eq(sql_defined_pk_names[0].as_str()) {
                    return Err(RwError::from(ProtocolError(format!(
                        "upsert {}'s key column {} not match with sql defined primary key {}",
                        encode, key_column_name, sql_defined_pk_names[0]
                    ))));
                }
                sql_defined_pk_names
            } else {
                return if key_column_name.is_none() {
                    Err(RwError::from(ProtocolError(format!(
                        "INCLUDE KEY clause must be set for FORMAT UPSERT ENCODE {:?}",
                        encode
                    ))))
                } else {
                    Err(RwError::from(ProtocolError(format!(
                        "Primary key must be specified to {} when creating source with FORMAT UPSERT ENCODE {:?}",
                        key_column_name.unwrap(), encode))))
                };
            }
        }

        (Format::Debezium, Encode::Json) => {
            if !additional_column_names.is_empty() {
                return Err(RwError::from(ProtocolError(format!(
                    "FORMAT DEBEZIUM forbids additional columns, but got {:?}",
                    additional_column_names
                ))));
            }
            if !sql_defined_pk {
                return Err(RwError::from(ProtocolError(
                    "Primary key must be specified when creating source with FORMAT DEBEZIUM."
                        .to_string(),
                )));
            }
            sql_defined_pk_names
        }
        (Format::Debezium, Encode::Avro) => {
            if !additional_column_names.is_empty() {
                return Err(RwError::from(ProtocolError(format!(
                    "FORMAT DEBEZIUM forbids additional columns, but got {:?}",
                    additional_column_names
                ))));
            }
            if sql_defined_pk {
                sql_defined_pk_names
            } else {
                let pk_names =
                    extract_debezium_avro_table_pk_columns(source_info, with_properties).await?;
                // extract pk(s) from schema registry
                for pk_name in &pk_names {
                    columns
                        .iter()
                        .find(|c: &&ColumnCatalog| c.name().eq(pk_name))
                        .ok_or_else(|| {
                            RwError::from(ProtocolError(format!(
                                "avro's key column {} not exists in avro's row schema",
                                pk_name
                            )))
                        })?;
                }
                pk_names
            }
        }
        (Format::DebeziumMongo, Encode::Json) => {
            if !additional_column_names.is_empty() {
                return Err(RwError::from(ProtocolError(format!(
                    "FORMAT DEBEZIUMMONGO forbids additional columns, but got {:?}",
                    additional_column_names
                ))));
            }
            if sql_defined_pk {
                sql_defined_pk_names
            } else {
                vec!["_id".to_string()]
            }
        }

        (Format::Maxwell, Encode::Json) => {
            if !additional_column_names.is_empty() {
                return Err(RwError::from(ProtocolError(format!(
                    "FORMAT MAXWELL forbids additional columns, but got {:?}",
                    additional_column_names
                ))));
            }
            if !sql_defined_pk {
                return Err(RwError::from(ProtocolError(
    "Primary key must be specified when creating source with FORMAT MAXWELL ENCODE JSON."
    .to_string(),
    )));
            }
            sql_defined_pk_names
        }

        (Format::Canal, Encode::Json) => {
            if !additional_column_names.is_empty() {
                return Err(RwError::from(ProtocolError(format!(
                    "FORMAT CANAL forbids additional columns, but got {:?}",
                    additional_column_names
                ))));
            }
            if !sql_defined_pk {
                return Err(RwError::from(ProtocolError(
    "Primary key must be specified when creating source with FORMAT CANAL ENCODE JSON."
    .to_string(),
    )));
            }
            sql_defined_pk_names
        }
        (format, encoding) => {
            return Err(RwError::from(ProtocolError(format!(
                "Unknown combination {:?} {:?}",
                format, encoding
            ))));
        }
    };
    Ok(res)
}

// Add a hidden column `_rw_kafka_timestamp` to each message from Kafka source.
fn check_and_add_timestamp_column(
    with_properties: &HashMap<String, String>,
    columns: &mut Vec<ColumnCatalog>,
) {
    if is_kafka_connector(with_properties) {
        let kafka_timestamp_column = ColumnCatalog {
            column_desc: ColumnDesc::named(
                KAFKA_TIMESTAMP_COLUMN_NAME,
                ColumnId::placeholder(),
                DataType::Timestamptz,
            ),

            is_hidden: true,
        };
        columns.push(kafka_timestamp_column);
    }
}

pub(super) fn bind_source_watermark(
    session: &SessionImpl,
    name: String,
    source_watermarks: Vec<SourceWatermark>,
    column_catalogs: &[ColumnCatalog],
) -> Result<Vec<WatermarkDesc>> {
    let mut binder = Binder::new_for_ddl(session);
    binder.bind_columns_to_context(name.clone(), column_catalogs)?;

    let watermark_descs = source_watermarks
        .into_iter()
        .map(|source_watermark| {
            let col_name = source_watermark.column.real_value();
            let watermark_idx = binder.get_column_binding_index(name.clone(), &col_name)?;

            let expr = binder.bind_expr(source_watermark.expr)?;
            let watermark_col_type = column_catalogs[watermark_idx].data_type();
            let watermark_expr_type = &expr.return_type();
            if watermark_col_type != watermark_expr_type {
                Err(RwError::from(ErrorCode::BindError(
                    format!("The return value type of the watermark expression must be identical to the watermark column data type. Current data type of watermark return value: `{}`, column `{}`",watermark_expr_type, watermark_col_type),
                )))
            } else {
                let expr_proto = expr.to_expr_proto();
                Ok::<_, RwError>(WatermarkDesc {
                    watermark_idx: watermark_idx as u32,
                    expr: Some(expr_proto),
                })
            }
        })
        .try_collect()?;
    Ok(watermark_descs)
}

// TODO: Better design if we want to support ENCODE KEY where we will have 4 dimensional array
static CONNECTORS_COMPATIBLE_FORMATS: LazyLock<HashMap<String, HashMap<Format, Vec<Encode>>>> =
    LazyLock::new(|| {
        convert_args!(hashmap!(
                KAFKA_CONNECTOR => hashmap!(
                    Format::Plain => vec![Encode::Json, Encode::Protobuf, Encode::Avro, Encode::Bytes, Encode::Csv],
                    Format::Upsert => vec![Encode::Json, Encode::Avro],
                    Format::Debezium => vec![Encode::Json, Encode::Avro],
                    Format::Maxwell => vec![Encode::Json],
                    Format::Canal => vec![Encode::Json],
                    Format::DebeziumMongo => vec![Encode::Json],
                ),
                PULSAR_CONNECTOR => hashmap!(
                    Format::Plain => vec![Encode::Json, Encode::Protobuf, Encode::Avro, Encode::Bytes],
                    Format::Upsert => vec![Encode::Json, Encode::Avro],
                    Format::Debezium => vec![Encode::Json],
                    Format::Maxwell => vec![Encode::Json],
                    Format::Canal => vec![Encode::Json],
                ),
                KINESIS_CONNECTOR => hashmap!(
                    Format::Plain => vec![Encode::Json, Encode::Protobuf, Encode::Avro, Encode::Bytes],
                    Format::Upsert => vec![Encode::Json, Encode::Avro],
                    Format::Debezium => vec![Encode::Json],
                    Format::Maxwell => vec![Encode::Json],
                    Format::Canal => vec![Encode::Json],
                ),
                GOOGLE_PUBSUB_CONNECTOR => hashmap!(
                    Format::Plain => vec![Encode::Json, Encode::Protobuf, Encode::Avro, Encode::Bytes],
                    Format::Debezium => vec![Encode::Json],
                    Format::Maxwell => vec![Encode::Json],
                    Format::Canal => vec![Encode::Json],
                ),
                NEXMARK_CONNECTOR => hashmap!(
                    Format::Native => vec![Encode::Native],
                    Format::Plain => vec![Encode::Bytes],
                ),
                DATAGEN_CONNECTOR => hashmap!(
                    Format::Native => vec![Encode::Native],
                    Format::Plain => vec![Encode::Bytes, Encode::Json],
                ),
                S3_CONNECTOR => hashmap!(
                    Format::Plain => vec![Encode::Csv, Encode::Json],
                ),
                OPENDAL_S3_CONNECTOR => hashmap!(
                    Format::Plain => vec![Encode::Csv, Encode::Json],
                ),
                GCS_CONNECTOR => hashmap!(
                    Format::Plain => vec![Encode::Csv, Encode::Json],
                ),
                MYSQL_CDC_CONNECTOR => hashmap!(
                    Format::Debezium => vec![Encode::Json],
                    // support source stream job
                    Format::Plain => vec![Encode::Json],
                ),
                POSTGRES_CDC_CONNECTOR => hashmap!(
                    Format::Debezium => vec![Encode::Json],
                    // support source stream job
                    Format::Plain => vec![Encode::Json],
                ),
                CITUS_CDC_CONNECTOR => hashmap!(
                    Format::Debezium => vec![Encode::Json],
                ),
                NATS_CONNECTOR => hashmap!(
                    Format::Plain => vec![Encode::Json],
                ),
                TEST_CONNECTOR => hashmap!(
                    Format::Plain => vec![Encode::Json],
                )
        ))
    });

pub fn validate_compatibility(
    source_schema: &ConnectorSchema,
    props: &mut HashMap<String, String>,
) -> Result<()> {
    let connector = get_connector(props)
        .ok_or_else(|| RwError::from(ProtocolError("missing field 'connector'".to_string())))?;

    let compatible_formats = CONNECTORS_COMPATIBLE_FORMATS
        .get(&connector)
        .ok_or_else(|| {
            RwError::from(ProtocolError(format!(
                "connector {:?} is not supported, accept {:?}",
                connector,
                CONNECTORS_COMPATIBLE_FORMATS.keys()
            )))
        })?;
    if connector != KAFKA_CONNECTOR {
        let res = match (&source_schema.format, &source_schema.row_encode) {
            (Format::Plain, Encode::Protobuf) | (Format::Plain, Encode::Avro) => {
                let mut options = WithOptions::try_from(source_schema.row_options())?;
                let (_, use_schema_registry) = get_schema_location(options.inner_mut())?;
                use_schema_registry
            }
            (Format::Debezium, Encode::Avro) => true,
            (_, _) => false,
        };
        if res {
            return Err(RwError::from(ProtocolError(format!(
                "The {} must be kafka when schema registry is used",
                UPSTREAM_SOURCE_KEY
            ))));
        }
    }

    let compatible_encodes = compatible_formats
        .get(&source_schema.format)
        .ok_or_else(|| {
            RwError::from(ProtocolError(format!(
                "connector {} does not support format {:?}",
                connector, source_schema.format
            )))
        })?;
    if !compatible_encodes.contains(&source_schema.row_encode) {
        return Err(RwError::from(ProtocolError(format!(
            "connector {} does not support format {:?} with encode {:?}",
            connector, source_schema.format, source_schema.row_encode
        ))));
    }

    if connector == POSTGRES_CDC_CONNECTOR || connector == CITUS_CDC_CONNECTOR {
        if !props.contains_key("slot.name") {
            // Build a random slot name with UUID
            // e.g. "rw_cdc_f9a3567e6dd54bf5900444c8b1c03815"
            let uuid = uuid::Uuid::new_v4().to_string().replace('-', "");
            props.insert("slot.name".into(), format!("rw_cdc_{}", uuid));
        }
        if !props.contains_key("schema.name") {
            // Default schema name is "public"
            props.insert("schema.name".into(), "public".into());
        }
        if !props.contains_key("publication.name") {
            // Default publication name is "rw_publication"
            props.insert("publication.name".into(), "rw_publication".into());
        }
        if !props.contains_key("publication.create.enable") {
            // Default auto create publication if doesn't exist
            props.insert("publication.create.enable".into(), "true".into());
        }
    }
    Ok(())
}

/// Performs early stage checking in frontend to see if the schema of the given `columns` is
/// compatible with the connector extracted from the properties. Currently this only works for
/// `nexmark` connector since it's in chunk format.
///
/// One should only call this function after all properties of all columns are resolved, like
/// generated column descriptors.
pub(super) fn check_source_schema(
    props: &HashMap<String, String>,
    row_id_index: Option<usize>,
    columns: &[ColumnCatalog],
) -> Result<()> {
    let Some(connector) = get_connector(props) else {
        return Ok(());
    };

    if connector != NEXMARK_CONNECTOR {
        return Ok(());
    }

    let table_type = props
        .get("nexmark.table.type")
        .map(|t| t.to_ascii_lowercase());

    let event_type = match table_type.as_deref() {
        None => None,
        Some("bid") => Some(EventType::Bid),
        Some("auction") => Some(EventType::Auction),
        Some("person") => Some(EventType::Person),
        Some(t) => {
            return Err(RwError::from(ProtocolError(format!(
                "unsupported table type for nexmark source: {}",
                t
            ))))
        }
    };

    // Ignore the generated columns and map the index of row_id column.
    let user_defined_columns = columns.iter().filter(|c| !c.is_generated());
    let row_id_index = if let Some(index) = row_id_index {
        let col_id = columns[index].column_id();
        user_defined_columns
            .clone()
            .position(|c| c.column_id() == col_id)
            .unwrap()
            .into()
    } else {
        None
    };

    let expected = get_event_data_types_with_names(event_type, row_id_index);
    let user_defined = user_defined_columns
        .map(|c| {
            (
                c.column_desc.name.to_ascii_lowercase(),
                c.column_desc.data_type.to_owned(),
            )
        })
        .collect_vec();

    if expected != user_defined {
        let cmp = pretty_assertions::Comparison::new(&expected, &user_defined);
        return Err(RwError::from(ProtocolError(format!(
            "The schema of the nexmark source must specify all columns in order:\n{cmp}",
        ))));
    }
    Ok(())
}

pub async fn handle_create_source(
    handler_args: HandlerArgs,
    stmt: CreateSourceStatement,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();

    if let Either::Right(resp) = session.check_relation_name_duplicated(
        stmt.source_name.clone(),
        StatementType::CREATE_SOURCE,
        stmt.if_not_exists,
    )? {
        return Ok(resp);
    }

    let db_name = session.database();
    let (schema_name, name) = Binder::resolve_schema_qualified_name(db_name, stmt.source_name)?;
    let (database_id, schema_id) =
        session.get_database_and_schema_id_for_create(schema_name.clone())?;

    if handler_args.with_options.is_empty() {
        return Err(RwError::from(InvalidInputSyntax(
            "missing WITH clause".to_string(),
        )));
    }

    let source_schema = stmt.source_schema.into_v2_with_warning();

    let mut with_properties = handler_args
        .with_options
        .clone()
        .into_inner()
        .into_iter()
        .collect();
    validate_compatibility(&source_schema, &mut with_properties)?;

    ensure_table_constraints_supported(&stmt.constraints)?;
    let sql_pk_names = bind_sql_pk_names(&stmt.columns, &stmt.constraints)?;

    // gated the feature with a session variable
    let create_cdc_source_job = if is_cdc_connector(&with_properties) {
        CdcTableType::from_properties(&with_properties).can_backfill()
    } else {
        false
    };

    let (columns_from_resolve_source, source_info) = if create_cdc_source_job {
        bind_columns_from_source_for_cdc(&session, &source_schema, &with_properties)?
    } else {
        bind_columns_from_source(&session, &source_schema, &with_properties).await?
    };
    let columns_from_sql = bind_sql_columns(&stmt.columns)?;

    let mut columns = bind_all_columns(
        &source_schema,
        columns_from_resolve_source,
        columns_from_sql,
        &stmt.columns,
    )?;
    // add additional columns before bind pk, because `format upsert` requires the key column
    handle_addition_columns(&with_properties, stmt.include_column_options, &mut columns)?;
    let pk_names = bind_source_pk(
        &source_schema,
        &source_info,
        &mut columns,
        sql_pk_names,
        &with_properties,
    )
    .await?;

    if create_cdc_source_job {
        // set connector to backfill mode
        with_properties.insert(CDC_SNAPSHOT_MODE_KEY.into(), CDC_SNAPSHOT_BACKFILL.into());
        // enable cdc sharing mode, which will capture all tables in the given `database.name`
        with_properties.insert(CDC_SHARING_MODE_KEY.into(), "true".into());
    }

    check_and_add_timestamp_column(&with_properties, &mut columns);

    let mut col_id_gen = ColumnIdGenerator::new_initial();
    for c in &mut columns {
        c.column_desc.column_id = col_id_gen.generate(c.name())
    }

    if !pk_names.is_empty() {
        return Err(ErrorCode::InvalidInputSyntax(
            "Source does not support PRIMARY KEY constraint, please use \"CREATE TABLE\" instead"
                .to_owned(),
        )
        .into());
    }

    let (mut columns, pk_column_ids, row_id_index) = bind_pk_on_relation(columns, pk_names)?;

    debug_assert!(is_column_ids_dedup(&columns));

    let watermark_descs =
        bind_source_watermark(&session, name.clone(), stmt.source_watermarks, &columns)?;
    // TODO(yuhao): allow multiple watermark on source.
    assert!(watermark_descs.len() <= 1);

    bind_sql_column_constraints(
        &session,
        name.clone(),
        &mut columns,
        stmt.columns,
        &pk_column_ids,
    )?;

    check_source_schema(&with_properties, row_id_index, &columns)?;

    let pk_column_ids = pk_column_ids.into_iter().map(Into::into).collect();

    let mut with_options = WithOptions::new(with_properties);
    // resolve privatelink connection for Kafka source
    let connection_id =
        resolve_privatelink_in_with_option(&mut with_options, &schema_name, &session)?;
    let definition = handler_args.normalized_sql.clone();

    let source = PbSource {
        id: TableId::placeholder().table_id,
        schema_id,
        database_id,
        name,
        row_id_index: row_id_index.map(|idx| idx as u32),
        columns: columns.iter().map(|c| c.to_protobuf()).collect_vec(),
        pk_column_ids,
        with_properties: with_options.into_inner().into_iter().collect(),
        info: Some(source_info),
        owner: session.user_id(),
        watermark_descs,
        definition,
        connection_id,
        initialized_at_epoch: None,
        created_at_epoch: None,
        optional_associated_table_id: None,
        version: INITIAL_SOURCE_VERSION_ID,
        initialized_at_cluster_version: None,
        created_at_cluster_version: None,
    };

    let catalog_writer = session.catalog_writer()?;

    if create_cdc_source_job {
        // create a streaming job for the cdc source, which will mark as *singleton* in the Fragmenter
        let graph = {
            let context = OptimizerContext::from_handler_args(handler_args);
            // cdc source is an append-only source in plain json format
            let source_node = LogicalSource::new(
                Some(Rc::new(SourceCatalog::from(&source))),
                columns.clone(),
                row_id_index,
                false,
                false,
                context.into(),
            )?;

            // generate stream graph for cdc source job
            let stream_plan = source_node.to_stream(&mut ToStreamContext::new(false))?;
            let mut graph = build_graph(stream_plan);
            graph.parallelism =
                session
                    .config()
                    .streaming_parallelism()
                    .map(|parallelism| Parallelism {
                        parallelism: parallelism.get(),
                    });
            graph
        };
        catalog_writer
            .create_source_with_graph(source, graph)
            .await?;
    } else {
        // For other sources we don't create a streaming job
        catalog_writer.create_source(source).await?;
    }

    Ok(PgResponse::empty_result(StatementType::CREATE_SOURCE))
}

fn format_to_prost(format: &Format) -> FormatType {
    match format {
        Format::Native => FormatType::Native,
        Format::Plain => FormatType::Plain,
        Format::Upsert => FormatType::Upsert,
        Format::Debezium => FormatType::Debezium,
        Format::DebeziumMongo => FormatType::DebeziumMongo,
        Format::Maxwell => FormatType::Maxwell,
        Format::Canal => FormatType::Canal,
    }
}
fn row_encode_to_prost(row_encode: &Encode) -> EncodeType {
    match row_encode {
        Encode::Native => EncodeType::Native,
        Encode::Json => EncodeType::Json,
        Encode::Avro => EncodeType::Avro,
        Encode::Protobuf => EncodeType::Protobuf,
        Encode::Csv => EncodeType::Csv,
        Encode::Bytes => EncodeType::Bytes,
        Encode::Template => EncodeType::Template,
    }
}

#[cfg(test)]
pub mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use risingwave_common::catalog::{
        CDC_SOURCE_COLUMN_NUM, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, OFFSET_COLUMN_NAME,
        ROWID_PREFIX, TABLE_NAME_COLUMN_NAME,
    };
    use risingwave_common::types::DataType;

    use crate::catalog::root_catalog::SchemaPath;
    use crate::catalog::source_catalog::SourceCatalog;
    use crate::handler::create_source::debezium_cdc_source_schema;
    use crate::test_utils::{create_proto_file, LocalFrontend, PROTO_FILE_DATA};

    const GET_COLUMN_FROM_CATALOG: fn(&Arc<SourceCatalog>) -> HashMap<&str, DataType> =
        |catalog: &Arc<SourceCatalog>| -> HashMap<&str, DataType> {
            catalog
                .columns
                .iter()
                .map(|col| (col.name(), col.data_type().clone()))
                .collect::<HashMap<&str, DataType>>()
        };

    #[tokio::test]
    async fn test_create_source_handler() {
        let proto_file = create_proto_file(PROTO_FILE_DATA);
        let sql = format!(
            r#"CREATE SOURCE t
    WITH (connector = 'kinesis')
    FORMAT PLAIN ENCODE PROTOBUF (message = '.test.TestRecord', schema.location = 'file://{}')"#,
            proto_file.path().to_str().unwrap()
        );
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql).await.unwrap();

        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        // Check source exists.
        let (source, _) = catalog_reader
            .get_source_by_name(DEFAULT_DATABASE_NAME, schema_path, "t")
            .unwrap();
        assert_eq!(source.name, "t");

        let columns = GET_COLUMN_FROM_CATALOG(source);

        let city_type = DataType::new_struct(
            vec![DataType::Varchar, DataType::Varchar],
            vec!["address".to_string(), "zipcode".to_string()],
        );
        let expected_columns = maplit::hashmap! {
            ROWID_PREFIX => DataType::Serial,
            "id" => DataType::Int32,
            "zipcode" => DataType::Int64,
            "rate" => DataType::Float32,
            "country" => DataType::new_struct(
                vec![DataType::Varchar,city_type,DataType::Varchar],
                vec!["address".to_string(), "city".to_string(), "zipcode".to_string()],
            ),
        };
        assert_eq!(columns, expected_columns);
    }

    #[tokio::test]
    async fn test_duplicate_props_options() {
        let proto_file = create_proto_file(PROTO_FILE_DATA);
        let sql = format!(
            r#"CREATE SOURCE t
    WITH (
        connector = 'kinesis',
        aws.region='user_test_topic',
        endpoint='172.10.1.1:9090,172.10.1.2:9090',
        aws.credentials.access_key_id = 'your_access_key_1',
        aws.credentials.secret_access_key = 'your_secret_key_1'
    )
    FORMAT PLAIN ENCODE PROTOBUF (
        message = '.test.TestRecord',
        aws.credentials.access_key_id = 'your_access_key_2',
        aws.credentials.secret_access_key = 'your_secret_key_2',
        schema.location = 'file://{}',
    )"#,
            proto_file.path().to_str().unwrap()
        );
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql).await.unwrap();

        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        // Check source exists.
        let (source, _) = catalog_reader
            .get_source_by_name(DEFAULT_DATABASE_NAME, schema_path, "t")
            .unwrap();
        assert_eq!(source.name, "t");

        // AwsAuth params exist in options.
        assert_eq!(
            source
                .info
                .format_encode_options
                .get("aws.credentials.access_key_id")
                .unwrap(),
            "your_access_key_2"
        );
        assert_eq!(
            source
                .info
                .format_encode_options
                .get("aws.credentials.secret_access_key")
                .unwrap(),
            "your_secret_key_2"
        );

        // AwsAuth params exist in props.
        assert_eq!(
            source
                .with_properties
                .get("aws.credentials.access_key_id")
                .unwrap(),
            "your_access_key_1"
        );
        assert_eq!(
            source
                .with_properties
                .get("aws.credentials.secret_access_key")
                .unwrap(),
            "your_secret_key_1"
        );

        // Options are not merged into props.
        assert!(source.with_properties.get("schema.location").is_none());
    }

    #[tokio::test]
    async fn test_multi_table_cdc_create_source_handler() {
        let sql =
            "CREATE SOURCE t2 WITH (connector = 'mysql-cdc') FORMAT PLAIN ENCODE JSON".to_string();
        let frontend = LocalFrontend::new(Default::default()).await;
        let session = frontend.session_ref();

        frontend
            .run_sql_with_session(session.clone(), sql)
            .await
            .unwrap();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        // Check source exists.
        let (source, _) = catalog_reader
            .get_source_by_name(DEFAULT_DATABASE_NAME, schema_path, "t2")
            .unwrap();
        assert_eq!(source.name, "t2");

        let columns = source
            .columns
            .iter()
            .map(|col| (col.name(), col.data_type().clone()))
            .collect::<HashMap<&str, DataType>>();

        let expected_columns = maplit::hashmap! {
            ROWID_PREFIX => DataType::Serial,
            "payload" => DataType::Jsonb,
            OFFSET_COLUMN_NAME => DataType::Varchar,
            TABLE_NAME_COLUMN_NAME => DataType::Varchar,
        };
        assert_eq!(columns, expected_columns);
    }

    #[tokio::test]
    async fn test_cdc_source_job_schema() {
        let columns = debezium_cdc_source_schema();
        // make sure it doesn't broken by future PRs
        assert_eq!(CDC_SOURCE_COLUMN_NUM, columns.len() as u32);
    }

    #[tokio::test]
    async fn test_source_addition_columns() {
        // test derive include column for format plain
        let sql =
            "CREATE SOURCE s (v1 int) include key as _rw_kafka_key with (connector = 'kafka') format plain encode json"
                .to_string();
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql).await.unwrap();
        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let (source, _) = catalog_reader
            .get_source_by_name(
                DEFAULT_DATABASE_NAME,
                SchemaPath::Name(DEFAULT_SCHEMA_NAME),
                "s",
            )
            .unwrap();
        assert_eq!(source.name, "s");

        let columns = GET_COLUMN_FROM_CATALOG(source);
        let expect_columns = maplit::hashmap! {
            ROWID_PREFIX => DataType::Serial,
            "v1" => DataType::Int32,
            "_rw_kafka_key" => DataType::Bytea,
            // todo: kafka connector will automatically derive the column
            // will change to a required field in the include clause
            "_rw_kafka_timestamp" => DataType::Timestamptz,
        };
        assert_eq!(columns, expect_columns);

        // test derive include column for format upsert
        let sql = "CREATE SOURCE s1 (v1 int) with (connector = 'kafka') format upsert encode json"
            .to_string();
        match frontend.run_sql(sql).await {
            Err(e) => {
                assert_eq!(
                    e.to_string(),
                    "Protocol error: INCLUDE KEY clause must be set for FORMAT UPSERT ENCODE Json"
                )
            }
            _ => unreachable!(),
        }

        let sql = "CREATE SOURCE s2 (v1 int) include key as _rw_kafka_key with (connector = 'kafka') format upsert encode json"
            .to_string();
        match frontend.run_sql(sql).await {
            Err(e) => {
                assert_eq!(e.to_string(), "Protocol error: Primary key must be specified to _rw_kafka_key when creating source with FORMAT UPSERT ENCODE Json")
            }
            _ => unreachable!(),
        }
    }
}
