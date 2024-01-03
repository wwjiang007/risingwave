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

use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::{Arc, LazyLock};

use anyhow::Context;
use arrow_schema::DataType as ArrowDataType;
use either::Either;
use icelake::catalog::load_catalog;
use icelake::TableIdentifier;
use itertools::Itertools;
use maplit::{convert_args, hashmap};
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::catalog::{ConnectionId, DatabaseId, SchemaId, UserId};
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::types::Datum;
use risingwave_common::util::value_encoding::DatumFromProtoExt;
use risingwave_common::{bail, catalog};
use risingwave_connector::sink::catalog::{SinkCatalog, SinkFormatDesc, SinkType};
use risingwave_connector::sink::iceberg::{IcebergConfig, ICEBERG_SINK};
use risingwave_connector::sink::{
    CONNECTOR_TYPE_KEY, SINK_TYPE_OPTION, SINK_USER_FORCE_APPEND_ONLY_OPTION,
};
use risingwave_pb::catalog::{PbSource, Table};
use risingwave_pb::ddl_service::ReplaceTablePlan;
use risingwave_pb::plan_common::column_desc::GeneratedOrDefaultColumn;
use risingwave_pb::stream_plan::stream_fragment_graph::Parallelism;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{DispatcherType, MergeNode, StreamFragmentGraph, StreamNode};
use risingwave_sqlparser::ast::{
    ConnectorSchema, CreateSink, CreateSinkStatement, EmitMode, Encode, Format, ObjectName, Query,
    Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins,
};
use risingwave_sqlparser::parser::Parser;

use super::create_mv::get_column_names;
use super::create_source::UPSTREAM_SOURCE_KEY;
use super::RwPgResponse;
use crate::binder::Binder;
use crate::catalog::catalog_service::CatalogReadGuard;
use crate::expr::{ExprImpl, InputRef, Literal};
use crate::handler::alter_table_column::fetch_table_catalog_for_alter;
use crate::handler::create_table::{generate_stream_graph_for_table, ColumnIdGenerator};
use crate::handler::privilege::resolve_query_privileges;
use crate::handler::util::SourceSchemaCompatExt;
use crate::handler::HandlerArgs;
use crate::optimizer::plan_node::{
    generic, IcebergPartitionInfo, LogicalSource, PartitionComputeInfo, StreamProject,
};
use crate::optimizer::{OptimizerContext, OptimizerContextRef, PlanRef, RelationCollectorVisitor};
use crate::scheduler::streaming_manager::CreatingStreamingJobInfo;
use crate::session::SessionImpl;
use crate::stream_fragmenter::build_graph;
use crate::utils::resolve_privatelink_in_with_option;
use crate::{Explain, Planner, TableCatalog, WithOptions};

pub fn gen_sink_query_from_name(from_name: ObjectName) -> Result<Query> {
    let table_factor = TableFactor::Table {
        name: from_name,
        alias: None,
        for_system_time_as_of_proctime: false,
    };
    let from = vec![TableWithJoins {
        relation: table_factor,
        joins: vec![],
    }];
    let select = Select {
        from,
        projection: vec![SelectItem::Wildcard(None)],
        ..Default::default()
    };
    let body = SetExpr::Select(Box::new(select));
    Ok(Query {
        with: None,
        body,
        order_by: vec![],
        limit: None,
        offset: None,
        fetch: None,
    })
}

// used to store result of `gen_sink_plan`
pub struct SinkPlanContext {
    pub query: Box<Query>,
    pub sink_plan: PlanRef,
    pub sink_catalog: SinkCatalog,
    pub target_table_catalog: Option<Arc<TableCatalog>>,
}

pub fn gen_sink_plan(
    session: &SessionImpl,
    context: OptimizerContextRef,
    stmt: CreateSinkStatement,
    partition_info: Option<PartitionComputeInfo>,
) -> Result<SinkPlanContext> {
    let db_name = session.database();
    let (sink_schema_name, sink_table_name) =
        Binder::resolve_schema_qualified_name(db_name, stmt.sink_name.clone())?;

    // Used for debezium's table name
    let sink_from_table_name;
    let query = match stmt.sink_from {
        CreateSink::From(from_name) => {
            sink_from_table_name = from_name.0.last().unwrap().real_value();
            Box::new(gen_sink_query_from_name(from_name)?)
        }
        CreateSink::AsQuery(query) => {
            sink_from_table_name = sink_table_name.clone();
            query
        }
    };

    let sink_into_table_name = stmt.into_table_name.as_ref().map(|name| name.real_value());

    let (sink_database_id, sink_schema_id) =
        session.get_database_and_schema_id_for_create(sink_schema_name.clone())?;

    let definition = context.normalized_sql().to_owned();

    let (dependent_relations, bound) = {
        let mut binder = Binder::new_for_stream(session);
        let bound = binder.bind_query(*query.clone())?;
        (binder.included_relations(), bound)
    };

    let check_items = resolve_query_privileges(&bound);
    session.check_privileges(&check_items)?;

    // If column names not specified, use the name in materialized view.
    let col_names = get_column_names(&bound, session, stmt.columns)?;

    let mut with_options = context.with_options().clone();

    if sink_into_table_name.is_some() {
        let prev = with_options
            .inner_mut()
            .insert(CONNECTOR_TYPE_KEY.to_string(), "table".to_string());

        if prev.is_some() {
            return Err(RwError::from(ErrorCode::BindError(
                "In the case of sinking into table, the 'connector' parameter should not be provided.".to_string(),
            )));
        }
    }

    let connection_id = {
        let conn_id =
            resolve_privatelink_in_with_option(&mut with_options, &sink_schema_name, session)?;
        conn_id.map(ConnectionId)
    };

    let emit_on_window_close = stmt.emit_mode == Some(EmitMode::OnWindowClose);
    if emit_on_window_close {
        context.warn_to_user("EMIT ON WINDOW CLOSE is currently an experimental feature. Please use it with caution.");
    }

    let connector = with_options
        .get(CONNECTOR_TYPE_KEY)
        .cloned()
        .ok_or_else(|| ErrorCode::BindError(format!("missing field '{CONNECTOR_TYPE_KEY}'")))?;

    let format_desc = match stmt.sink_schema {
        // Case A: new syntax `format ... encode ...`
        Some(f) => {
            validate_compatibility(&connector, &f)?;
            Some(bind_sink_format_desc(f)?)
        }
        None => match with_options.get(SINK_TYPE_OPTION) {
            // Case B: old syntax `type = '...'`
            Some(t) => SinkFormatDesc::from_legacy_type(&connector, t)?.map(|mut f| {
                session.notice_to_user("Consider using the newer syntax `FORMAT ... ENCODE ...` instead of `type = '...'`.");
                if let Some(v) = with_options.get(SINK_USER_FORCE_APPEND_ONLY_OPTION) {
                    f.options.insert(SINK_USER_FORCE_APPEND_ONLY_OPTION.into(), v.into());
                }
                f
            }),
            // Case C: no format + encode required
            None => None,
        },
    };

    let mut plan_root = Planner::new(context).plan_query(bound)?;
    if let Some(col_names) = col_names {
        plan_root.set_out_names(col_names)?;
    };

    let target_table_catalog = stmt
        .into_table_name
        .as_ref()
        .map(|table_name| fetch_table_catalog_for_alter(session, table_name))
        .transpose()?;

    let target_table = target_table_catalog.as_ref().map(|catalog| catalog.id());

    let sink_plan = plan_root.gen_sink_plan(
        sink_table_name,
        definition,
        with_options,
        emit_on_window_close,
        db_name.to_owned(),
        sink_from_table_name,
        format_desc,
        target_table,
        partition_info,
    )?;
    let sink_desc = sink_plan.sink_desc().clone();

    let mut sink_plan: PlanRef = sink_plan.into();

    let ctx = sink_plan.ctx();
    let explain_trace = ctx.is_explain_trace();
    if explain_trace {
        ctx.trace("Create Sink:");
        ctx.trace(sink_plan.explain_to_string());
    }

    let dependent_relations =
        RelationCollectorVisitor::collect_with(dependent_relations, sink_plan.clone());

    let sink_catalog = sink_desc.into_catalog(
        SchemaId::new(sink_schema_id),
        DatabaseId::new(sink_database_id),
        UserId::new(session.user_id()),
        connection_id,
        dependent_relations.into_iter().collect_vec(),
    );

    if let Some(table_catalog) = &target_table_catalog {
        for column in sink_catalog.full_columns() {
            if column.is_generated() {
                unreachable!("can not derive generated columns in a sink's catalog, but meet one");
            }
        }

        let user_defined_primary_key_table =
            !(table_catalog.append_only || table_catalog.row_id_index.is_some());

        if !(user_defined_primary_key_table
            || sink_catalog.sink_type == SinkType::AppendOnly
            || sink_catalog.sink_type == SinkType::ForceAppendOnly)
        {
            return Err(RwError::from(ErrorCode::BindError(
                "Only append-only sinks can sink to a table without primary keys.".to_string(),
            )));
        }

        let exprs = derive_default_column_project_for_sink(&sink_catalog, table_catalog)?;

        let logical_project = generic::Project::new(exprs, sink_plan);

        sink_plan = StreamProject::new(logical_project).into();

        let exprs =
            LogicalSource::derive_output_exprs_from_generated_columns(table_catalog.columns())?;
        if let Some(exprs) = exprs {
            let logical_project = generic::Project::new(exprs, sink_plan);
            sink_plan = StreamProject::new(logical_project).into();
        }
    };

    Ok(SinkPlanContext {
        query,
        sink_plan,
        sink_catalog,
        target_table_catalog,
    })
}

// # TODO
// We need return None for range partition
pub async fn get_partition_compute_info(
    with_options: &WithOptions,
) -> Option<PartitionComputeInfo> {
    let properties = HashMap::from_iter(with_options.clone().into_inner().into_iter());
    let connector = properties.get(UPSTREAM_SOURCE_KEY)?;
    match connector.as_str() {
        ICEBERG_SINK => {
            let iceberg_config = IcebergConfig::from_hashmap(properties).ok()?;
            let catalog = load_catalog(&iceberg_config.build_iceberg_configs().ok()?)
                .await
                .ok()?;
            let table_id = TableIdentifier::new(iceberg_config.table_name.split('.')).ok()?;
            let table = catalog.load_table(&table_id).await.ok()?;
            let partition_spec = table
                .current_table_metadata()
                .current_partition_spec()
                .ok()?;

            if partition_spec.is_unpartitioned() {
                return None;
            }

            let arrow_type: ArrowDataType = table.current_partition_type().ok()?.try_into().ok()?;
            let partition_fields = table
                .current_table_metadata()
                .current_partition_spec()
                .ok()?
                .fields
                .iter()
                .map(|f| (f.name.clone(), f.transform.clone()))
                .collect_vec();

            Some(PartitionComputeInfo::Iceberg(IcebergPartitionInfo {
                partition_type: arrow_type.try_into().ok()?,
                partition_fields,
            }))
        }
        _ => None,
    }
}

pub async fn handle_create_sink(
    handle_args: HandlerArgs,
    stmt: CreateSinkStatement,
) -> Result<RwPgResponse> {
    let session = handle_args.session.clone();

    if let Either::Right(resp) = session.check_relation_name_duplicated(
        stmt.sink_name.clone(),
        StatementType::CREATE_SINK,
        stmt.if_not_exists,
    )? {
        return Ok(resp);
    }

    let partition_info = get_partition_compute_info(&handle_args.with_options).await;

    let (sink, graph, target_table_catalog) = {
        let context = Rc::new(OptimizerContext::from_handler_args(handle_args));

        let SinkPlanContext {
            query,
            sink_plan: plan,
            sink_catalog: sink,
            target_table_catalog,
        } = gen_sink_plan(&session, context.clone(), stmt, partition_info)?;

        let has_order_by = !query.order_by.is_empty();
        if has_order_by {
            context.warn_to_user(
                r#"The ORDER BY clause in the CREATE SINK statement has no effect at all."#
                    .to_string(),
            );
        }

        let mut graph = build_graph(plan);

        graph.parallelism =
            session
                .config()
                .streaming_parallelism()
                .map(|parallelism| Parallelism {
                    parallelism: parallelism.get(),
                });

        (sink, graph, target_table_catalog)
    };

    let mut target_table_replace_plan = None;
    if let Some(table_catalog) = target_table_catalog {
        check_cycle_for_sink(session.as_ref(), sink.clone(), table_catalog.id())?;

        let (mut graph, mut table, source) =
            reparse_table_for_sink(&session, &table_catalog).await?;

        table.incoming_sinks = table_catalog.incoming_sinks.clone();

        for _ in 0..(table_catalog.incoming_sinks.len() + 1) {
            for fragment in graph.fragments.values_mut() {
                if let Some(node) = &mut fragment.node {
                    insert_merger_to_union(node);
                }
            }
        }

        target_table_replace_plan = Some(ReplaceTablePlan {
            source,
            table: Some(table),
            fragment_graph: Some(graph),
            table_col_index_mapping: None,
        });
    }

    let _job_guard =
        session
            .env()
            .creating_streaming_job_tracker()
            .guard(CreatingStreamingJobInfo::new(
                session.session_id(),
                sink.database_id.database_id,
                sink.schema_id.schema_id,
                sink.name.clone(),
            ));

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .create_sink(sink.to_proto(), graph, target_table_replace_plan)
        .await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_SINK))
}

fn check_cycle_for_sink(
    session: &SessionImpl,
    sink_catalog: SinkCatalog,
    table_id: catalog::TableId,
) -> Result<()> {
    let reader = session.env().catalog_reader().read_guard();

    let mut sinks = HashMap::new();
    let db_name = session.database();
    for schema in reader.iter_schemas(db_name)? {
        for sink in schema.iter_sink() {
            sinks.insert(sink.id.sink_id, sink.as_ref());
        }
    }
    fn visit_sink(
        session: &SessionImpl,
        reader: &CatalogReadGuard,
        sink_index: &HashMap<u32, &SinkCatalog>,
        sink: &SinkCatalog,
        visited_tables: &mut HashSet<u32>,
    ) -> Result<()> {
        for table_id in &sink.dependent_relations {
            if let Ok(table) = reader.get_table_by_id(table_id) {
                visit_table(session, reader, sink_index, table.as_ref(), visited_tables)?
            } else {
                bail!("table not found: {:?}", table_id);
            }
        }

        Ok(())
    }

    fn visit_table(
        session: &SessionImpl,
        reader: &CatalogReadGuard,
        sink_index: &HashMap<u32, &SinkCatalog>,
        table: &TableCatalog,
        visited_tables: &mut HashSet<u32>,
    ) -> Result<()> {
        if visited_tables.contains(&table.id.table_id) {
            return Err(RwError::from(ErrorCode::BindError(
                "Creating such a sink will result in circular dependency.".to_string(),
            )));
        }

        let _ = visited_tables.insert(table.id.table_id);
        for sink_id in &table.incoming_sinks {
            if let Some(sink) = sink_index.get(sink_id) {
                visit_sink(session, reader, sink_index, sink, visited_tables)?
            } else {
                bail!("sink not found: {:?}", sink_id);
            }
        }

        Ok(())
    }

    let mut visited_tables = HashSet::new();
    visited_tables.insert(table_id.table_id);

    visit_sink(session, &reader, &sinks, &sink_catalog, &mut visited_tables)
}

pub(crate) async fn reparse_table_for_sink(
    session: &Arc<SessionImpl>,
    table_catalog: &Arc<TableCatalog>,
) -> Result<(StreamFragmentGraph, Table, Option<PbSource>)> {
    // Retrieve the original table definition and parse it to AST.
    let [definition]: [_; 1] = Parser::parse_sql(&table_catalog.definition)
        .context("unable to parse original table definition")?
        .try_into()
        .unwrap();
    let Statement::CreateTable {
        name,
        source_schema,
        ..
    } = &definition
    else {
        panic!("unexpected statement: {:?}", definition);
    };

    let table_name = name.clone();
    let source_schema = source_schema
        .clone()
        .map(|source_schema| source_schema.into_v2_with_warning());

    // Create handler args as if we're creating a new table with the altered definition.
    let handler_args = HandlerArgs::new(session.clone(), &definition, Arc::from(""))?;
    let col_id_gen = ColumnIdGenerator::new_alter(table_catalog);
    let Statement::CreateTable {
        columns,
        constraints,
        source_watermarks,
        append_only,
        ..
    } = definition
    else {
        panic!("unexpected statement type: {:?}", definition);
    };

    let (graph, table, source) = generate_stream_graph_for_table(
        session,
        table_name,
        table_catalog,
        source_schema,
        handler_args,
        col_id_gen,
        columns,
        constraints,
        source_watermarks,
        append_only,
    )
    .await?;

    Ok((graph, table, source))
}

pub(crate) fn insert_merger_to_union(node: &mut StreamNode) {
    if let Some(NodeBody::Union(_union_node)) = &mut node.node_body {
        node.input.push(StreamNode {
            identity: "Merge (sink into table)".to_string(),
            fields: node.fields.clone(),
            node_body: Some(NodeBody::Merge(MergeNode {
                upstream_dispatcher_type: DispatcherType::Hash as _,
                ..Default::default()
            })),
            ..Default::default()
        });

        return;
    }

    for input in &mut node.input {
        insert_merger_to_union(input);
    }
}
fn derive_default_column_project_for_sink(
    sink: &SinkCatalog,
    target_table_catalog: &Arc<TableCatalog>,
) -> Result<Vec<ExprImpl>> {
    let mut exprs = vec![];

    let sink_visible_columns = sink
        .full_columns()
        .iter()
        .enumerate()
        .filter(|(_i, c)| !c.is_hidden())
        .collect_vec();

    for (idx, table_column) in target_table_catalog.columns().iter().enumerate() {
        if table_column.is_generated() {
            continue;
        }

        let data_type = table_column.data_type();

        if idx < sink_visible_columns.len() {
            let (sink_col_idx, sink_column) = sink_visible_columns[idx];

            let sink_col_type = sink_column.data_type();

            if data_type != sink_col_type {
                bail!(
                    "column type mismatch: {:?} vs {:?}",
                    data_type,
                    sink_col_type
                );
            } else {
                exprs.push(ExprImpl::InputRef(Box::new(InputRef::new(
                    sink_col_idx,
                    data_type.clone(),
                ))));
            }
        } else {
            let data = match table_column
                .column_desc
                .generated_or_default_column
                .as_ref()
            {
                // default column with default value
                Some(GeneratedOrDefaultColumn::DefaultColumn(default_column)) => {
                    Datum::from_protobuf(default_column.get_snapshot_value().unwrap(), data_type)
                        .unwrap()
                }
                // default column with no default value
                None => None,

                // generated column is unreachable
                _ => unreachable!(),
            };

            exprs.push(ExprImpl::Literal(Box::new(Literal::new(
                data,
                data_type.clone(),
            ))));
        };
    }
    Ok(exprs)
}

/// Transforms the (format, encode, options) from sqlparser AST into an internal struct `SinkFormatDesc`.
/// This is an analogy to (part of) [`crate::handler::create_source::bind_columns_from_source`]
/// which transforms sqlparser AST `SourceSchemaV2` into `StreamSourceInfo`.
fn bind_sink_format_desc(value: ConnectorSchema) -> Result<SinkFormatDesc> {
    use risingwave_connector::sink::catalog::{SinkEncode, SinkFormat};
    use risingwave_connector::sink::encoder::TimestamptzHandlingMode;
    use risingwave_sqlparser::ast::{Encode as E, Format as F};

    let format = match value.format {
        F::Plain => SinkFormat::AppendOnly,
        F::Upsert => SinkFormat::Upsert,
        F::Debezium => SinkFormat::Debezium,
        f @ (F::Native | F::DebeziumMongo | F::Maxwell | F::Canal) => {
            return Err(ErrorCode::BindError(format!("sink format unsupported: {f}")).into());
        }
    };
    let encode = match value.row_encode {
        E::Json => SinkEncode::Json,
        E::Protobuf => SinkEncode::Protobuf,
        E::Avro => SinkEncode::Avro,
        E::Template => SinkEncode::Template,
        e @ (E::Native | E::Csv | E::Bytes) => {
            return Err(ErrorCode::BindError(format!("sink encode unsupported: {e}")).into());
        }
    };
    let mut options = WithOptions::try_from(value.row_options.as_slice())?.into_inner();

    options
        .entry(TimestamptzHandlingMode::OPTION_KEY.to_owned())
        .or_insert(TimestamptzHandlingMode::FRONTEND_DEFAULT.to_owned());

    Ok(SinkFormatDesc {
        format,
        encode,
        options,
    })
}

static CONNECTORS_COMPATIBLE_FORMATS: LazyLock<HashMap<String, HashMap<Format, Vec<Encode>>>> =
    LazyLock::new(|| {
        use risingwave_connector::sink::kafka::KafkaSink;
        use risingwave_connector::sink::kinesis::KinesisSink;
        use risingwave_connector::sink::pulsar::PulsarSink;
        use risingwave_connector::sink::redis::RedisSink;
        use risingwave_connector::sink::Sink as _;

        convert_args!(hashmap!(
                KafkaSink::SINK_NAME => hashmap!(
                    Format::Plain => vec![Encode::Json, Encode::Protobuf],
                    Format::Upsert => vec![Encode::Json, Encode::Avro],
                    Format::Debezium => vec![Encode::Json],
                ),
                KinesisSink::SINK_NAME => hashmap!(
                    Format::Plain => vec![Encode::Json],
                    Format::Upsert => vec![Encode::Json],
                    Format::Debezium => vec![Encode::Json],
                ),
                PulsarSink::SINK_NAME => hashmap!(
                    Format::Plain => vec![Encode::Json],
                    Format::Upsert => vec![Encode::Json],
                    Format::Debezium => vec![Encode::Json],
                ),
                RedisSink::SINK_NAME => hashmap!(
                    Format::Plain => vec![Encode::Json,Encode::Template],
                    Format::Upsert => vec![Encode::Json,Encode::Template],
                ),
        ))
    });

pub fn validate_compatibility(connector: &str, format_desc: &ConnectorSchema) -> Result<()> {
    let compatible_formats = CONNECTORS_COMPATIBLE_FORMATS
        .get(connector)
        .ok_or_else(|| {
            ErrorCode::BindError(format!(
                "connector {} is not supported by FORMAT ... ENCODE ... syntax",
                connector
            ))
        })?;
    let compatible_encodes = compatible_formats.get(&format_desc.format).ok_or_else(|| {
        ErrorCode::BindError(format!(
            "connector {} does not support format {:?}",
            connector, format_desc.format
        ))
    })?;
    if !compatible_encodes.contains(&format_desc.row_encode) {
        return Err(ErrorCode::BindError(format!(
            "connector {} does not support format {:?} with encode {:?}",
            connector, format_desc.format, format_desc.row_encode
        ))
        .into());
    }
    Ok(())
}

/// For `planner_test` crate so that it does not depend directly on `connector` crate just for `SinkFormatDesc`.
impl TryFrom<&WithOptions> for Option<SinkFormatDesc> {
    type Error = risingwave_connector::sink::SinkError;

    fn try_from(value: &WithOptions) -> std::result::Result<Self, Self::Error> {
        let connector = value.get(CONNECTOR_TYPE_KEY);
        let r#type = value.get(SINK_TYPE_OPTION);
        match (connector, r#type) {
            (Some(c), Some(t)) => SinkFormatDesc::from_legacy_type(c, t),
            _ => Ok(None),
        }
    }
}

#[cfg(test)]
pub mod tests {
    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};

    use crate::catalog::root_catalog::SchemaPath;
    use crate::test_utils::{create_proto_file, LocalFrontend, PROTO_FILE_DATA};

    #[tokio::test]
    async fn test_create_sink_handler() {
        let proto_file = create_proto_file(PROTO_FILE_DATA);
        let sql = format!(
            r#"CREATE SOURCE t1
    WITH (connector = 'kafka', kafka.topic = 'abc', kafka.servers = 'localhost:1001')
    FORMAT PLAIN ENCODE PROTOBUF (message = '.test.TestRecord', schema.location = 'file://{}')"#,
            proto_file.path().to_str().unwrap()
        );
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql).await.unwrap();

        let sql = "create materialized view mv1 as select t1.country from t1;";
        frontend.run_sql(sql).await.unwrap();

        let sql = r#"CREATE SINK snk1 FROM mv1
                    WITH (connector = 'jdbc', mysql.endpoint = '127.0.0.1:3306', mysql.table =
                        '<table_name>', mysql.database = '<database_name>', mysql.user = '<user_name>',
                        mysql.password = '<password>', type = 'append-only', force_append_only = 'true');"#.to_string();
        frontend.run_sql(sql).await.unwrap();

        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        // Check source exists.
        let (source, _) = catalog_reader
            .get_source_by_name(DEFAULT_DATABASE_NAME, schema_path, "t1")
            .unwrap();
        assert_eq!(source.name, "t1");

        // Check table exists.
        let (table, schema_name) = catalog_reader
            .get_table_by_name(DEFAULT_DATABASE_NAME, schema_path, "mv1")
            .unwrap();
        assert_eq!(table.name(), "mv1");

        // Check sink exists.
        let (sink, _) = catalog_reader
            .get_sink_by_name(DEFAULT_DATABASE_NAME, SchemaPath::Name(schema_name), "snk1")
            .unwrap();
        assert_eq!(sink.name, "snk1");
    }
}
