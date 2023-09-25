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

use std::rc::Rc;

use anyhow::Context;
use itertools::Itertools;
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::catalog::{ConnectionId, DatabaseId, SchemaId, UserId};
use risingwave_common::error::Result;
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_connector::sink::catalog::SinkCatalog;
use risingwave_pb::catalog::table::OptionalAssociatedSourceId;
use risingwave_pb::catalog::Table;
use risingwave_pb::stream_plan::stream_fragment_graph::Parallelism;
use risingwave_pb::stream_plan::StreamFragmentGraph;
use risingwave_sqlparser::ast::{
    CreateSink, CreateSinkStatement, EmitMode, ObjectName, Query, Select, SelectItem, SetExpr,
    Statement, TableFactor, TableWithJoins,
};
use risingwave_sqlparser::parser::Parser;

use super::create_mv::get_column_names;
use super::RwPgResponse;
use crate::binder::Binder;
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::table_catalog::TableType;
use crate::handler::create_table::{
    gen_create_table_plan, gen_create_table_plan_with_source, ColumnIdGenerator,
};
use crate::handler::privilege::resolve_query_privileges;
use crate::handler::HandlerArgs;
use crate::optimizer::plan_node::{Explain, PlanTreeNode};
use crate::optimizer::{OptimizerContext, OptimizerContextRef, PlanRef, RelationCollectorVisitor};
use crate::scheduler::streaming_manager::CreatingStreamingJobInfo;
use crate::session::SessionImpl;
use crate::stream_fragmenter::build_graph;
use crate::utils::resolve_connection_in_with_option;
use crate::Planner;

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

pub fn gen_sink_plan(
    session: &SessionImpl,
    context: OptimizerContextRef,
    stmt: CreateSinkStatement,
) -> Result<(Box<Query>, PlanRef, SinkCatalog)> {
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

    let sink_into_table_name = stmt.into_table_name.map(|name| name.real_value());

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
    let connection_id = {
        let conn_id =
            resolve_connection_in_with_option(&mut with_options, &sink_schema_name, session)?;
        conn_id.map(ConnectionId)
    };

    let emit_on_window_close = stmt.emit_mode == Some(EmitMode::OnWindowClose);
    if emit_on_window_close {
        context.warn_to_user("EMIT ON WINDOW CLOSE is currently an experimental feature. Please use it with caution.");
    }

    let mut plan_root = Planner::new(context).plan_query(bound)?;
    if let Some(col_names) = col_names {
        plan_root.set_out_names(col_names)?;
    };

    let sink_plan = plan_root.gen_sink_plan(
        sink_table_name,
        definition,
        with_options,
        emit_on_window_close,
        db_name.to_owned(),
        sink_from_table_name,
        sink_into_table_name,
    )?;
    let sink_desc = sink_plan.sink_desc().clone();
    let sink_plan: PlanRef = sink_plan.into();

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

    Ok((query, sink_plan, sink_catalog))
}

pub async fn handle_create_sink(
    handle_args: HandlerArgs,
    stmt: CreateSinkStatement,
) -> Result<RwPgResponse> {
    let session = handle_args.session.clone();

    session.check_relation_name_duplicated(stmt.sink_name.clone())?;

    let target_table_object_name = stmt.into_table_name.clone();

    let context = Rc::new(OptimizerContext::from_handler_args(handle_args));
    let (query, sink_plan, sink_catalog) = gen_sink_plan(&session, context.clone(), stmt)?;

    let (sink, graph) = {
        let has_order_by = !query.order_by.is_empty();
        if has_order_by {
            context.warn_to_user(
                r#"The ORDER BY clause in the CREATE SINK statement has no effect at all."#
                    .to_string(),
            );
        }
        let mut sink_graph = build_graph(sink_plan);
        sink_graph.parallelism = session
            .config()
            .get_streaming_parallelism()
            .map(|parallelism| Parallelism { parallelism });
        (sink_catalog, sink_graph)
    };

    //Stream

    if let Some(table_name) = target_table_object_name.as_ref() {
        let db_name = session.database();
        let (schema_name, real_table_name) =
            Binder::resolve_schema_qualified_name(db_name, table_name.clone())?;
        let search_path = session.config().get_search_path();
        let user_name = &session.auth_context().user_name;

        let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

        let target_table = {
            let reader = session.env().catalog_reader().read_guard();
            let (table, schema_name) =
                reader.get_table_by_name(db_name, schema_path, &real_table_name)?;

            // match table.table_type() {
            //     TableType::Table => {}
            //
            //     _ => Err(ErrorCode::InvalidInputSyntax(format!(
            //         "\"{table_name}\" is not a table or cannot be altered"
            //     )))?,
            // }

            session.check_privilege_for_drop_alter(schema_name, &**table)?;

            table.clone()
        };

        // Retrieve the original table definition and parse it to AST.
        let [mut definition]: [_; 1] = Parser::parse_sql(&target_table.definition)
            .context("unable to parse original table definition")?
            .try_into()
            .unwrap();

        let Statement::CreateTable {
            columns,
            source_schema,
            ..
        } = &mut definition
        else {
            panic!("unexpected statement: {:?}", definition);
        };
        let source_schema = source_schema
            .clone()
            .map(|source_schema| source_schema.into_source_schema_v2().0);

        // Create handler args as if we're creating a new table with the altered definition.
        let handler_args = HandlerArgs::new(session.clone(), &definition, "")?;
        let col_id_gen = ColumnIdGenerator::new_alter(&target_table);
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

        let (table_graph, table, table_source) = {
            let context = OptimizerContext::from_handler_args(handler_args);
            let (plan, source, table) = match source_schema {
                Some(source_schema) => {
                    gen_create_table_plan_with_source(
                        context,
                        target_table_object_name.unwrap().clone(),
                        columns,
                        constraints,
                        source_schema,
                        source_watermarks,
                        col_id_gen,
                        append_only,
                    )
                    .await?
                }
                None => gen_create_table_plan(
                    context,
                    target_table_object_name.unwrap().clone(),
                    columns,
                    constraints,
                    col_id_gen,
                    source_watermarks,
                    append_only,
                )?,
            };

            // // TODO: avoid this backward conversion.
            // if TableCatalog::from(&table).pk_column_ids() != target_table.pk_column_ids() {
            //     Err(ErrorCode::InvalidInputSyntax(
            //         "alter primary key of table is not supported".to_owned(),
            //     ))?
            // }

            // let materialize = plan
            //     .as_stream_materialize()
            //     .context("not a materialized view")?;
            //
            // let mut plans = vec![];
            // // let prev = None;
            // let mut p = plan.clone();
            // loop {
            //     if let Some(union) = p.as_stream_union() {
            //         break;
            //     } else {
            //         println!(
            //             "found other {} {}",
            //             p.explain_myself_to_string(),
            //             p.as_stream_union().is_some()
            //         );
            //         plans.push(p.clone());
            //         p = p.inputs().first().cloned().unwrap();
            //     }
            // }
            //
            // let mut inputs = p.inputs().to_vec();
            // inputs.push(sink_plan.clone());
            //
            // let mut new_p = p.clone_with_inputs(inputs.as_ref());
            //
            // loop {
            //     if plans.is_empty() {
            //         break;
            //     }
            //
            //     let first = plans.pop().unwrap();
            //     new_p = first.clone_with_inputs(vec![new_p].as_ref());
            // }
            //
            // println!("new p {:#?}", new_p);

            let graph = StreamFragmentGraph {
                parallelism: session
                    .config()
                    .get_streaming_parallelism()
                    .map(|parallelism| Parallelism { parallelism }),
                ..build_graph(plan)
            };

            // Fill the original table ID.
            let table = Table {
                id: target_table.id().table_id(),
                optional_associated_source_id: target_table.associated_source_id().map(
                    |source_id| OptionalAssociatedSourceId::AssociatedSourceId(source_id.into()),
                ),
                ..table
            };

            (graph, table, source)
        };

        // Calculate the mapping from the original columns to the new columns.
        let col_index_mapping = ColIndexMapping::with_target_size(
            target_table
                .columns()
                .iter()
                .map(|old_c| {
                    table.columns.iter().position(|new_c| {
                        new_c.get_column_desc().unwrap().column_id == old_c.column_id().get_id()
                    })
                })
                .collect(),
            table.columns.len(),
        );

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
            .create_sink_into_table(
                sink.to_proto(),
                graph,
                table_source,
                table,
                table_graph,
                col_index_mapping,
            )
            .await?;

        return Ok(PgResponse::empty_result(StatementType::CREATE_SINK));
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
    catalog_writer.create_sink(sink.to_proto(), graph).await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_SINK))
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
                    WITH (connector = 'mysql', mysql.endpoint = '127.0.0.1:3306', mysql.table =
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
