use anyhow::Context;
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
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::error::Result;
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_pb::catalog::table::PbTableVersion;
use risingwave_pb::catalog::Table;
use risingwave_pb::stream_plan::stream_fragment_graph::Parallelism;
use risingwave_pb::stream_plan::StreamFragmentGraph;
use risingwave_sqlparser::ast::{ColumnDef, ObjectName, Statement};
use risingwave_sqlparser::parser::Parser;

use super::{HandlerArgs, RwPgResponse};
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::table_catalog::{TableType, TableVersion};
use crate::catalog::ColumnId;
use crate::handler::create_mv::gen_create_mv_plan;
use crate::{build_graph, Binder, OptimizerContext};

pub async fn handle_alter_materialized_view_to_table(
    handler_args: HandlerArgs,
    view_name: ObjectName,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let db_name = session.database();
    let (schema_name, real_view_name) =
        Binder::resolve_schema_qualified_name(db_name, view_name.clone())?;

    let search_path = session.config().get_search_path();
    let user_name = &session.auth_context().user_name;

    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let original_catalog = {
        let reader = session.env().catalog_reader().read_guard();
        let (table, schema_name) =
            reader.get_table_by_name(db_name, schema_path, &real_view_name)?;

        assert_eq!(table.table_type(), TableType::MaterializedView);
        session.check_privilege_for_drop_alter(schema_name, &**table)?;
        table.clone()
    };

    // Retrieve the original table definition and parse it to AST.
    let [definition]: [_; 1] = Parser::parse_sql(&original_catalog.definition)
        .context("unable to parse original table definition")?
        .try_into()
        .unwrap();

    // Create handler args as if we're creating a new table with the altered definition.
    let handler_args = HandlerArgs::new(session.clone(), &definition, "")?;

    let Statement::CreateView {
        name, columns, query, emit_mode,
        ..
    } = definition else {
        panic!("unexpected statement: {:?}", definition);
    };

    let (graph, table) = {
        let context = OptimizerContext::from_handler_args(handler_args);

        let (plan, table) =
            gen_create_mv_plan(&session, context.into(), *query, name, columns, emit_mode)?;

        let graph = StreamFragmentGraph {
            parallelism: session
                .config()
                .get_streaming_parallelism()
                .map(|parallelism| Parallelism { parallelism }),
            ..build_graph(plan)
        };

//        println!("graph {:#?}", graph);

        let table_version = Some(PbTableVersion {
            version: 0,
            next_column_id: 0,
        });

        // Fill the original table ID.
        let table = Table {
            id: original_catalog.id().table_id(),
            version: table_version,
            ..table
        };

        (graph, table)
    };

    // Calculate the mapping from the original columns to the new columns.
    let col_index_mapping = ColIndexMapping::with_target_size(
        original_catalog
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

    let catalog_writer = session.catalog_writer()?;

    catalog_writer
        .replace_table(table, graph, col_index_mapping)
        .await?;

    Ok(PgResponse::empty_result(
        StatementType::ALTER_MATERIALIZED_VIEW,
    ))
}
