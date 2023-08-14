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
use risingwave_sqlparser::ast::ObjectName;
use risingwave_sqlparser::parser::Parser;

use super::{HandlerArgs, RwPgResponse};
use crate::catalog::root_catalog::SchemaPath;
use crate::{Binder, OptimizerContext};
use crate::handler::create_mv::gen_create_mv_plan;

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

    // let view_id = {
    //     let reader = session.env().catalog_reader().read_guard();
    //     let (view, schema_name) =
    //         reader.get_table_by_name(db_name, schema_path, &real_view_name)?;
    //     session.check_privilege_for_drop_alter(schema_name, &**view)?;
    //     view.id
    // };
    //
    // let catalog_writer = session.catalog_writer()?;
    // catalog_writer
    //     .alter_materialized_view_to_table(view_id.table_id())
    //     .await?;

    let original_catalog = {
        let reader = session.env().catalog_reader().read_guard();
        let (table, schema_name) =
            reader.get_table_by_name(db_name, schema_path, &real_view_name)?;

        // match table.table_type() {
        //     // Do not allow altering a table with a connector. It should be done passively
        // according     // to the messages from the connector.
        //     TableType::Table if table.has_associated_source() => {
        //         Err(ErrorCode::InvalidInputSyntax(format!(
        //             "cannot alter table \"{table_name}\" because it has a connector"
        //         )))?
        //     }
        //     TableType::Table => {}
        //
        //     _ => Err(ErrorCode::InvalidInputSyntax(format!(
        //         "\"{table_name}\" is not a table or cannot be altered"
        //     )))?,
        // }

        session.check_privilege_for_drop_alter(schema_name, &**table)?;

        table.clone()
    };

    // // TODO(yuhao): alter table with generated columns.
    // if original_catalog.has_generated_column() {
    //     return Err(RwError::from(ErrorCode::BindError(
    //         "Alter a table with generated column has not been implemented.".to_string(),
    //     )));
    // }

    // Retrieve the original table definition and parse it to AST.
    let [mut definition]: [_; 1] = Parser::parse_sql(&original_catalog.definition)
        .context("unable to parse original table definition")?
        .try_into()
        .unwrap();

    // let Statement::CreateTable { columns, .. } = &mut definition else {
    //     panic!("unexpected statement: {:?}", definition);
    // };

    // Duplicated names can actually be checked by `StreamMaterialize`. We do here for
    // better error reporting.
    // let new_column_name = new_column.name.real_value();
    // if columns
    //     .iter()
    //     .any(|c| c.name.real_value() == new_column_name)
    // {
    //     Err(ErrorCode::InvalidInputSyntax(format!(
    //         "column \"{new_column_name}\" of table \"{table_name}\" already exists"
    //     )))?
    // }
    //
    // if new_column
    //     .options
    //     .iter()
    //     .any(|x| matches!(x.option, ColumnOption::GeneratedColumns(_)))
    // {
    //     Err(ErrorCode::InvalidInputSyntax(
    //         "alter table add generated columns is not supported".to_string(),
    //     ))?
    // }
    //
    // // Add the new column to the table definition.
    // columns.push(new_column);
    //        }

    // Create handler args as if we're creating a new table with the altered definition.
    // let handler_args = HandlerArgs::new(session.clone(), &definition, "")?;
    // let col_id_gen = ColumnIdGenerator::new_alter(&original_catalog);
    // let Statement::CreateTable {
    //     columns,
    //     constraints,
    //     source_watermarks,
    //     append_only,
    //     ..
    // } = definition
    //     else {
    //         panic!("unexpected statement type: {:?}", definition);
    //     };

    let (graph, table) = {
        let context = OptimizerContext::from_handler_args(handler_args);
        let (plan, source, table) = gen_create_mv_plan(
            context,
            table_name,
            columns,
            constraints,
            col_id_gen,
            source_watermarks,
            append_only,
        )?;

        // We should already have rejected the case where the table has a connector.
        assert!(source.is_none());

        // TODO: avoid this backward conversion.
        if TableCatalog::from(&table).pk_column_ids() != original_catalog.pk_column_ids() {
            Err(ErrorCode::InvalidInputSyntax(
                "alter primary key of table is not supported".to_owned(),
            ))?
        }

        let graph = StreamFragmentGraph {
            parallelism: session
                .config()
                .get_streaming_parallelism()
                .map(|parallelism| Parallelism { parallelism }),
            ..build_graph(plan)
        };

        // Fill the original table ID.
        let table = Table {
            id: original_catalog.id().table_id(),
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
