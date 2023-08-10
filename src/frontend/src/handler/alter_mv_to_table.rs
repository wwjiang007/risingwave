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
use risingwave_common::error::{Result};
use risingwave_sqlparser::ast::ObjectName;

use super::{HandlerArgs, RwPgResponse};
use crate::catalog::root_catalog::SchemaPath;
use crate::Binder;

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

    let view_id = {
        let reader = session.env().catalog_reader().read_guard();
        let (view, schema_name) = reader.get_view_by_name(db_name, schema_path, &real_view_name)?;
        session.check_privilege_for_drop_alter(schema_name, &**view)?;
        view.id
    };

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .alter_materialized_view_to_table(view_id)
        .await?;

    Ok(PgResponse::empty_result(
        StatementType::ALTER_MATERIALIZED_VIEW,
    ))
}
