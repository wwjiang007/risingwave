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

//! This module provide jni catalog.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use icelake::catalog::models::{CommitTableRequest, CommitTableResponse, LoadTableResult};
use icelake::catalog::{
    BaseCatalogConfig, Catalog, CatalogRef, IcebergTableIoArgs, OperatorCreator, UpdateTable,
};
use icelake::{ErrorKind, Table, TableIdentifier};
use jni::objects::{GlobalRef, JObject, JString};
use jni::JavaVM;
use risingwave_jni_core::jvm_runtime::{execute_with_jni_env, JVM};

use crate::sink::{Result, SinkError};

pub struct JniCatalog {
    java_catalog: GlobalRef,
    jvm: &'static JavaVM,
    config: BaseCatalogConfig,
}

#[async_trait]
impl Catalog for JniCatalog {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn load_table(self: Arc<Self>, table_name: &TableIdentifier) -> icelake::Result<Table> {
        let _guard = self.jvm.attach_current_thread().map_err(|e| {
            icelake::Error::new(
                ErrorKind::Unexpected,
                "Failed to attach current thread to jvm",
            )
            .set_source(e)
        })?;

        let mut env = self.jvm.get_env().map_err(|e| {
            icelake::Error::new(ErrorKind::Unexpected, "Failed to get jni env").set_source(e)
        })?;

        let table_name_str = table_name.to_string();

        let table_name_jstr = env.new_string(&table_name_str).unwrap();

        let result_json = env
            .call_method(
                self.java_catalog.as_obj(),
                "loadTable",
                "(Ljava/lang/String;)Ljava/lang/String;",
                &[(&table_name_jstr).into()],
            )
            .map_err(|e| {
                icelake::Error::new(
                    ErrorKind::Unexpected,
                    "Failed to call loadTable in jni catalog",
                )
                .set_source(e)
                .with_context("table_name", &table_name_str)
            })?;

        let rust_json_str = unsafe {
            let jvalue = result_json.as_jni();
            let jstring = JString::from_raw(jvalue.l);
            let java_str = env.get_string(&jstring).map_err(|e| {
                icelake::Error::new(
                    ErrorKind::Unexpected,
                    "Failed to get string from return value of loadTable method of jni catalog",
                )
                .set_source(e)
            })?;

            java_str
                .to_str()
                .map_err(|e| {
                    icelake::Error::new(
                        ErrorKind::Unexpected,
                        "Failed to convert java string to rust utf8 string",
                    )
                    .set_source(e)
                    .with_context("Java string bytes", format!("{:?}", java_str.to_bytes()))
                })?
                .to_string()
        };

        let resp: LoadTableResult = serde_json::from_str(&rust_json_str)?;

        let metadata_location = resp.metadata_location.clone().ok_or_else(|| {
            icelake::Error::new(
                icelake::ErrorKind::IcebergFeatureUnsupported,
                "Loading uncommitted table is not supported!",
            )
        })?;

        tracing::info!("Table metadata location of {table_name} is {metadata_location}");

        let table_metadata = resp.table_metadata()?;

        let iceberg_io_args = IcebergTableIoArgs::builder_from_path(&table_metadata.location)?
            .with_args(self.config.table_io_configs.iter())
            .build()?;
        let table_op = iceberg_io_args.create()?;

        Ok(
            Table::builder_from_catalog(table_op, self.clone(), table_metadata, table_name.clone())
                .build()?,
        )
    }

    async fn update_table(self: Arc<Self>, update_table: &UpdateTable) -> icelake::Result<Table> {
        let _guard = self.jvm.attach_current_thread().map_err(|e| {
            icelake::Error::new(
                ErrorKind::Unexpected,
                "Failed to attach current thread to jvm",
            )
            .set_source(e)
        })?;

        let request_str = serde_json::to_string(&CommitTableRequest::try_from(update_table)?)?;

        let mut env = self.jvm.get_env().map_err(|e| {
            icelake::Error::new(ErrorKind::Unexpected, "Failed to get jni env").set_source(e)
        })?;

        let request_jni_str = env.new_string(&request_str).map_err(|e| {
            icelake::Error::new(
                ErrorKind::Unexpected,
                "Failed to create jni string from request json",
            )
            .set_source(e)
            .with_context("request", &request_str)
        })?;

        let result_json = env
            .call_method(
                self.java_catalog.as_obj(),
                "updateTable",
                "(Ljava/lang/String;)Ljava/lang/String;",
                &[(&request_jni_str).into()],
            )
            .map_err(|e| {
                icelake::Error::new(
                    ErrorKind::Unexpected,
                    "Failed to call updateTable in jni catalog",
                )
                .set_source(e)
                .with_context("request json", &request_str)
            })?;

        let rust_json_str = unsafe {
            let jvalue = result_json.as_jni();
            let jstring = JString::from_raw(jvalue.l);
            let java_str = env.get_string(&jstring).map_err(|e| {
                icelake::Error::new(
                    ErrorKind::Unexpected,
                    "Failed to get string from return value of updateTable method of jni catalog",
                )
                .set_source(e)
            })?;

            java_str
                .to_str()
                .map_err(|e| {
                    icelake::Error::new(
                        ErrorKind::Unexpected,
                        "Failed to convert java string to rust utf8 string",
                    )
                    .set_source(e)
                    .with_context("Java string bytes", format!("{:?}", java_str.to_bytes()))
                })?
                .to_string()
        };

        let response: CommitTableResponse = serde_json::from_str(&rust_json_str)?;

        tracing::info!(
            "Table metadata location of {} is {}",
            update_table.table_name(),
            response.metadata_location
        );

        let table_metadata = response.metadata()?;

        let args = IcebergTableIoArgs::builder_from_path(&table_metadata.location)?
            .with_args(self.config.table_io_configs.iter())
            .build()?;
        let table_op = args.create()?;

        Ok(Table::builder_from_catalog(
            table_op,
            self.clone(),
            table_metadata,
            update_table.table_name().clone(),
        )
        .build()?)
    }
}

impl JniCatalog {
    pub fn build(
        base_config: BaseCatalogConfig,
        name: impl ToString,
        catalog_impl: impl ToString,
        java_catalog_props: HashMap<String, String>,
    ) -> Result<CatalogRef> {
        let jvm = JVM.get_or_init()?;

        execute_with_jni_env(jvm, |env| {
            // Convert props to string array
            let props = env.new_object_array(
                (java_catalog_props.len() * 2) as i32,
                "java/lang/String",
                JObject::null(),
            )?;
            for (i, (key, value)) in java_catalog_props.iter().enumerate() {
                let key_j_str = env.new_string(key).unwrap();
                let value_j_str = env.new_string(value).unwrap();
                env.set_object_array_element(&props, i as i32 * 2, key_j_str)?;
                env.set_object_array_element(&props, i as i32 * 2 + 1, value_j_str)?;
            }

            let jni_catalog_wrapper = env
                .call_static_method(
                    "com/risingwave/connector/catalog/JniCatalogWrapper",
                    "create",
                    "(Ljava/lang/String;Ljava/lang/String;[Ljava/lang/String;)V",
                    &[
                        (&env.new_string(name.to_string()).unwrap()).into(),
                        (&env.new_string(catalog_impl.to_string()).unwrap()).into(),
                        (&props).into(),
                    ],
                )
                .unwrap();

            let jni_catalog = env
                .new_global_ref(jni_catalog_wrapper.l().unwrap())
                .unwrap();

            Ok(Arc::new(Self {
                java_catalog: jni_catalog,
                jvm,
                config: base_config,
            }) as CatalogRef)
        })
        .map_err(SinkError::Iceberg)
    }
}
