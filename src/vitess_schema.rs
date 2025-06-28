use std::collections::HashMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use crate::vitess_grpc::tabletmanagerdata::TableDefinition;
use crate::vitess_grpc::vtctldata::GetSchemaRequest;
use crate::vitess_shards::KeyspaceName;
use crate::{
    vitess_grpc::{query::Field, vtctlservice::vtctld_client::VtctldClient},
    vitess_shards,
};

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub(crate) struct TableName(String);
impl From<String> for TableName {
    fn from(value: String) -> Self {
        TableName(value)
    }
}
impl Display for TableName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub(crate) struct FieldName(String);
impl Display for FieldName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl Into<String> for FieldName {
    fn into(self) -> String {
        self.0
    }
}

#[derive(Debug, Clone)]
pub(crate) struct VitessSchema {
    pub(crate) table: TableName,
    pub(crate) schema: Vec<(FieldName, Field)>,
    pub(crate) primary_keys: Vec<String>,
}

#[derive(Debug)]
#[non_exhaustive]
pub struct VitessSchemaError {
    pub keyspace: Box<KeyspaceName>,
    pub kind: VitessSchemaErrorKind,
}

impl Display for VitessSchemaError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "error fetching schemas for keyspace `{}`", self.keyspace)
    }
}

impl Error for VitessSchemaError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            VitessSchemaErrorKind::VitessClientCallFailed(e) => Some(e),
            VitessSchemaErrorKind::SchemasNotFound(e) => Some(e),
        }
    }
}

#[derive(Debug)]
pub enum VitessSchemaErrorKind {
    VitessClientCallFailed(tonic::Status),
    SchemasNotFound(VitessSchemasNotFoundError),
}

#[derive(Debug, Clone)]
pub struct VitessSchemasNotFoundError {
    tables: Vec<TableName>,
}

impl Display for VitessSchemasNotFoundError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "could not find a schema definition for tables `{:?}`",
            self.tables
        )
    }
}
impl Error for VitessSchemasNotFoundError {}

pub(crate) async fn get_schema_for_tables(
    vtctld_client: &mut VtctldClient<tonic::transport::Channel>,
    keyspace: &KeyspaceName,
    tables: &[TableName],
) -> Result<HashMap<TableName, VitessSchema>, VitessSchemaError> {
    log::info!("Finding primary tablet...");
    let primary_tablet =
        vitess_shards::get_first_primary_tablet_for_first_shard(vtctld_client, keyspace)
            .await
            .map_err(|e| VitessSchemaError {
                keyspace: Box::new(keyspace.clone()),
                kind: VitessSchemaErrorKind::VitessClientCallFailed(e),
            })?;

    log::info!("Getting schemas for all tables...");
    let schema_response = vtctld_client
        .get_schema(tonic::Request::new(GetSchemaRequest {
            tables: tables.iter().map(|t| t.to_string()).collect(),
            tablet_alias: primary_tablet.alias.clone(),
            table_names_only: false,
            table_sizes_only: false,
            table_schema_only: false,
            include_views: true,
            exclude_tables: vec![],
        }))
        .await
        .map_err(|e| VitessSchemaError {
            keyspace: Box::new(keyspace.clone()),
            kind: VitessSchemaErrorKind::VitessClientCallFailed(e),
        })?;

    let schemas = schema_response
        .into_inner()
        .schema
        .expect("There should be a schema defined for the table!")
        .table_definitions
        .iter()
        .map(|table_def| get_schema_from_table_def(keyspace, table_def))
        .collect();

    validate_all_schemas_present(keyspace, tables, schemas)
}

fn validate_all_schemas_present(
    keyspace: &KeyspaceName,
    tables: &[TableName],
    schemas: HashMap<TableName, VitessSchema>,
) -> Result<HashMap<TableName, VitessSchema>, VitessSchemaError> {
    let missing_tables: Vec<TableName> = tables
        .iter()
        .map(|t| (t, schemas.contains_key(t)))
        .filter(|(_, present)| !present)
        .map(|(t, _)| t.clone())
        .collect();

    if missing_tables.len() > 0 {
        Err(VitessSchemaError {
            keyspace: Box::new(keyspace.clone()),
            kind: VitessSchemaErrorKind::SchemasNotFound(VitessSchemasNotFoundError {
                tables: missing_tables,
            }),
        })
    } else {
        Ok(schemas)
    }
}

fn get_schema_from_table_def(
    keyspace: &KeyspaceName,
    table_def: &TableDefinition,
) -> (TableName, VitessSchema) {
    let table_name = TableName(format!("{}.{}", keyspace, table_def.name));
    (
        table_name.clone(),
        VitessSchema {
            table: table_name,
            schema: table_def
                .columns
                .iter()
                .zip(table_def.fields.iter())
                .map(|(col, field)| (FieldName(col.clone()), field.clone()))
                .collect(),
            primary_keys: table_def.primary_key_columns.clone(),
        },
    )
}
