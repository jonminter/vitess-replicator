use std::{
    collections::HashMap,
    error::Error,
    fmt::{self, Display, Formatter},
    sync::mpsc::{Receiver, RecvError, SendError, Sender},
};

use serde_json::Map;

use crate::{
    replication_row_event::{ReplicationRowEvent, ReplicationRowEventEnvelope},
    table_row_deserializer::{
        DeserializeRowError, deserialize_row_values, transform_string_to_json_value,
    },
    vitess_schema::{TableName, VitessSchema},
    vitess_shards::KeyspaceName,
};

#[derive(Debug)]
#[non_exhaustive]
pub struct RowJsonConverterError {
    pub kind: RowJsonConverterErrorKind,
}

impl Display for RowJsonConverterError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "error converting rows to json",)
    }
}

impl Error for RowJsonConverterError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            RowJsonConverterErrorKind::RecvFailed(e) => Some(e),
            RowJsonConverterErrorKind::TableSchemaNotFound(e) => Some(e),
            RowJsonConverterErrorKind::ConvertToJsonFailed(e) => Some(e),
            RowJsonConverterErrorKind::SendFailed(e) => Some(e),
        }
    }
}

#[derive(Debug)]
pub(crate) struct MissingTableSchemaError {
    pub(crate) keyspace: Box<KeyspaceName>,
    pub(crate) table: Box<TableName>,
}
impl Display for MissingTableSchemaError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Cannot find schema for table {} in keyspace {}. Did you configure this table as one of the ones to watch?",
            self.table, self.keyspace
        )
    }
}
impl Error for MissingTableSchemaError {}

#[derive(Debug)]
pub enum RowJsonConverterErrorKind {
    RecvFailed(RecvError),
    TableSchemaNotFound(MissingTableSchemaError),
    ConvertToJsonFailed(DeserializeRowError),
    SendFailed(SendError<serde_json::Value>),
}

pub(crate) fn start_row_change_json_converter(
    incoming_rows: Receiver<ReplicationRowEventEnvelope>,
    outgoing_rows: Sender<serde_json::Value>,
    schemas: HashMap<TableName, VitessSchema>,
) -> Result<(), RowJsonConverterError> {
    loop {
        let event_envelope = incoming_rows.recv().map_err(|e| RowJsonConverterError {
            kind: RowJsonConverterErrorKind::RecvFailed(e),
        })?;

        let schema = schemas
            .get(&event_envelope.table)
            .ok_or(RowJsonConverterError {
                kind: RowJsonConverterErrorKind::TableSchemaNotFound(MissingTableSchemaError {
                    keyspace: Box::new(event_envelope.keyspace.clone()),
                    table: Box::new(event_envelope.table.clone()),
                }),
            })?;

        let json =
            row_event_to_json(event_envelope.event, schema).map_err(|e| RowJsonConverterError {
                kind: RowJsonConverterErrorKind::ConvertToJsonFailed(e),
            })?;

        outgoing_rows
            .send(json)
            .map_err(|e| RowJsonConverterError {
                kind: RowJsonConverterErrorKind::SendFailed(e),
            })?;
    }
}

fn row_event_to_json(
    row_event: ReplicationRowEvent,
    schema: &VitessSchema,
) -> Result<serde_json::Value, DeserializeRowError> {
    let mut data: Map<String, serde_json::Value> = Map::new();
    data.insert(
        "op".to_string(),
        serde_json::Value::String(event_to_op_name(&row_event)),
    );

    let row = match row_event {
        ReplicationRowEvent::Insert(row) => row,
        ReplicationRowEvent::Update { before: _, after } => after,
        ReplicationRowEvent::Delete(row) => row,
        ReplicationRowEvent::SnapshotRead(row) => row,
    };

    let field_value_strings = deserialize_row_values(row, schema)?;
    for (column_number, (value, (field_name, field))) in field_value_strings
        .into_iter()
        .zip(schema.schema.iter())
        .enumerate()
    {
        data.insert(
            field_name.clone().into(),
            transform_string_to_json_value(
                value,
                &schema.table,
                column_number,
                &field_name,
                &field,
            )?,
        );
    }

    Ok(serde_json::Value::Object(data))
}

fn event_to_op_name(row_event: &ReplicationRowEvent) -> String {
    match row_event {
        ReplicationRowEvent::Insert(_) => "I".to_string(),
        ReplicationRowEvent::SnapshotRead(_) => "R".to_string(),
        ReplicationRowEvent::Update {
            before: _,
            after: _,
        } => "U".to_string(),
        ReplicationRowEvent::Delete(_) => "D".to_string(),
    }
}
