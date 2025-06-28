use std::{
    collections::HashMap,
    error::Error,
    fmt::{self, Display, Formatter},
    sync::mpsc::SendError,
};

use tokio_stream::StreamExt;
use tonic::Streaming;

use crate::{
    replication_row_event::ReplicationRowEventEnvelope,
    vitess_grpc::{
        query::BoundQuery,
        vtgate::{StreamExecuteRequest, StreamExecuteResponse},
        vtgateservice::vitess_client::VitessClient,
    },
    vitess_schema::{TableName, VitessSchema},
    vitess_shards::KeyspaceName,
};

#[derive(Debug)]
#[non_exhaustive]
pub struct VitessSnapshotError {
    pub keyspace: Box<KeyspaceName>,
    pub table: Box<TableName>,
    pub kind: VitessSnapshotErrorKind,
}

impl Display for VitessSnapshotError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "error while taking incremental snapshot `{}`",
            self.keyspace
        )
    }
}

impl Error for VitessSnapshotError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            VitessSnapshotErrorKind::VitessClientCallFailed(e) => Some(e),
            VitessSnapshotErrorKind::SendFailed(e) => Some(e),
        }
    }
}

#[derive(Debug)]
pub enum VitessSnapshotErrorKind {
    VitessClientCallFailed(tonic::Status),
    SendFailed(SendError<ReplicationRowEventEnvelope>),
}

pub(crate) async fn perform_snapshot_query(
    client: &mut VitessClient<tonic::transport::Channel>,
    keyspace: &KeyspaceName,
    table: &TableName,
    schema: &VitessSchema,
    chunk_size: usize,
    last_pk: Option<String>,
) -> Result<Streaming<StreamExecuteResponse>, tonic::Status> {
    let pks = schema.primary_keys.join(",");
    let query_response = client
        .stream_execute(tonic::Request::new(StreamExecuteRequest {
            caller_id: None,
            session: None,
            query: Some(BoundQuery {
                sql: format!(
                    "SELECT * FROM {}.{} ORDER BY {} LIMIT {}",
                    keyspace, table, pks, chunk_size,
                ),
                bind_variables: HashMap::new(),
            }),
        }))
        .await?;

    Ok(query_response.into_inner())
}

async fn process_query_result(
    keyspace: &KeyspaceName,
    table: &TableName,
    schema: &VitessSchema,
    mut results: Streaming<StreamExecuteResponse>,
) -> Result<(), VitessSnapshotError> {
    while let Some(message_result) = results.next().await {
        let message = message_result.map_err(|e| VitessSnapshotError {
            keyspace: Box::new(keyspace.clone()),
            table: Box::new(table.clone()),
            kind: VitessSnapshotErrorKind::VitessClientCallFailed(e),
        })?;

        if let Some(result) = message.result {}
    }

    Ok(())
}
