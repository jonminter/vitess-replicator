use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    sync::mpsc::{SendError, Sender},
};

use tokio_stream::StreamExt;
use tonic::Streaming;

use crate::{
    replication_row_event::ReplicationRowEventEnvelope,
    vitess_grpc::{
        binlogdata::VGtid,
        vtctlservice::vtctld_client::VtctldClient,
        vtgate::{VStreamRequest, VStreamResponse},
        vtgateservice::vitess_client::VitessClient,
    },
    vitess_schema::TableName,
    vitess_shards::{KeyspaceName, get_current_shard_gtids},
};

#[derive(Debug)]
#[non_exhaustive]
pub struct VstreamListenerError {
    pub keyspace: Box<KeyspaceName>,
    pub kind: VstreamListenerErrorKind,
}

impl Display for VstreamListenerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "error while streaming changes from vitess `{}`",
            self.keyspace
        )
    }
}

impl Error for VstreamListenerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            VstreamListenerErrorKind::VitessClientCallFailed(e) => Some(e),
            VstreamListenerErrorKind::SendFailed(e) => Some(e),
        }
    }
}

#[derive(Debug)]
pub enum VstreamListenerErrorKind {
    VitessClientCallFailed(tonic::Status),
    SendFailed(SendError<ReplicationRowEventEnvelope>),
}

pub(crate) async fn start_vitess_vstream_listener(
    mut vtctld_client: VtctldClient<tonic::transport::Channel>,
    mut vtgate_client: VitessClient<tonic::transport::Channel>,
    keyspace: KeyspaceName,
    outgoing_row_changes: Sender<ReplicationRowEventEnvelope>,
) -> Result<(), VstreamListenerError> {
    let shard_gtids = get_current_shard_gtids(&mut vtctld_client, &keyspace)
        .await
        .map_err(|e| VstreamListenerError {
            keyspace: Box::new(keyspace.clone()),
            kind: VstreamListenerErrorKind::VitessClientCallFailed(e),
        })?;
    let request = tonic::Request::new(VStreamRequest {
        caller_id: None,
        vgtid: Some(VGtid { shard_gtids }),
        filter: None,
        tablet_type: 0,
        flags: None,
    });

    let response = vtgate_client
        .v_stream(request)
        .await
        .map_err(|e| VstreamListenerError {
            keyspace: Box::new(keyspace.clone()),
            kind: VstreamListenerErrorKind::VitessClientCallFailed(e),
        })?;
    let mut stream = response.into_inner();
    process_stream(&keyspace, &mut stream, outgoing_row_changes).await?;

    Ok(())
}

async fn process_stream(
    keyspace: &KeyspaceName,
    stream: &mut Streaming<VStreamResponse>,
    outgoing_row_changes: Sender<ReplicationRowEventEnvelope>,
) -> Result<(), VstreamListenerError> {
    while let Some(message_result) = stream.next().await {
        let message = message_result.map_err(|e| VstreamListenerError {
            keyspace: Box::new(keyspace.clone()),
            kind: VstreamListenerErrorKind::VitessClientCallFailed(e),
        })?;

        for event in message.events {
            if let Some(row_event) = event.row_event {
                let keyspace: KeyspaceName = row_event.keyspace.into();
                let table: TableName = row_event.table_name.into();
                for row_change in row_event.row_changes {
                    outgoing_row_changes
                        .send(ReplicationRowEventEnvelope {
                            keyspace: keyspace.clone(),
                            table: table.clone(),
                            event: row_change.into(),
                        })
                        .map_err(|e| VstreamListenerError {
                            keyspace: Box::new(keyspace.clone()),
                            kind: VstreamListenerErrorKind::SendFailed(e),
                        })?;
                }
            }
        }
    }

    Ok(())
}
