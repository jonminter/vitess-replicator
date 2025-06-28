use crate::{
    vitess_grpc::{binlogdata::RowChange, query::Row},
    vitess_schema::TableName,
    vitess_shards::KeyspaceName,
};

pub(crate) enum ReplicationRowEvent {
    Insert(Row),
    SnapshotRead(Row),
    Update { before: Row, after: Row },
    Delete(Row),
}

impl From<RowChange> for ReplicationRowEvent {
    fn from(value: RowChange) -> Self {
        match (value.before, value.after) {
            (Some(before), Some(after)) => ReplicationRowEvent::Update { before, after },
            (None, Some(new_row)) => ReplicationRowEvent::Insert(new_row),
            (Some(before_delete), None) => ReplicationRowEvent::Delete(before_delete),
            _ => unreachable!(
                "Should be one of update, insert, delete. Does not have either before or after!"
            ),
        }
    }
}

pub(crate) struct ReplicationRowEventEnvelope {
    pub(crate) keyspace: KeyspaceName,
    pub(crate) table: TableName,
    pub(crate) event: ReplicationRowEvent,
}
