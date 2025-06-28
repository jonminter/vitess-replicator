use std::fmt::Display;

use crate::vitess_grpc::{
    binlogdata::ShardGtid,
    topodata::{Tablet, TabletType},
    vtctldata::{FindAllShardsInKeyspaceRequest, GetTabletsRequest},
    vtctlservice::vtctld_client::VtctldClient,
};

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub(crate) struct KeyspaceName(String);
impl KeyspaceName {
    fn to_string(&self) -> String {
        self.0.clone()
    }
}
impl From<String> for KeyspaceName {
    fn from(value: String) -> Self {
        KeyspaceName(value)
    }
}
impl Into<String> for KeyspaceName {
    fn into(self) -> String {
        self.0
    }
}
impl Display for KeyspaceName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

pub(crate) async fn get_current_shard_gtids(
    vtctld_client: &mut VtctldClient<tonic::transport::Channel>,
    keyspace: &KeyspaceName,
) -> Result<Vec<ShardGtid>, tonic::Status> {
    let keyspace_response = vtctld_client
        .find_all_shards_in_keyspace(tonic::Request::new(FindAllShardsInKeyspaceRequest {
            keyspace: keyspace.to_string(),
        }))
        .await?;

    let all_shards = keyspace_response.into_inner();
    let mut shard_gtids: Vec<ShardGtid> = Vec::new();
    for (shard_id, _) in all_shards.shards.iter() {
        shard_gtids.push(ShardGtid {
            keyspace: keyspace.to_string(),
            shard: shard_id.clone(),
            gtid: "current".to_string(),
            table_p_ks: Vec::new(),
        });
    }
    Ok(shard_gtids)
}

pub(crate) async fn get_all_shard_names<'a>(
    vtctld_client: &'a mut VtctldClient<tonic::transport::Channel>,
    keyspace: &KeyspaceName,
) -> Result<Vec<String>, tonic::Status> {
    let keyspace_response = vtctld_client
        .find_all_shards_in_keyspace(tonic::Request::new(FindAllShardsInKeyspaceRequest {
            keyspace: keyspace.to_string(),
        }))
        .await?;

    let all_shards = keyspace_response.into_inner();
    Ok(all_shards
        .shards
        .iter()
        .map(|(shard_id, _)| shard_id.to_string())
        .collect())
}

pub(crate) async fn get_first_primary_tablet_for_first_shard<'a>(
    vtctld_client: &'a mut VtctldClient<tonic::transport::Channel>,
    keyspace: &KeyspaceName,
) -> Result<Tablet, tonic::Status> {
    let shard_ids = get_all_shard_names(vtctld_client, keyspace).await?;

    let tablets_response = vtctld_client
        .get_tablets(tonic::Request::new(GetTabletsRequest {
            keyspace: keyspace.to_string(),
            shard: shard_ids
                .get(0)
                .expect("There should be at least one shard!")
                .to_string(),
            cells: vec![],
            strict: false,
            tablet_aliases: vec![],
            tablet_type: TabletType::Primary.into(),
        }))
        .await?;

    let tablets = tablets_response.into_inner();
    Ok(tablets
        .tablets
        .iter()
        .next()
        .expect("There should be at least one primary tablet")
        .clone())
}
