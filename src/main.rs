mod command_line_args;
mod console_stream_producer;
mod replication_row_event;
mod table_row_change_json_converter;
mod table_row_deserializer;
mod vitess_clients;
mod vitess_grpc;
mod vitess_schema;
mod vitess_shards;
mod vitess_snapshot;
mod vitess_vstream_listener;

use std::sync::mpsc;

use tokio::select;

use crate::console_stream_producer::run_console_stream_producer;
use crate::table_row_change_json_converter::start_row_change_json_converter;
use crate::vitess_clients::{create_vtctld_client, create_vtgate_client};
use crate::vitess_schema::{TableName, get_schema_for_tables};
use crate::vitess_shards::KeyspaceName;
use crate::vitess_vstream_listener::start_vitess_vstream_listener;
use clap::Parser;

use env_logger;
use log;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    log::info!("Starting vitess replicator...");

    let args = command_line_args::Args::parse();
    log::info!("Connecting to vtctld...");
    let mut vtctld_client = create_vtctld_client(&args).await?;

    log::info!("Connecting to vtgate...");
    let vtgate_client = create_vtgate_client(&args).await?;

    let keyspace: KeyspaceName = args.keyspace.into();

    let (outgoing_row_changes, incoming_row_changes) = mpsc::channel();
    let (outgoing_json, incoming_json) = mpsc::channel();

    let tables_to_replicate: Vec<TableName> =
        args.tables.into_iter().map(TableName::from).collect();

    log::info!(
        "Tables to replicate: {:?}; fetching schemas...",
        tables_to_replicate
    );
    let schemas = get_schema_for_tables(
        &mut vtctld_client,
        &keyspace,
        tables_to_replicate.as_slice(),
    )
    .await?;
    log::info!("Schemas = {:?}", schemas);

    log::info!("Starting vstream listener...");
    let vstream_listener_handle = tokio::task::spawn(start_vitess_vstream_listener(
        vtctld_client,
        vtgate_client,
        keyspace,
        outgoing_row_changes,
    ));

    let row_change_converter_thread = tokio::task::spawn_blocking(move || {
        start_row_change_json_converter(incoming_row_changes, outgoing_json, schemas.clone())
    });

    let console_producer_thread =
        tokio::task::spawn_blocking(move || run_console_stream_producer(incoming_json));

    select! {
        _ = vstream_listener_handle => (),
        _ = row_change_converter_thread => (),
        _ = console_producer_thread => (),
    }

    Ok(())
}
