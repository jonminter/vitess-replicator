use clap::{Parser, arg, command};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub(crate) struct Args {
    #[arg(long)]
    pub(crate) keyspace: String,

    #[arg(long)]
    pub(crate) vtctld_endpoint: String,

    #[arg(long)]
    pub(crate) vtgate_endpoint: String,

    #[arg(long)]
    pub(crate) tables: Vec<String>,
}
