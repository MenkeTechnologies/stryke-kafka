//! `stryke-kafka-helper` — bridge binary for the stryke `kafka` package.
//!
//! Subcommands cover producer, consumer, and admin paths against any
//! Kafka 0.10+ cluster reachable via the configured bootstrap brokers.
//! Output is NDJSON (streams) or JSON (single objects).

use anyhow::Result;
use clap::{Args, Parser, Subcommand};

mod admin;
mod common;
mod consume;
mod produce;

use crate::common::KafkaConn;

#[derive(Parser, Debug)]
#[command(
    name = "stryke-kafka-helper",
    version,
    about = "Apache Kafka bridge for the stryke `kafka` package"
)]
struct Cli {
    #[command(flatten)]
    conn: KafkaConn,

    #[command(subcommand)]
    cmd: Top,
}

#[derive(Subcommand, Debug)]
enum Top {
    /// Produce — send / stream messages.
    #[command(subcommand)]
    Produce(produce::ProduceCmd),

    /// Consume — pull messages as NDJSON.
    Consume(ConsumeFlat),

    /// Admin / metadata.
    #[command(subcommand)]
    Admin(admin::AdminCmd),

    // Convenience aliases at the top level so users get a flatter CLI.
    /// Alias: `admin topics`.
    Topics,
    /// Alias: `admin groups`.
    Groups,
    /// Alias: `admin cluster`.
    Cluster,
    /// Alias: `admin ping`.
    Ping,
    /// Alias: `admin lag`.
    Lag {
        #[arg(long, short = 'g')]
        group: String,
        #[arg(long, short = 't')]
        topic: Option<String>,
    },
}

/// Flat shape so users write `kafka consume TOPIC --group=...` without an
/// extra subcommand layer.
#[derive(Args, Debug)]
struct ConsumeFlat {
    #[command(flatten)]
    args: consume::ConsumeArgs,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let cli = Cli::parse();
    if let Err(e) = run(cli).await {
        eprintln!("stryke-kafka-helper: {e:#}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<()> {
    let conn = cli.conn;
    match cli.cmd {
        Top::Produce(c) => produce::dispatch(&conn, c).await,
        Top::Consume(c) => consume::run(&conn, c.args).await,
        Top::Admin(c) => admin::dispatch(&conn, c).await,
        Top::Topics => admin::dispatch(&conn, admin::AdminCmd::Topics).await,
        Top::Groups => admin::dispatch(&conn, admin::AdminCmd::Groups).await,
        Top::Cluster => admin::dispatch(&conn, admin::AdminCmd::Cluster).await,
        Top::Ping => admin::dispatch(&conn, admin::AdminCmd::Ping).await,
        Top::Lag { group, topic } => {
            admin::dispatch(&conn, admin::AdminCmd::Lag { group, topic }).await
        }
    }
}
