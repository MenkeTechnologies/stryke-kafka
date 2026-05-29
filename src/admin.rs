//! Admin / metadata / lag commands.

use std::io::{self, BufWriter};
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Subcommand;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer, DefaultConsumerContext};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use serde_json::json;

use crate::common::{emit_json, emit_ndjson_line, KafkaConn};

#[derive(Subcommand, Debug)]
pub enum AdminCmd {
    /// List all topics in the cluster.
    Topics,
    /// Detailed info for one topic (partitions, replicas, leaders).
    Describe { topic: String },
    /// List consumer groups.
    Groups,
    /// Cluster info: brokers, controller.
    Cluster,
    /// Compute consumer-group lag for a group across one or all of its
    /// assigned topics.
    Lag {
        #[arg(long, short = 'g')]
        group: String,
        #[arg(long, short = 't')]
        topic: Option<String>,
    },
    /// Create a topic.
    CreateTopic {
        name: String,
        #[arg(long, default_value_t = 1)]
        partitions: i32,
        #[arg(long, default_value_t = 1)]
        replication: i32,
        /// `K=V` config entries (e.g. `cleanup.policy=compact`). Repeatable.
        #[arg(long = "conf", short = 'c', value_name = "K=V")]
        configs: Vec<String>,
    },
    /// Delete a topic.
    DeleteTopic { name: String },
    /// `SELECT 1` for Kafka — fetch cluster metadata, exit 0 on success.
    Ping,
}

pub async fn dispatch(conn: &KafkaConn, cmd: AdminCmd) -> Result<()> {
    match cmd {
        AdminCmd::Topics => topics(conn),
        AdminCmd::Describe { topic } => describe(conn, &topic),
        AdminCmd::Groups => groups(conn),
        AdminCmd::Cluster => cluster(conn),
        AdminCmd::Lag { group, topic } => lag(conn, &group, topic.as_deref()),
        AdminCmd::CreateTopic {
            name,
            partitions,
            replication,
            configs,
        } => create_topic(conn, &name, partitions, replication, &configs).await,
        AdminCmd::DeleteTopic { name } => delete_topic(conn, &name).await,
        AdminCmd::Ping => ping(conn),
    }
}

fn metadata_consumer(conn: &KafkaConn) -> Result<BaseConsumer<DefaultConsumerContext>> {
    let cfg = conn.build_config()?;
    cfg.create().context("create metadata consumer")
}

fn topics(conn: &KafkaConn) -> Result<()> {
    let consumer = metadata_consumer(conn)?;
    let md = consumer
        .fetch_metadata(None, conn.timeout())
        .context("fetch_metadata")?;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    for t in md.topics() {
        emit_ndjson_line(
            &mut out,
            &json!({
                "name": t.name(),
                "partitions": t.partitions().len(),
                "error": t.error().map(|e| format!("{e:?}")),
            }),
        )?;
    }
    Ok(())
}

fn describe(conn: &KafkaConn, topic: &str) -> Result<()> {
    let consumer = metadata_consumer(conn)?;
    let md = consumer
        .fetch_metadata(Some(topic), conn.timeout())
        .context("fetch_metadata")?;
    let t = md
        .topics()
        .iter()
        .find(|t| t.name() == topic)
        .ok_or_else(|| anyhow::anyhow!("topic `{topic}` not found"))?;
    let parts: Vec<_> = t
        .partitions()
        .iter()
        .map(|p| {
            json!({
                "id": p.id(),
                "leader": p.leader(),
                "replicas": p.replicas(),
                "isr": p.isr(),
                "error": p.error().map(|e| format!("{e:?}")),
            })
        })
        .collect();
    emit_json(&json!({
        "name": t.name(),
        "partition_count": parts.len(),
        "partitions": parts,
    }))
}

fn groups(conn: &KafkaConn) -> Result<()> {
    let consumer = metadata_consumer(conn)?;
    let list = consumer
        .fetch_group_list(None, conn.timeout())
        .context("fetch_group_list")?;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    for g in list.groups() {
        emit_ndjson_line(
            &mut out,
            &json!({
                "name": g.name(),
                "state": g.state(),
                "protocol": g.protocol(),
                "protocol_type": g.protocol_type(),
                "members": g.members().len(),
            }),
        )?;
    }
    Ok(())
}

fn cluster(conn: &KafkaConn) -> Result<()> {
    let consumer = metadata_consumer(conn)?;
    let md = consumer
        .fetch_metadata(None, conn.timeout())
        .context("fetch_metadata")?;
    let brokers: Vec<_> = md
        .brokers()
        .iter()
        .map(|b| {
            json!({
                "id": b.id(),
                "host": b.host(),
                "port": b.port(),
            })
        })
        .collect();
    emit_json(&json!({
        "broker_count": brokers.len(),
        "controller_id": md.orig_broker_id(),
        "brokers": brokers,
        "topic_count": md.topics().len(),
    }))
}

fn lag(conn: &KafkaConn, group: &str, topic_filter: Option<&str>) -> Result<()> {
    let mut cfg = conn.build_config()?;
    cfg.set("group.id", group)
        .set("enable.auto.commit", "false");
    let consumer: BaseConsumer<DefaultConsumerContext> =
        cfg.create().context("create consumer for lag")?;

    let md = consumer
        .fetch_metadata(topic_filter, conn.timeout())
        .context("fetch_metadata")?;

    // Build the TPL covering every (topic, partition) we care about.
    let mut tpl = TopicPartitionList::new();
    for t in md.topics() {
        for p in t.partitions() {
            tpl.add_partition(t.name(), p.id());
        }
    }
    // Skip the call when nothing matched (would return error otherwise).
    if tpl.count() == 0 {
        return emit_json(&json!({ "group": group, "topics": [] }));
    }

    let committed = consumer
        .committed_offsets(tpl, conn.timeout())
        .context("committed_offsets")?;

    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    let mut grand_total: i64 = 0;
    for entry in committed.elements() {
        let topic = entry.topic();
        let partition = entry.partition();
        let committed_offset = match entry.offset() {
            Offset::Offset(n) => n,
            _ => -1,
        };
        let (low, high) = consumer
            .fetch_watermarks(topic, partition, conn.timeout())
            .context("fetch_watermarks")?;
        let lag = if committed_offset < 0 {
            high - low
        } else {
            (high - committed_offset).max(0)
        };
        grand_total += lag;
        emit_ndjson_line(
            &mut out,
            &json!({
                "group": group,
                "topic": topic,
                "partition": partition,
                "committed": committed_offset,
                "low_watermark": low,
                "high_watermark": high,
                "lag": lag,
            }),
        )?;
    }
    emit_ndjson_line(
        &mut out,
        &json!({ "group": group, "total_lag": grand_total }),
    )?;
    Ok(())
}

async fn create_topic(
    conn: &KafkaConn,
    name: &str,
    partitions: i32,
    replication: i32,
    configs: &[String],
) -> Result<()> {
    let admin: AdminClient<DefaultClientContext> = conn
        .build_config()?
        .create()
        .context("create AdminClient")?;
    let mut new_topic = NewTopic::new(name, partitions, TopicReplication::Fixed(replication));
    for kv in configs {
        if let Some((k, v)) = kv.split_once('=') {
            new_topic = new_topic.set(k, v);
        }
    }
    let opts = AdminOptions::new().request_timeout(Some(conn.timeout()));
    let results = admin
        .create_topics(std::iter::once(&new_topic), &opts)
        .await
        .context("create_topics")?;
    for r in results {
        match r {
            Ok(t) => emit_json(&json!({ "name": t, "created": true }))?,
            Err((t, e)) => {
                emit_json(&json!({ "name": t, "created": false, "error": format!("{e:?}") }))?
            }
        }
    }
    Ok(())
}

async fn delete_topic(conn: &KafkaConn, name: &str) -> Result<()> {
    let admin: AdminClient<DefaultClientContext> = conn
        .build_config()?
        .create()
        .context("create AdminClient")?;
    let opts = AdminOptions::new().request_timeout(Some(conn.timeout()));
    let results = admin
        .delete_topics(&[name], &opts)
        .await
        .context("delete_topics")?;
    for r in results {
        match r {
            Ok(t) => emit_json(&json!({ "name": t, "deleted": true }))?,
            Err((t, e)) => {
                emit_json(&json!({ "name": t, "deleted": false, "error": format!("{e:?}") }))?
            }
        }
    }
    Ok(())
}

fn ping(conn: &KafkaConn) -> Result<()> {
    let consumer = metadata_consumer(conn)?;
    let md = consumer
        .fetch_metadata(None, conn.timeout())
        .context("fetch_metadata")?;
    println!(
        "ok brokers={} topics={}",
        md.brokers().len(),
        md.topics().len()
    );
    Ok(())
}

/// Quiet a `time::Duration` unused-import lint if it leaks through.
#[allow(dead_code)]
fn _force_duration_link() -> Duration {
    Duration::from_secs(0)
}
