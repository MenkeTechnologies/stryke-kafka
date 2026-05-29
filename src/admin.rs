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

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    /// Wrap AdminCmd in a top-level parser so we can exercise clap's
    /// argument-binding rules from tests without standing up the full
    /// `stryke-kafka-helper` binary.
    #[derive(Parser, Debug)]
    struct TestCli {
        #[command(subcommand)]
        cmd: AdminCmd,
    }

    fn parse(args: &[&str]) -> Result<AdminCmd, clap::Error> {
        let mut argv = vec!["stryke-kafka-helper"];
        argv.extend_from_slice(args);
        TestCli::try_parse_from(argv).map(|c| c.cmd)
    }

    // ─── enum parsing ───────────────────────────────────────────────────

    #[test]
    fn topics_subcommand_parses() {
        let cmd = parse(&["topics"]).expect("parse");
        assert!(matches!(cmd, AdminCmd::Topics));
    }

    #[test]
    fn describe_subcommand_takes_one_positional_topic() {
        let cmd = parse(&["describe", "my-topic"]).expect("parse");
        match cmd {
            AdminCmd::Describe { topic } => assert_eq!(topic, "my-topic"),
            other => panic!("expected Describe, got {other:?}"),
        }
    }

    #[test]
    fn describe_subcommand_requires_topic() {
        let err = parse(&["describe"]).expect_err("missing topic must error");
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn groups_subcommand_parses() {
        let cmd = parse(&["groups"]).expect("parse");
        assert!(matches!(cmd, AdminCmd::Groups));
    }

    #[test]
    fn cluster_subcommand_parses() {
        let cmd = parse(&["cluster"]).expect("parse");
        assert!(matches!(cmd, AdminCmd::Cluster));
    }

    #[test]
    fn ping_subcommand_parses() {
        let cmd = parse(&["ping"]).expect("parse");
        assert!(matches!(cmd, AdminCmd::Ping));
    }

    // ─── lag flags ──────────────────────────────────────────────────────

    #[test]
    fn lag_requires_group_via_long_and_short_flags() {
        let long = parse(&["lag", "--group", "g1"]).expect("--group");
        let short = parse(&["lag", "-g", "g1"]).expect("-g");
        for cmd in [long, short] {
            match cmd {
                AdminCmd::Lag { group, topic } => {
                    assert_eq!(group, "g1");
                    assert!(topic.is_none(), "topic filter defaults to None");
                }
                other => panic!("expected Lag, got {other:?}"),
            }
        }
    }

    #[test]
    fn lag_topic_filter_is_optional() {
        let cmd = parse(&["lag", "-g", "g1", "-t", "orders"]).expect("parse");
        match cmd {
            AdminCmd::Lag { group, topic } => {
                assert_eq!(group, "g1");
                assert_eq!(topic.as_deref(), Some("orders"));
            }
            _ => panic!("expected Lag"),
        }
    }

    #[test]
    fn lag_without_group_fails() {
        let err = parse(&["lag"]).expect_err("must require --group");
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
    }

    // ─── create-topic defaults + repeatable --conf ─────────────────────

    #[test]
    fn create_topic_defaults_match_documented_invariants() {
        let cmd = parse(&["create-topic", "my-topic"]).expect("parse");
        match cmd {
            AdminCmd::CreateTopic {
                name,
                partitions,
                replication,
                configs,
            } => {
                assert_eq!(name, "my-topic");
                // Documented defaults: partitions = 1, replication = 1.
                // Bumping either silently would land a regression where
                // production topics get under-replicated by accident.
                assert_eq!(partitions, 1, "partitions default must be 1");
                assert_eq!(replication, 1, "replication default must be 1");
                assert!(configs.is_empty(), "configs default must be empty");
            }
            _ => panic!("expected CreateTopic"),
        }
    }

    #[test]
    fn create_topic_accepts_partitions_and_replication_flags() {
        let cmd = parse(&[
            "create-topic",
            "t",
            "--partitions",
            "12",
            "--replication",
            "3",
        ])
        .expect("parse");
        match cmd {
            AdminCmd::CreateTopic {
                partitions,
                replication,
                ..
            } => {
                assert_eq!(partitions, 12);
                assert_eq!(replication, 3);
            }
            _ => panic!("expected CreateTopic"),
        }
    }

    #[test]
    fn create_topic_conf_flag_is_repeatable() {
        // Pin the contract that --conf can appear multiple times — needed
        // so users can set `cleanup.policy=compact` AND `retention.ms=…`
        // in one invocation.
        let cmd = parse(&[
            "create-topic",
            "t",
            "--conf",
            "cleanup.policy=compact",
            "-c",
            "retention.ms=86400000",
            "--conf",
            "min.insync.replicas=2",
        ])
        .expect("parse");
        match cmd {
            AdminCmd::CreateTopic { configs, .. } => {
                assert_eq!(configs.len(), 3);
                assert!(configs.iter().any(|c| c == "cleanup.policy=compact"));
                assert!(configs.iter().any(|c| c == "retention.ms=86400000"));
                assert!(configs.iter().any(|c| c == "min.insync.replicas=2"));
            }
            _ => panic!("expected CreateTopic"),
        }
    }

    #[test]
    fn create_topic_requires_name() {
        let err = parse(&["create-topic"]).expect_err("must require name");
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
    }

    // ─── delete-topic ────────────────────────────────────────────────────

    #[test]
    fn delete_topic_takes_one_positional_name() {
        let cmd = parse(&["delete-topic", "t"]).expect("parse");
        match cmd {
            AdminCmd::DeleteTopic { name } => assert_eq!(name, "t"),
            _ => panic!("expected DeleteTopic"),
        }
    }

    #[test]
    fn delete_topic_requires_name() {
        let err = parse(&["delete-topic"]).expect_err("must require name");
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
    }

    // ─── _force_duration_link is a side-effect-free linker shim ─────────

    #[test]
    fn force_duration_link_returns_zero_duration() {
        // Pin the contract that the shim returns a benign value so a
        // future refactor doesn't accidentally turn it into a real call.
        assert_eq!(_force_duration_link(), Duration::from_secs(0));
    }
}
