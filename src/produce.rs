//! Producer commands.

use std::io::{self, BufRead, BufReader};
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Subcommand;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json::json;

use crate::common::{emit_json, KafkaConn};

#[derive(Subcommand, Debug)]
pub enum ProduceCmd {
    /// Send a single message.
    Send {
        topic: String,
        #[arg(long)]
        value: String,
        #[arg(long)]
        key: Option<String>,
        /// Repeatable `--header k=v`.
        #[arg(long = "header", value_name = "K=V")]
        headers: Vec<String>,
        /// Partition (default: librdkafka chooses by key/round-robin).
        #[arg(long)]
        partition: Option<i32>,
    },
    /// Read NDJSON from stdin, publish each line as a message.
    /// Line shape: `{ "topic": "t", "value": "...", "key": "...",
    ///                "headers": {k:v,...}, "partition": N }`. `topic`
    /// defaults to --default-topic.
    Stream {
        #[arg(long)]
        default_topic: Option<String>,
        /// Flush ack timeout per batch (ms).
        #[arg(long, default_value_t = 10_000)]
        ack_timeout_ms: u64,
        /// Stop after N messages (for testing).
        #[arg(long)]
        limit: Option<usize>,
    },
}

pub async fn dispatch(conn: &KafkaConn, cmd: ProduceCmd) -> Result<()> {
    let producer: FutureProducer = conn
        .build_config()?
        .set("message.timeout.ms", conn.timeout_ms.to_string())
        .create()
        .context("creating FutureProducer")?;

    match cmd {
        ProduceCmd::Send { topic, value, key, headers, partition } => {
            send_one(&producer, &topic, &value, key.as_deref(), &headers, partition, conn.timeout()).await
        }
        ProduceCmd::Stream { default_topic, ack_timeout_ms, limit } => {
            stream(&producer, default_topic.as_deref(), Duration::from_millis(ack_timeout_ms), limit).await
        }
    }
}

fn parse_headers(kvs: &[String]) -> OwnedHeaders {
    let mut headers = OwnedHeaders::new();
    for kv in kvs {
        if let Some((k, v)) = kv.split_once('=') {
            headers = headers.insert(Header {
                key: k,
                value: Some(v.as_bytes()),
            });
        }
    }
    headers
}

async fn send_one(
    producer: &FutureProducer,
    topic: &str,
    value: &str,
    key: Option<&str>,
    headers: &[String],
    partition: Option<i32>,
    timeout: Duration,
) -> Result<()> {
    let owned_headers = parse_headers(headers);
    let mut record = FutureRecord::to(topic).payload(value);
    if let Some(k) = key {
        record = record.key(k);
    }
    if let Some(p) = partition {
        record = record.partition(p);
    }
    if headers.iter().any(|h| h.contains('=')) {
        record = record.headers(owned_headers);
    }
    let delivery = producer
        .send(record, timeout)
        .await
        .map_err(|(e, _)| anyhow::anyhow!("send: {e}"))?;
    emit_json(&json!({
        "topic": topic,
        "partition": delivery.partition,
        "offset": delivery.offset,
    }))
}

async fn stream(
    producer: &FutureProducer,
    default_topic: Option<&str>,
    ack_timeout: Duration,
    limit: Option<usize>,
) -> Result<()> {
    let stdin = io::stdin();
    let reader = BufReader::new(stdin.lock());

    let mut pending: Vec<(String, i32, i64)> = Vec::new();
    let mut sent: usize = 0;

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let v: serde_json::Value = serde_json::from_str(&line)
            .with_context(|| format!("parsing NDJSON line: {line}"))?;
        let topic = v
            .get("topic")
            .and_then(|x| x.as_str())
            .map(String::from)
            .or_else(|| default_topic.map(String::from))
            .ok_or_else(|| anyhow::anyhow!("each line needs `topic` or pass --default-topic"))?;
        let value = v
            .get("value")
            .and_then(|x| x.as_str())
            .ok_or_else(|| anyhow::anyhow!("each line needs `value`"))?
            .to_string();
        let key = v.get("key").and_then(|x| x.as_str()).map(String::from);
        let partition = v
            .get("partition")
            .and_then(|x| x.as_i64())
            .map(|i| i as i32);

        let mut headers = OwnedHeaders::new();
        if let Some(h) = v.get("headers").and_then(|x| x.as_object()) {
            for (k, val) in h {
                let s = val.as_str().map(String::from).unwrap_or_default();
                headers = headers.insert(Header {
                    key: k,
                    value: Some(s.as_bytes()),
                });
            }
        }

        let value_owned = value;
        let key_owned = key.unwrap_or_default();
        let topic_owned = topic.clone();
        let mut record: FutureRecord<'_, str, str> = FutureRecord::to(&topic_owned)
            .payload(value_owned.as_str())
            .headers(headers);
        if !key_owned.is_empty() {
            record = record.key(key_owned.as_str());
        }
        if let Some(p) = partition {
            record = record.partition(p);
        }
        let delivery = producer
            .send(record, ack_timeout)
            .await
            .map_err(|(e, _)| anyhow::anyhow!("send: {e}"))?;
        pending.push((topic_owned, delivery.partition, delivery.offset));
        sent += 1;
        if limit.is_some_and(|l| sent >= l) {
            break;
        }
    }
    emit_json(&json!({
        "sent": sent,
        "last": pending.last().map(|(t, p, o)| json!({"topic": t, "partition": p, "offset": o})),
    }))
}
