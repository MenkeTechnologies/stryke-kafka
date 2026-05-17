//! Consumer commands.

use std::io::{self, BufWriter};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use clap::Args;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, Consumer, DefaultConsumerContext};
use rdkafka::message::{Headers, Message};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use serde_json::{json, Map as JMap, Value};

use crate::common::{emit_ndjson_line, KafkaConn};

#[derive(Args, Debug)]
pub struct ConsumeArgs {
    /// Comma-separated topics (you must pass at least one).
    pub topics: String,

    /// Consumer group ID. Defaults to a one-shot UUID so each invocation
    /// starts fresh.
    #[arg(long)]
    pub group: Option<String>,

    /// `earliest` or `latest`. Default: `latest`.
    #[arg(long, default_value = "latest")]
    pub offset_reset: String,

    /// Stop after this many messages.
    #[arg(long)]
    pub max: Option<usize>,

    /// Stop after this many milliseconds of idle (no new messages).
    #[arg(long, default_value_t = 5_000)]
    pub idle_ms: u64,

    /// Decode message values as text (`text`, default), `binary`
    /// (base64 string), or `json` (attempt to parse as JSON).
    #[arg(long, default_value = "text")]
    pub value_mode: String,

    /// Auto-commit offsets after each message (default: false — read-only).
    #[arg(long)]
    pub commit: bool,
}

pub async fn run(conn: &KafkaConn, args: ConsumeArgs) -> Result<()> {
    let group = args
        .group
        .clone()
        .unwrap_or_else(|| format!("stryke-kafka-{}", uuid_like()));

    let mut cfg = conn.build_config()?;
    cfg.set("group.id", &group)
        .set("auto.offset.reset", &args.offset_reset)
        .set("enable.auto.commit", if args.commit { "true" } else { "false" })
        .set("enable.partition.eof", "false")
        .set_log_level(RDKafkaLogLevel::Warning);

    let consumer: BaseConsumer<DefaultConsumerContext> = cfg.create().context("create consumer")?;

    let topics: Vec<&str> = args.topics.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()).collect();
    if topics.is_empty() {
        anyhow::bail!("at least one topic required");
    }
    consumer.subscribe(&topics).context("subscribe")?;

    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());

    let mut count: usize = 0;
    let idle = Duration::from_millis(args.idle_ms);
    let mut last_received = Instant::now();

    loop {
        match consumer.poll(Duration::from_millis(500)) {
            None => {
                if last_received.elapsed() >= idle {
                    break;
                }
            }
            Some(Err(e)) => {
                eprintln!("stryke-kafka-helper: poll error: {e}");
            }
            Some(Ok(msg)) => {
                last_received = Instant::now();
                let value = msg.payload();
                let value_json = match args.value_mode.as_str() {
                    "binary" => match value {
                        Some(b) => Value::String(format!("base64:{}", B64.encode(b))),
                        None => Value::Null,
                    },
                    "json" => match value {
                        Some(b) => serde_json::from_slice(b).unwrap_or_else(|_| {
                            Value::String(String::from_utf8_lossy(b).to_string())
                        }),
                        None => Value::Null,
                    },
                    _ /* text */ => match value {
                        Some(b) => Value::String(String::from_utf8_lossy(b).to_string()),
                        None => Value::Null,
                    },
                };
                let key = msg.key().map(|k| String::from_utf8_lossy(k).to_string());
                let mut headers_obj = JMap::new();
                if let Some(h) = msg.headers() {
                    for hdr in h.iter() {
                        let v = hdr
                            .value
                            .map(|b| String::from_utf8_lossy(b).to_string())
                            .unwrap_or_default();
                        headers_obj.insert(hdr.key.to_string(), Value::String(v));
                    }
                }
                emit_ndjson_line(
                    &mut out,
                    &json!({
                        "topic": msg.topic(),
                        "partition": msg.partition(),
                        "offset": msg.offset(),
                        "timestamp": msg.timestamp().to_millis(),
                        "key": key,
                        "value": value_json,
                        "headers": headers_obj,
                    }),
                )?;
                count += 1;
                if args.commit {
                    let mut tpl = TopicPartitionList::new();
                    tpl.add_partition_offset(msg.topic(), msg.partition(), Offset::Offset(msg.offset() + 1))?;
                    consumer.commit(&tpl, rdkafka::consumer::CommitMode::Async).ok();
                }
                if args.max.is_some_and(|l| count >= l) {
                    break;
                }
            }
        }
    }
    Ok(())
}

fn uuid_like() -> String {
    // Cheap unique tag without pulling in the `uuid` crate.
    use std::time::SystemTime;
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{:x}", nanos)
}
