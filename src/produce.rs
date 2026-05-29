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
        ProduceCmd::Send {
            topic,
            value,
            key,
            headers,
            partition,
        } => {
            send_one(
                &producer,
                &topic,
                &value,
                key.as_deref(),
                &headers,
                partition,
                conn.timeout(),
            )
            .await
        }
        ProduceCmd::Stream {
            default_topic,
            ack_timeout_ms,
            limit,
        } => {
            stream(
                &producer,
                default_topic.as_deref(),
                Duration::from_millis(ack_timeout_ms),
                limit,
            )
            .await
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
        let v: serde_json::Value =
            serde_json::from_str(&line).with_context(|| format!("parsing NDJSON line: {line}"))?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::message::Headers;

    #[test]
    fn parse_headers_empty_input_yields_empty() {
        let h = parse_headers(&[]);
        assert_eq!(h.count(), 0);
    }

    #[test]
    fn parse_headers_pairs_inserted_in_order() {
        let h = parse_headers(&["a=1".into(), "b=two".into(), "c=3".into()]);
        assert_eq!(h.count(), 3);
        // Iteration order matches insertion in rdkafka's OwnedHeaders.
        let h0 = h.get(0);
        let h1 = h.get(1);
        let h2 = h.get(2);
        assert_eq!(h0.key, "a");
        assert_eq!(h0.value, Some(b"1".as_ref()));
        assert_eq!(h1.key, "b");
        assert_eq!(h1.value, Some(b"two".as_ref()));
        assert_eq!(h2.key, "c");
    }

    #[test]
    fn parse_headers_malformed_silently_dropped() {
        // No '=' → skipped (no error path).
        let h = parse_headers(&["a=1".into(), "no-equals".into(), "b=2".into()]);
        assert_eq!(h.count(), 2);
        assert_eq!(h.get(0).key, "a");
        assert_eq!(h.get(1).key, "b");
    }

    #[test]
    fn parse_headers_value_with_equals_preserved() {
        // First '=' splits — value can contain additional '='.
        let h = parse_headers(&["jwt=a.b=c".into()]);
        assert_eq!(h.count(), 1);
        assert_eq!(h.get(0).key, "jwt");
        assert_eq!(h.get(0).value, Some(b"a.b=c".as_ref()));
    }

    #[test]
    fn parse_headers_empty_value_allowed() {
        let h = parse_headers(&["k=".into()]);
        assert_eq!(h.count(), 1);
        assert_eq!(h.get(0).key, "k");
        assert_eq!(h.get(0).value, Some(b"".as_ref()));
    }

    #[test]
    fn parse_headers_binary_safe_bytes() {
        // Non-ASCII bytes in the value pass through unchanged (UTF-8 here).
        let h = parse_headers(&["msg=héllo".into()]);
        assert_eq!(h.count(), 1);
        assert_eq!(h.get(0).value, Some("héllo".as_bytes()));
    }

    #[test]
    fn parse_headers_duplicate_keys_both_kept() {
        // OwnedHeaders allows duplicate keys (Kafka spec permits it).
        let h = parse_headers(&["k=1".into(), "k=2".into()]);
        assert_eq!(h.count(), 2);
        assert_eq!(h.get(0).value, Some(b"1".as_ref()));
        assert_eq!(h.get(1).value, Some(b"2".as_ref()));
    }

    #[test]
    fn parse_headers_empty_key_allowed() {
        let h = parse_headers(&["=v".into()]);
        assert_eq!(h.count(), 1);
        assert_eq!(h.get(0).key, "");
        assert_eq!(h.get(0).value, Some(b"v".as_ref()));
    }

    #[test]
    fn parse_headers_whitespace_in_value_preserved() {
        let h = parse_headers(&["x= hello ".into()]);
        assert_eq!(h.get(0).value, Some(b" hello ".as_ref()));
    }

    #[test]
    fn parse_headers_tab_in_value() {
        let h = parse_headers(&["k=v\tw".into()]);
        assert_eq!(h.get(0).value, Some(b"v\tw".as_ref()));
    }

    #[test]
    fn parse_headers_single_header_count_one() {
        assert_eq!(parse_headers(&["trace=1".into()]).count(), 1);
    }

    #[test]
    fn parse_headers_many_pairs() {
        let specs: Vec<String> = (0..10).map(|i| format!("h{i}={i}")).collect();
        assert_eq!(parse_headers(&specs).count(), 10);
    }

    #[test]
    fn parse_headers_key_with_dashes() {
        let h = parse_headers(&["x-correlation-id=abc".into()]);
        assert_eq!(h.get(0).key, "x-correlation-id");
    }

    #[test]
    fn parse_headers_utf8_value() {
        let h = parse_headers(&["msg=日本語".into()]);
        assert_eq!(h.get(0).value, Some("日本語".as_bytes()));
    }

    #[test]
    fn parse_headers_only_equals_sign_pair() {
        assert_eq!(parse_headers(&["=".into()]).count(), 1);
    }

    #[test]
    fn parse_headers_value_is_digits() {
        let h = parse_headers(&["code=404".into()]);
        assert_eq!(h.get(0).value, Some(b"404".as_ref()));
    }

    #[test]
    fn parse_headers_long_value() {
        let long = "x".repeat(256);
        let spec = format!("payload={long}");
        let h = parse_headers(&[spec]);
        assert_eq!(h.get(0).value.unwrap().len(), 256);
    }

    #[test]
    fn parse_headers_key_with_underscore() {
        let h = parse_headers(&["trace_id=abc".into()]);
        assert_eq!(h.get(0).key, "trace_id");
    }

    #[test]
    fn parse_headers_three_malformed_one_valid() {
        let h = parse_headers(&["bad".into(), "a=1".into(), "also-bad".into(), "b=2".into()]);
        assert_eq!(h.count(), 2);
    }

    #[test]
    fn parse_headers_value_newline() {
        let h = parse_headers(&["body=a\nb".into()]);
        assert_eq!(h.get(0).value, Some(b"a\nb".as_ref()));
    }

    #[test]
    fn parse_headers_order_preserved_five() {
        let h = parse_headers(&[
            "h1=1".into(),
            "h2=2".into(),
            "h3=3".into(),
            "h4=4".into(),
            "h5=5".into(),
        ]);
        assert_eq!(h.count(), 5);
        assert_eq!(h.get(4).key, "h5");
    }

    #[test]
    fn parse_headers_colon_not_separator() {
        // Only '=' splits; colon stays in value if using '=' form.
        let h = parse_headers(&["k=v:w".into()]);
        assert_eq!(h.get(0).value, Some(b"v:w".as_ref()));
    }

    #[test]
    fn parse_headers_zero_length_value_after_equals() {
        assert_eq!(
            parse_headers(&["k=".into()]).get(0).value,
            Some(b"".as_ref())
        );
    }

    #[test]
    fn parse_headers_binary_null_byte_in_value() {
        let h = parse_headers(&["bin=a\0b".into()]);
        assert_eq!(h.get(0).value, Some(b"a\0b".as_ref()));
    }

    #[test]
    fn parse_headers_key_starts_with_x() {
        let h = parse_headers(&["x-trace=1".into()]);
        assert_eq!(h.get(0).key, "x-trace");
    }

    #[test]
    fn parse_headers_two_valid_one_invalid() {
        assert_eq!(
            parse_headers(&["a=1".into(), "bad".into(), "b=2".into()]).count(),
            2,
        );
    }

    #[test]
    fn parse_headers_value_is_zero() {
        assert_eq!(
            parse_headers(&["n=0".into()]).get(0).value,
            Some(b"0".as_ref())
        );
    }

    #[test]
    fn parse_headers_seven_pairs() {
        let specs: Vec<String> = (0..7).map(|i| format!("h{i}={i}")).collect();
        assert_eq!(parse_headers(&specs).count(), 7);
    }

    #[test]
    fn parse_headers_key_only_equals() {
        assert_eq!(parse_headers(&["=".into()]).get(0).key, "");
    }

    #[test]
    fn parse_headers_value_pipe_char() {
        assert_eq!(
            parse_headers(&["k=a|b".into()]).get(0).value,
            Some(b"a|b".as_ref()),
        );
    }

    #[test]
    fn parse_headers_preserves_key_case() {
        assert_eq!(
            parse_headers(&["X-Request-ID=1".into()]).get(0).key,
            "X-Request-ID"
        );
    }

    #[test]
    fn parse_headers_content_type_json() {
        let h = parse_headers(&["content-type=application/json".into()]);
        assert_eq!(h.get(0).key, "content-type");
    }

    #[test]
    fn parse_headers_eight_pairs() {
        let specs: Vec<String> = (0..8).map(|i| format!("h{i}={i}")).collect();
        assert_eq!(parse_headers(&specs).count(), 8);
    }

    #[test]
    fn parse_headers_value_with_slash() {
        assert_eq!(
            parse_headers(&["path=/a/b".into()]).get(0).value,
            Some(b"/a/b".as_ref()),
        );
    }

    #[test]
    fn parse_headers_key_with_dot() {
        assert_eq!(
            parse_headers(&["meta.version=1".into()]).get(0).key,
            "meta.version"
        );
    }

    #[test]
    fn parse_headers_only_malformed_returns_empty() {
        assert_eq!(parse_headers(&["bad".into()]).count(), 0);
    }

    #[test]
    fn parse_headers_value_utf8_emoji() {
        let h = parse_headers(&["msg=🦀".into()]);
        assert_eq!(h.get(0).value, Some("🦀".as_bytes()));
    }

    #[test]
    fn parse_headers_two_equals_in_value() {
        assert_eq!(
            parse_headers(&["jwt=a=b=c".into()]).get(0).value,
            Some(b"a=b=c".as_ref()),
        );
    }

    #[test]
    fn parse_headers_first_key_wins_order() {
        let h = parse_headers(&["a=1".into(), "b=2".into()]);
        assert_eq!(h.get(0).key, "a");
        assert_eq!(h.get(1).key, "b");
    }
}
