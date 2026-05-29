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

    // ─── parse_headers edge-case pins ────────────────────────────────
    //
    // Kafka headers commonly carry tracing IDs (`traceparent=...`),
    // empty markers (`retry=`), and JWT-shaped values (multi-`=`).
    // Pin those shapes so a tightening of the parser (e.g. switching
    // from `split_once` to `splitn`) doesn't silently drop them.

    #[test]
    fn parse_headers_empty_value_is_emitted_not_dropped() {
        // `flag=` → header with key=`flag`, value=`b""`. Empty values
        // are semantically distinct from "no header set" and must
        // round-trip.
        let h = parse_headers(&["flag=".into()]);
        assert_eq!(h.get(0).key, "flag");
        assert_eq!(h.get(0).value, Some(b"".as_slice()));
    }

    #[test]
    fn parse_headers_value_with_multiple_equals_keeps_full_value() {
        // JWT / base64 / `k=v;k2=v2` values have inner `=` chars;
        // splitting on the first `=` is the documented contract.
        let h = parse_headers(&["jwt=eyJhbGciOiJIUzI1NiJ9.payload.sig".into()]);
        assert_eq!(h.get(0).key, "jwt");
        assert_eq!(
            h.get(0).value,
            Some(b"eyJhbGciOiJIUzI1NiJ9.payload.sig".as_slice())
        );
    }

    #[test]
    fn parse_headers_no_equals_bareword_is_dropped() {
        // A bareword like `oops` (no `=`) has no value half — the
        // documented behavior is silent drop; pin it so a future
        // panic-on-bad-input change is intentional.
        let h = parse_headers(&["oops".into(), "ok=1".into()]);
        // Only `ok=1` makes it in; the bareword is gone.
        assert_eq!(h.get(0).key, "ok");
    }

    #[test]
    fn parse_headers_empty_key_with_value_is_emitted() {
        // `=v` → key="", value=b"v". Kafka allows zero-length keys;
        // dropping these would break interop with brokers that send
        // them in transactional metadata.
        let h = parse_headers(&["=onlyvalue".into()]);
        assert_eq!(h.get(0).key, "");
        assert_eq!(h.get(0).value, Some(b"onlyvalue".as_slice()));
    }

    // ─── clap parsing — ProduceCmd subcommand routing ──────────────────
    // Previous rounds pinned ConsumeArgs defaults + AdminCmd routing.
    // ProduceCmd had only parse_headers helper coverage; the clap surface
    // — Send required topic+value, Stream defaults (ack_timeout_ms=10000,
    // limit None), --partition optional, --header repeatable — was
    // untested.

    use clap::Parser;

    #[derive(Parser, Debug)]
    struct TestProduceCli {
        #[command(subcommand)]
        cmd: ProduceCmd,
    }

    fn parse_produce(args: &[&str]) -> Result<ProduceCmd, clap::Error> {
        let mut argv = vec!["stryke-kafka-helper"];
        argv.extend_from_slice(args);
        TestProduceCli::try_parse_from(argv).map(|c| c.cmd)
    }

    #[test]
    fn produce_send_requires_topic_and_value_flag() {
        // Pin: clap rejects send without topic positional or --value.
        // Drift would let an empty produce reach librdkafka and timeout.
        use clap::error::ErrorKind::MissingRequiredArgument;
        assert_eq!(
            parse_produce(&["send"]).unwrap_err().kind(),
            MissingRequiredArgument
        );
        assert_eq!(
            parse_produce(&["send", "events"]).unwrap_err().kind(),
            MissingRequiredArgument
        );
        let cmd = parse_produce(&["send", "events", "--value", "hello"]).expect("parse");
        match cmd {
            ProduceCmd::Send {
                topic,
                value,
                key,
                headers,
                partition,
            } => {
                assert_eq!(topic, "events");
                assert_eq!(value, "hello");
                assert!(key.is_none());
                assert!(headers.is_empty());
                assert!(partition.is_none(), "partition None = librdkafka picks");
            }
            _ => panic!("expected Send"),
        }
    }

    #[test]
    fn produce_send_repeatable_headers_collect_into_vec() {
        // Pin: --header k=v is repeatable. Drift to last-wins would
        // silently drop tracing/auth headers on each Send.
        let cmd = parse_produce(&[
            "send",
            "events",
            "--value",
            "x",
            "--header",
            "a=1",
            "--header",
            "b=2",
            "--key",
            "k1",
            "--partition",
            "3",
        ])
        .expect("parse");
        match cmd {
            ProduceCmd::Send {
                headers,
                key,
                partition,
                ..
            } => {
                assert_eq!(headers, vec!["a=1", "b=2"]);
                assert_eq!(key.as_deref(), Some("k1"));
                assert_eq!(partition, Some(3));
            }
            _ => panic!("expected Send"),
        }
    }

    #[test]
    fn produce_stream_defaults_ack_timeout_10s_and_no_limit() {
        // Pin: --ack-timeout-ms defaults 10_000 (10s — the librdkafka
        // recommended ack window). --limit unset = stream forever.
        let cmd = parse_produce(&["stream"]).expect("parse");
        match cmd {
            ProduceCmd::Stream {
                default_topic,
                ack_timeout_ms,
                limit,
            } => {
                assert!(default_topic.is_none());
                assert_eq!(ack_timeout_ms, 10_000);
                assert!(limit.is_none());
            }
            _ => panic!("expected Stream"),
        }
    }

    #[test]
    fn produce_stream_optional_flags_thread_through() {
        // Pin: --default-topic Some(_), --ack-timeout-ms override,
        // --limit Some(N). Drift here would silently bound or unbound
        // streaming runs against operator intent.
        let cmd = parse_produce(&[
            "stream",
            "--default-topic",
            "fallback",
            "--ack-timeout-ms",
            "500",
            "--limit",
            "1000",
        ])
        .expect("parse");
        match cmd {
            ProduceCmd::Stream {
                default_topic,
                ack_timeout_ms,
                limit,
            } => {
                assert_eq!(default_topic.as_deref(), Some("fallback"));
                assert_eq!(ack_timeout_ms, 500);
                assert_eq!(limit, Some(1000));
            }
            _ => panic!("expected Stream"),
        }
    }

    #[test]
    fn produce_partition_negative_allowed_partition_value_is_i32() {
        // Pin: --partition is i32. Negative values are accepted at clap
        // level (validation against actual partition count is broker-side).
        // Drift to u32 would silently reject -1 (= "let broker pick").
        // Use `--partition=-1` form: clap rejects leading-hyphen as a
        // bare flag argument; the `=` form binds it without ambiguity.
        let cmd =
            parse_produce(&["send", "events", "--value", "x", "--partition=-1"]).expect("parse");
        match cmd {
            ProduceCmd::Send { partition, .. } => assert_eq!(partition, Some(-1)),
            _ => panic!("expected Send"),
        }
    }
}
