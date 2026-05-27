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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid_like_returns_lowercase_hex() {
        let s = uuid_like();
        assert!(!s.is_empty());
        assert!(s.chars().all(|c| c.is_ascii_hexdigit() && (c.is_ascii_digit() || c.is_ascii_lowercase())));
    }

    #[test]
    fn uuid_like_two_calls_differ() {
        // Nanosecond resolution should make consecutive calls distinct.
        // Sleep a touch to be safe under fast clocks.
        let a = uuid_like();
        std::thread::sleep(std::time::Duration::from_millis(1));
        let b = uuid_like();
        assert_ne!(a, b);
    }

    #[test]
    fn uuid_like_length_reasonable() {
        // u128 nanos since epoch in hex: ~14-16 chars in 2024-2030 range.
        let s = uuid_like();
        assert!(s.len() >= 8, "len = {} for {s}", s.len());
        assert!(s.len() <= 32, "len = {} for {s}", s.len());
    }

    #[test]
    fn uuid_like_no_uppercase_hex() {
        let s = uuid_like();
        assert!(!s.chars().any(|c| c.is_ascii_uppercase()));
    }

    #[test]
    fn uuid_like_monotonic_non_decreasing_under_rapid_calls() {
        let a = uuid_like();
        let b = uuid_like();
        let c = uuid_like();
        assert!(a <= b);
        assert!(b <= c);
    }

    #[test]
    fn uuid_like_only_hex_digits() {
        assert!(uuid_like().chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn uuid_like_starts_with_non_zero_nanos() {
        // Nanos since epoch in 2026 → hex string not empty and not all zeros.
        let s = uuid_like();
        assert_ne!(s, "0");
    }

    #[test]
    fn uuid_like_is_hex_parseable() {
        let s = uuid_like();
        assert!(u128::from_str_radix(&s, 16).is_ok());
    }

    #[test]
    fn uuid_like_not_all_same_char() {
        let s = uuid_like();
        assert!(!s.chars().all(|c| c == s.chars().next().unwrap()));
    }

    #[test]
    fn uuid_like_even_length() {
        let s = uuid_like();
        assert_eq!(s.len() % 2, 0);
    }

    #[test]
    fn uuid_like_no_prefix_zeros_only() {
        let s = uuid_like();
        assert_ne!(s, "0000000000000000");
    }

    #[test]
    fn uuid_like_hex_digit_count_matches_len() {
        let s = uuid_like();
        assert_eq!(s.chars().filter(|c| c.is_ascii_hexdigit()).count(), s.len());
    }

    #[test]
    fn uuid_like_burst_samples_non_empty() {
        for _ in 0..20 {
            assert!(!uuid_like().is_empty());
        }
    }

    #[test]
    fn uuid_like_first_char_is_hex() {
        let s = uuid_like();
        assert!(s.chars().next().unwrap().is_ascii_hexdigit());
    }

    #[test]
    fn uuid_like_last_char_is_hex() {
        let s = uuid_like();
        assert!(s.chars().last().unwrap().is_ascii_hexdigit());
    }

    #[test]
    fn uuid_like_u128_roundtrip_order() {
        let a = u128::from_str_radix(&uuid_like(), 16).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1));
        let b = u128::from_str_radix(&uuid_like(), 16).unwrap();
        assert!(a < b);
    }

    #[test]
    fn uuid_like_no_whitespace() {
        let s = uuid_like();
        assert!(!s.chars().any(|c| c.is_whitespace()));
    }

    #[test]
    fn uuid_like_min_len_at_least_eight() {
        assert!(uuid_like().len() >= 8);
    }

    #[test]
    fn uuid_like_all_lowercase_or_digit() {
        let s = uuid_like();
        assert!(s.chars().all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c)));
    }

    #[test]
    fn uuid_like_parseable_as_u128() {
        assert!(u128::from_str_radix(&uuid_like(), 16).is_ok());
    }

    #[test]
    fn uuid_like_not_empty_string() {
        assert!(!uuid_like().is_empty());
    }

    #[test]
    fn uuid_like_three_samples_monotonic() {
        let a = uuid_like();
        std::thread::sleep(std::time::Duration::from_millis(1));
        let b = uuid_like();
        std::thread::sleep(std::time::Duration::from_millis(1));
        let c = uuid_like();
        assert!(a <= b && b <= c);
    }

    #[test]
    fn uuid_like_no_plus_or_minus() {
        let s = uuid_like();
        assert!(!s.contains('+') && !s.contains('-'));
    }

    #[test]
    fn uuid_like_hex_only_chars() {
        assert!(uuid_like().chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn uuid_like_differs_from_zero_padded() {
        assert_ne!(uuid_like(), "00000000000000000000000000000000");
    }

    #[test]
    fn uuid_like_len_at_most_32() {
        assert!(uuid_like().len() <= 32);
    }

    #[test]
    fn uuid_like_starts_with_digit_allowed() {
        let s = uuid_like();
        assert!(s.chars().next().unwrap().is_ascii_hexdigit());
    }

    #[test]
    fn uuid_like_two_consecutive_differ_with_sleep() {
        let a = uuid_like();
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert_ne!(a, uuid_like());
    }

    #[test]
    fn uuid_like_no_uppercase_a_f() {
        let s = uuid_like();
        assert!(!s.chars().any(|c| ('A'..='F').contains(&c)));
    }

    #[test]
    fn uuid_like_from_str_radix_hex() {
        let n = u128::from_str_radix(&uuid_like(), 16).unwrap();
        assert!(n > 0);
    }

    #[test]
    fn uuid_like_consistent_format() {
        let s = uuid_like();
        assert!(!s.starts_with("0x"));
    }

    #[test]
    fn uuid_like_batch_non_empty() {
        let ids: Vec<_> = (0..5).map(|_| uuid_like()).collect();
        assert!(ids.iter().all(|s| !s.is_empty()));
    }

    #[test]
    fn uuid_like_sorted_pair() {
        let a = uuid_like();
        std::thread::sleep(std::time::Duration::from_millis(1));
        let b = uuid_like();
        assert!(a <= b);
    }

    #[test]
    fn uuid_like_len_at_least_sixteen() {
        assert!(uuid_like().len() >= 16);
    }

    #[test]
    fn uuid_like_lowercase_only() {
        let s = uuid_like();
        assert_eq!(s, s.to_ascii_lowercase());
    }

    #[test]
    fn uuid_like_parse_u128_nonzero() {
        let n = u128::from_str_radix(&uuid_like(), 16).unwrap();
        assert!(n > 0);
    }

    #[test]
    fn uuid_like_three_samples_all_nonempty() {
        let a = uuid_like();
        let b = uuid_like();
        let c = uuid_like();
        assert!(!a.is_empty() && !b.is_empty() && !c.is_empty());
    }

    #[test]
    fn uuid_like_hex_len_even() {
        assert_eq!(uuid_like().len() % 2, 0);
    }

    #[test]
    fn uuid_like_does_not_start_with_minus() {
        assert!(!uuid_like().starts_with('-'));
    }

    #[test]
    fn uuid_like_batch_all_parseable() {
        for _ in 0..10 {
            assert!(u128::from_str_radix(&uuid_like(), 16).is_ok());
        }
    }
}
