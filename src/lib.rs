//! stryke-kafka — Apache Kafka cdylib loaded in-process by stryke via dlopen.
//!
//! Each `#[no_mangle] extern "C" fn kafka__*` is a JSON-string-in /
//! JSON-string-out wrapper around `rdkafka`'s async client API. stryke's
//! FFI bridge (`rust_ffi.rs::load_cdylib`) resolves these symbols at
//! first `use Kafka`, registers each one as a stryke-callable function,
//! and on each call passes a JSON-encoded args dict and copies the
//! returned JSON into a stryke string.
//!
//! Persistent state:
//!   * `RUNTIME` — one shared `tokio` runtime drives every async call.
//!   * `PRODUCERS` — `FutureProducer` cache per brokers tuple. The v1
//!     helper rebuilt the producer (broker discovery + metadata refresh)
//!     per fork — defeating producer batching/compression entirely.
//!     With this cache, batching works as designed.
//!   * `ADMINS` — `AdminClient` cache per brokers tuple.

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic::AssertUnwindSafe;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use rdkafka::admin::{
    AdminClient, AdminOptions, AlterConfig, NewPartitions, NewTopic, ResourceSpecifier,
    TopicReplication,
};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::message::{Header, Headers, Message, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::{Offset, TopicPartitionList};
use serde_json::{json, Value};
use tokio::runtime::{Builder, Runtime};

// ── runtime + client caches ─────────────────────────────────────────────────

static RUNTIME: OnceCell<Runtime> = OnceCell::new();

fn rt() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime")
    })
}

static PRODUCERS: OnceCell<Mutex<HashMap<String, FutureProducer>>> = OnceCell::new();

fn producers() -> &'static Mutex<HashMap<String, FutureProducer>> {
    PRODUCERS.get_or_init(|| Mutex::new(HashMap::new()))
}

// Note: no admin-client cache. `AdminClient` doesn't impl Clone, so a
// HashMap<String, AdminClient> can never serve a cache-hit (would need
// `Arc<AdminClient>` and rdkafka's `AdminClient::create` is cheap enough
// that the per-call construction isn't a hot path). Pre-fix a dead cache
// was here that the lookup never read from and never wrote to, giving the
// false impression admin clients were reused.

// ── connection options ──────────────────────────────────────────────────────

fn brokers_from_opts(opts: &Value) -> String {
    opts.get("brokers")
        .and_then(|v| v.as_str())
        .map(String::from)
        .or_else(|| std::env::var("KAFKA_BROKERS").ok())
        .unwrap_or_else(|| "127.0.0.1:9092".to_string())
}

fn base_config(brokers: &str) -> ClientConfig {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", brokers);
    cfg
}

fn get_producer(opts: &Value) -> Result<FutureProducer> {
    let brokers = brokers_from_opts(opts);
    {
        let map = producers().lock();
        if let Some(p) = map.get(&brokers) {
            return Ok(p.clone());
        }
    }
    let mut cfg = base_config(&brokers);
    cfg.set("message.timeout.ms", "10000");
    let producer: FutureProducer = cfg.create()?;
    producers().lock().insert(brokers, producer.clone());
    Ok(producer)
}

fn get_admin(opts: &Value) -> Result<AdminClient<DefaultClientContext>> {
    let brokers = brokers_from_opts(opts);
    let cfg = base_config(&brokers);
    let admin: AdminClient<_> = cfg.create()?;
    Ok(admin)
}

fn make_base_consumer(opts: &Value, group: Option<&str>) -> Result<BaseConsumer> {
    let brokers = brokers_from_opts(opts);
    let mut cfg = base_config(&brokers);
    cfg.set("enable.auto.commit", "false");
    cfg.set("auto.offset.reset", "earliest");
    if let Some(g) = group {
        cfg.set("group.id", g);
    } else {
        cfg.set("group.id", "stryke-kafka-snapshot");
    }
    Ok(cfg.create()?)
}

// ── payload framing + headers ────────────────────────────────────────────────

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (b[0] as u32) << 16 | (b[1] as u32) << 8 | b[2] as u32;
        out.push(B64[(n >> 18 & 0x3f) as usize] as char);
        out.push(B64[(n >> 12 & 0x3f) as usize] as char);
        out.push(if chunk.len() > 1 {
            B64[(n >> 6 & 0x3f) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            B64[(n & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    out
}

fn base64_decode(s: &str) -> Result<Vec<u8>> {
    fn val(c: u8) -> Result<u32> {
        match c {
            b'A'..=b'Z' => Ok((c - b'A') as u32),
            b'a'..=b'z' => Ok((c - b'a' + 26) as u32),
            b'0'..=b'9' => Ok((c - b'0' + 52) as u32),
            b'+' => Ok(62),
            b'/' => Ok(63),
            _ => Err(anyhow!("invalid base64 character")),
        }
    }
    let s: Vec<u8> = s.bytes().filter(|b| !b.is_ascii_whitespace()).collect();
    if !s.len().is_multiple_of(4) {
        return Err(anyhow!("base64 length must be a multiple of 4"));
    }
    let mut out = Vec::with_capacity(s.len() / 4 * 3);
    for chunk in s.chunks(4) {
        let pad = chunk.iter().filter(|&&c| c == b'=').count();
        let n = (val(chunk[0])? << 18)
            | (val(chunk[1])? << 12)
            | (if chunk[2] == b'=' { 0 } else { val(chunk[2])? } << 6)
            | (if chunk[3] == b'=' { 0 } else { val(chunk[3])? });
        out.push((n >> 16) as u8);
        if pad < 2 {
            out.push((n >> 8) as u8);
        }
        if pad < 1 {
            out.push(n as u8);
        }
    }
    Ok(out)
}

/// Decode a produce payload/key string into bytes per `encoding`
/// (default "utf8"; "base64"/"hex" carry arbitrary bytes).
fn decode_field(value: &str, encoding: &str) -> Result<Vec<u8>> {
    match encoding {
        "utf8" | "text" => Ok(value.as_bytes().to_vec()),
        "base64" | "b64" => base64_decode(value),
        "hex" => {
            if !value.len().is_multiple_of(2) {
                return Err(anyhow!("hex payload must have an even length"));
            }
            (0..value.len())
                .step_by(2)
                .map(|i| {
                    u8::from_str_radix(&value[i..i + 2], 16)
                        .map_err(|_| anyhow!("invalid hex byte at offset {i}"))
                })
                .collect()
        }
        other => Err(anyhow!("unknown encoding: {other} (want utf8|base64|hex)")),
    }
}

/// Encode received bytes into a stryke string per `encoding` (default "utf8",
/// lossy; "base64"/"hex" preserve arbitrary bytes).
fn encode_field(bytes: &[u8], encoding: &str) -> String {
    match encoding {
        "base64" | "b64" => base64_encode(bytes),
        "hex" => {
            let mut s = String::with_capacity(bytes.len() * 2);
            for b in bytes {
                s.push(char::from_digit((b >> 4) as u32, 16).unwrap());
                s.push(char::from_digit((b & 0xf) as u32, 16).unwrap());
            }
            s
        }
        _ => String::from_utf8_lossy(bytes).into_owned(),
    }
}

/// Build librdkafka `OwnedHeaders` from a JSON object `{ key: "value" }`.
/// Header values are always UTF-8 strings here (the common case); returns None
/// when no headers are present so callers can skip attaching them.
fn build_headers(opts: &Value) -> Option<OwnedHeaders> {
    let obj = opts.get("headers")?.as_object()?;
    if obj.is_empty() {
        return None;
    }
    let mut headers = OwnedHeaders::new_with_capacity(obj.len());
    for (k, v) in obj {
        let value = v.as_str().unwrap_or("");
        headers = headers.insert(Header {
            key: k,
            value: Some(value),
        });
    }
    Some(headers)
}

/// Snapshot a consumed message's headers into a JSON object (UTF-8 lossy).
fn headers_to_json(msg: &rdkafka::message::BorrowedMessage<'_>) -> Value {
    let mut obj = serde_json::Map::new();
    if let Some(hs) = msg.headers() {
        for i in 0..hs.count() {
            let h = hs.get(i);
            let v = h.value.map(|b| String::from_utf8_lossy(b).into_owned());
            obj.insert(h.key.to_string(), json!(v));
        }
    }
    Value::Object(obj)
}

/// Map a JSON `{type, name}` spec to an owned resource specifier pair so the
/// borrow can outlive the lookup. type: "topic" | "broker" | "group".
fn resource_parts(opts: &Value) -> Result<(String, Option<i32>)> {
    let kind = opts
        .get("resource_type")
        .and_then(Value::as_str)
        .unwrap_or("topic");
    match kind {
        "topic" | "group" => {
            let name = opts
                .get("resource_name")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("missing resource_name"))?;
            Ok((format!("{kind}:{name}"), None))
        }
        "broker" => {
            let id = opts
                .get("resource_name")
                .and_then(|v| {
                    v.as_i64()
                        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                })
                .ok_or_else(|| anyhow!("broker resource_name must be a broker id"))?;
            Ok((format!("broker:{id}"), Some(id as i32)))
        }
        other => Err(anyhow!(
            "unknown resource_type `{other}` (want topic|broker|group)"
        )),
    }
}

/// Build a `ResourceSpecifier` from the parsed `{kind}:{name}` tag + broker id.
fn make_specifier<'a>(tag: &'a str, broker_id: Option<i32>) -> Result<ResourceSpecifier<'a>> {
    let (kind, name) = tag
        .split_once(':')
        .ok_or_else(|| anyhow!("bad resource tag"))?;
    Ok(match kind {
        "topic" => ResourceSpecifier::Topic(name),
        "group" => ResourceSpecifier::Group(name),
        "broker" => ResourceSpecifier::Broker(broker_id.unwrap_or_default()),
        _ => return Err(anyhow!("bad resource kind")),
    })
}

// ── ops ─────────────────────────────────────────────────────────────────────

async fn op_ping(opts: Value) -> Result<Value> {
    let admin = get_admin(&opts)?;
    let md = admin
        .inner()
        .fetch_metadata(None, Timeout::After(Duration::from_secs(5)))?;
    Ok(json!({"ok": true, "broker_count": md.brokers().len()}))
}

async fn op_cluster(opts: Value) -> Result<Value> {
    let admin = get_admin(&opts)?;
    let md = admin
        .inner()
        .fetch_metadata(None, Timeout::After(Duration::from_secs(5)))?;
    let brokers: Vec<Value> = md
        .brokers()
        .iter()
        .map(|b| json!({"id": b.id(), "host": b.host(), "port": b.port()}))
        .collect();
    Ok(json!({
        "brokers": brokers,
        "topic_count": md.topics().len(),
    }))
}

async fn op_topics(opts: Value) -> Result<Value> {
    let admin = get_admin(&opts)?;
    let md = admin
        .inner()
        .fetch_metadata(None, Timeout::After(Duration::from_secs(5)))?;
    let names: Vec<String> = md.topics().iter().map(|t| t.name().to_string()).collect();
    Ok(json!({"topics": names}))
}

async fn op_describe(opts: Value) -> Result<Value> {
    let admin = get_admin(&opts)?;
    let topic = opts["topic"]
        .as_str()
        .ok_or_else(|| anyhow!("missing topic"))?;
    let md = admin
        .inner()
        .fetch_metadata(Some(topic), Timeout::After(Duration::from_secs(5)))?;
    let info = md
        .topics()
        .iter()
        .find(|t| t.name() == topic)
        .ok_or_else(|| anyhow!("topic `{}` not found", topic))?;
    let partitions: Vec<Value> = info
        .partitions()
        .iter()
        .map(|p| {
            json!({
                "id": p.id(),
                "leader": p.leader(),
                "replicas": p.replicas(),
                "isr": p.isr(),
            })
        })
        .collect();
    Ok(json!({
        "topic": topic,
        "partition_count": info.partitions().len(),
        "partitions": partitions,
    }))
}

async fn op_groups(opts: Value) -> Result<Value> {
    let admin = get_admin(&opts)?;
    let list = admin
        .inner()
        .fetch_group_list(None, Timeout::After(Duration::from_secs(5)))?;
    let groups: Vec<Value> = list
        .groups()
        .iter()
        .map(|g| {
            json!({
                "name": g.name(),
                "state": g.state(),
                "protocol": g.protocol(),
                "protocol_type": g.protocol_type(),
                "members": g.members().len(),
            })
        })
        .collect();
    Ok(json!({"groups": groups}))
}

async fn op_produce(opts: Value) -> Result<Value> {
    let topic = opts["topic"]
        .as_str()
        .ok_or_else(|| anyhow!("missing topic"))?
        .to_string();
    let encoding = opts["encoding"].as_str().unwrap_or("utf8");
    let value = decode_field(opts["value"].as_str().unwrap_or(""), encoding)?;
    let key = match opts["key"].as_str() {
        Some(k) => Some(decode_field(k, encoding)?),
        None => None,
    };
    let partition = opts["partition"].as_i64().map(|p| p as i32);
    let timestamp = opts["timestamp"].as_i64();
    let headers = build_headers(&opts);
    let producer = get_producer(&opts)?;

    let mut record: FutureRecord<[u8], [u8]> = FutureRecord::to(&topic).payload(&value);
    if let Some(k) = &key {
        record = record.key(k.as_slice());
    }
    if let Some(p) = partition {
        record = record.partition(p);
    }
    if let Some(ts) = timestamp {
        record = record.timestamp(ts);
    }
    if let Some(h) = headers {
        record = record.headers(h);
    }
    let delivery = producer
        .send(record, Duration::from_secs(10))
        .await
        .map_err(|(e, _)| anyhow!("send: {}", e))?;
    Ok(json!({
        "topic": topic,
        "partition": delivery.partition,
        "offset": delivery.offset,
    }))
}

async fn op_produce_many(opts: Value) -> Result<Value> {
    let topic = opts["topic"]
        .as_str()
        .ok_or_else(|| anyhow!("missing topic"))?
        .to_string();
    let rows = opts["rows"]
        .as_array()
        .ok_or_else(|| anyhow!("missing rows (array of {{key?, value}} objects)"))?
        .clone();
    // Default encoding applies to every row; a row may override with its own
    // `encoding`. Each row may also carry key, partition, and headers.
    let default_encoding = opts["encoding"].as_str().unwrap_or("utf8").to_string();
    let producer = get_producer(&opts)?;
    let mut sent = 0i64;
    let mut errors = Vec::new();
    for row in &rows {
        let encoding = row
            .get("encoding")
            .and_then(|v| v.as_str())
            .unwrap_or(&default_encoding);
        let value = match decode_field(
            row.get("value").and_then(|v| v.as_str()).unwrap_or(""),
            encoding,
        ) {
            Ok(v) => v,
            Err(e) => {
                errors.push(e.to_string());
                continue;
            }
        };
        let key = match row.get("key").and_then(|v| v.as_str()) {
            Some(k) => match decode_field(k, encoding) {
                Ok(b) => Some(b),
                Err(e) => {
                    errors.push(e.to_string());
                    continue;
                }
            },
            None => None,
        };
        let mut record: FutureRecord<[u8], [u8]> = FutureRecord::to(&topic).payload(&value);
        if let Some(k) = &key {
            record = record.key(k.as_slice());
        }
        if let Some(p) = row.get("partition").and_then(|v| v.as_i64()) {
            record = record.partition(p as i32);
        }
        if let Some(h) = build_headers(row) {
            record = record.headers(h);
        }
        match producer.send(record, Duration::from_secs(10)).await {
            Ok(_) => sent += 1,
            Err((e, _)) => errors.push(format!("{}", e)),
        }
    }
    Ok(json!({
        "topic": topic,
        "sent": sent,
        "errors": errors,
    }))
}

async fn op_consume(opts: Value) -> Result<Value> {
    let topic = opts["topic"]
        .as_str()
        .ok_or_else(|| anyhow!("missing topic"))?;
    let limit = opts["limit"].as_u64().unwrap_or(10) as usize;
    let timeout_ms = opts["timeout_ms"].as_u64().unwrap_or(5000);
    let group = opts["group"].as_str();
    let encoding = opts["encoding"].as_str().unwrap_or("utf8");
    // Commit offsets after draining (only meaningful with an explicit group so
    // the next consume resumes past these messages).
    let commit = opts["commit"].as_bool().unwrap_or(false);
    // Snapshot-style: subscribe to topic, poll up to `limit` messages or
    // until `timeout_ms` runs out. Streaming consumption is deferred.
    let consumer = make_base_consumer(&opts, group)?;
    consumer
        .subscribe(&[topic])
        .context("subscribing to topic")?;
    let mut out: Vec<Value> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_millis(timeout_ms);
    while out.len() < limit {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match consumer.poll(Timeout::After(remaining)) {
            Some(Ok(m)) => {
                let key = m.key().map(|k| encode_field(k, encoding));
                let value = m.payload().map(|v| encode_field(v, encoding));
                out.push(json!({
                    "topic": m.topic(),
                    "partition": m.partition(),
                    "offset": m.offset(),
                    "timestamp": m.timestamp().to_millis(),
                    "key": key,
                    "value": value,
                    "headers": headers_to_json(&m),
                }));
            }
            Some(Err(e)) => return Err(anyhow!("consumer error: {}", e)),
            None => break,
        }
    }
    if commit && group.is_some() && !out.is_empty() {
        consumer
            .commit_consumer_state(CommitMode::Sync)
            .context("committing consumer offsets")?;
    }
    Ok(json!({"messages": out, "committed": commit && group.is_some()}))
}

async fn op_create_topic(opts: Value) -> Result<Value> {
    let name = opts["name"]
        .as_str()
        .ok_or_else(|| anyhow!("missing name"))?
        .to_string();
    let partitions = opts["partitions"].as_i64().unwrap_or(1) as i32;
    let replication = opts["replication"].as_i64().unwrap_or(1) as i32;
    // Optional topic config (`retention.ms`, `cleanup.policy`, …). Held as
    // owned strings so the &str borrows in NewTopic outlive the build.
    let config: Vec<(String, String)> = opts["config"]
        .as_object()
        .map(|o| {
            o.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default();
    let admin = get_admin(&opts)?;
    let mut topic = NewTopic::new(&name, partitions, TopicReplication::Fixed(replication));
    for (k, v) in &config {
        topic = topic.set(k, v);
    }
    let results = admin.create_topics(&[topic], &AdminOptions::new()).await?;
    let mut created = Vec::new();
    let mut errors = Vec::new();
    for r in results {
        match r {
            Ok(n) => created.push(n),
            Err((n, e)) => errors.push(format!("{}: {}", n, e)),
        }
    }
    Ok(json!({"created": created, "errors": errors}))
}

async fn op_delete_topic(opts: Value) -> Result<Value> {
    let name = opts["name"]
        .as_str()
        .ok_or_else(|| anyhow!("missing name"))?
        .to_string();
    let admin = get_admin(&opts)?;
    let results = admin.delete_topics(&[&name], &AdminOptions::new()).await?;
    let mut deleted = Vec::new();
    let mut errors = Vec::new();
    for r in results {
        match r {
            Ok(n) => deleted.push(n),
            Err((n, e)) => errors.push(format!("{}: {}", n, e)),
        }
    }
    Ok(json!({"deleted": deleted, "errors": errors}))
}

/// Enumerate a topic's partition ids from broker metadata.
fn topic_partition_ids(consumer: &BaseConsumer, topic: &str) -> Result<Vec<i32>> {
    let md = consumer.fetch_metadata(Some(topic), Timeout::After(Duration::from_secs(5)))?;
    let t = md
        .topics()
        .iter()
        .find(|t| t.name() == topic)
        .ok_or_else(|| anyhow!("topic `{topic}` not found"))?;
    Ok(t.partitions().iter().map(|p| p.id()).collect())
}

async fn op_watermarks(opts: Value) -> Result<Value> {
    let topic = opts["topic"]
        .as_str()
        .ok_or_else(|| anyhow!("missing topic"))?
        .to_string();
    let consumer = make_base_consumer(&opts, None)?;
    // A specific partition, or all partitions of the topic.
    let parts: Vec<i32> = match opts["partition"].as_i64() {
        Some(p) => vec![p as i32],
        None => topic_partition_ids(&consumer, &topic)?,
    };
    let mut out = Vec::new();
    let mut total = 0i64;
    for p in parts {
        let (low, high) =
            consumer.fetch_watermarks(&topic, p, Timeout::After(Duration::from_secs(5)))?;
        total += high - low;
        out.push(json!({"partition": p, "low": low, "high": high, "count": high - low}));
    }
    Ok(json!({"topic": topic, "partitions": out, "total": total}))
}

async fn op_lag(opts: Value) -> Result<Value> {
    let group = opts["group"]
        .as_str()
        .ok_or_else(|| anyhow!("missing group"))?
        .to_string();
    let topic = opts["topic"]
        .as_str()
        .ok_or_else(|| anyhow!("missing topic (lag is reported per topic)"))?
        .to_string();
    let consumer = make_base_consumer(&opts, Some(&group))?;
    let parts = topic_partition_ids(&consumer, &topic)?;
    // Committed offsets for the group across the topic's partitions.
    let mut tpl = TopicPartitionList::new();
    for p in &parts {
        tpl.add_partition(&topic, *p);
    }
    let committed = consumer.committed_offsets(tpl, Timeout::After(Duration::from_secs(5)))?;
    let mut rows = Vec::new();
    let mut total_lag = 0i64;
    for elem in committed.elements() {
        let p = elem.partition();
        let (_, high) =
            consumer.fetch_watermarks(&topic, p, Timeout::After(Duration::from_secs(5)))?;
        let committed_offset = match elem.offset() {
            Offset::Offset(o) => o,
            _ => 0, // no commit yet → treat as 0 (lag = full backlog)
        };
        let lag = (high - committed_offset).max(0);
        total_lag += lag;
        rows.push(json!({
            "partition": p,
            "committed": committed_offset,
            "high": high,
            "lag": lag,
        }));
    }
    Ok(json!({"group": group, "topic": topic, "partitions": rows, "total_lag": total_lag}))
}

async fn op_offsets_for_times(opts: Value) -> Result<Value> {
    let topic = opts["topic"]
        .as_str()
        .ok_or_else(|| anyhow!("missing topic"))?
        .to_string();
    let timestamp = opts["timestamp"]
        .as_i64()
        .ok_or_else(|| anyhow!("missing timestamp (epoch millis)"))?;
    let consumer = make_base_consumer(&opts, None)?;
    let parts = topic_partition_ids(&consumer, &topic)?;
    let mut tpl = TopicPartitionList::new();
    for p in &parts {
        tpl.add_partition_offset(&topic, *p, Offset::Offset(timestamp))
            .context("seeding offsets_for_times request")?;
    }
    let resolved = consumer.offsets_for_times(tpl, Timeout::After(Duration::from_secs(5)))?;
    let rows: Vec<Value> = resolved
        .elements()
        .iter()
        .map(|e| {
            let off = match e.offset() {
                Offset::Offset(o) => Some(o),
                _ => None, // no message at/after the timestamp
            };
            json!({"partition": e.partition(), "offset": off})
        })
        .collect();
    Ok(json!({"topic": topic, "timestamp": timestamp, "partitions": rows}))
}

async fn op_create_partitions(opts: Value) -> Result<Value> {
    let name = opts["name"]
        .as_str()
        .ok_or_else(|| anyhow!("missing name"))?
        .to_string();
    let count = opts["partitions"]
        .as_u64()
        .ok_or_else(|| anyhow!("missing partitions (new total partition count)"))?
        as usize;
    let admin = get_admin(&opts)?;
    let np = NewPartitions::new(&name, count);
    let results = admin.create_partitions(&[np], &AdminOptions::new()).await?;
    let mut ok = Vec::new();
    let mut errors = Vec::new();
    for r in results {
        match r {
            Ok(n) => ok.push(n),
            Err((n, e)) => errors.push(format!("{}: {}", n, e)),
        }
    }
    Ok(json!({"topic": name, "altered": ok, "errors": errors}))
}

async fn op_describe_configs(opts: Value) -> Result<Value> {
    let (tag, broker_id) = resource_parts(&opts)?;
    let admin = get_admin(&opts)?;
    let spec = make_specifier(&tag, broker_id)?;
    let results = admin
        .describe_configs(&[spec], &AdminOptions::new())
        .await?;
    let mut resources = Vec::new();
    let mut errors = Vec::new();
    for r in results {
        match r {
            Ok(cr) => {
                let entries: Vec<Value> = cr
                    .entries
                    .iter()
                    .map(|e| {
                        json!({
                            "name": e.name,
                            "value": e.value,
                            "read_only": e.is_read_only,
                            "default": e.is_default,
                            "sensitive": e.is_sensitive,
                        })
                    })
                    .collect();
                resources
                    .push(json!({"resource": format!("{:?}", cr.specifier), "entries": entries}));
            }
            Err(e) => errors.push(e.to_string()),
        }
    }
    Ok(json!({"resources": resources, "errors": errors}))
}

async fn op_alter_configs(opts: Value) -> Result<Value> {
    let (tag, broker_id) = resource_parts(&opts)?;
    let entries: Vec<(String, String)> = opts["entries"]
        .as_object()
        .ok_or_else(|| anyhow!("missing entries (object of config key => value)"))?
        .iter()
        .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
        .collect();
    let admin = get_admin(&opts)?;
    let spec = make_specifier(&tag, broker_id)?;
    let mut cfg = AlterConfig::new(spec);
    for (k, v) in &entries {
        cfg = cfg.set(k, v);
    }
    let results = admin.alter_configs(&[cfg], &AdminOptions::new()).await?;
    let mut ok = Vec::new();
    let mut errors = Vec::new();
    for r in results {
        match r {
            Ok(spec) => ok.push(format!("{:?}", spec)),
            Err((spec, e)) => errors.push(format!("{:?}: {}", spec, e)),
        }
    }
    Ok(json!({"altered": ok, "errors": errors}))
}

async fn op_delete_groups(opts: Value) -> Result<Value> {
    let groups: Vec<String> = opts["groups"]
        .as_array()
        .ok_or_else(|| anyhow!("missing groups (array of group ids)"))?
        .iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect();
    if groups.is_empty() {
        return Err(anyhow!("groups must be a non-empty array of group ids"));
    }
    let admin = get_admin(&opts)?;
    let refs: Vec<&str> = groups.iter().map(String::as_str).collect();
    let results = admin.delete_groups(&refs, &AdminOptions::new()).await?;
    let mut deleted = Vec::new();
    let mut errors = Vec::new();
    for r in results {
        match r {
            Ok(n) => deleted.push(n),
            Err((n, e)) => errors.push(format!("{}: {}", n, e)),
        }
    }
    Ok(json!({"deleted": deleted, "errors": errors}))
}

// ── FFI plumbing ────────────────────────────────────────────────────────────

fn ffi_call_async<F, Fut>(args: *const c_char, handler: F) -> *const c_char
where
    F: FnOnce(Value) -> Fut,
    Fut: std::future::Future<Output = Result<Value>>,
{
    let input = if args.is_null() {
        Value::Null
    } else {
        let cs = unsafe { CStr::from_ptr(args) };
        serde_json::from_slice::<Value>(cs.to_bytes()).unwrap_or(Value::Null)
    };
    let fut = handler(input);
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| rt().block_on(fut)));
    let out = match result {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => json!({ "error": e.to_string() }),
        Err(_) => json!({ "error": "stryke-kafka handler panicked" }),
    };
    let s =
        serde_json::to_string(&out).unwrap_or_else(|_| String::from(r#"{"error":"serialize"}"#));
    match CString::new(s) {
        Ok(c) => c.into_raw() as *const c_char,
        Err(_) => std::ptr::null(),
    }
}

/// Free a C string allocated by any export from this cdylib.
///
/// # Safety
///
/// `p` must be a pointer previously returned by an export from this cdylib,
/// or null.
#[no_mangle]
pub unsafe extern "C" fn stryke_free_cstring(p: *mut c_char) {
    if p.is_null() {
        return;
    }
    drop(CString::from_raw(p));
}

// ── pure helpers (no broker) ─────────────────────────────────────────────────

/// Validate a Kafka topic name against the broker's rule: 1–249 chars from
/// `[a-zA-Z0-9._-]`, and not `.` or `..`. Returns `{valid, reason}` — `reason`
/// is null when valid. Pure.
fn op_valid_topic_name(opts: Value) -> Result<Value> {
    let name = opts
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing name"))?;
    let reason: Option<&str> = if name.is_empty() {
        Some("empty")
    } else if name == "." || name == ".." {
        Some("reserved (`.` / `..`)")
    } else if name.len() > 249 {
        Some("longer than 249 characters")
    } else if !name
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-')
    {
        Some("illegal character (allowed: a-z A-Z 0-9 . _ -)")
    } else {
        None
    };
    Ok(json!({"name": name, "valid": reason.is_none(), "reason": reason}))
}

/// Whether a topic is a Kafka internal topic (the `__` prefix, e.g.
/// `__consumer_offsets`, `__transaction_state`). Pure.
fn op_is_internal_topic(opts: Value) -> Result<Value> {
    let name = opts
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing name"))?;
    Ok(json!({"name": name, "internal": name.starts_with("__")}))
}

/// Whether two topic names collide in Kafka's metric namespace. Faithful port of
/// `Topic.hasCollision`: `unifyCollisionChars` replaces every `.` with `_`, and
/// two topics collide when their unified forms are equal (e.g. `my.topic` and
/// `my_topic` both become `my_topic`). Also reports whether each name contains a
/// collision char (`.` or `_`), per `Topic.hasCollisionChars`. Pure.
fn op_topics_collide(opts: Value) -> Result<Value> {
    let a = opts
        .get("a")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing a"))?;
    let b = opts
        .get("b")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing b"))?;
    let unify = |s: &str| s.replace('.', "_");
    let has_chars = |s: &str| s.contains('.') || s.contains('_');
    let unified_a = unify(a);
    let unified_b = unify(b);
    Ok(json!({
        "collide": unified_a == unified_b,
        "unified_a": unified_a,
        "unified_b": unified_b,
        "a_has_collision_chars": has_chars(a),
        "b_has_collision_chars": has_chars(b),
    }))
}

/// Parse a `bootstrap.servers` string `host1:9092,host2:9092` into a broker
/// list `[{host, port}]`. Whitespace around entries is trimmed. Pure.
fn op_parse_brokers(opts: Value) -> Result<Value> {
    let s = opts
        .get("brokers")
        .or_else(|| opts.get("bootstrap"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing brokers"))?;
    let brokers: Vec<Value> = s
        .split(',')
        .map(str::trim)
        .filter(|x| !x.is_empty())
        .map(|hp| match hp.rsplit_once(':') {
            Some((h, p)) => match p.parse::<u32>() {
                Ok(port) => json!({"host": h, "port": port}),
                Err(_) => json!({"host": hp, "port": Value::Null}),
            },
            None => json!({"host": hp, "port": Value::Null}),
        })
        .collect();
    if brokers.is_empty() {
        return Err(anyhow!("no brokers in `{s}`"));
    }
    let count = brokers.len();
    Ok(json!({"brokers": brokers, "count": count}))
}

/// Build a `bootstrap.servers` string from a broker list — the inverse of
/// `parse_brokers`. opts: `brokers`, an array of `{host, port?}` (or bare host
/// strings). A broker with a port becomes `host:port`, otherwise just `host`.
/// Joined with `,`. Pure.
fn op_build_brokers(opts: Value) -> Result<Value> {
    let list = opts
        .get("brokers")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("missing brokers (array)"))?;
    let mut parts = Vec::new();
    for b in list {
        let host = if let Some(s) = b.as_str() {
            s.to_string()
        } else {
            let h = b
                .get("host")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| anyhow!("broker entry missing host"))?;
            match b.get("port").and_then(Value::as_u64) {
                Some(port) => format!("{h}:{port}"),
                None => h.to_string(),
            }
        };
        parts.push(host);
    }
    if parts.is_empty() {
        return Err(anyhow!("broker list is empty"));
    }
    Ok(json!({"bootstrap": parts.join(",")}))
}

/// Kafka's `Utils.murmur2` — the 32-bit MurmurHash2 variant (seed `0x9747b28c`)
/// that the producer's default partitioner hashes record keys with. Faithful
/// port: `i32` wrapping arithmetic and logical (`>>>`) shifts, matching the JVM.
fn murmur2(data: &[u8]) -> i32 {
    let length = data.len();
    let m: i32 = 0x5bd1e995;
    let r = 24u32;
    let mut h: i32 = (0x9747b28cu32 as i32) ^ (length as i32);
    let length4 = length / 4;
    for i in 0..length4 {
        let i4 = i * 4;
        let mut k: i32 = (data[i4] as i32 & 0xff)
            + ((data[i4 + 1] as i32 & 0xff) << 8)
            + ((data[i4 + 2] as i32 & 0xff) << 16)
            + ((data[i4 + 3] as i32 & 0xff) << 24);
        k = k.wrapping_mul(m);
        k ^= ((k as u32) >> r) as i32;
        k = k.wrapping_mul(m);
        h = h.wrapping_mul(m);
        h ^= k;
    }
    // Tail bytes (switch fall-through in the original): xor remaining bytes high
    // to low, then a final multiply.
    let base = length & !3;
    let rem = length % 4;
    if rem >= 3 {
        h ^= (data[base + 2] as i32 & 0xff) << 16;
    }
    if rem >= 2 {
        h ^= (data[base + 1] as i32 & 0xff) << 8;
    }
    if rem >= 1 {
        h ^= data[base] as i32 & 0xff;
        h = h.wrapping_mul(m);
    }
    h ^= ((h as u32) >> 13) as i32;
    h = h.wrapping_mul(m);
    h ^= ((h as u32) >> 15) as i32;
    h
}

/// Predict which partition Kafka's default partitioner sends a keyed record to,
/// client-side: `toPositive(murmur2(key)) % partitions`, where `toPositive` is
/// `hash & 0x7fffffff`. Lets you compute partition assignment offline. opts:
/// `key` (string), `partitions` (count > 0). Returns `{partition, hash}`. Pure.
fn op_partition_for_key(opts: Value) -> Result<Value> {
    let key = opts
        .get("key")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing key"))?;
    let partitions = opts
        .get("partitions")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("missing partitions"))?;
    if partitions == 0 {
        return Err(anyhow!("partitions must be > 0"));
    }
    let hash = murmur2(key.as_bytes());
    let positive = (hash & 0x7fff_ffff) as u64;
    let partition = positive % partitions;
    Ok(json!({"partition": partition, "hash": hash}))
}

/// Standard CRC-32 (IEEE 802.3, reflected, poly 0xEDB88320, init/xorout
/// 0xFFFFFFFF) — the same `rd_crc32` librdkafka hashes keys with. Bitwise rather
/// than table-driven for readability; key hashing is never hot. The published
/// CRC-32/ISO-HDLC check value `crc32("123456789") == 0xCBF43926` pins it.
fn crc32_ieee(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

/// Predict the partition for a keyed record under librdkafka's `consistent`
/// partitioner — `crc32(key) % partitions` — the default for every non-JVM
/// Kafka client (C/C++, Python, Go, Rust). Distinct from `partition_for_key`,
/// which models the JVM client's murmur2 partitioner; the two disagree for the
/// same key. opts: `key` (string), `partitions` (count > 0). Returns
/// `{partition, crc32}`. Pure.
fn op_partition_for_key_crc32(opts: Value) -> Result<Value> {
    let key = opts
        .get("key")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing key"))?;
    let partitions = opts
        .get("partitions")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("missing partitions"))?;
    if partitions == 0 {
        return Err(anyhow!("partitions must be > 0"));
    }
    let crc = crc32_ieee(key.as_bytes());
    let partition = crc as u64 % partitions;
    Ok(json!({"partition": partition, "crc32": crc}))
}

/// Java `String.hashCode()` — `s[0]*31^(n-1) + … + s[n-1]` over UTF-16 code
/// units, wrapping at i32. Kafka keys group-coordinator placement on this exact
/// hash, so it must match the JVM bit-for-bit (e.g. `"abc"` → 96354).
fn java_string_hashcode(s: &str) -> i32 {
    let mut h: i32 = 0;
    for unit in s.encode_utf16() {
        h = h.wrapping_mul(31).wrapping_add(unit as i32);
    }
    h
}

/// Which `__consumer_offsets` partition holds a consumer group's offsets — and
/// thus which broker coordinates it. Kafka computes
/// `Utils.abs(groupId.hashCode()) % offsets.topic.num.partitions`, where
/// `Utils.abs` is the `& 0x7fffffff` bitmask (NOT `Math.abs`) and the partition
/// count defaults to 50. opts: `group` (required), `partitions` (default 50).
/// Returns `{group, partition, hash, partitions}`. Pure.
fn op_group_coordinator_partition(opts: Value) -> Result<Value> {
    let group = opts
        .get("group")
        .or_else(|| opts.get("group_id"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing group"))?;
    let partitions = opts.get("partitions").and_then(Value::as_u64).unwrap_or(50);
    if partitions == 0 {
        return Err(anyhow!("partitions must be > 0"));
    }
    let hash = java_string_hashcode(group);
    let partition = (hash & 0x7fff_ffff) as u64 % partitions;
    Ok(json!({"group": group, "partition": partition, "hash": hash, "partitions": partitions}))
}

/// Predict a consumer group's partition assignment under Kafka's default
/// `RangeAssignor` (the `partition.assignment.strategy` default) for a single
/// topic. Faithful port: members are sorted, then with `base = partitions /
/// members` and `extra = partitions % members`, member `i` gets `base` (+1 if
/// `i < extra`) contiguous partitions — so earlier members absorb the remainder.
/// opts: `partitions` (count > 0), `consumers` (array of member-id strings).
/// Returns `{assignment: {member: [partition…]}, partitions, consumers}`. Pure.
/// Parse and validate the shared inputs of the partition assignors: a positive
/// `partitions` count and a non-empty `consumers` (alias `members`) array of
/// string ids, returned sorted (every built-in Kafka assignor lays members out
/// in sorted member-id order before assigning).
fn parse_assignment_inputs(opts: &Value) -> Result<(u64, Vec<String>)> {
    let partitions = opts
        .get("partitions")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("missing partitions (count)"))?;
    if partitions == 0 {
        return Err(anyhow!("partitions must be > 0"));
    }
    let members_raw = opts
        .get("consumers")
        .or_else(|| opts.get("members"))
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("missing consumers (array of member ids)"))?;
    let mut members: Vec<String> = members_raw
        .iter()
        .map(|m| {
            m.as_str()
                .map(str::to_string)
                .ok_or_else(|| anyhow!("consumer id must be a string"))
        })
        .collect::<Result<_>>()?;
    if members.is_empty() {
        return Err(anyhow!("consumers is empty"));
    }
    members.sort();
    Ok((partitions, members))
}

fn op_range_assignment(opts: Value) -> Result<Value> {
    let (partitions, members) = parse_assignment_inputs(&opts)?;
    let n = members.len() as u64;
    let base = partitions / n;
    let extra = partitions % n;
    let mut assignment = serde_json::Map::new();
    for (i, member) in members.iter().enumerate() {
        let i = i as u64;
        let start = base * i + i.min(extra);
        let length = base + if i < extra { 1 } else { 0 };
        let parts: Vec<u64> = (start..start + length).collect();
        assignment.insert(member.clone(), json!(parts));
    }
    Ok(json!({
        "assignment": assignment,
        "partitions": partitions,
        "consumers": members,
    }))
}

/// Kafka's RoundRobinAssignor for a single subscribed topic: lay out partitions
/// `0..partitions` and assign each in turn to the next member in sorted member-id
/// order, so partition `p` goes to member `p % n`. Counts land within one of each
/// other. opts: `partitions` (count) + `consumers`/`members` (array of ids).
/// Returns `{assignment, partitions, consumers}`. Pure. Companion of
/// `range_assignment` (a distinct strategy: round-robin vs contiguous ranges).
fn op_roundrobin_assignment(opts: Value) -> Result<Value> {
    let (partitions, members) = parse_assignment_inputs(&opts)?;
    let n = members.len() as u64;
    let mut assignment = serde_json::Map::new();
    for member in &members {
        assignment.insert(member.clone(), json!(Vec::<u64>::new()));
    }
    for p in 0..partitions {
        let member = &members[(p % n) as usize];
        assignment[member].as_array_mut().unwrap().push(json!(p));
    }
    Ok(json!({
        "assignment": assignment,
        "partitions": partitions,
        "consumers": members,
    }))
}

/// Map between Kafka's special offset sentinels and their names: `-1` ⇄
/// `latest`, `-2` ⇄ `earliest`. A concrete (non-negative) offset has a null
/// name. Pass `offset` (number) or `name` (string). Pure.
fn op_format_offset(opts: Value) -> Result<Value> {
    if let Some(off) = opts.get("offset").and_then(Value::as_i64) {
        let name = match off {
            -1 => Some("latest"),
            -2 => Some("earliest"),
            _ => None,
        };
        return Ok(json!({"offset": off, "name": name}));
    }
    if let Some(name) = opts.get("name").and_then(Value::as_str) {
        let off = match name.to_ascii_lowercase().as_str() {
            "latest" | "end" => -1,
            "earliest" | "beginning" | "start" => -2,
            other => return Err(anyhow!("unknown offset name `{other}` (latest|earliest)")),
        };
        return Ok(json!({"name": name, "offset": off}));
    }
    Err(anyhow!("format_offset requires `offset` or `name`"))
}

// ── exports ─────────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn kafka__pkg_version(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |_| async {
        Ok(json!({"version": env!("CARGO_PKG_VERSION")}))
    })
}

#[no_mangle]
pub extern "C" fn kafka__ping(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_ping)
}

#[no_mangle]
pub extern "C" fn kafka__cluster(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_cluster)
}

#[no_mangle]
pub extern "C" fn kafka__topics(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_topics)
}

#[no_mangle]
pub extern "C" fn kafka__describe(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_describe)
}

#[no_mangle]
pub extern "C" fn kafka__groups(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_groups)
}

#[no_mangle]
pub extern "C" fn kafka__produce(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_produce)
}

#[no_mangle]
pub extern "C" fn kafka__produce_many(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_produce_many)
}

#[no_mangle]
pub extern "C" fn kafka__consume(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_consume)
}

#[no_mangle]
pub extern "C" fn kafka__create_topic(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_create_topic)
}

#[no_mangle]
pub extern "C" fn kafka__delete_topic(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_delete_topic)
}

#[no_mangle]
pub extern "C" fn kafka__watermarks(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_watermarks)
}

#[no_mangle]
pub extern "C" fn kafka__lag(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_lag)
}

#[no_mangle]
pub extern "C" fn kafka__offsets_for_times(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_offsets_for_times)
}

#[no_mangle]
pub extern "C" fn kafka__create_partitions(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_create_partitions)
}

#[no_mangle]
pub extern "C" fn kafka__describe_configs(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_describe_configs)
}

#[no_mangle]
pub extern "C" fn kafka__alter_configs(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_alter_configs)
}

#[no_mangle]
pub extern "C" fn kafka__delete_groups(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_delete_groups)
}

#[no_mangle]
pub extern "C" fn kafka__valid_topic_name(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_valid_topic_name(opts) })
}

#[no_mangle]
pub extern "C" fn kafka__is_internal_topic(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_is_internal_topic(opts) })
}

#[no_mangle]
pub extern "C" fn kafka__topics_collide(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_topics_collide(opts) })
}

#[no_mangle]
pub extern "C" fn kafka__parse_brokers(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_parse_brokers(opts) })
}

#[no_mangle]
pub extern "C" fn kafka__build_brokers(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_build_brokers(opts) })
}

#[no_mangle]
pub extern "C" fn kafka__partition_for_key(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_partition_for_key(opts) })
}

#[no_mangle]
pub extern "C" fn kafka__partition_for_key_crc32(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_partition_for_key_crc32(opts) })
}

#[no_mangle]
pub extern "C" fn kafka__group_coordinator_partition(args: *const c_char) -> *const c_char {
    ffi_call_async(
        args,
        |opts| async move { op_group_coordinator_partition(opts) },
    )
}

#[no_mangle]
pub extern "C" fn kafka__range_assignment(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_range_assignment(opts) })
}

#[no_mangle]
pub extern "C" fn kafka__roundrobin_assignment(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_roundrobin_assignment(opts) })
}

#[no_mangle]
pub extern "C" fn kafka__format_offset(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_format_offset(opts) })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_env<F: FnOnce()>(f: F) {
        let _lock = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let saved = std::env::var("KAFKA_BROKERS").ok();
        std::env::remove_var("KAFKA_BROKERS");
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
        match saved {
            Some(s) => std::env::set_var("KAFKA_BROKERS", s),
            None => std::env::remove_var("KAFKA_BROKERS"),
        }
        if let Err(p) = result {
            std::panic::resume_unwind(p);
        }
    }

    #[test]
    fn brokers_opts_string_wins() {
        with_env(|| {
            std::env::set_var("KAFKA_BROKERS", "from-env:9092");
            assert_eq!(
                brokers_from_opts(&json!({"brokers": "from-opts:9092"})),
                "from-opts:9092"
            );
        });
    }

    #[test]
    fn brokers_falls_back_to_env() {
        with_env(|| {
            std::env::set_var("KAFKA_BROKERS", "env-host:9092");
            assert_eq!(brokers_from_opts(&json!({})), "env-host:9092");
        });
    }

    #[test]
    fn brokers_default_when_unset() {
        with_env(|| {
            assert_eq!(brokers_from_opts(&json!({})), "127.0.0.1:9092");
        });
    }

    #[test]
    fn brokers_ignores_non_string_opts() {
        with_env(|| {
            // `{"brokers": 9092}` shouldn't stringify the integer.
            assert_eq!(
                brokers_from_opts(&json!({"brokers": 9092})),
                "127.0.0.1:9092"
            );
        });
    }

    /// A comma-separated multi-broker list must pass through to librdkafka
    /// VERBATIM — no trimming, splitting, de-duping, or reordering.
    /// librdkafka's `bootstrap.servers` is itself a raw CSV string; any
    /// massaging here (a well-intentioned `.trim()`, a split/rejoin to
    /// "normalize", or whitespace collapsing) would corrupt a valid
    /// multi-broker config. The interior spaces around `b:2` are
    /// deliberately preserved to catch exactly such a normalization
    /// regression — the function's contract is byte-identity, not
    /// "a reasonable broker list".
    #[test]
    fn brokers_multibroker_csv_passes_through_byte_identical() {
        with_env(|| {
            // KAFKA_BROKERS is cleared by with_env, so opts must win as-is.
            let raw = "host-a:9092, host-b:9092 ,host-c:9092";
            assert_eq!(
                brokers_from_opts(&json!({ "brokers": raw })),
                raw,
                "broker CSV must reach librdkafka unmodified (no trim/split/rejoin)"
            );
        });
    }

    /// The producer cache is the crate's entire reason to exist (module
    /// doc: "The v1 helper rebuilt the producer per fork — defeating
    /// producer batching/compression entirely"). A cache HIT must return
    /// the stored handle WITHOUT calling `cfg.create()` again and WITHOUT
    /// inserting a second entry. We pre-seed the global `PRODUCERS` map
    /// with a real `FutureProducer` under a unique broker key, then call
    /// `get_producer`; the early `if let Some(p) = map.get(&brokers)`
    /// branch (lib.rs ~83) must fire. The invariant we pin: the map still
    /// holds exactly ONE entry for that key (no rebuild-and-reinsert,
    /// no duplicate). A regression that drops the lookup and always
    /// rebuilds would silently destroy batching on every produce.
    ///
    /// Uses a unique, syntactically-valid-but-unroutable broker so it can
    /// never collide with another test's cache entry and never opens a
    /// socket (`FutureProducer::create` constructs lazily — `send` is what
    /// would connect, and we never call it).
    #[test]
    fn get_producer_cache_hit_does_not_rebuild() {
        with_env(|| {
            let brokers = "stryke-kafka-cache-test-unique-host:9092";
            // Seed the cache directly with a real producer handle.
            let seeded: FutureProducer = base_config(brokers)
                .create()
                .expect("FutureProducer::create must be a non-connecting constructor");
            producers().lock().insert(brokers.to_string(), seeded);

            let before = producers().lock().keys().filter(|k| *k == brokers).count();
            assert_eq!(before, 1, "precondition: exactly one seeded entry");

            // Cache HIT path: must return Ok and must NOT insert a duplicate.
            let got = get_producer(&json!({ "brokers": brokers }));
            assert!(got.is_ok(), "cache hit must succeed: {:?}", got.err());

            let after = producers().lock().keys().filter(|k| *k == brokers).count();
            assert_eq!(
                after, 1,
                "cache hit must reuse the stored producer, not rebuild-and-reinsert"
            );

            // Cleanup: don't leak the unique entry into other tests' view.
            producers().lock().remove(brokers);
        });
    }

    #[test]
    fn base_config_sets_bootstrap_servers() {
        let cfg = base_config("host-a:9092,host-b:9092");
        assert_eq!(
            cfg.get("bootstrap.servers"),
            Some("host-a:9092,host-b:9092")
        );
    }

    #[test]
    fn base_config_creates_clean_per_call() {
        // Two calls must yield independent configs so consumer-vs-producer
        // overrides (`enable.auto.commit`, `message.timeout.ms`) on one
        // don't bleed into the other.
        let mut a = base_config("h:1");
        a.set("enable.auto.commit", "false");
        let b = base_config("h:1");
        assert_eq!(b.get("enable.auto.commit"), None);
    }

    /// Consumer config overrides must take effect — auto.offset.reset and
    /// enable.auto.commit are both required for consume_stream's snapshot
    /// semantics. Regression here would silently move offsets.
    #[test]
    fn make_consumer_sets_explicit_offset_and_commit_policy() {
        with_env(|| {
            std::env::set_var("KAFKA_BROKERS", "h:1");
            // Default group_id (no override): the documented "snapshot" name
            // must appear so observers can spot stryke-kafka consumers in
            // broker-side `kafka-consumer-groups --list`.
            let consumer = make_base_consumer(&json!({}), None);
            assert!(
                consumer.is_ok(),
                "default consumer build failed: {:?}",
                consumer.err()
            );

            // Explicit group override flows through verbatim.
            let consumer_g = make_base_consumer(&json!({}), Some("my-test-group"));
            assert!(consumer_g.is_ok(), "explicit-group consumer build failed");
        });
    }

    // ── FFI-boundary safety tests ─────────────────────────────────────────
    // The cdylib is dlopened in-process by stryke; a handler panic that
    // escaped `catch_unwind` would tear the host shell down. A null/garbled
    // FFI return would manifest as a stryke crash, not a Kafka error.
    // These tests pin the safety contracts that keep the host alive.

    /// A handler that panics MUST NOT unwind across the FFI boundary.
    /// `ffi_call_async` wraps `rt().block_on(...)` in `catch_unwind` for
    /// exactly this reason; removing it would convert any rdkafka panic
    /// (e.g. inside librdkafka's tokio task) into a host-shell abort.
    #[test]
    fn ffi_handler_panic_is_caught_and_reported_as_json() {
        let raw = ffi_call_async(std::ptr::null(), |_| async {
            panic!("simulated rdkafka panic");
            #[allow(unreachable_code)]
            Ok::<_, anyhow::Error>(json!({}))
        });
        assert!(!raw.is_null(), "panic path returned null *const c_char");
        // SAFETY: ffi_call_async returns a CString::into_raw pointer.
        let s = unsafe { CStr::from_ptr(raw).to_str().expect("utf8").to_owned() };
        unsafe { stryke_free_cstring(raw as *mut c_char) };
        let v: Value = serde_json::from_str(&s).expect("ffi return must be valid JSON");
        let err = v.get("error").and_then(|e| e.as_str()).unwrap_or("");
        assert!(
            err.contains("panicked"),
            "expected panic-marker in error field; got: {s}"
        );
    }

    /// A handler that returns `Err(anyhow!(...))` MUST surface as
    /// `{"error": "<msg>"}` — flat, no nesting, no `cause` chain — so
    /// stryke's bridge can match on a single key.
    #[test]
    fn ffi_handler_error_returns_flat_error_object() {
        let raw = ffi_call_async(std::ptr::null(), |_| async {
            Err::<Value, _>(anyhow!("missing topic"))
        });
        assert!(!raw.is_null());
        let s = unsafe { CStr::from_ptr(raw).to_str().expect("utf8").to_owned() };
        unsafe { stryke_free_cstring(raw as *mut c_char) };
        let v: Value = serde_json::from_str(&s).expect("must be JSON");
        let obj = v
            .as_object()
            .expect("must be a JSON object, not array/string");
        assert_eq!(obj.len(), 1, "error response must have exactly one key");
        assert_eq!(
            obj.get("error").and_then(|e| e.as_str()),
            Some("missing topic"),
            "error message must round-trip verbatim"
        );
    }

    /// End-to-end FFI round-trip for the one export that needs no broker:
    /// `kafka__pkg_version(null)` → JSON with the same version as
    /// `CARGO_PKG_VERSION` → `stryke_free_cstring` reclaims the allocation.
    /// Catches: null returns from `CString::new` (would crash the host on
    /// deref), null-arg handling in `ffi_call_async`, version drift
    /// between `env!("CARGO_PKG_VERSION")` and the exported JSON, and
    /// allocator-boundary mismatch in `stryke_free_cstring`
    /// (`CString::from_raw` on a non-`CString::into_raw` pointer is UB).
    #[test]
    fn kafka_pkg_version_ffi_roundtrip_matches_cargo_metadata() {
        let raw = kafka__pkg_version(std::ptr::null());
        assert!(
            !raw.is_null(),
            "kafka__pkg_version returned null — CString::new failed on its own output"
        );
        let s = unsafe { CStr::from_ptr(raw).to_str().expect("utf8").to_owned() };
        // Free immediately to surface any allocator-boundary bug under
        // test runners (miri / asan / debug allocator) before assertions.
        unsafe { stryke_free_cstring(raw as *mut c_char) };
        let v: Value = serde_json::from_str(&s).expect("must be JSON");
        let version = v
            .get("version")
            .and_then(|x| x.as_str())
            .expect("`version` key must be present and a string");
        assert_eq!(
            version,
            env!("CARGO_PKG_VERSION"),
            "FFI-reported version drifted from Cargo.toml"
        );
        // stryke_free_cstring(null) must be a no-op, not a deref.
        unsafe { stryke_free_cstring(std::ptr::null_mut()) };
    }

    // ── Argument-validation contracts on exported FFI ops ────────────────
    // These pin the "fail fast before touching a broker" contract for the
    // three exports most likely to be mis-shaped by stryke callers. Each
    // one MUST surface as a flat `{"error": "<msg>"}` immediately, with no
    // broker handshake, no 10-second message.timeout.ms wait, and no
    // panic across the FFI boundary. A regression here would convert a
    // user typo (e.g. `messages:` instead of `rows:`) into a multi-second
    // hang under the runtime.
    //
    // We invoke each export with a JSON arg that is structurally valid
    // (parses as a JSON object) but missing the required key. The validator
    // must short-circuit before `get_producer` / `get_admin` builds its
    // librdkafka client. If validation regresses to "build first, validate
    // later", the test would hang or fail with a network-style error,
    // not the documented message.

    fn ffi_call_with_json_arg(
        f: extern "C" fn(*const c_char) -> *const c_char,
        arg: &str,
    ) -> Value {
        let cs = CString::new(arg).expect("arg must not contain NUL");
        let raw = f(cs.as_ptr());
        assert!(!raw.is_null(), "FFI export returned null pointer");
        let s = unsafe { CStr::from_ptr(raw).to_str().expect("utf8").to_owned() };
        unsafe { stryke_free_cstring(raw as *mut c_char) };
        serde_json::from_str::<Value>(&s).expect("FFI return must be JSON")
    }

    /// `kafka__produce` with no `topic` key must short-circuit with the
    /// exact documented error string — NOT block on a broker handshake,
    /// NOT panic, NOT return a network/timeout error. Catches a class of
    /// regression where someone refactors `op_produce` to build the
    /// producer before validating args, which would turn a user typo
    /// into a 10-second hang under `message.timeout.ms`.
    #[test]
    fn kafka_produce_missing_topic_fails_before_broker_contact() {
        let start = std::time::Instant::now();
        let v = ffi_call_with_json_arg(kafka__produce, r#"{"value":"hi"}"#);
        let elapsed = start.elapsed();
        // Must short-circuit well under message.timeout.ms (10s); a 1s cap
        // is generous for a pure-validation path on any CI runner.
        assert!(
            elapsed < Duration::from_secs(1),
            "validation path took {elapsed:?} — regression to build-before-validate?"
        );
        let obj = v.as_object().expect("must be flat JSON object");
        assert_eq!(obj.len(), 1, "error response must be flat single-key");
        assert_eq!(
            obj.get("error").and_then(|e| e.as_str()),
            Some("missing topic"),
            "exact contract string required; stryke matches on this"
        );
    }

    /// `kafka__produce_many` with `rows` as a JSON object (not array)
    /// must surface the documented error verbatim. The validator uses
    /// `as_array().ok_or_else(...)` — if a future refactor swaps to
    /// `as_array().unwrap_or_default()`, the test catches the silent
    /// no-op (sent=0, no error) that would mislead callers who passed
    /// `{"rows": {"k":"v"}}` thinking objects iterate.
    #[test]
    fn kafka_produce_many_rejects_non_array_rows() {
        let v = ffi_call_with_json_arg(kafka__produce_many, r#"{"topic":"t","rows":{"k":"v"}}"#);
        let obj = v.as_object().expect("must be flat JSON object");
        let err = obj
            .get("error")
            .and_then(|e| e.as_str())
            .expect("must surface as error, not silent sent=0");
        assert!(
            err.starts_with("missing rows"),
            "expected `missing rows (...)` contract; got: {err}"
        );
        // Specifically: must NOT have silently treated the object as empty
        // and returned a success-shaped {sent:0} payload.
        assert!(
            obj.get("sent").is_none(),
            "non-array rows must not produce a success-shaped response"
        );
    }

    /// `kafka__delete_topic` with `name` as a JSON integer must NOT
    /// coerce the integer to a string and attempt deletion of a topic
    /// literally named "42". `opts["name"].as_str()` returns None for
    /// non-strings, so the early `missing name` error fires — this test
    /// pins that contract. A regression to `name.to_string()` would
    /// silently delete the wrong topic.
    #[test]
    fn kafka_delete_topic_does_not_coerce_int_name_to_string() {
        let v = ffi_call_with_json_arg(kafka__delete_topic, r#"{"name":42}"#);
        let obj = v.as_object().expect("must be flat JSON object");
        let err = obj
            .get("error")
            .and_then(|e| e.as_str())
            .expect("integer name must surface as error, not be coerced");
        assert_eq!(
            err, "missing name",
            "integer `name` must be rejected as missing, never coerced"
        );
        // Crucially: must not have reached the admin client at all.
        // If it had, the error string would include broker/network
        // language ("failed to fetch", "BrokerTransportFailure", etc.).
        assert!(
            !err.to_lowercase().contains("broker")
                && !err.to_lowercase().contains("transport")
                && !err.to_lowercase().contains("connect"),
            "validator must reject before admin client contact; got: {err}"
        );
    }

    // ── Malformed-input robustness on FFI parse boundary ─────────────────
    // `ffi_call_async` line ~361 does `serde_json::from_slice(...).unwrap_or(Value::Null)`.
    // The `unwrap_or` (not `expect`) is load-bearing: stryke's FFI bridge
    // can hand us truncated, doubly-escaped, or otherwise-malformed JSON
    // (e.g. from a partial pipe read or a quoting bug in caller code). If
    // a future refactor swaps to `.expect(...)` or `.unwrap()`, every such
    // call would panic across the FFI boundary; `catch_unwind` would then
    // be the only thing keeping the host shell alive. We pin both layers:
    // (1) malformed JSON must NOT panic, (2) the missing-required-key
    // error must still surface (because Null["topic"] == Null, .as_str() == None).

    /// Garbage bytes on stdin/args must surface as the documented
    /// "missing topic" error, not as a panic, JSON-parse error, or hang.
    /// Catches the regression class: someone replaces
    /// `serde_json::from_slice(...).unwrap_or(Value::Null)` with a
    /// strict variant. Garbage-in → host crash via panic-across-FFI
    /// would be the resulting silent disaster.
    #[test]
    fn ffi_malformed_json_input_does_not_panic_and_falls_through_to_validator() {
        // Truncated JSON, not even close to valid.
        let v = ffi_call_with_json_arg(kafka__produce, "not-json-{[}");
        let obj = v
            .as_object()
            .expect("must be flat JSON object even on malformed input");
        let err = obj
            .get("error")
            .and_then(|e| e.as_str())
            .expect("malformed-input path must produce error field");
        // Specifically: the validator's "missing topic" message — proves
        // the parse failed silently (→ Value::Null) and then opts["topic"]
        // returned Null whose .as_str() is None, triggering the documented
        // arg-validation error. If we got a panic-marker error here, the
        // parse path itself panicked instead of falling through.
        assert_eq!(
            err, "missing topic",
            "malformed JSON must fall through to validator with documented error; \
             got: {err} (a panic-marker here means the parse panicked)"
        );
        assert!(
            !err.contains("panicked"),
            "malformed-input path panicked across FFI: {err}"
        );
    }

    /// Empty-string JSON args must also fall through to validation, not
    /// panic. serde_json returns `Err` on empty input; the `.unwrap_or`
    /// contract must hold. Distinct test from the malformed-bytes case
    /// because empty input hits a different serde_json error path.
    #[test]
    fn ffi_empty_string_input_does_not_panic() {
        let v = ffi_call_with_json_arg(kafka__produce, "");
        let obj = v
            .as_object()
            .expect("empty input must still produce flat JSON object");
        assert_eq!(
            obj.get("error").and_then(|e| e.as_str()),
            Some("missing topic"),
            "empty-input parse must silently coerce to Null and surface as missing-key"
        );
    }

    /// `kafka__produce_many` with `rows: null` (JSON null literal, not
    /// missing-key, not non-array-object) must surface the documented
    /// "missing rows" error. This is a different Value variant from
    /// the existing non-array-object test: `Value::Null.as_array()`
    /// returns None via a different match arm than `Value::Object.as_array()`.
    /// Catches a regression where someone adds `if rows.is_null() {
    /// return Ok(empty) }` as a "convenience" no-op — silently coercing
    /// caller mistakes into success-shaped responses.
    #[test]
    fn kafka_produce_many_rejects_null_rows_distinctly_from_missing() {
        let v = ffi_call_with_json_arg(kafka__produce_many, r#"{"topic":"t","rows":null}"#);
        let obj = v.as_object().expect("must be flat JSON object");
        let err = obj
            .get("error")
            .and_then(|e| e.as_str())
            .expect("null rows must surface as error, not as silent sent=0");
        assert!(
            err.starts_with("missing rows"),
            "expected `missing rows (...)` contract for JSON null; got: {err}"
        );
        // The success-shape sentinel: a regression that no-ops on null
        // would emit `{topic, sent: 0, errors: []}`. We must not see
        // `sent` in the response at all.
        assert!(
            obj.get("sent").is_none(),
            "null rows must not produce success-shaped {{sent: 0}} response"
        );
        assert!(
            obj.get("errors").is_none(),
            "null rows must not produce success-shaped {{errors: []}} response"
        );
    }

    /// `op_describe` requires both a topic AND that the topic appear in
    /// the broker metadata response (line ~169 `find(|t| t.name() == topic)`).
    /// The validator must reject a missing `topic` key BEFORE attempting
    /// metadata fetch — otherwise a user-typo turns into a 5-second
    /// blocking metadata fetch. This pins the same "validate-before-network"
    /// contract as the produce/delete-topic tests, but for the admin-read
    /// path, which has a different broker-contact pattern (fetch_metadata
    /// vs producer.send vs admin.delete_topics).
    #[test]
    fn kafka_describe_missing_topic_fails_before_metadata_fetch() {
        let start = std::time::Instant::now();
        let v = ffi_call_with_json_arg(kafka__describe, r#"{}"#);
        let elapsed = start.elapsed();
        // fetch_metadata has a 5s timeout; validation must short-circuit
        // well under that. 1s cap is generous for any CI runner.
        assert!(
            elapsed < Duration::from_secs(1),
            "describe validation took {elapsed:?} — regression to fetch-before-validate? \
             That would hang for 5s on every typo."
        );
        let obj = v.as_object().expect("must be flat JSON object");
        assert_eq!(
            obj.get("error").and_then(|e| e.as_str()),
            Some("missing topic"),
            "exact contract string required"
        );
    }

    // ── new-surface helper + validation coverage ─────────────────────────────

    #[test]
    fn base64_round_trips_and_hex_field_codec() {
        let raw = [0u8, 1, 2, 255, 254, 128, b'k'];
        assert_eq!(base64_decode(&base64_encode(&raw)).unwrap(), raw);
        assert_eq!(base64_encode(b"Man"), "TWFu");
        // field codecs: utf8 default, base64, hex round-trip arbitrary bytes
        assert_eq!(decode_field("00ff2a", "hex").unwrap(), vec![0u8, 255, 42]);
        assert_eq!(encode_field(&[0u8, 255, 42], "hex"), "00ff2a");
        assert_eq!(decode_field("TWFu", "base64").unwrap(), b"Man");
        assert_eq!(decode_field("hi", "utf8").unwrap(), b"hi");
        assert!(
            decode_field("zzz", "snappy").is_err(),
            "unknown encoding must error"
        );
    }

    #[test]
    fn build_headers_from_object_and_absent() {
        // No headers key → None so the producer skips attaching.
        assert!(build_headers(&json!({"topic": "t"})).is_none());
        assert!(build_headers(&json!({"headers": {}})).is_none());
        let h =
            build_headers(&json!({"headers": {"a": "1", "b": "2"}})).expect("two headers built");
        assert_eq!(h.count(), 2);
    }

    #[test]
    fn resource_parts_and_specifier() {
        // topic default
        let (tag, id) = resource_parts(&json!({"resource_name": "orders"})).unwrap();
        assert_eq!(tag, "topic:orders");
        assert_eq!(id, None);
        assert!(matches!(
            make_specifier(&tag, id).unwrap(),
            ResourceSpecifier::Topic("orders")
        ));
        // broker id parsed
        let (tag, id) =
            resource_parts(&json!({"resource_type": "broker", "resource_name": 3})).unwrap();
        assert_eq!(id, Some(3));
        assert!(matches!(
            make_specifier(&tag, id).unwrap(),
            ResourceSpecifier::Broker(3)
        ));
        // missing name for topic errors
        assert!(resource_parts(&json!({"resource_type": "topic"})).is_err());
        // unknown type errors
        assert!(
            resource_parts(&json!({"resource_type": "cluster", "resource_name": "x"})).is_err()
        );
    }

    /// New broker-touching exports must validate required args BEFORE building
    /// any librdkafka client — a typo must surface instantly, not hang on a
    /// broker handshake. Pins the validate-before-network contract for each.
    #[test]
    fn new_ops_validate_before_broker_contact() {
        let cases: &[(extern "C" fn(*const c_char) -> *const c_char, &str, &str)] = &[
            (kafka__watermarks, r#"{}"#, "missing topic"),
            (kafka__lag, r#"{"topic":"t"}"#, "missing group"),
            (kafka__lag, r#"{"group":"g"}"#, "missing topic"),
            (
                kafka__offsets_for_times,
                r#"{"topic":"t"}"#,
                "missing timestamp",
            ),
            (
                kafka__create_partitions,
                r#"{"name":"t"}"#,
                "missing partitions",
            ),
            (kafka__describe_configs, r#"{}"#, "missing resource_name"),
            (
                kafka__alter_configs,
                r#"{"resource_name":"t"}"#,
                "missing entries",
            ),
            (kafka__delete_groups, r#"{}"#, "missing groups"),
        ];
        for (f, arg, want) in cases {
            let start = std::time::Instant::now();
            let v = ffi_call_with_json_arg(*f, arg);
            assert!(
                start.elapsed() < Duration::from_secs(1),
                "validation for {want} took too long — build-before-validate regression?"
            );
            let err = v
                .as_object()
                .and_then(|o| o.get("error"))
                .and_then(|e| e.as_str())
                .unwrap_or("");
            assert!(
                err.contains(want),
                "expected `{want}` for arg {arg}; got: {err}"
            );
        }
    }

    // ── pure helpers (no broker) ─────────────────────────────────────────────

    #[test]
    fn valid_topic_name_accepts_legal_and_flags_each_violation() {
        assert_eq!(
            op_valid_topic_name(json!({"name": "orders.us-east_1"})).unwrap()["valid"],
            json!(true)
        );
        // Each rejection carries a specific reason.
        let dot = op_valid_topic_name(json!({"name": ".."})).unwrap();
        assert_eq!(dot["valid"], json!(false));
        assert!(dot["reason"].as_str().unwrap().contains("reserved"));
        let bad = op_valid_topic_name(json!({"name": "has space"})).unwrap();
        assert_eq!(bad["valid"], json!(false));
        assert!(bad["reason"].as_str().unwrap().contains("illegal"));
        let long = op_valid_topic_name(json!({"name": "a".repeat(250)})).unwrap();
        assert_eq!(long["valid"], json!(false));
        assert!(long["reason"].as_str().unwrap().contains("249"));
        // 249 chars is the boundary — still valid.
        assert_eq!(
            op_valid_topic_name(json!({"name": "a".repeat(249)})).unwrap()["valid"],
            json!(true)
        );
    }

    #[test]
    fn is_internal_topic_detects_double_underscore_prefix() {
        assert_eq!(
            op_is_internal_topic(json!({"name": "__consumer_offsets"})).unwrap()["internal"],
            json!(true)
        );
        assert_eq!(
            op_is_internal_topic(json!({"name": "orders"})).unwrap()["internal"],
            json!(false)
        );
    }

    #[test]
    fn topics_collide_unifies_periods_to_underscores() {
        // The canonical metric-namespace collision: `my.topic` vs `my_topic`.
        let r = op_topics_collide(json!({"a": "my.topic", "b": "my_topic"})).unwrap();
        assert_eq!(r["collide"], json!(true));
        assert_eq!(r["unified_a"], json!("my_topic"));
        assert_eq!(r["unified_b"], json!("my_topic"));
        assert_eq!(r["a_has_collision_chars"], json!(true));
        assert_eq!(r["b_has_collision_chars"], json!(true));
        // Distinct names with no shared unified form do not collide.
        assert_eq!(
            op_topics_collide(json!({"a": "orders", "b": "events"})).unwrap()["collide"],
            json!(false)
        );
        // A plain name has no collision chars.
        assert_eq!(
            op_topics_collide(json!({"a": "orders", "b": "orders"})).unwrap()
                ["a_has_collision_chars"],
            json!(false)
        );
        // Multiple periods all unify.
        assert_eq!(
            op_topics_collide(json!({"a": "a.b.c", "b": "a_b_c"})).unwrap()["collide"],
            json!(true)
        );
        assert!(op_topics_collide(json!({"a": "x"})).is_err());
    }

    #[test]
    fn parse_brokers_splits_host_port_list() {
        let v = op_parse_brokers(json!({"brokers": "b1:9092, b2:9093 ,b3"})).unwrap();
        let brokers = v["brokers"].as_array().unwrap();
        assert_eq!(v["count"], json!(3));
        assert_eq!(brokers[0]["host"], json!("b1"));
        assert_eq!(brokers[0]["port"], json!(9092));
        assert_eq!(brokers[1]["port"], json!(9093), "whitespace trimmed");
        assert_eq!(brokers[2]["port"], Value::Null, "portless broker → null");
        assert!(op_parse_brokers(json!({"brokers": ""})).is_err());
    }

    #[test]
    fn build_brokers_inverts_parse_brokers() {
        // Parse → build round-trips the bootstrap string (modulo whitespace).
        let parsed = op_parse_brokers(json!({"brokers": "b1:9092,b2:9093,b3"})).unwrap();
        let built = op_build_brokers(json!({"brokers": parsed["brokers"]})).unwrap();
        assert_eq!(built["bootstrap"], json!("b1:9092,b2:9093,b3"));
        // Accepts {host,port} objects and bare host strings, mixed.
        assert_eq!(
            op_build_brokers(
                json!({"brokers": [{"host": "h1", "port": 9092}, "h2", {"host": "h3"}]})
            )
            .unwrap()["bootstrap"],
            json!("h1:9092,h2,h3")
        );
        // Missing host and empty list error.
        assert!(op_build_brokers(json!({"brokers": [{"port": 9092}]})).is_err());
        assert!(op_build_brokers(json!({"brokers": []})).is_err());
    }

    #[test]
    fn murmur2_matches_kafka_reference_vectors() {
        // Canonical values from Apache Kafka's own UtilsTest.testMurmur2.
        assert_eq!(murmur2(b"21"), -973_932_308);
        assert_eq!(murmur2(b"foobar"), -790_332_482);
        assert_eq!(murmur2(b"a-little-bit-long-string"), -985_981_536);
        assert_eq!(murmur2(b"a-little-bit-longer-string"), -1_486_304_829);
        assert_eq!(
            murmur2(b"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8"),
            -58_897_971
        );
    }

    #[test]
    fn partition_for_key_uses_to_positive_modulo() {
        // toPositive(murmur2("foobar")) = -790332482 & 0x7fffffff = 1357151166.
        // 1357151166 % 10 = 6.
        let v = op_partition_for_key(json!({"key": "foobar", "partitions": 10})).unwrap();
        assert_eq!(v["hash"], json!(-790_332_482));
        assert_eq!(v["partition"], json!(6));
        // Deterministic and always in [0, partitions).
        for p in 1u64..=8 {
            let r = op_partition_for_key(json!({"key": "user-42", "partitions": p})).unwrap();
            let part = r["partition"].as_u64().unwrap();
            assert!(part < p, "partition {part} must be < {p}");
        }
        assert!(op_partition_for_key(json!({"key": "x", "partitions": 0})).is_err());
    }

    #[test]
    fn crc32_matches_standard_iso_hdlc_check_value() {
        // Published CRC-32/ISO-HDLC check value for "123456789" is 0xCBF43926.
        assert_eq!(crc32_ieee(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32_ieee(b""), 0);
    }

    #[test]
    fn partition_for_key_crc32_uses_consistent_partitioner() {
        // librdkafka `consistent`: crc32(key) % partitions, unsigned (no
        // toPositive masking). crc32("foobar") = 0x9EF61F95 = 2666930069.
        let v = op_partition_for_key_crc32(json!({"key": "foobar", "partitions": 10})).unwrap();
        assert_eq!(v["crc32"], json!(0x9EF6_1F95u32));
        assert_eq!(v["partition"], json!(2_666_930_069u64 % 10));
        // The crc32 and murmur2 partitioners disagree for the same key/topology —
        // that's the whole point of exposing both.
        let murmur = op_partition_for_key(json!({"key": "foobar", "partitions": 10})).unwrap();
        assert_ne!(v["partition"], murmur["partition"]);
        // Deterministic and always in [0, partitions).
        for p in 1u64..=8 {
            let r = op_partition_for_key_crc32(json!({"key": "user-42", "partitions": p})).unwrap();
            assert!(r["partition"].as_u64().unwrap() < p);
        }
        assert!(op_partition_for_key_crc32(json!({"key": "x", "partitions": 0})).is_err());
    }

    #[test]
    fn java_string_hashcode_matches_jvm() {
        // Published JVM String.hashCode values.
        assert_eq!(java_string_hashcode(""), 0);
        assert_eq!(java_string_hashcode("a"), 97);
        assert_eq!(java_string_hashcode("abc"), 96354);
        assert_eq!(java_string_hashcode("test"), 3_556_498);
    }

    #[test]
    fn group_coordinator_partition_matches_kafka_formula() {
        // Utils.abs(hashCode) % 50 — verified against a JVM-hashCode oracle.
        let p = |g: &str| {
            op_group_coordinator_partition(json!({ "group": g })).unwrap()["partition"]
                .as_u64()
                .unwrap()
        };
        assert_eq!(p("abc"), 4, "abc → hashCode 96354 → partition 4 of 50");
        assert_eq!(p("test"), 48);
        assert_eq!(p("my-consumer-group"), 13);
        // The hash is the raw (signed) JVM hashCode.
        assert_eq!(
            op_group_coordinator_partition(json!({"group": "abc"})).unwrap()["hash"],
            json!(96354)
        );
        // Default partition count is 50; an explicit count is honored.
        assert_eq!(
            op_group_coordinator_partition(json!({"group": "abc"})).unwrap()["partitions"],
            json!(50)
        );
        for parts in 1u64..=64 {
            let v =
                op_group_coordinator_partition(json!({"group": "g", "partitions": parts})).unwrap();
            assert!(v["partition"].as_u64().unwrap() < parts);
        }
        assert!(op_group_coordinator_partition(json!({"group": "g", "partitions": 0})).is_err());
        assert!(op_group_coordinator_partition(json!({})).is_err());
    }

    #[test]
    fn range_assignment_matches_kafka_range_assignor() {
        // 7 partitions, 3 consumers: the first absorbs the remainder → 3/2/2.
        let v =
            op_range_assignment(json!({"partitions": 7, "consumers": ["c0", "c1", "c2"]})).unwrap();
        let a = &v["assignment"];
        assert_eq!(a["c0"], json!([0, 1, 2]));
        assert_eq!(a["c1"], json!([3, 4]));
        assert_eq!(a["c2"], json!([5, 6]));
        // Even split: 6 partitions, 3 consumers → 2 each, contiguous.
        let even =
            op_range_assignment(json!({"partitions": 6, "consumers": ["a", "b", "c"]})).unwrap();
        assert_eq!(even["assignment"]["a"], json!([0, 1]));
        assert_eq!(even["assignment"]["c"], json!([4, 5]));
        // More consumers than partitions: extras get nothing.
        let sparse =
            op_range_assignment(json!({"partitions": 2, "consumers": ["x", "y", "z"]})).unwrap();
        assert_eq!(sparse["assignment"]["x"], json!([0]));
        assert_eq!(sparse["assignment"]["y"], json!([1]));
        assert_eq!(sparse["assignment"]["z"], json!([]));
        // Member ids are sorted before slicing, regardless of input order.
        let unsorted =
            op_range_assignment(json!({"partitions": 4, "consumers": ["c2", "c0", "c1"]})).unwrap();
        assert_eq!(unsorted["assignment"]["c0"], json!([0, 1]));
        assert_eq!(unsorted["assignment"]["c2"], json!([3]));
        // Errors.
        assert!(op_range_assignment(json!({"consumers": ["a"]})).is_err());
        assert!(op_range_assignment(json!({"partitions": 4})).is_err());
        assert!(op_range_assignment(json!({"partitions": 0, "consumers": ["a"]})).is_err());
        assert!(op_range_assignment(json!({"partitions": 4, "consumers": []})).is_err());
    }

    #[test]
    fn roundrobin_assignment_matches_kafka_roundrobin_assignor() {
        // 7 partitions, 3 consumers: partition p → member p%3, so counts 3/2/2
        // but interleaved (vs RangeAssignor's contiguous blocks).
        let v = op_roundrobin_assignment(json!({"partitions": 7, "consumers": ["c0", "c1", "c2"]}))
            .unwrap();
        let a = &v["assignment"];
        assert_eq!(a["c0"], json!([0, 3, 6]));
        assert_eq!(a["c1"], json!([1, 4]));
        assert_eq!(a["c2"], json!([2, 5]));
        // Even split: counts within one, still interleaved.
        let even = op_roundrobin_assignment(json!({"partitions": 6, "consumers": ["a", "b", "c"]}))
            .unwrap();
        assert_eq!(even["assignment"]["a"], json!([0, 3]));
        assert_eq!(even["assignment"]["b"], json!([1, 4]));
        assert_eq!(even["assignment"]["c"], json!([2, 5]));
        // More consumers than partitions: the tail members get nothing.
        let sparse =
            op_roundrobin_assignment(json!({"partitions": 2, "consumers": ["x", "y", "z"]}))
                .unwrap();
        assert_eq!(sparse["assignment"]["x"], json!([0]));
        assert_eq!(sparse["assignment"]["y"], json!([1]));
        assert_eq!(sparse["assignment"]["z"], json!([]));
        // Member ids are sorted before the round-robin walk.
        let unsorted =
            op_roundrobin_assignment(json!({"partitions": 4, "consumers": ["c2", "c0", "c1"]}))
                .unwrap();
        assert_eq!(unsorted["assignment"]["c0"], json!([0, 3]));
        assert_eq!(unsorted["assignment"]["c1"], json!([1]));
        assert_eq!(unsorted["assignment"]["c2"], json!([2]));
        // Errors mirror range_assignment (shared input parsing).
        assert!(op_roundrobin_assignment(json!({"consumers": ["a"]})).is_err());
        assert!(op_roundrobin_assignment(json!({"partitions": 0, "consumers": ["a"]})).is_err());
        assert!(op_roundrobin_assignment(json!({"partitions": 4, "consumers": []})).is_err());
    }

    #[test]
    fn format_offset_maps_sentinels_both_directions() {
        assert_eq!(
            op_format_offset(json!({"offset": -1})).unwrap()["name"],
            json!("latest")
        );
        assert_eq!(
            op_format_offset(json!({"offset": -2})).unwrap()["name"],
            json!("earliest")
        );
        assert_eq!(
            op_format_offset(json!({"offset": 42})).unwrap()["name"],
            Value::Null,
            "concrete offset has no sentinel name"
        );
        assert_eq!(
            op_format_offset(json!({"name": "EARLIEST"})).unwrap()["offset"],
            json!(-2),
            "name lookup is case-insensitive"
        );
        assert!(op_format_offset(json!({"name": "whoknows"})).is_err());
        assert!(op_format_offset(json!({})).is_err());
    }
}
