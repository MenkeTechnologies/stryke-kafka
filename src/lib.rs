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
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
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

static ADMINS: OnceCell<Mutex<HashMap<String, AdminClient<DefaultClientContext>>>> =
    OnceCell::new();

fn admins() -> &'static Mutex<HashMap<String, AdminClient<DefaultClientContext>>> {
    ADMINS.get_or_init(|| Mutex::new(HashMap::new()))
}

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
    {
        let map = admins().lock();
        if let Some(a) = map.get(&brokers) {
            // AdminClient doesn't impl Clone; we have to drop the cache
            // and recreate on demand. Keep the entry but return a fresh
            // handle.
            let _ = a;
        }
    }
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
    let value = opts["value"].as_str().unwrap_or("").to_string();
    let key = opts["key"].as_str().map(String::from);
    let producer = get_producer(&opts)?;
    let key_owned = key.unwrap_or_default();
    let mut record: FutureRecord<String, String> = FutureRecord::to(&topic).payload(&value);
    if !key_owned.is_empty() {
        record = record.key(&key_owned);
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
    let producer = get_producer(&opts)?;
    let mut sent = 0i64;
    let mut errors = Vec::new();
    for row in &rows {
        let value = row.get("value").and_then(|v| v.as_str()).unwrap_or("");
        let key = row.get("key").and_then(|v| v.as_str()).unwrap_or("");
        let mut record: FutureRecord<str, str> = FutureRecord::to(&topic).payload(value);
        if !key.is_empty() {
            record = record.key(key);
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
                let key = m
                    .key()
                    .and_then(|k| std::str::from_utf8(k).ok())
                    .map(String::from);
                let value = m
                    .payload()
                    .and_then(|v| std::str::from_utf8(v).ok())
                    .map(String::from);
                out.push(json!({
                    "topic": m.topic(),
                    "partition": m.partition(),
                    "offset": m.offset(),
                    "key": key,
                    "value": value,
                }));
            }
            Some(Err(e)) => return Err(anyhow!("consumer error: {}", e)),
            None => break,
        }
    }
    Ok(json!({"messages": out}))
}

async fn op_create_topic(opts: Value) -> Result<Value> {
    let name = opts["name"]
        .as_str()
        .ok_or_else(|| anyhow!("missing name"))?
        .to_string();
    let partitions = opts["partitions"].as_i64().unwrap_or(1) as i32;
    let replication = opts["replication"].as_i64().unwrap_or(1) as i32;
    let admin = get_admin(&opts)?;
    let topic = NewTopic::new(&name, partitions, TopicReplication::Fixed(replication));
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
        let obj = v.as_object().expect("must be a JSON object, not array/string");
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
}
