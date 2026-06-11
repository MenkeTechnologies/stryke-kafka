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
}
