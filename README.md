```
 ███████╗████████╗██████╗ ██╗   ██╗██╗  ██╗███████╗
 ██╔════╝╚══██╔══╝██╔══██╗╚██╗ ██╔╝██║ ██╔╝██╔════╝
 ███████╗   ██║   ██████╔╝ ╚████╔╝ █████╔╝ █████╗
 ╚════██║   ██║   ██╔══██╗  ╚██╔╝  ██╔═██╗ ██╔══╝
 ███████║   ██║   ██║  ██║   ██║   ██║  ██╗███████╗
 ╚══════╝   ╚═╝   ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝╚══════╝
                   [ k a f k a ]
```

[![CI](https://github.com/MenkeTechnologies/stryke-kafka/actions/workflows/ci.yml/badge.svg)](https://github.com/MenkeTechnologies/stryke-kafka/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![stryke](https://img.shields.io/badge/stryke-package-cyan.svg)](https://github.com/MenkeTechnologies/strykelang)

### `[APACHE KAFKA CLIENT FOR STRYKE // PRODUCER + CONSUMER + TOPIC/CLUSTER ADMIN]`

> *"Streams without the JVM."*

Apache Kafka client for stryke — producer (keys, headers, partitions,
binary payloads), consumer (offset commit, headers), consumer-group lag /
watermarks / time-based offsets, and topic / cluster / config admin. Opt-in
package tier, kept out of the stryke core binary so the daily-driver install
stays slim.

### [`strykelang`](https://github.com/MenkeTechnologies/strykelang) &middot; [`MenkeTechnologiesMeta`](https://github.com/MenkeTechnologies/MenkeTechnologiesMeta) · [`stryke-redis`](https://github.com/MenkeTechnologies/stryke-redis) · [`stryke-mongo`](https://github.com/MenkeTechnologies/stryke-mongo) · [`stryke-demo`](https://github.com/MenkeTechnologies/stryke-demo)

---

## Table of Contents

- [\[0x00\] Why this is a package, not a builtin](#0x00-why-this-is-a-package-not-a-builtin)
- [\[0x01\] Install](#0x01-install)
- [\[0x02\] Quick start](#0x02-quick-start)
- [\[0x03\] CLI: `kafka`](#0x03-cli-kafka)
- [\[0x04\] API reference](#0x04-api-reference)
- [\[0x05\] FFI layer](#0x05-ffi-layer)
- [\[0x06\] Tests](#0x06-tests)
- [\[0x07\] Dev workflow](#0x07-dev-workflow)
- [\[0x08\] Layout](#0x08-layout)
- [\[0x09\] Roadmap](#0x09-roadmap)
- [\[0xFF\] License](#0xff-license)

---

## [0x00] Why this is a package, not a builtin

Kafka integration requires librdkafka, the canonical C client that every
Kafka tool eventually wraps. The Rust binding (`rdkafka`) builds against
either a system library or a vendored cmake-compiled archive. Either way
the artifact is big enough that it doesn't belong in stryke core. Opt in
once, get producer + consumer + topic/cluster admin tooling.

`stryke-kafka` ships a thin stryke library plus a Rust cdylib
(`libstryke_kafka.{dylib,so}`) dlopened in-process. The cdylib
statically links librdkafka via `rdkafka`'s `cmake-build` feature, so
it is portable across glibc / musl / macOS without depending on a
system `librdkafka.so`.

## [0x01] Install

From a release (no rustc + librdkafka build on the consumer machine):

```sh
s pkg install -g github.com/MenkeTechnologies/stryke-kafka
```

From a local checkout:

```sh
cd ~/projects/stryke-kafka
cargo build --release            # first build compiles librdkafka via cmake (~3-5 min)
s pkg install -g .               # cdylib lands in ~/.stryke/store/kafka@<version>/
```

Or:

```sh
make install
```

The cdylib is dlopened in-process on first `use Kafka`. A shared tokio
runtime + `FutureProducer` cache per brokers tuple is held in `OnceCell`
— producer batching/compression now works as designed (the v1 helper's
fork-per-call defeated it). The first build is the slow one; subsequent
builds reuse the cached librdkafka archive.

## [0x02] Quick start

```stryke
use Kafka

$ENV{KAFKA_BROKERS} = "localhost:9092"

# Ping (metadata fetch).
p Kafka::ping() ? "alive" : "down"

# Produce.
my $r = Kafka::produce "events", "hello stryke", key => "k1"
p "wrote $r->{topic}/$r->{partition}@$r->{offset}"

# Bulk produce — array of { value, key? } hashrefs; topic is a named arg.
Kafka::produce_many [
    { value => "a", key => "1" },
    { value => "b", key => "2" },
    { value => "c", key => "3" },
], topic => "events"

# Snapshot consume — up to `limit` messages within `timeout_ms`.
my @msgs = Kafka::consume "events",
    group      => "stryke-demo",
    limit      => 100,
    timeout_ms => 5_000
@msgs |> ep

# Callback per message (snapshot pull, then iterate).
Kafka::consume_stream "events",
    callback => sub ($m) {
        p "$m->{partition}.$m->{offset}: $m->{value}"
    }

# Admin / metadata.
p $_ for Kafka::topics()

Kafka::create_topic "new-topic", partitions => 6, replication => 1
my $info = Kafka::describe "new-topic"
p to_json $info
```

> `Kafka::lag $group, topic => "..."` reports per-partition lag (committed
> offset vs. high watermark). `Kafka::watermarks` and
> `Kafka::offsets_for_times` cover raw offset introspection.

Brokers — pass on every call OR set the env var:

```stryke
$ENV{KAFKA_BROKERS} = "localhost:9092"        # default
Kafka::topics(brokers => "kafka-1:9094,kafka-2:9094") |> ep
```

SASL / SSL connection options are deferred in v0.2.x — the only
per-call connection opt is `brokers`.

## [0x03] CLI: `kafka`

```sh
kafka produce send my-topic --value='hello' --key=k1
kafka produce send my-topic --value='hello' --header source=cli
cat msgs.ndjson | kafka produce stream --default-topic=my-topic

kafka consume my-topic --group=stryke-test --offset-reset=earliest --max=100
kafka consume my-topic --value-mode=json --idle-ms=60000

kafka topics
kafka admin describe my-topic
kafka groups
kafka cluster
kafka lag --group=my-consumer-group [--topic=my-topic]

kafka admin create-topic new-topic --partitions=6 --replication=3 \
    --conf cleanup.policy=compact --conf retention.ms=86400000
kafka admin delete-topic new-topic

kafka ping
kafka build                                  # cargo build --release
kafka version
```

Global flags (env vars in brackets):

```
-b, --brokers HOST1:9092,HOST2:9092  [$KAFKA_BROKERS]
    --security-protocol PLAINTEXT|SSL|SASL_PLAINTEXT|SASL_SSL
                                     [$KAFKA_SECURITY_PROTOCOL]
    --sasl-mechanism PLAIN|SCRAM-SHA-256|SCRAM-SHA-512|GSSAPI|OAUTHBEARER
                                     [$KAFKA_SASL_MECHANISM]
    --sasl-username U                [$KAFKA_SASL_USERNAME]
    --sasl-password PW               [$KAFKA_SASL_PASSWORD]
    --ssl-ca PATH                    [$KAFKA_SSL_CA]
    --ssl-cert PATH                  [$KAFKA_SSL_CERT]
    --ssl-key PATH                   [$KAFKA_SSL_KEY]
    --ssl-key-password PW            [$KAFKA_SSL_KEY_PASSWORD]
-X, --extra-conf K=V                 raw librdkafka override (repeatable)
    --client-id NAME                 default: stryke-kafka-helper
    --timeout-ms MS                  default: 10000
```

## [0x04] API reference

### Producer

```stryke
Kafka::produce       $topic, $value, %opts → { topic, partition, offset }
Kafka::produce_many  \@rows, %opts → $sent          # requires topic => $name
```

`produce` opts: `key`, `partition`, `timestamp` (epoch millis),
`headers => { k => v }`, `encoding` (`utf8` default, `base64`, `hex`).
`produce_many` rows: `{ value, key?, partition?, headers?, encoding? }`;
the target topic is the required `topic => "..."` named arg (an `encoding`
opt sets the default for all rows).

### Consumer

```stryke
Kafka::consume         $topic, %opts → @messages
Kafka::consume_stream  $topic, %opts → $count             # callback per msg
```

Opts: `group`, `limit` (default 10), `timeout_ms` (default 5000),
`encoding` (`utf8`/`base64`/`hex`), `commit` (commit offsets after the
drain — needs `group`). Snapshot-style: subscribe, poll up to `limit`
messages or until `timeout_ms` runs out. `consume_stream` pulls the same
snapshot and fires `callback` per message.

Message shape:

```
{
  topic, partition, offset, timestamp,
  key,                         # framed per `encoding` (null if missing)
  value,                       # framed per `encoding` (utf8 lossy by default)
  headers,                     # { k => v } object
}
```

### Consumer offsets

```stryke
Kafka::lag                $group, %opts → { group, topic, partitions, total_lag }
                                          # opts: topic (required)
Kafka::watermarks         $topic, %opts → { topic, partitions, total }
                                          # partitions: [{partition, low, high, count}]
Kafka::offsets_for_times  $topic, $ts_ms, %opts → { topic, timestamp, partitions }
```

### Admin

```stryke
Kafka::topics             %opts → @names
Kafka::describe           $topic, %opts → { topic, partition_count, partitions }
                                          # partitions: [{id, leader, replicas, isr}]
Kafka::groups             %opts → @groups            # [{name, state, ...}]
Kafka::cluster            %opts → { brokers, topic_count }   # brokers: [{id, host, port}]
Kafka::create_topic       $name, %opts → { created, errors } # opts: partitions, replication, config
Kafka::delete_topic       $name, %opts → { deleted, errors }
Kafka::create_partitions  $name, $partitions, %opts → { topic, altered, errors }
Kafka::describe_configs   %opts → { resources, errors }      # resource_type, resource_name
Kafka::alter_configs      %opts → { altered, errors }        # + entries => { k => v }
Kafka::delete_groups      \@groups, %opts → { deleted, errors }
Kafka::ping               %opts → 1 | ""
```

### Plumbing

```stryke
Kafka::version()        → $version_string       # cdylib's CARGO_PKG_VERSION
```

### Pure helpers (no broker)

```stryke
Kafka::valid_topic_name($name)  → { name, valid, reason }   # 1-249 chars of [a-zA-Z0-9._-], not . / ..
Kafka::sanitize_topic_name($name, $replacement?) → { name, sanitized, changed }   # coerce arbitrary input into a valid topic name (illegal → _, truncate 249, reserved/empty → replacement)
Kafka::is_internal_topic($name) → 1 | ""                    # `__` prefix
Kafka::topics_collide($a, $b)   → 1 | ""                    # metric-namespace collision: equal after `.`→`_` (my.topic vs my_topic)
Kafka::parse_brokers($str)      → @{ {host, port} }          # bootstrap.servers list
Kafka::build_brokers(\@brokers) → $str                      # {host,port} list → bootstrap.servers; inverse of parse_brokers
Kafka::normalize_brokers($str, %opts) → $str                # fill default :9092 (opts default_port), trim, dedupe → canonical bootstrap.servers
Kafka::partition_for_key($key, $partitions) → { partition, hash }   # JVM default partitioner: toPositive(murmur2(key)) % partitions
Kafka::partition_for_key_crc32($key, $partitions) → { partition, crc32 }   # librdkafka `consistent` partitioner: crc32(key) % partitions (non-JVM clients)
Kafka::partition_for_key_fnv1a($key, $partitions) → { partition, fnv1a }   # librdkafka `fnv1a` partitioner: fnv1a(key) % partitions (the third option)
Kafka::group_coordinator_partition($group, $partitions=50) → { group, partition, hash, partitions }   # __consumer_offsets partition for a group: abs(groupId.hashCode()) % 50
Kafka::transaction_coordinator_partition($transactional_id, $partitions=50) → { transactional_id, partition, hash, partitions }   # __transaction_state partition: same formula over the transaction log
Kafka::range_assignment($partitions, @consumers) → { assignment:{member:[partition…]}, partitions, consumers }   # default RangeAssignor: predict a rebalance's partition assignment
Kafka::roundrobin_assignment($partitions, @consumers) → { assignment:{member:[partition…]}, partitions, consumers }   # RoundRobinAssignor: interleaved (partition p → member p%N)
Kafka::sticky_assignment($partitions, \@consumers, \%previous?) → { assignment:{member:[partition…]}, partitions, consumers }   # StickyAssignor (KIP-54): balanced but preserves \%previous to minimize rebalance movement
Kafka::replica_assignment($partitions, $replication_factor, \@brokers, %opts) → { assignment:[{partition, replicas:[broker…]}], partitions, replication_factor }   # default rack-unaware partition→broker placement (kafka-topics --create); opts: start_index, start_partition
Kafka::assignment_by_broker(\@assignment) → { brokers:[{broker, leader, replicas, leader_count, replica_count}] }   # invert a partition→broker assignment to a per-broker view (what each broker hosts/leads); inverse of replica_assignment
Kafka::partition_owners(\%assignment) → { owners:{partition=>consumer}, partitions, consumers }   # invert a consumer assignment ({consumer=>[partitions]}) to "who owns partition N"; consumer-side analog of assignment_by_broker (overlap rejected)
Kafka::assignment_diff(\%previous, \%current) → { revoked:{member:[partition…]}, assigned:{member:[partition…]}, moved }   # rebalance plan: what each member loses/gains; `revoked` = the cooperative-protocol give-up set
Kafka::format_offset($n|$name)  → { offset, name }          # -1 ⇄ latest, -2 ⇄ earliest
```

`partition_for_key` is a faithful port of Kafka's `Utils.murmur2` (seed
`0x9747b28c`) plus the default partitioner's `toPositive(hash) %
partitions`, so it predicts a keyed record's partition offline — no broker
round trip.

## [0x05] FFI layer

Each `Kafka::*` wrapper builds a JSON args dict and calls a sibling
`kafka__*` symbol resolved out of `libstryke_kafka.{dylib,so}`. The
cdylib is dlopened in-process on first `use Kafka` (via stryke's
`pkg::commands::try_load_ffi_for` resolver hook). Its exports cover the
admin/produce/consume surface (`kafka__pkg_version`, `kafka__ping`,
`kafka__cluster`, `kafka__topics`, `kafka__describe`, `kafka__groups`,
`kafka__produce`, `kafka__consume`, `kafka__create_topic`,
`kafka__delete_topic`, …) plus broker-free helpers
(`kafka__valid_topic_name`, `kafka__is_internal_topic`,
`kafka__topics_collide`,
`kafka__parse_brokers`, `kafka__build_brokers`, `kafka__normalize_brokers`, `kafka__partition_for_key`,
`kafka__format_offset`). The authoritative list is
`[ffi].exports` in `stryke.toml`.

Errors come back as a `{error}` JSON payload; the stryke wrapper dies
with `Kafka::<op>: <reason>`.

<details>
<summary>v1 wire shape (historical helper binary)</summary>

```sh
stryke-kafka-helper -b localhost:9092 produce send my-topic --value=hello
stryke-kafka-helper -b localhost:9092 consume my-topic --max=10 --idle-ms=5000
stryke-kafka-helper -b localhost:9092 lag --group=my-group
stryke-kafka-helper -b localhost:9092 admin create-topic new --partitions=3
stryke-kafka-helper -b localhost:9092 topics
```

Output:

* `produce send` → `{topic, partition, offset}`
* `produce stream` → reads NDJSON from stdin, emits `{sent, last}` at the end
* `consume` → NDJSON, one message per line
* `topics`, `groups`, `lag` → NDJSON, one row per line
* `cluster`, `describe`, `create-topic`, `delete-topic`, `ping` → single JSON
* `ping` also prints `ok brokers=N topics=N` on stdout, exit 0

</details>

## [0x06] Tests

```sh
cargo test                                          # compiles, no live calls
KAFKA_BROKERS=localhost:9092 s test t/              # live round-trip
```

The end-to-end suite creates a temp topic, produces single + bulk messages,
consumes them back, checks the count, and deletes the topic. Skips cleanly
when `$KAFKA_BROKERS` isn't set or the broker isn't reachable.

Local test broker (KRaft mode, no ZooKeeper):

```sh
docker run --rm -p 9092:9092 \
    -e KAFKA_CFG_NODE_ID=0 \
    -e KAFKA_CFG_PROCESS_ROLES=controller,broker \
    -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@127.0.0.1:9093 \
    -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
    -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:9092 \
    -e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT \
    -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    bitnami/kafka:3.9
```

## [0x07] Dev workflow

```sh
make             # release build (first time: ~3-5 min for librdkafka)
make debug
make test
make install
make clean
```

## [0x08] Layout

```
stryke-kafka/
  stryke.toml                      # stryke package manifest
  Cargo.toml                       # Rust helper crate manifest
  Makefile
  src/lib.rs                       # single-file cdylib
  lib/
    Kafka.stk                      # `use Kafka`
  t/
    test_kafka.stk                 # live round-trip
    test_stryke_kafka_surface.stk
  examples/
    produce_one.stk
    tail_topic.stk
    group_lag.stk
    discover.stk
    topics.stk
  .github/workflows/
    ci.yml                         # cargo + bitnami/kafka service for live tests
    release.yml                    # cross-compile + GH release on tag push
```

## [0x09] Roadmap

Shipped: produce with keys/headers/partitions/timestamps + binary framing,
consume with headers/binary/offset-commit, consumer-group lag, watermarks,
time-based offset lookup, topic config on create, create_partitions,
describe/alter configs, and delete_groups.

| Open | Later |
|---|---|
| SASL / SSL connection options | Schema Registry (Avro / Protobuf) value modes |
| Long-running consumer daemon (callback streaming) | Streams DSL (joins, windowed agg) |
| `delete_records` (truncate to offset) | Transactional producer (EOS) |

## [0xFF] License

MIT.
