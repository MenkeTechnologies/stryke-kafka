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

### `[APACHE KAFKA CLIENT FOR STRYKE // PRODUCER + CONSUMER + ADMIN + CONSUMER-GROUP LAG]`

> *"Streams without the JVM."*

Apache Kafka client for stryke — producer, consumer, admin, and
consumer-group lag. Opt-in package tier, kept out of the stryke core
binary so the daily-driver install stays slim.

### [`strykelang`](https://github.com/MenkeTechnologies/strykelang) &middot; [`MenkeTechnologiesMeta`](https://github.com/MenkeTechnologies/MenkeTechnologiesMeta) · [`stryke-redis`](https://github.com/MenkeTechnologies/stryke-redis) · [`stryke-mongo`](https://github.com/MenkeTechnologies/stryke-mongo) · [`stryke-demo`](https://github.com/MenkeTechnologies/stryke-demo)

---

## Table of Contents

- [\[0x00\] Why this is a package, not a builtin](#0x00-why-this-is-a-package-not-a-builtin)
- [\[0x01\] Install](#0x01-install)
- [\[0x02\] Quick start](#0x02-quick-start)
- [\[0x03\] CLI: `kafka`](#0x03-cli-kafka)
- [\[0x04\] API reference](#0x04-api-reference)
- [\[0x05\] Helper protocol](#0x05-helper-protocol)
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
once, get full producer + consumer + admin + lag tooling.

`stryke-kafka` ships a thin stryke library plus a Rust helper binary
(`stryke-kafka-helper`, ~2.4 MB). The helper statically links librdkafka
via `rdkafka`'s `cmake-build` feature, so the binary is portable across
glibc / musl / macOS without depending on a system `libkafka.so`.

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
my $r = Kafka::produce "events", "hello stryke", key => "k1",
                       headers => { source => "stryke" }
p "wrote $r->{topic}/$r->{partition}@$r->{offset}"

# Bulk produce — array of hashrefs.
Kafka::produce_many [
    { topic => "events", value => "a", key => "1" },
    { topic => "events", value => "b", key => "2" },
    { topic => "events", value => "c", key => "3" },
]

# Consume up to N messages from a topic.
my @msgs = Kafka::consume "events",
    group        => "stryke-demo",
    offset_reset => "earliest",
    max          => 100,
    idle_ms      => 5_000
@msgs |> ep

# Streaming consume — callback per message, no buffering.
Kafka::consume_stream "events",
    value_mode => "json",
    callback   => sub ($m) {
        p "$m->{partition}.$m->{offset}: $m->{value}"
    }

# Admin / metadata.
my @topics = Kafka::topics()
p to_json $_ for @topics

Kafka::create_topic "new-topic", partitions => 6, replication => 1,
                                 configs => { "cleanup.policy" => "compact" }
my $info = Kafka::describe "new-topic"
p to_json $info

# Consumer-group lag (the killer feature).
my @lag = Kafka::lag "my-consumer-group"
for my $row (@lag) {
    next if defined $row->{total_lag}
    p "$row->{topic}.$row->{partition}: $row->{lag}"
}
```

SASL / SSL — pass on every call OR set env vars:

```stryke
my %prod = (
    brokers           => "kafka-1:9094,kafka-2:9094",
    security_protocol => "SASL_SSL",
    sasl_mechanism    => "SCRAM-SHA-512",
    sasl_username     => "user",
    sasl_password     => $ENV{KAFKA_PWD},
    ssl_ca            => "/etc/ssl/ca.pem",
)
Kafka::topics(%prod) |> ep
Kafka::produce("events", "x", %prod)
```

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
Kafka::produce_many  \@rows, %opts → { sent, last }
```

`produce` opts: `key`, `partition`, `headers` (hashref).
`produce_many` rows: `{ topic?, value, key?, partition?, headers? }`. Pass
`default_topic => "..."` to omit `topic` per row.

### Consumer

```stryke
Kafka::consume         $topics, %opts → @messages
Kafka::consume_stream  $topics, %opts → $count            # callback per msg
```

`$topics` is comma-separated. Opts: `group`, `offset_reset` (`earliest` /
`latest`), `max`, `idle_ms`, `value_mode` (`text` / `binary` / `json`),
`commit` (bool — defaults off, leave offsets untouched).

Message shape:

```
{
  topic, partition, offset, timestamp,
  key,                         # null if missing
  value,                       # decoded per --value-mode
  headers,                     # { k => v, ... }
}
```

### Admin

```stryke
Kafka::topics        %opts → @topics            # [{name, partitions, error}]
Kafka::describe      $topic, %opts → { name, partition_count, partitions }
Kafka::groups        %opts → @groups            # [{name, state, ...}]
Kafka::cluster       %opts → { broker_count, controller_id, brokers, topic_count }
Kafka::lag           $group, %opts → @rows      # ends with {total_lag}
Kafka::create_topic  $name, %opts → { name, created, error? }
Kafka::delete_topic  $name, %opts → { name, deleted, error? }
Kafka::ping          %opts → 1 | ""
```

### Helper plumbing

```stryke
Kafka::helper_path()    → $abs_path
Kafka::ensure_built()   → $abs_path
Kafka::version()        → "stryke-kafka-helper X.Y.Z"
```

## [0x05] Helper protocol

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
  src/
    main.rs                        # CLI dispatch
    common.rs                      # config + output helpers
    produce.rs                     # producer commands
    consume.rs                     # consumer commands
    admin.rs                       # admin + metadata + lag
  lib/
    Kafka.stk                      # `use Kafka`
  bin/
    kafka.stk                      # `kafka` CLI
    kafka-build.stk
  t/
    test_kafka.stk                 # live round-trip
  examples/
    produce_one.stk
    tail_topic.stk
    group_lag.stk
  .github/workflows/
    ci.yml                         # cargo + bitnami/kafka service for live tests
    release.yml                    # cross-compile + GH release on tag push
```

## [0x09] Roadmap

| v1 (this release) | v2+ |
|---|---|
| Produce / consume / admin / lag | Schema Registry (Avro / Protobuf) |
| SASL / SSL config flags | Streams DSL (joins, windowed agg) |
| One-shot consume per call | Long-running consumer daemon over a Unix socket |
| Text / binary / JSON value modes | Avro / Protobuf value modes via Schema Registry |

## [0xFF] License

MIT.
