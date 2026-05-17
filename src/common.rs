//! Shared plumbing: ClientConfig builder, output writers.

use std::io::{self, BufWriter, Write};
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Args;
use rdkafka::config::ClientConfig;

/// Global flags every subcommand pulls in via clap's flatten.
#[derive(Args, Debug, Clone)]
pub struct KafkaConn {
    /// `host1:9092,host2:9092` — bootstrap servers.
    #[arg(long, short = 'b', env = "KAFKA_BROKERS", global = true)]
    pub brokers: Option<String>,

    /// `PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL`.
    #[arg(long, env = "KAFKA_SECURITY_PROTOCOL", global = true)]
    pub security_protocol: Option<String>,

    #[arg(long, env = "KAFKA_SASL_MECHANISM", global = true)]
    pub sasl_mechanism: Option<String>,

    #[arg(long, env = "KAFKA_SASL_USERNAME", global = true)]
    pub sasl_username: Option<String>,

    #[arg(
        long,
        env = "KAFKA_SASL_PASSWORD",
        global = true,
        hide_env_values = true
    )]
    pub sasl_password: Option<String>,

    #[arg(long, env = "KAFKA_SSL_CA", global = true)]
    pub ssl_ca: Option<String>,
    #[arg(long, env = "KAFKA_SSL_CERT", global = true)]
    pub ssl_cert: Option<String>,
    #[arg(long, env = "KAFKA_SSL_KEY", global = true)]
    pub ssl_key: Option<String>,
    #[arg(
        long,
        env = "KAFKA_SSL_KEY_PASSWORD",
        global = true,
        hide_env_values = true
    )]
    pub ssl_key_password: Option<String>,

    /// Repeatable raw librdkafka config overrides.
    #[arg(long = "extra-conf", short = 'X', global = true, value_name = "K=V")]
    pub extra_conf: Vec<String>,

    /// Connection / request timeout in milliseconds.
    #[arg(long, default_value_t = 10_000, global = true)]
    pub timeout_ms: u64,

    /// Client.id reported to the broker.
    #[arg(
        long,
        default_value = "stryke-kafka-helper",
        global = true
    )]
    pub client_id: String,
}

impl KafkaConn {
    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms)
    }

    pub fn build_config(&self) -> Result<ClientConfig> {
        let brokers = self
            .brokers
            .clone()
            .ok_or_else(|| anyhow::anyhow!("--brokers required (or $KAFKA_BROKERS)"))?;
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", brokers)
            .set("client.id", &self.client_id)
            .set("socket.timeout.ms", self.timeout_ms.to_string());

        if let Some(p) = &self.security_protocol {
            cfg.set("security.protocol", p);
        }
        if let Some(m) = &self.sasl_mechanism {
            cfg.set("sasl.mechanism", m);
        }
        if let Some(u) = &self.sasl_username {
            cfg.set("sasl.username", u);
        }
        if let Some(p) = &self.sasl_password {
            cfg.set("sasl.password", p);
        }
        if let Some(ca) = &self.ssl_ca {
            cfg.set("ssl.ca.location", ca);
        }
        if let Some(c) = &self.ssl_cert {
            cfg.set("ssl.certificate.location", c);
        }
        if let Some(k) = &self.ssl_key {
            cfg.set("ssl.key.location", k);
        }
        if let Some(pw) = &self.ssl_key_password {
            cfg.set("ssl.key.password", pw);
        }
        for kv in &self.extra_conf {
            let (k, v) = kv
                .split_once('=')
                .with_context(|| format!("invalid -X k=v form: {kv}"))?;
            cfg.set(k, v);
        }
        Ok(cfg)
    }
}

pub fn emit_json<T: serde::Serialize>(v: &T) -> Result<()> {
    let stdout = io::stdout();
    let mut w = BufWriter::new(stdout.lock());
    serde_json::to_writer(&mut w, v)?;
    w.write_all(b"\n")?;
    Ok(())
}

pub fn emit_ndjson_line<T: serde::Serialize, W: Write>(w: &mut W, v: &T) -> Result<()> {
    serde_json::to_writer(&mut *w, v)?;
    w.write_all(b"\n")?;
    Ok(())
}
