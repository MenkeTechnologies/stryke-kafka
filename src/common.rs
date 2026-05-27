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

#[cfg(test)]
mod tests {
    use super::*;

    fn base_conn() -> KafkaConn {
        KafkaConn {
            brokers: Some("localhost:9092".into()),
            security_protocol: None,
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
            ssl_ca: None,
            ssl_cert: None,
            ssl_key: None,
            ssl_key_password: None,
            extra_conf: vec![],
            timeout_ms: 5_000,
            client_id: "test-client".into(),
        }
    }

    // ─── KafkaConn::timeout ──────────────────────────────────────────

    #[test]
    fn timeout_converts_ms_to_duration() {
        let mut c = base_conn();
        c.timeout_ms = 12_345;
        assert_eq!(c.timeout(), Duration::from_millis(12_345));
    }

    #[test]
    fn timeout_zero_allowed() {
        let mut c = base_conn();
        c.timeout_ms = 0;
        assert_eq!(c.timeout(), Duration::from_millis(0));
    }

    // ─── KafkaConn::build_config ─────────────────────────────────────

    #[test]
    fn build_config_requires_brokers() {
        let mut c = base_conn();
        c.brokers = None;
        let err = c.build_config().unwrap_err();
        assert!(format!("{err}").contains("brokers"));
    }

    #[test]
    fn build_config_sets_bootstrap_and_client_id() {
        let c = base_conn();
        let cfg = c.build_config().unwrap();
        assert_eq!(cfg.get("bootstrap.servers"), Some("localhost:9092"));
        assert_eq!(cfg.get("client.id"), Some("test-client"));
        assert_eq!(cfg.get("socket.timeout.ms"), Some("5000"));
    }

    #[test]
    fn build_config_omits_unset_security_keys() {
        let cfg = base_conn().build_config().unwrap();
        assert_eq!(cfg.get("security.protocol"), None);
        assert_eq!(cfg.get("sasl.mechanism"), None);
        assert_eq!(cfg.get("sasl.username"), None);
        assert_eq!(cfg.get("ssl.ca.location"), None);
    }

    #[test]
    fn build_config_propagates_sasl_and_ssl_fields() {
        let mut c = base_conn();
        c.security_protocol = Some("SASL_SSL".into());
        c.sasl_mechanism = Some("PLAIN".into());
        c.sasl_username = Some("alice".into());
        c.sasl_password = Some("hunter2".into());
        c.ssl_ca = Some("/etc/ssl/ca.pem".into());
        c.ssl_cert = Some("/etc/ssl/c.pem".into());
        c.ssl_key = Some("/etc/ssl/k.pem".into());
        c.ssl_key_password = Some("kpw".into());
        let cfg = c.build_config().unwrap();
        assert_eq!(cfg.get("security.protocol"), Some("SASL_SSL"));
        assert_eq!(cfg.get("sasl.mechanism"), Some("PLAIN"));
        assert_eq!(cfg.get("sasl.username"), Some("alice"));
        assert_eq!(cfg.get("sasl.password"), Some("hunter2"));
        assert_eq!(cfg.get("ssl.ca.location"), Some("/etc/ssl/ca.pem"));
        assert_eq!(cfg.get("ssl.certificate.location"), Some("/etc/ssl/c.pem"));
        assert_eq!(cfg.get("ssl.key.location"), Some("/etc/ssl/k.pem"));
        assert_eq!(cfg.get("ssl.key.password"), Some("kpw"));
    }

    #[test]
    fn build_config_extra_conf_applied_in_order() {
        let mut c = base_conn();
        c.extra_conf = vec![
            "linger.ms=5".into(),
            "compression.type=snappy".into(),
            "client.id=override".into(), // last-wins
        ];
        let cfg = c.build_config().unwrap();
        assert_eq!(cfg.get("linger.ms"), Some("5"));
        assert_eq!(cfg.get("compression.type"), Some("snappy"));
        // Later -X k=v overrides earlier explicit field of same key.
        assert_eq!(cfg.get("client.id"), Some("override"));
    }

    #[test]
    fn build_config_extra_conf_with_equals_in_value_splits_first() {
        let mut c = base_conn();
        c.extra_conf = vec!["sasl.jaas.config=org.x.PlainLoginModule required user=alice".into()];
        let cfg = c.build_config().unwrap();
        // First '=' boundary preserves '=' inside the value.
        assert_eq!(
            cfg.get("sasl.jaas.config"),
            Some("org.x.PlainLoginModule required user=alice"),
        );
    }

    #[test]
    fn build_config_extra_conf_malformed_errors() {
        let mut c = base_conn();
        c.extra_conf = vec!["no-equals".into()];
        let err = c.build_config().unwrap_err();
        assert!(format!("{err:#}").contains("no-equals"));
    }

    // ─── emit_ndjson_line ────────────────────────────────────────────

    #[test]
    fn emit_ndjson_line_appends_newline() {
        let mut buf = Vec::new();
        emit_ndjson_line(&mut buf, &serde_json::json!({"k": 1})).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "{\"k\":1}\n");
    }

    #[test]
    fn emit_ndjson_line_multi_call() {
        let mut buf = Vec::new();
        for i in 0..5 {
            emit_ndjson_line(&mut buf, &serde_json::json!({"i": i})).unwrap();
        }
        assert_eq!(String::from_utf8(buf).unwrap().lines().count(), 5);
    }

    #[test]
    fn build_config_multi_broker_list_preserved() {
        let mut c = base_conn();
        c.brokers = Some("b1:9092,b2:9092,b3:9092".into());
        let cfg = c.build_config().unwrap();
        assert_eq!(cfg.get("bootstrap.servers"), Some("b1:9092,b2:9092,b3:9092"));
    }

    #[test]
    fn build_config_socket_timeout_matches_timeout_ms() {
        let mut c = base_conn();
        c.timeout_ms = 42_000;
        let cfg = c.build_config().unwrap();
        assert_eq!(cfg.get("socket.timeout.ms"), Some("42000"));
    }

    #[test]
    fn build_config_empty_extra_conf_ok() {
        let cfg = base_conn().build_config().unwrap();
        assert_eq!(cfg.get("linger.ms"), None);
    }

    #[test]
    fn build_config_client_id_with_dots() {
        let mut c = base_conn();
        c.client_id = "app.prod.worker".into();
        assert_eq!(c.build_config().unwrap().get("client.id"), Some("app.prod.worker"));
    }

    #[test]
    fn build_config_sasl_password_only_without_username() {
        let mut c = base_conn();
        c.sasl_password = Some("secret".into());
        let cfg = c.build_config().unwrap();
        assert_eq!(cfg.get("sasl.username"), None);
        assert_eq!(cfg.get("sasl.password"), Some("secret"));
    }

    #[test]
    fn timeout_large_ms() {
        let mut c = base_conn();
        c.timeout_ms = 600_000;
        assert_eq!(c.timeout(), Duration::from_millis(600_000));
    }

    #[test]
    fn build_config_extra_conf_empty_value() {
        let mut c = base_conn();
        c.extra_conf = vec!["allow.auto.create.topics=".into()];
        assert_eq!(c.build_config().unwrap().get("allow.auto.create.topics"), Some(""));
    }

    #[test]
    fn build_config_security_protocol_plaintext() {
        let mut c = base_conn();
        c.security_protocol = Some("PLAINTEXT".into());
        assert_eq!(c.build_config().unwrap().get("security.protocol"), Some("PLAINTEXT"));
    }

    #[test]
    fn build_config_ssl_ca_only() {
        let mut c = base_conn();
        c.ssl_ca = Some("/etc/ssl/ca.pem".into());
        let cfg = c.build_config().unwrap();
        assert_eq!(cfg.get("ssl.ca.location"), Some("/etc/ssl/ca.pem"));
        assert_eq!(cfg.get("ssl.certificate.location"), None);
    }

    #[test]
    fn build_config_brokers_single_host() {
        let mut c = base_conn();
        c.brokers = Some("kafka:9092".into());
        assert_eq!(c.build_config().unwrap().get("bootstrap.servers"), Some("kafka:9092"));
    }

    #[test]
    fn build_config_extra_conf_overrides_bootstrap() {
        let mut c = base_conn();
        c.extra_conf = vec!["bootstrap.servers=override:9092".into()];
        assert_eq!(c.build_config().unwrap().get("bootstrap.servers"), Some("override:9092"));
    }

    #[test]
    fn build_config_sasl_mechanism_only() {
        let mut c = base_conn();
        c.sasl_mechanism = Some("SCRAM-SHA-512".into());
        assert_eq!(
            c.build_config().unwrap().get("sasl.mechanism"),
            Some("SCRAM-SHA-512"),
        );
    }

    #[test]
    fn build_config_ssl_cert_and_key_without_ca() {
        let mut c = base_conn();
        c.ssl_cert = Some("/c.pem".into());
        c.ssl_key = Some("/k.pem".into());
        let cfg = c.build_config().unwrap();
        assert_eq!(cfg.get("ssl.certificate.location"), Some("/c.pem"));
        assert_eq!(cfg.get("ssl.key.location"), Some("/k.pem"));
        assert_eq!(cfg.get("ssl.ca.location"), None);
    }

    #[test]
    fn timeout_zero_ms() {
        let mut c = base_conn();
        c.timeout_ms = 0;
        assert_eq!(c.timeout(), Duration::from_millis(0));
    }

    #[test]
    fn build_config_extra_conf_multiple_same_key_last_wins() {
        let mut c = base_conn();
        c.extra_conf = vec!["a=1".into(), "a=2".into()];
        assert_eq!(c.build_config().unwrap().get("a"), Some("2"));
    }

    #[test]
    fn emit_ndjson_line_null() {
        let mut buf = Vec::new();
        emit_ndjson_line(&mut buf, &serde_json::Value::Null).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "null\n");
    }

    #[test]
    fn build_config_security_protocol_sasl_plaintext() {
        let mut c = base_conn();
        c.security_protocol = Some("SASL_PLAINTEXT".into());
        assert_eq!(
            c.build_config().unwrap().get("security.protocol"),
            Some("SASL_PLAINTEXT"),
        );
    }

    #[test]
    fn build_config_brokers_with_spaces_preserved() {
        let mut c = base_conn();
        c.brokers = Some(" host:9092 ".into());
        assert_eq!(
            c.build_config().unwrap().get("bootstrap.servers"),
            Some(" host:9092 "),
        );
    }

    #[test]
    fn build_config_ssl_key_password_only() {
        let mut c = base_conn();
        c.ssl_key_password = Some("pw".into());
        assert_eq!(c.build_config().unwrap().get("ssl.key.password"), Some("pw"));
    }

    #[test]
    fn build_config_sasl_username_only() {
        let mut c = base_conn();
        c.sasl_username = Some("alice".into());
        assert_eq!(c.build_config().unwrap().get("sasl.username"), Some("alice"));
    }

    #[test]
    fn build_config_extra_conf_numeric_key() {
        let mut c = base_conn();
        c.extra_conf = vec!["socket.keepalive.enable=1".into()];
        assert_eq!(
            c.build_config().unwrap().get("socket.keepalive.enable"),
            Some("1"),
        );
    }

    #[test]
    fn timeout_one_ms() {
        let mut c = base_conn();
        c.timeout_ms = 1;
        assert_eq!(c.timeout(), Duration::from_millis(1));
    }

    #[test]
    fn build_config_brokers_ipv6_literal() {
        let mut c = base_conn();
        c.brokers = Some("[::1]:9092".into());
        assert_eq!(
            c.build_config().unwrap().get("bootstrap.servers"),
            Some("[::1]:9092"),
        );
    }

    #[test]
    fn emit_ndjson_line_object() {
        let mut buf = Vec::new();
        emit_ndjson_line(&mut buf, &serde_json::json!({"k": 1})).unwrap();
        assert!(String::from_utf8(buf).unwrap().contains("\"k\":1"));
    }

    #[test]
    fn build_config_security_protocol_ssl() {
        let mut c = base_conn();
        c.security_protocol = Some("SSL".into());
        assert_eq!(c.build_config().unwrap().get("security.protocol"), Some("SSL"));
    }

    #[test]
    fn build_config_client_id_from_conn() {
        assert_eq!(
            base_conn().build_config().unwrap().get("client.id"),
            Some("test-client"),
        );
    }

    #[test]
    fn build_config_extra_conf_trims_nothing_on_value() {
        let mut c = base_conn();
        c.extra_conf = vec!["k= v ".into()];
        assert_eq!(c.build_config().unwrap().get("k"), Some(" v "));
    }

    #[test]
    fn build_config_sasl_plain_mechanism() {
        let mut c = base_conn();
        c.sasl_mechanism = Some("PLAIN".into());
        assert_eq!(c.build_config().unwrap().get("sasl.mechanism"), Some("PLAIN"));
    }

    #[test]
    fn build_config_multiple_extra_conf_keys() {
        let mut c = base_conn();
        c.extra_conf = vec!["a=1".into(), "b=2".into()];
        let cfg = c.build_config().unwrap();
        assert_eq!(cfg.get("a"), Some("1"));
        assert_eq!(cfg.get("b"), Some("2"));
    }

    #[test]
    fn timeout_five_seconds() {
        let mut c = base_conn();
        c.timeout_ms = 5_000;
        assert_eq!(c.timeout(), Duration::from_millis(5_000));
    }

    #[test]
    fn build_config_brokers_port_only() {
        let mut c = base_conn();
        c.brokers = Some("localhost:9092".into());
        assert_eq!(
            c.build_config().unwrap().get("bootstrap.servers"),
            Some("localhost:9092"),
        );
    }

    #[test]
    fn emit_ndjson_line_false() {
        let mut buf = Vec::new();
        emit_ndjson_line(&mut buf, &serde_json::json!(false)).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "false\n");
    }

    #[test]
    fn build_config_ssl_key_password_set() {
        let mut c = base_conn();
        c.ssl_key_password = Some("pw".into());
        assert_eq!(c.build_config().unwrap().get("ssl.key.password"), Some("pw"));
    }

    #[test]
    fn build_config_no_security_protocol_by_default() {
        assert_eq!(base_conn().build_config().unwrap().get("security.protocol"), None);
    }

    #[test]
    fn build_config_socket_timeout_matches() {
        let mut c = base_conn();
        c.timeout_ms = 15_000;
        assert_eq!(c.build_config().unwrap().get("socket.timeout.ms"), Some("15000"));
    }

    #[test]
    fn build_config_sasl_username_password() {
        let mut c = base_conn();
        c.sasl_username = Some("u".into());
        c.sasl_password = Some("p".into());
        let cfg = c.build_config().unwrap();
        assert_eq!(cfg.get("sasl.username"), Some("u"));
        assert_eq!(cfg.get("sasl.password"), Some("p"));
    }

    #[test]
    fn build_config_ssl_ca_location() {
        let mut c = base_conn();
        c.ssl_ca = Some("/etc/ca.pem".into());
        assert_eq!(c.build_config().unwrap().get("ssl.ca.location"), Some("/etc/ca.pem"));
    }

    #[test]
    fn timeout_one_second() {
        let mut c = base_conn();
        c.timeout_ms = 1_000;
        assert_eq!(c.timeout(), Duration::from_secs(1));
    }

    #[test]
    fn build_config_client_id_from_base_conn() {
        assert_eq!(
            base_conn().build_config().unwrap().get("client.id"),
            Some("test-client"),
        );
    }

    #[test]
    fn build_config_extra_conf_overwrites_broker() {
        let mut c = base_conn();
        c.brokers = Some("a:9092".into());
        c.extra_conf = vec!["bootstrap.servers=b:9092".into()];
        assert_eq!(
            c.build_config().unwrap().get("bootstrap.servers"),
            Some("b:9092"),
        );
    }

    #[test]
    fn build_config_sasl_mechanism_scram() {
        let mut c = base_conn();
        c.sasl_mechanism = Some("SCRAM-SHA-512".into());
        assert_eq!(
            c.build_config().unwrap().get("sasl.mechanism"),
            Some("SCRAM-SHA-512"),
        );
    }
}
