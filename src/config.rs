use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::env::VarError;

pub const DEFAULT_BACKEND_POOL: &str = "default";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkloadKind {
    ChatCompletions,
    Embeddings,
}

impl WorkloadKind {
    pub fn default_proxy_workloads() -> Vec<Self> {
        vec![Self::ChatCompletions, Self::Embeddings]
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendConfig {
    /// Stable backend identifier used by metrics, logs, and future routing policy.
    pub id: String,
    /// Base URL including the API prefix, e.g. `http://node-a:8000/v1`.
    pub url: String,
    /// Optional node identifier, e.g. `node-a` or `node-b`.
    #[serde(default)]
    pub node_id: Option<String>,
    /// Backend pool name. Defaults to `default` for backward compatibility.
    #[serde(default = "default_backend_pool")]
    pub pool: String,
    /// Workloads this backend can serve.
    #[serde(default = "WorkloadKind::default_proxy_workloads")]
    pub workloads: Vec<WorkloadKind>,
    /// Optional health probe path or absolute URL.
    ///
    /// Defaults to appending `models` to the backend API base URL, e.g.
    /// `http://node-a:8000/v1` -> `http://node-a:8000/v1/models`.
    /// Use `/health` for servers that expose process health at the origin root.
    #[serde(default)]
    pub health_path: Option<String>,
}

impl BackendConfig {
    pub fn from_legacy_url(index: usize, url: String) -> Self {
        Self {
            id: format!("backend-{index}"),
            url,
            node_id: None,
            pool: default_backend_pool(),
            workloads: WorkloadKind::default_proxy_workloads(),
            health_path: None,
        }
    }
}

fn default_backend_pool() -> String {
    DEFAULT_BACKEND_POOL.to_string()
}

#[derive(Debug)]
pub struct Config {
    /// HTTP listen port. Default: 7000.
    pub port: u16,
    /// Tracing filter string (e.g. "info", "fairlead=debug"). Default: "info".
    pub log_level: String,
    /// Emit structured JSON logs when true. Default: false (human-readable).
    pub log_format_json: bool,
    /// Ordered list of configured backends.
    ///
    /// `BACKENDS_JSON` enables node-aware metadata. `BACKENDS` remains supported
    /// as a comma-separated URL list and is converted into default-pool backends.
    pub backends: Vec<BackendConfig>,
    /// Consecutive failures required to open a circuit. Default: 3.
    pub circuit_failure_threshold: u32,
    /// Seconds to wait in Open state before probing again (Half-open). Default: 30.
    pub circuit_cooldown_secs: u64,
    /// Seconds between background health probes per backend. Default: 10.
    pub health_probe_interval_secs: u64,
    /// Seconds before a resource report is considered stale. Default: 30.
    pub resource_report_ttl_secs: u64,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|k| std::env::var(k))
    }

    /// Internal constructor that accepts an arbitrary key lookup — used by tests
    /// to avoid touching global process environment state.
    fn from_lookup(get: impl Fn(&str) -> Result<String, VarError>) -> Result<Self> {
        Ok(Config {
            port: get("PORT")
                .unwrap_or_else(|_| "7000".to_string())
                .parse()
                .map_err(|e| anyhow!("invalid PORT: {}", e))?,

            log_level: get("LOG_LEVEL").unwrap_or_else(|_| "info".to_string()),

            log_format_json: get("LOG_FORMAT")
                .map(|v| v.to_lowercase() == "json")
                .unwrap_or(false),

            backends: parse_backends(&get)?,

            circuit_failure_threshold: get("CIRCUIT_FAILURE_THRESHOLD")
                .unwrap_or_else(|_| "3".to_string())
                .parse()
                .map_err(|e| anyhow!("invalid CIRCUIT_FAILURE_THRESHOLD: {}", e))?,

            circuit_cooldown_secs: get("CIRCUIT_COOLDOWN_SECS")
                .unwrap_or_else(|_| "30".to_string())
                .parse()
                .map_err(|e| anyhow!("invalid CIRCUIT_COOLDOWN_SECS: {}", e))?,

            health_probe_interval_secs: get("HEALTH_PROBE_INTERVAL_SECS")
                .unwrap_or_else(|_| "10".to_string())
                .parse()
                .map_err(|e| anyhow!("invalid HEALTH_PROBE_INTERVAL_SECS: {}", e))?,

            resource_report_ttl_secs: get("RESOURCE_REPORT_TTL_SECS")
                .unwrap_or_else(|_| "30".to_string())
                .parse()
                .map_err(|e| anyhow!("invalid RESOURCE_REPORT_TTL_SECS: {}", e))?,
        })
    }
}

fn parse_backends(get: &impl Fn(&str) -> Result<String, VarError>) -> Result<Vec<BackendConfig>> {
    match get("BACKENDS_JSON") {
        Ok(raw) => {
            let backends: Vec<BackendConfig> =
                serde_json::from_str(&raw).map_err(|e| anyhow!("invalid BACKENDS_JSON: {}", e))?;
            validate_backends(&backends)?;
            Ok(backends)
        }
        Err(_) => Ok(get("BACKENDS")
            .map(|v| {
                v.split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned)
                    .enumerate()
                    .map(|(i, url)| BackendConfig::from_legacy_url(i, url))
                    .collect()
            })
            .unwrap_or_default()),
    }
}

fn validate_backends(backends: &[BackendConfig]) -> Result<()> {
    for backend in backends {
        if backend.id.trim().is_empty() {
            return Err(anyhow!("invalid BACKENDS_JSON: backend id cannot be empty"));
        }
        if backend.url.trim().is_empty() {
            return Err(anyhow!(
                "invalid BACKENDS_JSON: backend '{}' url cannot be empty",
                backend.id
            ));
        }
        if backend.pool.trim().is_empty() {
            return Err(anyhow!(
                "invalid BACKENDS_JSON: backend '{}' pool cannot be empty",
                backend.id
            ));
        }
        if backend.workloads.is_empty() {
            return Err(anyhow!(
                "invalid BACKENDS_JSON: backend '{}' must support at least one workload",
                backend.id
            ));
        }
        if backend
            .health_path
            .as_deref()
            .map(str::trim)
            .is_some_and(str::is_empty)
        {
            return Err(anyhow!(
                "invalid BACKENDS_JSON: backend '{}' health_path cannot be empty",
                backend.id
            ));
        }
    }

    for i in 0..backends.len() {
        for other in &backends[i + 1..] {
            if backends[i].id == other.id {
                return Err(anyhow!(
                    "invalid BACKENDS_JSON: duplicate backend id '{}'",
                    backends[i].id
                ));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Result<String, VarError> + 'a {
        |key| {
            pairs
                .iter()
                .find(|(k, _)| *k == key)
                .map(|(_, v)| v.to_string())
                .ok_or(VarError::NotPresent)
        }
    }

    #[test]
    fn defaults_when_env_absent() {
        let cfg = Config::from_lookup(env(&[])).unwrap();
        assert_eq!(cfg.port, 7000);
        assert_eq!(cfg.log_level, "info");
        assert!(!cfg.log_format_json);
    }

    #[test]
    fn reads_port_from_env() {
        let cfg = Config::from_lookup(env(&[("PORT", "9090")])).unwrap();
        assert_eq!(cfg.port, 9090);
    }

    #[test]
    fn invalid_port_returns_err() {
        let result = Config::from_lookup(env(&[("PORT", "not_a_number")]));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("invalid PORT"));
    }

    #[test]
    fn json_log_format_enabled() {
        let cfg = Config::from_lookup(env(&[("LOG_FORMAT", "json")])).unwrap();
        assert!(cfg.log_format_json);
    }

    #[test]
    fn json_log_format_case_insensitive() {
        let cfg = Config::from_lookup(env(&[("LOG_FORMAT", "JSON")])).unwrap();
        assert!(cfg.log_format_json);
    }

    #[test]
    fn non_json_log_format_is_false() {
        let cfg = Config::from_lookup(env(&[("LOG_FORMAT", "pretty")])).unwrap();
        assert!(!cfg.log_format_json);
    }

    #[test]
    fn backends_empty_by_default() {
        let cfg = Config::from_lookup(env(&[])).unwrap();
        assert!(cfg.backends.is_empty());
    }

    #[test]
    fn circuit_defaults() {
        let cfg = Config::from_lookup(env(&[])).unwrap();
        assert_eq!(cfg.circuit_failure_threshold, 3);
        assert_eq!(cfg.circuit_cooldown_secs, 30);
        assert_eq!(cfg.health_probe_interval_secs, 10);
        assert_eq!(cfg.resource_report_ttl_secs, 30);
    }

    #[test]
    fn circuit_env_overrides() {
        let cfg = Config::from_lookup(env(&[
            ("CIRCUIT_FAILURE_THRESHOLD", "5"),
            ("CIRCUIT_COOLDOWN_SECS", "60"),
            ("HEALTH_PROBE_INTERVAL_SECS", "15"),
            ("RESOURCE_REPORT_TTL_SECS", "45"),
        ]))
        .unwrap();
        assert_eq!(cfg.circuit_failure_threshold, 5);
        assert_eq!(cfg.circuit_cooldown_secs, 60);
        assert_eq!(cfg.health_probe_interval_secs, 15);
        assert_eq!(cfg.resource_report_ttl_secs, 45);
    }

    #[test]
    fn invalid_circuit_failure_threshold_returns_err() {
        let result = Config::from_lookup(env(&[("CIRCUIT_FAILURE_THRESHOLD", "abc")]));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("invalid CIRCUIT_FAILURE_THRESHOLD"));
    }

    #[test]
    fn invalid_circuit_cooldown_secs_returns_err() {
        let result = Config::from_lookup(env(&[("CIRCUIT_COOLDOWN_SECS", "abc")]));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("invalid CIRCUIT_COOLDOWN_SECS"));
    }

    #[test]
    fn invalid_health_probe_interval_returns_err() {
        let result = Config::from_lookup(env(&[("HEALTH_PROBE_INTERVAL_SECS", "abc")]));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("invalid HEALTH_PROBE_INTERVAL_SECS"));
    }

    #[test]
    fn invalid_resource_report_ttl_returns_err() {
        let result = Config::from_lookup(env(&[("RESOURCE_REPORT_TTL_SECS", "abc")]));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("invalid RESOURCE_REPORT_TTL_SECS"));
    }

    #[test]
    fn backends_parsed_from_comma_separated_env() {
        let cfg = Config::from_lookup(env(&[(
            "BACKENDS",
            "http://node-a:8000/v1, http://node-b:8000/v1",
        )]))
        .unwrap();
        assert_eq!(
            cfg.backends,
            vec![
                BackendConfig::from_legacy_url(0, "http://node-a:8000/v1".into()),
                BackendConfig::from_legacy_url(1, "http://node-b:8000/v1".into())
            ]
        );
    }

    #[test]
    fn backends_json_parses_node_aware_metadata() {
        let cfg = Config::from_lookup(env(&[(
            "BACKENDS_JSON",
            r#"[
                {
                    "id": "node-a-vllm",
                    "url": "http://node-a:8000/v1",
                    "node_id": "node-a",
                    "pool": "local-llm",
                    "workloads": ["chat_completions", "embeddings"],
                    "health_path": "/health"
                },
                {
                    "id": "node-b-vllm",
                    "url": "http://node-b:8000/v1",
                    "node_id": "node-b",
                    "pool": "local-llm",
                    "workloads": ["chat_completions"]
                }
            ]"#,
        )]))
        .unwrap();

        assert_eq!(cfg.backends.len(), 2);
        assert_eq!(cfg.backends[0].id, "node-a-vllm");
        assert_eq!(cfg.backends[0].node_id.as_deref(), Some("node-a"));
        assert_eq!(cfg.backends[0].pool, "local-llm");
        assert_eq!(
            cfg.backends[0].workloads,
            vec![WorkloadKind::ChatCompletions, WorkloadKind::Embeddings]
        );
        assert_eq!(cfg.backends[0].health_path.as_deref(), Some("/health"));
        assert_eq!(cfg.backends[1].id, "node-b-vllm");
        assert_eq!(
            cfg.backends[1].workloads,
            vec![WorkloadKind::ChatCompletions]
        );
    }

    #[test]
    fn backends_json_defaults_pool_and_workloads() {
        let cfg = Config::from_lookup(env(&[(
            "BACKENDS_JSON",
            r#"[{"id":"node-a-vllm","url":"http://node-a:8000/v1","node_id":"node-a"}]"#,
        )]))
        .unwrap();

        assert_eq!(cfg.backends[0].pool, DEFAULT_BACKEND_POOL);
        assert_eq!(
            cfg.backends[0].workloads,
            WorkloadKind::default_proxy_workloads()
        );
        assert_eq!(cfg.backends[0].health_path, None);
    }

    #[test]
    fn invalid_backends_json_returns_err() {
        let result = Config::from_lookup(env(&[("BACKENDS_JSON", "not json")]));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("invalid BACKENDS_JSON"));
    }

    #[test]
    fn duplicate_backend_id_returns_err() {
        let result = Config::from_lookup(env(&[(
            "BACKENDS_JSON",
            r#"[
                {"id":"same","url":"http://node-a:8000/v1"},
                {"id":"same","url":"http://node-b:8000/v1"}
            ]"#,
        )]));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("duplicate backend id"));
    }

    #[test]
    fn empty_health_path_returns_err() {
        let result = Config::from_lookup(env(&[(
            "BACKENDS_JSON",
            r#"[{"id":"node-a-vllm","url":"http://node-a:8000/v1","health_path":"   "}]"#,
        )]));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("health_path cannot be empty"));
    }
}
