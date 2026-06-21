use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    env::VarError,
};

use crate::callbacks::{
    DEFAULT_CALLBACK_MAX_ATTEMPTS, DEFAULT_CALLBACK_RETRY_DELAY_MS, DEFAULT_CALLBACK_TIMEOUT_SECS,
};

pub const DEFAULT_BACKEND_POOL: &str = "default";
pub const DEFAULT_JOB_DB_PATH: &str = "fairlead_jobs.sqlite3";

const KNOWN_POOL_WORKLOADS: &[&str] = &[
    "chat_completions",
    "embeddings",
    "vision_analysis",
    "embed_batch",
    "index_build",
    "cluster",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkloadKind {
    ChatCompletions,
    Embeddings,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkloadRoute {
    pub kind: WorkloadKind,
    pub upstream_path: &'static str,
    pub backend_pool: BackendPoolPolicy,
    pub retry_server_errors: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendPoolPolicy {
    Any,
    Named(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobStoreConfig {
    Memory,
    Sqlite { path: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoolConfig {
    pub id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
enum PoolConfigEntry {
    Id(String),
    Object(PoolConfig),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WorkloadPoolPolicy {
    pools_by_workload: BTreeMap<String, Vec<String>>,
}

impl WorkloadPoolPolicy {
    pub fn new(pools_by_workload: BTreeMap<String, Vec<String>>) -> Self {
        Self { pools_by_workload }
    }

    pub fn allows(&self, workload: &str, pool: &str) -> bool {
        self.pools_by_workload
            .get(workload)
            .is_none_or(|pools| pools.iter().any(|allowed| allowed == pool))
    }

    pub fn pools_for(&self, workload: &str) -> Option<&[String]> {
        self.pools_by_workload.get(workload).map(Vec::as_slice)
    }

    pub fn len(&self) -> usize {
        self.pools_by_workload.len()
    }

    #[cfg(test)]
    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.pools_by_workload.keys()
    }

    #[cfg(test)]
    pub fn contains_key(&self, workload: &str) -> bool {
        self.pools_by_workload.contains_key(workload)
    }

    #[cfg(test)]
    pub fn get(&self, workload: &str) -> Option<&Vec<String>> {
        self.pools_by_workload.get(workload)
    }
}

impl BackendPoolPolicy {
    pub fn allows(self, pool: &str) -> bool {
        match self {
            Self::Any => true,
            Self::Named(expected) => pool == expected,
        }
    }
}

impl WorkloadKind {
    pub fn default_proxy_workloads() -> Vec<Self> {
        vec![Self::ChatCompletions, Self::Embeddings]
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ChatCompletions => "chat_completions",
            Self::Embeddings => "embeddings",
        }
    }

    pub fn route(self) -> WorkloadRoute {
        match self {
            Self::ChatCompletions => WorkloadRoute {
                kind: self,
                upstream_path: "chat/completions",
                backend_pool: BackendPoolPolicy::Any,
                retry_server_errors: true,
            },
            Self::Embeddings => WorkloadRoute {
                kind: self,
                upstream_path: "embeddings",
                backend_pool: BackendPoolPolicy::Any,
                retry_server_errors: true,
            },
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Priority {
    #[default]
    Realtime,
    Batch,
    Background,
}

impl Priority {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Realtime => "realtime",
            Self::Batch => "batch",
            Self::Background => "background",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_lowercase().as_str() {
            "realtime" => Some(Self::Realtime),
            "batch" => Some(Self::Batch),
            "background" => Some(Self::Background),
            _ => None,
        }
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
    /// Named placement pools shared by sync backends and future async workers.
    ///
    /// When `POOLS_JSON` is absent, Fairlead derives pools from configured
    /// backends and always includes the backward-compatible `default` pool.
    pub pools: Vec<PoolConfig>,
    /// Workload-to-pool eligibility policy. Phase 7A validates this shape; later
    /// Phase 7 slices apply it to synchronous routing and async worker claims.
    pub workload_pools: WorkloadPoolPolicy,
    /// Reject worker registration when the worker's pool is not configured.
    /// Default: false.
    pub strict_worker_pools: bool,
    /// Consecutive failures required to open a circuit. Default: 3.
    pub circuit_failure_threshold: u32,
    /// Seconds to wait in Open state before probing again (Half-open). Default: 30.
    pub circuit_cooldown_secs: u64,
    /// Seconds between background health probes per backend. Default: 10.
    pub health_probe_interval_secs: u64,
    /// Seconds before a resource report is considered stale. Default: 30.
    pub resource_report_ttl_secs: u64,
    /// Enable resource-aware backend eligibility. Default: false.
    pub resource_aware_routing: bool,
    /// Coarse VRAM estimate for chat completion requests. Default: 1024 MB.
    pub chat_completions_required_vram_mb: u64,
    /// Coarse VRAM estimate for embedding requests. Default: 512 MB.
    pub embeddings_required_vram_mb: u64,
    /// Max in-flight realtime requests. Default: 8.
    pub priority_realtime_limit: usize,
    /// Max in-flight batch requests. Default: 4.
    pub priority_batch_limit: usize,
    /// Max in-flight background requests. Default: 2.
    pub priority_background_limit: usize,
    /// Job state persistence backend. Default: memory.
    pub job_store: JobStoreConfig,
    /// Max callback delivery attempts for terminal async jobs. Default: 3.
    pub callback_max_attempts: u32,
    /// Per-attempt callback timeout in seconds. Default: 5.
    pub callback_timeout_secs: u64,
    /// Delay between callback retry attempts in milliseconds. Default: 250.
    pub callback_retry_delay_ms: u64,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|k| std::env::var(k))
    }

    /// Internal constructor that accepts an arbitrary key lookup — used by tests
    /// to avoid touching global process environment state.
    fn from_lookup(get: impl Fn(&str) -> Result<String, VarError>) -> Result<Self> {
        let backends = parse_backends(&get)?;
        let pools = parse_pools(&get, &backends)?;
        let workload_pools = parse_workload_pools(&get, &pools)?;

        Ok(Config {
            port: get("PORT")
                .unwrap_or_else(|_| "7000".to_string())
                .parse()
                .map_err(|e| anyhow!("invalid PORT: {}", e))?,

            log_level: get("LOG_LEVEL").unwrap_or_else(|_| "info".to_string()),

            log_format_json: get("LOG_FORMAT")
                .map(|v| v.to_lowercase() == "json")
                .unwrap_or(false),

            backends,

            pools,

            workload_pools,

            strict_worker_pools: get("STRICT_WORKER_POOLS")
                .map(|v| v.trim().eq_ignore_ascii_case("true"))
                .unwrap_or(false),

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

            resource_aware_routing: get("RESOURCE_AWARE_ROUTING")
                .map(|v| v.to_lowercase() == "true")
                .unwrap_or(false),

            chat_completions_required_vram_mb: get("CHAT_COMPLETIONS_REQUIRED_VRAM_MB")
                .unwrap_or_else(|_| "1024".to_string())
                .parse()
                .map_err(|e| anyhow!("invalid CHAT_COMPLETIONS_REQUIRED_VRAM_MB: {}", e))?,

            embeddings_required_vram_mb: get("EMBEDDINGS_REQUIRED_VRAM_MB")
                .unwrap_or_else(|_| "512".to_string())
                .parse()
                .map_err(|e| anyhow!("invalid EMBEDDINGS_REQUIRED_VRAM_MB: {}", e))?,

            priority_realtime_limit: get("PRIORITY_REALTIME_LIMIT")
                .unwrap_or_else(|_| "8".to_string())
                .parse()
                .map_err(|e| anyhow!("invalid PRIORITY_REALTIME_LIMIT: {}", e))?,

            priority_batch_limit: get("PRIORITY_BATCH_LIMIT")
                .unwrap_or_else(|_| "4".to_string())
                .parse()
                .map_err(|e| anyhow!("invalid PRIORITY_BATCH_LIMIT: {}", e))?,

            priority_background_limit: get("PRIORITY_BACKGROUND_LIMIT")
                .unwrap_or_else(|_| "2".to_string())
                .parse()
                .map_err(|e| anyhow!("invalid PRIORITY_BACKGROUND_LIMIT: {}", e))?,

            job_store: parse_job_store(&get)?,

            callback_max_attempts: parse_nonzero(
                "CALLBACK_MAX_ATTEMPTS",
                DEFAULT_CALLBACK_MAX_ATTEMPTS,
                &get,
            )?,

            callback_timeout_secs: parse_nonzero(
                "CALLBACK_TIMEOUT_SECS",
                DEFAULT_CALLBACK_TIMEOUT_SECS,
                &get,
            )?,

            callback_retry_delay_ms: get("CALLBACK_RETRY_DELAY_MS")
                .unwrap_or_else(|_| DEFAULT_CALLBACK_RETRY_DELAY_MS.to_string())
                .parse()
                .map_err(|e| anyhow!("invalid CALLBACK_RETRY_DELAY_MS: {}", e))?,
        })
    }
}

fn parse_nonzero<T>(
    key: &str,
    default: T,
    get: &impl Fn(&str) -> Result<String, VarError>,
) -> Result<T>
where
    T: TryFrom<u64> + ToString + PartialEq + Default,
    <T as TryFrom<u64>>::Error: std::fmt::Display,
{
    let value: T = get(key)
        .unwrap_or_else(|_| default.to_string())
        .parse::<u64>()
        .map_err(|e| anyhow!("invalid {}: {}", key, e))?
        .try_into()
        .map_err(|e| anyhow!("invalid {}: {}", key, e))?;
    if value == T::default() {
        return Err(anyhow!("invalid {}: value must be greater than zero", key));
    }
    Ok(value)
}

fn parse_job_store(get: &impl Fn(&str) -> Result<String, VarError>) -> Result<JobStoreConfig> {
    match get("JOB_STORE")
        .unwrap_or_else(|_| "memory".to_string())
        .trim()
        .to_lowercase()
        .as_str()
    {
        "memory" => Ok(JobStoreConfig::Memory),
        "sqlite" => {
            let path = get("JOB_DB_PATH").unwrap_or_else(|_| DEFAULT_JOB_DB_PATH.to_string());
            let path = path.trim();
            if path.is_empty() {
                return Err(anyhow!("invalid JOB_DB_PATH: path cannot be empty"));
            }
            Ok(JobStoreConfig::Sqlite {
                path: path.to_string(),
            })
        }
        other => Err(anyhow!(
            "invalid JOB_STORE: expected 'memory' or 'sqlite', got '{}'",
            other
        )),
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

fn parse_pools(
    get: &impl Fn(&str) -> Result<String, VarError>,
    backends: &[BackendConfig],
) -> Result<Vec<PoolConfig>> {
    match get("POOLS_JSON") {
        Ok(raw) => {
            let entries: Vec<PoolConfigEntry> =
                serde_json::from_str(&raw).map_err(|e| anyhow!("invalid POOLS_JSON: {}", e))?;
            let pools = entries
                .into_iter()
                .map(|entry| match entry {
                    PoolConfigEntry::Id(id) => PoolConfig { id },
                    PoolConfigEntry::Object(pool) => pool,
                })
                .collect::<Vec<_>>();
            validate_pools(&pools)?;
            validate_backend_pool_refs(backends, &pools)?;
            Ok(normalize_pools(pools))
        }
        Err(_) => {
            let mut ids = BTreeSet::from([DEFAULT_BACKEND_POOL.to_string()]);
            ids.extend(
                backends
                    .iter()
                    .map(|backend| backend.pool.trim().to_string()),
            );
            Ok(ids.into_iter().map(|id| PoolConfig { id }).collect())
        }
    }
}

fn parse_workload_pools(
    get: &impl Fn(&str) -> Result<String, VarError>,
    pools: &[PoolConfig],
) -> Result<WorkloadPoolPolicy> {
    match get("WORKLOAD_POOLS_JSON") {
        Ok(raw) => {
            let parsed: BTreeMap<String, Vec<String>> = serde_json::from_str(&raw)
                .map_err(|e| anyhow!("invalid WORKLOAD_POOLS_JSON: {}", e))?;
            validate_workload_pools(parsed, pools).map(WorkloadPoolPolicy::new)
        }
        Err(_) => Ok(WorkloadPoolPolicy::new(default_workload_pools(pools))),
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

fn validate_pools(pools: &[PoolConfig]) -> Result<()> {
    if pools.is_empty() {
        return Err(anyhow!("invalid POOLS_JSON: at least one pool is required"));
    }

    let mut seen = BTreeSet::new();
    for pool in pools {
        let id = pool.id.trim();
        if id.is_empty() {
            return Err(anyhow!("invalid POOLS_JSON: pool id cannot be empty"));
        }
        if !seen.insert(id.to_string()) {
            return Err(anyhow!("invalid POOLS_JSON: duplicate pool id '{}'", id));
        }
    }

    Ok(())
}

fn validate_backend_pool_refs(backends: &[BackendConfig], pools: &[PoolConfig]) -> Result<()> {
    let pool_ids = pool_id_set(pools);
    for backend in backends {
        if !pool_ids.contains(backend.pool.trim()) {
            return Err(anyhow!(
                "invalid BACKENDS_JSON: backend '{}' references unknown pool '{}'",
                backend.id,
                backend.pool
            ));
        }
    }
    Ok(())
}

fn validate_workload_pools(
    parsed: BTreeMap<String, Vec<String>>,
    pools: &[PoolConfig],
) -> Result<BTreeMap<String, Vec<String>>> {
    if parsed.is_empty() {
        return Err(anyhow!(
            "invalid WORKLOAD_POOLS_JSON: at least one workload policy is required"
        ));
    }

    let known_workloads = known_pool_workloads();
    let pool_ids = pool_id_set(pools);
    let mut normalized = BTreeMap::new();

    for (workload, pools) in parsed {
        let workload = workload.trim().to_string();
        if !known_workloads.contains(workload.as_str()) {
            return Err(anyhow!(
                "invalid WORKLOAD_POOLS_JSON: unknown workload '{}'",
                workload
            ));
        }
        if pools.is_empty() {
            return Err(anyhow!(
                "invalid WORKLOAD_POOLS_JSON: workload '{}' must target at least one pool",
                workload
            ));
        }

        let mut seen = BTreeSet::new();
        let mut normalized_pools = Vec::new();
        for pool in pools {
            let pool = pool.trim().to_string();
            if pool.is_empty() {
                return Err(anyhow!(
                    "invalid WORKLOAD_POOLS_JSON: workload '{}' has an empty pool reference",
                    workload
                ));
            }
            if !pool_ids.contains(pool.as_str()) {
                return Err(anyhow!(
                    "invalid WORKLOAD_POOLS_JSON: workload '{}' references unknown pool '{}'",
                    workload,
                    pool
                ));
            }
            if !seen.insert(pool.clone()) {
                return Err(anyhow!(
                    "invalid WORKLOAD_POOLS_JSON: workload '{}' references pool '{}' more than once",
                    workload,
                    pool
                ));
            }
            normalized_pools.push(pool);
        }

        normalized.insert(workload, normalized_pools);
    }

    Ok(normalized)
}

fn normalize_pools(pools: Vec<PoolConfig>) -> Vec<PoolConfig> {
    pools
        .into_iter()
        .map(|pool| PoolConfig {
            id: pool.id.trim().to_string(),
        })
        .collect()
}

fn default_workload_pools(pools: &[PoolConfig]) -> BTreeMap<String, Vec<String>> {
    let pool_ids = pools.iter().map(|pool| pool.id.clone()).collect::<Vec<_>>();
    known_pool_workloads()
        .into_iter()
        .map(|workload| (workload.to_string(), pool_ids.clone()))
        .collect()
}

fn known_pool_workloads() -> BTreeSet<&'static str> {
    KNOWN_POOL_WORKLOADS.iter().copied().collect()
}

fn pool_id_set(pools: &[PoolConfig]) -> BTreeSet<String> {
    pools
        .iter()
        .map(|pool| pool.id.trim().to_string())
        .collect()
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

    fn pool_ids(cfg: &Config) -> Vec<&str> {
        cfg.pools.iter().map(|pool| pool.id.as_str()).collect()
    }

    #[test]
    fn defaults_when_env_absent() {
        let cfg = Config::from_lookup(env(&[])).unwrap();
        assert_eq!(cfg.port, 7000);
        assert_eq!(cfg.log_level, "info");
        assert!(!cfg.log_format_json);
        assert_eq!(pool_ids(&cfg), vec![DEFAULT_BACKEND_POOL]);
        assert_eq!(
            cfg.workload_pools.get("chat_completions").unwrap(),
            &vec![DEFAULT_BACKEND_POOL.to_string()]
        );
        let mut expected_workloads = KNOWN_POOL_WORKLOADS
            .iter()
            .map(|workload| workload.to_string())
            .collect::<Vec<_>>();
        expected_workloads.sort();
        assert_eq!(
            cfg.workload_pools.keys().cloned().collect::<Vec<_>>(),
            expected_workloads
        );
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
    fn priority_parse_accepts_known_values_case_insensitively() {
        assert_eq!(Priority::parse("realtime"), Some(Priority::Realtime));
        assert_eq!(Priority::parse("BATCH"), Some(Priority::Batch));
        assert_eq!(Priority::parse(" background "), Some(Priority::Background));
    }

    #[test]
    fn priority_parse_rejects_unknown_values() {
        assert_eq!(Priority::parse("urgent"), None);
    }

    #[test]
    fn workload_route_metadata_defines_upstream_paths() {
        let chat = WorkloadKind::ChatCompletions.route();
        assert_eq!(chat.kind, WorkloadKind::ChatCompletions);
        assert_eq!(chat.upstream_path, "chat/completions");
        assert_eq!(chat.backend_pool, BackendPoolPolicy::Any);
        assert!(chat.retry_server_errors);

        let embeddings = WorkloadKind::Embeddings.route();
        assert_eq!(embeddings.kind, WorkloadKind::Embeddings);
        assert_eq!(embeddings.upstream_path, "embeddings");
        assert_eq!(embeddings.backend_pool, BackendPoolPolicy::Any);
        assert!(embeddings.retry_server_errors);
    }

    #[test]
    fn backend_pool_policy_matches_named_or_any_pool() {
        assert!(BackendPoolPolicy::Any.allows("local-llm"));
        assert!(BackendPoolPolicy::Named("local-llm").allows("local-llm"));
        assert!(!BackendPoolPolicy::Named("local-llm").allows("vision"));
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
        assert!(!cfg.resource_aware_routing);
        assert_eq!(cfg.chat_completions_required_vram_mb, 1024);
        assert_eq!(cfg.embeddings_required_vram_mb, 512);
        assert_eq!(cfg.priority_realtime_limit, 8);
        assert_eq!(cfg.priority_batch_limit, 4);
        assert_eq!(cfg.priority_background_limit, 2);
        assert_eq!(cfg.job_store, JobStoreConfig::Memory);
        assert_eq!(cfg.callback_max_attempts, DEFAULT_CALLBACK_MAX_ATTEMPTS);
        assert_eq!(cfg.callback_timeout_secs, DEFAULT_CALLBACK_TIMEOUT_SECS);
        assert_eq!(cfg.callback_retry_delay_ms, DEFAULT_CALLBACK_RETRY_DELAY_MS);
        assert!(!cfg.strict_worker_pools);
    }

    #[test]
    fn strict_worker_pools_parses_true_case_insensitively() {
        let cfg = Config::from_lookup(env(&[("STRICT_WORKER_POOLS", "TRUE")])).unwrap();
        assert!(cfg.strict_worker_pools);
    }

    #[test]
    fn circuit_env_overrides() {
        let cfg = Config::from_lookup(env(&[
            ("CIRCUIT_FAILURE_THRESHOLD", "5"),
            ("CIRCUIT_COOLDOWN_SECS", "60"),
            ("HEALTH_PROBE_INTERVAL_SECS", "15"),
            ("RESOURCE_REPORT_TTL_SECS", "45"),
            ("RESOURCE_AWARE_ROUTING", "true"),
            ("CHAT_COMPLETIONS_REQUIRED_VRAM_MB", "2048"),
            ("EMBEDDINGS_REQUIRED_VRAM_MB", "256"),
            ("PRIORITY_REALTIME_LIMIT", "16"),
            ("PRIORITY_BATCH_LIMIT", "6"),
            ("PRIORITY_BACKGROUND_LIMIT", "3"),
            ("CALLBACK_MAX_ATTEMPTS", "5"),
            ("CALLBACK_TIMEOUT_SECS", "9"),
            ("CALLBACK_RETRY_DELAY_MS", "50"),
        ]))
        .unwrap();
        assert_eq!(cfg.circuit_failure_threshold, 5);
        assert_eq!(cfg.circuit_cooldown_secs, 60);
        assert_eq!(cfg.health_probe_interval_secs, 15);
        assert_eq!(cfg.resource_report_ttl_secs, 45);
        assert!(cfg.resource_aware_routing);
        assert_eq!(cfg.chat_completions_required_vram_mb, 2048);
        assert_eq!(cfg.embeddings_required_vram_mb, 256);
        assert_eq!(cfg.priority_realtime_limit, 16);
        assert_eq!(cfg.priority_batch_limit, 6);
        assert_eq!(cfg.priority_background_limit, 3);
        assert_eq!(cfg.callback_max_attempts, 5);
        assert_eq!(cfg.callback_timeout_secs, 9);
        assert_eq!(cfg.callback_retry_delay_ms, 50);
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
    fn invalid_chat_required_vram_returns_err() {
        let result = Config::from_lookup(env(&[("CHAT_COMPLETIONS_REQUIRED_VRAM_MB", "abc")]));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("invalid CHAT_COMPLETIONS_REQUIRED_VRAM_MB"));
    }

    #[test]
    fn invalid_embeddings_required_vram_returns_err() {
        let result = Config::from_lookup(env(&[("EMBEDDINGS_REQUIRED_VRAM_MB", "abc")]));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("invalid EMBEDDINGS_REQUIRED_VRAM_MB"));
    }

    #[test]
    fn invalid_priority_limit_returns_err() {
        let result = Config::from_lookup(env(&[("PRIORITY_REALTIME_LIMIT", "abc")]));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("invalid PRIORITY_REALTIME_LIMIT"));
    }

    #[test]
    fn invalid_callback_policy_returns_err() {
        for (key, value) in [
            ("CALLBACK_MAX_ATTEMPTS", "0"),
            ("CALLBACK_TIMEOUT_SECS", "0"),
            ("CALLBACK_RETRY_DELAY_MS", "abc"),
        ] {
            let result = Config::from_lookup(env(&[(key, value)]));
            assert!(result.is_err(), "expected {key}={value} to fail");
            assert!(result.unwrap_err().to_string().contains(key));
        }
    }

    #[test]
    fn job_store_defaults_to_memory() {
        let cfg = Config::from_lookup(env(&[])).unwrap();
        assert_eq!(cfg.job_store, JobStoreConfig::Memory);
    }

    #[test]
    fn job_store_parses_sqlite_with_default_path() {
        let cfg = Config::from_lookup(env(&[("JOB_STORE", "sqlite")])).unwrap();
        assert_eq!(
            cfg.job_store,
            JobStoreConfig::Sqlite {
                path: DEFAULT_JOB_DB_PATH.into(),
            }
        );
    }

    #[test]
    fn job_store_parses_sqlite_with_configured_path() {
        let cfg = Config::from_lookup(env(&[
            ("JOB_STORE", "SQLITE"),
            ("JOB_DB_PATH", " /tmp/fairlead-test.db "),
        ]))
        .unwrap();
        assert_eq!(
            cfg.job_store,
            JobStoreConfig::Sqlite {
                path: "/tmp/fairlead-test.db".into(),
            }
        );
    }

    #[test]
    fn invalid_job_store_returns_err() {
        let result = Config::from_lookup(env(&[("JOB_STORE", "postgres")]));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("invalid JOB_STORE"));
    }

    #[test]
    fn empty_job_db_path_returns_err() {
        let result = Config::from_lookup(env(&[("JOB_STORE", "sqlite"), ("JOB_DB_PATH", " ")]));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("invalid JOB_DB_PATH"));
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
        assert_eq!(pool_ids(&cfg), vec![DEFAULT_BACKEND_POOL]);
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
        assert_eq!(pool_ids(&cfg), vec![DEFAULT_BACKEND_POOL, "local-llm"]);
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
    fn empty_backend_id_returns_err() {
        let result = Config::from_lookup(env(&[(
            "BACKENDS_JSON",
            r#"[{"id":"   ","url":"http://node-a:8000/v1"}]"#,
        )]));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("backend id cannot be empty"));
    }

    #[test]
    fn empty_backend_url_returns_err() {
        let result = Config::from_lookup(env(&[(
            "BACKENDS_JSON",
            r#"[{"id":"node-a-vllm","url":"   "}]"#,
        )]));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("url cannot be empty"));
    }

    #[test]
    fn empty_backend_pool_returns_err() {
        let result = Config::from_lookup(env(&[(
            "BACKENDS_JSON",
            r#"[{"id":"node-a-vllm","url":"http://node-a:8000/v1","pool":"   "}]"#,
        )]));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("pool cannot be empty"));
    }

    #[test]
    fn empty_backend_workloads_returns_err() {
        let result = Config::from_lookup(env(&[(
            "BACKENDS_JSON",
            r#"[{"id":"node-a-vllm","url":"http://node-a:8000/v1","workloads":[]}]"#,
        )]));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("must support at least one workload"));
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

    #[test]
    fn pools_json_accepts_string_and_object_entries() {
        let cfg = Config::from_lookup(env(&[
            (
                "BACKENDS_JSON",
                r#"[{"id":"node-a-vllm","url":"http://node-a:8000/v1","pool":"local-llm"}]"#,
            ),
            (
                "POOLS_JSON",
                r#"["default", {"id": " local-llm "}, {"id": "vision"}]"#,
            ),
        ]))
        .unwrap();

        assert_eq!(pool_ids(&cfg), vec!["default", "local-llm", "vision"]);
    }

    #[test]
    fn pools_json_rejects_empty_or_duplicate_pool_ids() {
        for (raw, expected) in [
            (r#"[]"#, "at least one pool"),
            (r#"["default", " "]"#, "pool id cannot be empty"),
            (r#"["default", {"id": " default "}]"#, "duplicate pool id"),
        ] {
            let result = Config::from_lookup(env(&[("POOLS_JSON", raw)]));
            assert!(result.is_err(), "expected {raw} to fail");
            assert!(result.unwrap_err().to_string().contains(expected));
        }
    }

    #[test]
    fn invalid_pools_json_returns_err() {
        for raw in ["not json", r#"{"id":"default"}"#] {
            let result = Config::from_lookup(env(&[("POOLS_JSON", raw)]));
            assert!(result.is_err(), "expected {raw} to fail");
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("invalid POOLS_JSON"));
        }
    }

    #[test]
    fn explicit_pools_json_rejects_unknown_backend_pool_reference() {
        let result = Config::from_lookup(env(&[
            (
                "BACKENDS_JSON",
                r#"[{"id":"node-a-vllm","url":"http://node-a:8000/v1","pool":"local-llm"}]"#,
            ),
            ("POOLS_JSON", r#"["default"]"#),
        ]));

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("references unknown pool 'local-llm'"));
    }

    #[test]
    fn workload_pools_json_can_reference_derived_backend_pools() {
        let cfg = Config::from_lookup(env(&[
            (
                "BACKENDS_JSON",
                r#"[
                    {"id":"node-a-vllm","url":"http://node-a:8000/v1","pool":"local-llm"},
                    {"id":"node-b-vllm","url":"http://node-b:8000/v1","pool":"peer-llm"}
                ]"#,
            ),
            (
                "WORKLOAD_POOLS_JSON",
                r#"{"chat_completions": ["local-llm", "peer-llm"]}"#,
            ),
        ]))
        .unwrap();

        assert_eq!(
            cfg.workload_pools.get("chat_completions").unwrap(),
            &vec!["local-llm".to_string(), "peer-llm".to_string()]
        );
        assert_eq!(
            pool_ids(&cfg),
            vec![DEFAULT_BACKEND_POOL, "local-llm", "peer-llm"]
        );
    }

    #[test]
    fn workload_pools_json_parses_known_workload_policy() {
        let cfg = Config::from_lookup(env(&[
            ("POOLS_JSON", r#"["local-llm", "vision"]"#),
            (
                "WORKLOAD_POOLS_JSON",
                r#"{
                    "chat_completions": ["local-llm"],
                    "vision_analysis": ["vision", "local-llm"]
                }"#,
            ),
        ]))
        .unwrap();

        assert_eq!(
            cfg.workload_pools.get("chat_completions").unwrap(),
            &vec!["local-llm".to_string()]
        );
        assert_eq!(
            cfg.workload_pools.get("vision_analysis").unwrap(),
            &vec!["vision".to_string(), "local-llm".to_string()]
        );
        assert!(!cfg.workload_pools.contains_key("embeddings"));
    }

    #[test]
    fn invalid_workload_pools_json_returns_err() {
        for raw in ["not json", r#"["chat_completions"]"#] {
            let result = Config::from_lookup(env(&[("WORKLOAD_POOLS_JSON", raw)]));
            assert!(result.is_err(), "expected {raw} to fail");
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("invalid WORKLOAD_POOLS_JSON"));
        }
    }

    #[test]
    fn workload_pools_json_rejects_invalid_policy() {
        for (raw, expected) in [
            (r#"{}"#, "at least one workload policy"),
            (r#"{"rerank": ["default"]}"#, "unknown workload 'rerank'"),
            (
                r#"{"chat_completions": []}"#,
                "must target at least one pool",
            ),
            (
                r#"{"chat_completions": ["default", "default"]}"#,
                "more than once",
            ),
            (
                r#"{"chat_completions": ["missing"]}"#,
                "references unknown pool 'missing'",
            ),
            (
                r#"{"chat_completions": [" "]}"#,
                "has an empty pool reference",
            ),
        ] {
            let result = Config::from_lookup(env(&[("WORKLOAD_POOLS_JSON", raw)]));
            assert!(result.is_err(), "expected {raw} to fail");
            assert!(result.unwrap_err().to_string().contains(expected));
        }
    }
}
