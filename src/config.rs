use anyhow::{anyhow, Result};
use std::env::VarError;

#[derive(Debug)]
pub struct Config {
    /// HTTP listen port. Default: 7000.
    pub port: u16,
    /// Tracing filter string (e.g. "info", "fairlead=debug"). Default: "info".
    pub log_level: String,
    /// Emit structured JSON logs when true. Default: false (human-readable).
    pub log_format_json: bool,
    /// Ordered list of backend base URLs (e.g. ["http://loki:8000/v1"]).
    /// Parsed from BACKENDS env var (comma-separated). Empty means no backends.
    pub backends: Vec<String>,
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

            log_level: get("LOG_LEVEL")
                .unwrap_or_else(|_| "info".to_string()),

            log_format_json: get("LOG_FORMAT")
                .map(|v| v.to_lowercase() == "json")
                .unwrap_or(false),

            backends: get("BACKENDS")
                .map(|v| {
                    v.split(',')
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(str::to_owned)
                        .collect()
                })
                .unwrap_or_default(),
        })
    }
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
    fn backends_parsed_from_comma_separated_env() {
        let cfg = Config::from_lookup(env(&[(
            "BACKENDS",
            "http://loki:8000/v1, http://thor:8000/v1",
        )]))
        .unwrap();
        assert_eq!(
            cfg.backends,
            vec!["http://loki:8000/v1", "http://thor:8000/v1"]
        );
    }
}
