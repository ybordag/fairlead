use crate::{
    jobs::{JobRecord, JobResponse},
    metrics::{CallbackLabels, RoutingMetrics},
};
use std::time::Duration;

pub const DEFAULT_CALLBACK_MAX_ATTEMPTS: u32 = 3;
pub const DEFAULT_CALLBACK_TIMEOUT_SECS: u64 = 5;
pub const DEFAULT_CALLBACK_RETRY_DELAY_MS: u64 = 250;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CallbackPolicy {
    pub max_attempts: u32,
    pub timeout: Duration,
    pub retry_delay: Duration,
}

impl Default for CallbackPolicy {
    fn default() -> Self {
        Self {
            max_attempts: DEFAULT_CALLBACK_MAX_ATTEMPTS,
            timeout: Duration::from_secs(DEFAULT_CALLBACK_TIMEOUT_SECS),
            retry_delay: Duration::from_millis(DEFAULT_CALLBACK_RETRY_DELAY_MS),
        }
    }
}

pub fn dispatch_job_callback(
    client: reqwest::Client,
    metrics: RoutingMetrics,
    policy: CallbackPolicy,
    job: JobRecord,
) {
    let Some(callback_url) = job.callback_url.clone() else {
        return;
    };

    tokio::spawn(async move {
        let status = job.status.as_str().to_string();
        let kind = job.kind.as_str().to_string();
        let max_attempts = policy.max_attempts.max(1);

        for attempt in 1..=max_attempts {
            let result = tokio::time::timeout(
                policy.timeout,
                client
                    .post(&callback_url)
                    .json(&JobResponse { job: job.clone() })
                    .send(),
            )
            .await;

            let success = match result {
                Ok(Ok(response)) if response.status().is_success() => {
                    metrics.record_callback(CallbackLabels {
                        kind: kind.clone(),
                        status: status.clone(),
                        outcome: "success".into(),
                        http_status: response.status().as_u16(),
                    });
                    true
                }
                Ok(Ok(response)) => {
                    metrics.record_callback(CallbackLabels {
                        kind: kind.clone(),
                        status: status.clone(),
                        outcome: "failure".into(),
                        http_status: response.status().as_u16(),
                    });
                    false
                }
                Ok(Err(_)) | Err(_) => {
                    metrics.record_callback(CallbackLabels {
                        kind: kind.clone(),
                        status: status.clone(),
                        outcome: "failure".into(),
                        http_status: 0,
                    });
                    false
                }
            };

            if success || attempt == max_attempts {
                break;
            }

            if !policy.retry_delay.is_zero() {
                tokio::time::sleep(policy.retry_delay).await;
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn callback_policy_default_is_bounded() {
        let policy = CallbackPolicy::default();
        assert_eq!(policy.max_attempts, DEFAULT_CALLBACK_MAX_ATTEMPTS);
        assert_eq!(
            policy.timeout,
            Duration::from_secs(DEFAULT_CALLBACK_TIMEOUT_SECS)
        );
        assert_eq!(
            policy.retry_delay,
            Duration::from_millis(DEFAULT_CALLBACK_RETRY_DELAY_MS)
        );
    }
}
