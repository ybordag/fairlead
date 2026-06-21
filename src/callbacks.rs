use crate::{
    jobs::{JobRegistry, JobResponse},
    metrics::{CallbackLabels, RoutingMetrics},
};
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::Duration,
};

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

#[derive(Clone, Default)]
pub struct CallbackDispatcher {
    in_flight: Arc<Mutex<HashSet<String>>>,
}

impl CallbackDispatcher {
    pub fn dispatch(
        &self,
        client: reqwest::Client,
        metrics: RoutingMetrics,
        policy: CallbackPolicy,
        jobs: JobRegistry,
        job_id: String,
    ) {
        if !self.mark_in_flight(&job_id) {
            return;
        }

        let dispatcher = self.clone();
        tokio::spawn(async move {
            deliver_callback(client, metrics, policy, jobs, &job_id).await;
            dispatcher.clear_in_flight(&job_id);
        });
    }

    fn mark_in_flight(&self, job_id: &str) -> bool {
        self.in_flight
            .lock()
            .expect("callback dispatcher mutex poisoned")
            .insert(job_id.to_string())
    }

    fn clear_in_flight(&self, job_id: &str) {
        self.in_flight
            .lock()
            .expect("callback dispatcher mutex poisoned")
            .remove(job_id);
    }
}

pub async fn dispatch_pending_callbacks(
    dispatcher: CallbackDispatcher,
    client: reqwest::Client,
    metrics: RoutingMetrics,
    policy: CallbackPolicy,
    jobs: JobRegistry,
) {
    for job in jobs.pending_callback_jobs().await {
        dispatcher.dispatch(
            client.clone(),
            metrics.clone(),
            policy,
            jobs.clone(),
            job.id,
        );
    }
}

pub fn spawn_callback_recovery_loop(
    dispatcher: CallbackDispatcher,
    client: reqwest::Client,
    metrics: RoutingMetrics,
    policy: CallbackPolicy,
    jobs: JobRegistry,
) {
    tokio::spawn(async move {
        let interval = if policy.retry_delay.is_zero() {
            Duration::from_secs(1)
        } else {
            policy.retry_delay
        };

        loop {
            dispatch_pending_callbacks(
                dispatcher.clone(),
                client.clone(),
                metrics.clone(),
                policy,
                jobs.clone(),
            )
            .await;
            tokio::time::sleep(interval).await;
        }
    });
}

async fn deliver_callback(
    client: reqwest::Client,
    metrics: RoutingMetrics,
    policy: CallbackPolicy,
    jobs: JobRegistry,
    job_id: &str,
) {
    let max_attempts = policy.max_attempts.max(1);

    for attempt in 1..=max_attempts {
        let Some(job) = jobs.begin_callback_attempt(job_id).await else {
            break;
        };
        let Some(callback_url) = job.callback_url.clone() else {
            break;
        };

        let kind = job.kind.as_str().to_string();
        let status = job.status.as_str().to_string();
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
                let http_status = response.status().as_u16();
                jobs.record_callback_success(job_id, http_status).await;
                metrics.record_callback(CallbackLabels {
                    kind,
                    status,
                    outcome: "success".into(),
                    http_status,
                });
                true
            }
            Ok(Ok(response)) => {
                let http_status = response.status().as_u16();
                jobs.record_callback_failure(job_id, Some(http_status), None)
                    .await;
                metrics.record_callback(CallbackLabels {
                    kind,
                    status,
                    outcome: "failure".into(),
                    http_status,
                });
                false
            }
            Ok(Err(error)) => {
                jobs.record_callback_failure(job_id, None, Some(error.to_string()))
                    .await;
                metrics.record_callback(CallbackLabels {
                    kind,
                    status,
                    outcome: "failure".into(),
                    http_status: 0,
                });
                false
            }
            Err(_) => {
                jobs.record_callback_failure(job_id, None, Some("callback timed out".into()))
                    .await;
                metrics.record_callback(CallbackLabels {
                    kind,
                    status,
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
