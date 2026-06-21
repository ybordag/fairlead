use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::RwLock;

use crate::{config::Priority, storage::SqliteJobStore, AppState};

pub const ATTEMPT_TIMEOUT_ERROR: &str = "attempt timed out";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobKind {
    VisionAnalysis,
    EmbedBatch,
    IndexBuild,
    Cluster,
}

impl JobKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::VisionAnalysis => "vision_analysis",
            Self::EmbedBatch => "embed_batch",
            Self::IndexBuild => "index_build",
            Self::Cluster => "cluster",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Running,
    Complete,
    Failed,
    Cancelled,
}

impl JobStatus {
    fn is_terminal(&self) -> bool {
        matches!(self, Self::Complete | Self::Failed | Self::Cancelled)
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Complete => "complete",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SubmitJobRequest {
    #[serde(rename = "type")]
    pub kind: JobKind,
    #[serde(default)]
    pub priority: Priority,
    #[serde(default)]
    pub payload: Value,
    #[serde(default)]
    pub callback_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRecord {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: JobKind,
    pub priority: Priority,
    pub status: JobStatus,
    pub payload: Value,
    pub callback_url: Option<String>,
    pub result: Option<Value>,
    pub error: Option<JobFailure>,
    pub callback: Option<JobCallbackState>,
    pub attempts: u32,
    pub max_attempts: u32,
    pub lease: Option<JobLease>,
    pub created_at_unix_ms: u128,
    pub updated_at_unix_ms: u128,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobCallbackStatus {
    Pending,
    Delivered,
}

impl JobCallbackStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Delivered => "delivered",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobCallbackState {
    pub status: JobCallbackStatus,
    pub attempts: u32,
    pub last_attempt_at_unix_ms: Option<u128>,
    pub delivered_at_unix_ms: Option<u128>,
    pub last_http_status: Option<u16>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobFailure {
    pub message: String,
    pub retryable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobLease {
    pub worker_id: String,
    pub attempt: u32,
    pub claimed_at_unix_ms: u128,
    pub expires_at_unix_ms: u128,
}

#[derive(Debug, Clone, Serialize)]
pub struct JobResponse {
    pub job: JobRecord,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CompleteJobRequest {
    #[serde(default)]
    pub result: Value,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FailJobRequest {
    pub error: String,
    #[serde(default = "default_retryable_failure")]
    pub retryable: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct JobListResponse {
    pub jobs: Vec<JobRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct JobQueueSnapshot {
    pub priority: &'static str,
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub depth: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct JobQueueWaitSnapshot {
    pub priority: &'static str,
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub count: usize,
    pub wait_seconds_sum: f64,
    pub wait_seconds_max: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JobDurationSnapshot {
    pub priority: &'static str,
    pub kind: &'static str,
    pub status: &'static str,
    pub count: usize,
    pub duration_seconds_sum: f64,
    pub duration_seconds_max: f64,
}

#[derive(Debug, Clone, Default)]
pub struct LeaseExpiryReport {
    pub requeued: usize,
    pub failed: usize,
    pub released_workers: Vec<String>,
    pub failed_jobs: Vec<JobRecord>,
}

#[derive(Clone, Default)]
pub struct JobRegistry {
    inner: Arc<RwLock<JobRegistryInner>>,
    store: Option<SqliteJobStore>,
}

#[derive(Default)]
pub(crate) struct JobRegistryInner {
    pub(crate) next_id: u64,
    pub(crate) jobs: HashMap<String, JobRecord>,
    pub(crate) order: Vec<String>,
    pub(crate) queues: JobQueues,
}

#[derive(Default)]
pub(crate) struct JobQueues {
    pub(crate) realtime: VecDeque<String>,
    pub(crate) batch: VecDeque<String>,
    pub(crate) background: VecDeque<String>,
}

impl JobQueues {
    fn for_priority_mut(&mut self, priority: Priority) -> &mut VecDeque<String> {
        match priority {
            Priority::Realtime => &mut self.realtime,
            Priority::Batch => &mut self.batch,
            Priority::Background => &mut self.background,
        }
    }

    fn iter(&self) -> impl Iterator<Item = (Priority, &VecDeque<String>)> {
        [
            (Priority::Realtime, &self.realtime),
            (Priority::Batch, &self.batch),
            (Priority::Background, &self.background),
        ]
        .into_iter()
    }

    fn remove(&mut self, priority: Priority, id: &str) {
        let queue = self.for_priority_mut(priority);
        if let Some(position) = queue.iter().position(|queued_id| queued_id == id) {
            queue.remove(position);
        }
    }
}

impl JobRegistry {
    pub fn with_store(store: SqliteJobStore) -> anyhow::Result<Self> {
        let mut inner = store.load_registry_snapshot()?;
        let recovery_report = requeue_expired_leases_locked(&mut inner);
        if recovery_report.requeued > 0 || recovery_report.failed > 0 {
            store.replace_registry_snapshot(&inner)?;
        }
        Ok(Self {
            inner: Arc::new(RwLock::new(inner)),
            store: Some(store),
        })
    }

    pub async fn submit(&self, request: SubmitJobRequest) -> Result<JobRecord, String> {
        let callback_url = normalize_callback_url(request.callback_url)?;
        let mut guard = self.inner.write().await;
        guard.next_id += 1;
        let now = unix_ms();
        let job = JobRecord {
            id: format!("job-{}", guard.next_id),
            kind: request.kind,
            priority: request.priority,
            status: JobStatus::Queued,
            payload: request.payload,
            callback_url,
            result: None,
            error: None,
            callback: None,
            attempts: 0,
            max_attempts: 3,
            lease: None,
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
        };
        guard.jobs.insert(job.id.clone(), job.clone());
        guard.order.push(job.id.clone());
        guard
            .queues
            .for_priority_mut(job.priority)
            .push_back(job.id.clone());
        self.persist_locked(&guard)?;
        Ok(job)
    }

    pub async fn get(&self, id: &str) -> Option<JobRecord> {
        self.inner.read().await.jobs.get(id).cloned()
    }

    pub async fn list(&self) -> Vec<JobRecord> {
        let guard = self.inner.read().await;
        guard
            .order
            .iter()
            .filter_map(|id| guard.jobs.get(id).cloned())
            .collect()
    }

    pub async fn queue_snapshots(&self) -> Vec<JobQueueSnapshot> {
        let guard = self.inner.read().await;
        let mut snapshots: Vec<JobQueueSnapshot> = Vec::new();

        for (priority, queue) in guard.queues.iter() {
            for id in queue {
                let Some(job) = guard.jobs.get(id) else {
                    continue;
                };
                if job.status != JobStatus::Queued {
                    continue;
                }

                if let Some(snapshot) = snapshots
                    .iter_mut()
                    .find(|s| s.priority == priority.as_str() && s.kind == job.kind.as_str())
                {
                    snapshot.depth += 1;
                } else {
                    snapshots.push(JobQueueSnapshot {
                        priority: priority.as_str(),
                        kind: job.kind.as_str(),
                        depth: 1,
                    });
                }
            }
        }

        snapshots
            .sort_by_key(|snapshot| (priority_rank(snapshot.priority), kind_rank(snapshot.kind)));
        snapshots
    }

    pub async fn queue_wait_snapshots(&self) -> Vec<JobQueueWaitSnapshot> {
        let now = unix_ms();
        let guard = self.inner.read().await;
        let mut snapshots: Vec<JobQueueWaitSnapshot> = Vec::new();

        for (priority, queue) in guard.queues.iter() {
            for id in queue {
                let Some(job) = guard.jobs.get(id) else {
                    continue;
                };
                if job.status != JobStatus::Queued {
                    continue;
                }

                let wait_seconds = now.saturating_sub(job.created_at_unix_ms) as f64 / 1000.0;

                if let Some(snapshot) = snapshots
                    .iter_mut()
                    .find(|s| s.priority == priority.as_str() && s.kind == job.kind.as_str())
                {
                    snapshot.count += 1;
                    snapshot.wait_seconds_sum += wait_seconds;
                    snapshot.wait_seconds_max = snapshot.wait_seconds_max.max(wait_seconds);
                } else {
                    snapshots.push(JobQueueWaitSnapshot {
                        priority: priority.as_str(),
                        kind: job.kind.as_str(),
                        count: 1,
                        wait_seconds_sum: wait_seconds,
                        wait_seconds_max: wait_seconds,
                    });
                }
            }
        }

        snapshots
            .sort_by_key(|snapshot| (priority_rank(snapshot.priority), kind_rank(snapshot.kind)));
        snapshots
    }

    pub async fn terminal_duration_snapshots(&self) -> Vec<JobDurationSnapshot> {
        let guard = self.inner.read().await;
        let mut snapshots: Vec<JobDurationSnapshot> = Vec::new();

        for id in &guard.order {
            let Some(job) = guard.jobs.get(id) else {
                continue;
            };
            if !job.status.is_terminal() {
                continue;
            }

            let duration_seconds = job
                .updated_at_unix_ms
                .saturating_sub(job.created_at_unix_ms) as f64
                / 1000.0;

            if let Some(snapshot) = snapshots.iter_mut().find(|s| {
                s.priority == job.priority.as_str()
                    && s.kind == job.kind.as_str()
                    && s.status == job.status.as_str()
            }) {
                snapshot.count += 1;
                snapshot.duration_seconds_sum += duration_seconds;
                snapshot.duration_seconds_max = snapshot.duration_seconds_max.max(duration_seconds);
            } else {
                snapshots.push(JobDurationSnapshot {
                    priority: job.priority.as_str(),
                    kind: job.kind.as_str(),
                    status: job.status.as_str(),
                    count: 1,
                    duration_seconds_sum: duration_seconds,
                    duration_seconds_max: duration_seconds,
                });
            }
        }

        snapshots.sort_by_key(|snapshot| {
            (
                priority_rank(snapshot.priority),
                kind_rank(snapshot.kind),
                status_rank(snapshot.status),
            )
        });
        snapshots
    }

    pub async fn queued_jobs_by_priority(&self) -> Vec<JobRecord> {
        let guard = self.inner.read().await;
        let mut jobs = Vec::new();

        for (_priority, queue) in guard.queues.iter() {
            for id in queue {
                let Some(job) = guard.jobs.get(id) else {
                    continue;
                };
                if job.status == JobStatus::Queued {
                    jobs.push(job.clone());
                }
            }
        }

        jobs
    }

    pub async fn pending_callback_jobs(&self) -> Vec<JobRecord> {
        let guard = self.inner.read().await;
        guard
            .order
            .iter()
            .filter_map(|id| guard.jobs.get(id))
            .filter(|job| job.callback_url.is_some() && job.status.is_terminal())
            .filter(|job| {
                job.callback
                    .as_ref()
                    .is_some_and(|callback| callback.status == JobCallbackStatus::Pending)
            })
            .cloned()
            .collect()
    }

    pub async fn begin_callback_attempt(&self, id: &str) -> Option<JobRecord> {
        let mut guard = self.inner.write().await;
        let job = guard.jobs.get_mut(id)?;
        if job.callback_url.is_none() || !job.status.is_terminal() {
            return None;
        }
        let callback = job.callback.as_mut()?;
        if callback.status != JobCallbackStatus::Pending {
            return None;
        }

        callback.attempts += 1;
        callback.last_attempt_at_unix_ms = Some(unix_ms());
        let job = job.clone();
        self.persist_locked(&guard).ok()?;
        Some(job)
    }

    pub async fn record_callback_success(&self, id: &str, http_status: u16) -> Option<JobRecord> {
        let mut guard = self.inner.write().await;
        let job = guard.jobs.get_mut(id)?;
        let callback = job.callback.as_mut()?;
        callback.status = JobCallbackStatus::Delivered;
        callback.delivered_at_unix_ms = Some(unix_ms());
        callback.last_http_status = Some(http_status);
        callback.last_error = None;
        let job = job.clone();
        self.persist_locked(&guard).ok()?;
        Some(job)
    }

    pub async fn record_callback_failure(
        &self,
        id: &str,
        http_status: Option<u16>,
        error: Option<String>,
    ) -> Option<JobRecord> {
        let mut guard = self.inner.write().await;
        let job = guard.jobs.get_mut(id)?;
        let callback = job.callback.as_mut()?;
        if callback.status != JobCallbackStatus::Pending {
            return None;
        }
        callback.last_http_status = http_status;
        callback.last_error = error;
        let job = job.clone();
        self.persist_locked(&guard).ok()?;
        Some(job)
    }

    pub async fn claim_next_for_worker(
        &self,
        worker_id: &str,
        worker_job_types: &[JobKind],
        lease_duration_ms: u128,
    ) -> Option<JobRecord> {
        self.claim_next_for_worker_in_pool(
            worker_id,
            worker_job_types,
            "default",
            &crate::config::WorkloadPoolPolicy::default(),
            lease_duration_ms,
        )
        .await
    }

    pub async fn claim_next_for_worker_in_pool(
        &self,
        worker_id: &str,
        worker_job_types: &[JobKind],
        worker_pool: &str,
        workload_pools: &crate::config::WorkloadPoolPolicy,
        lease_duration_ms: u128,
    ) -> Option<JobRecord> {
        let mut guard = self.inner.write().await;
        let job_id = guard.queues.iter().find_map(|(_priority, queue)| {
            queue.iter().find_map(|id| {
                let job = guard.jobs.get(id)?;
                (job.status == JobStatus::Queued
                    && worker_job_types.contains(&job.kind)
                    && workload_pools.allows(job.kind.as_str(), worker_pool))
                .then(|| id.clone())
            })
        })?;

        let job = guard.jobs.get_mut(&job_id)?;
        let now = unix_ms();
        job.status = JobStatus::Running;
        job.attempts += 1;
        job.result = None;
        job.lease = Some(JobLease {
            worker_id: worker_id.to_string(),
            attempt: job.attempts,
            claimed_at_unix_ms: now,
            expires_at_unix_ms: now + lease_duration_ms,
        });
        job.updated_at_unix_ms = now;
        let priority = job.priority;
        let job = job.clone();
        guard.queues.remove(priority, &job_id);
        self.persist_locked(&guard).ok()?;

        Some(job)
    }

    pub async fn requeue_expired_leases(&self) -> LeaseExpiryReport {
        let mut guard = self.inner.write().await;
        let report = requeue_expired_leases_locked(&mut guard);
        self.persist_locked(&guard).ok();
        report
    }

    pub async fn renew_lease(
        &self,
        id: &str,
        worker_id: &str,
        lease_duration_ms: u128,
    ) -> RenewJobLeaseResult {
        let mut guard = self.inner.write().await;
        let Some(job) = guard.jobs.get_mut(id) else {
            return RenewJobLeaseResult::NotFound;
        };

        if job.status != JobStatus::Running {
            return RenewJobLeaseResult::NotRunning(job.clone());
        }

        let Some(lease) = &job.lease else {
            return RenewJobLeaseResult::NotRunning(job.clone());
        };

        if lease.worker_id != worker_id {
            return RenewJobLeaseResult::LeaseNotHeld(job.clone());
        }

        let now = unix_ms();
        if lease.expires_at_unix_ms <= now {
            return RenewJobLeaseResult::Expired(job.clone());
        }

        let job = {
            let lease = job.lease.as_mut().expect("lease checked above");
            lease.expires_at_unix_ms = now + lease_duration_ms;
            job.updated_at_unix_ms = now;
            job.clone()
        };
        if self.persist_locked(&guard).is_err() {
            return RenewJobLeaseResult::NotFound;
        }
        RenewJobLeaseResult::Renewed(job)
    }

    pub async fn complete_lease(
        &self,
        id: &str,
        worker_id: &str,
        result: Value,
    ) -> FinishJobResult {
        let mut guard = self.inner.write().await;
        let Some(job) = guard.jobs.get_mut(id) else {
            return FinishJobResult::NotFound;
        };

        if job.status != JobStatus::Running {
            return FinishJobResult::NotRunning(job.clone());
        }

        let Some(lease) = &job.lease else {
            return FinishJobResult::NotRunning(job.clone());
        };

        if lease.worker_id != worker_id {
            return FinishJobResult::LeaseNotHeld(job.clone());
        }

        let now = unix_ms();
        if lease.expires_at_unix_ms <= now {
            return FinishJobResult::Expired(job.clone());
        }

        let job = {
            job.status = JobStatus::Complete;
            job.result = Some(result);
            job.error = None;
            job.lease = None;
            job.updated_at_unix_ms = now;
            prepare_callback(job);
            job.clone()
        };
        if self.persist_locked(&guard).is_err() {
            return FinishJobResult::NotFound;
        }
        FinishJobResult::Completed(job)
    }

    pub async fn fail_lease(
        &self,
        id: &str,
        worker_id: &str,
        failure: JobFailure,
    ) -> FinishJobResult {
        let mut guard = self.inner.write().await;
        let Some(job) = guard.jobs.get_mut(id) else {
            return FinishJobResult::NotFound;
        };

        if job.status != JobStatus::Running {
            return FinishJobResult::NotRunning(job.clone());
        }

        let Some(lease) = &job.lease else {
            return FinishJobResult::NotRunning(job.clone());
        };

        if lease.worker_id != worker_id {
            return FinishJobResult::LeaseNotHeld(job.clone());
        }

        let now = unix_ms();
        if lease.expires_at_unix_ms <= now {
            return FinishJobResult::Expired(job.clone());
        }

        job.result = None;
        job.error = Some(failure.clone());
        job.lease = None;
        job.updated_at_unix_ms = now;

        if failure.retryable && job.attempts < job.max_attempts {
            job.status = JobStatus::Queued;
            let priority = job.priority;
            let id = job.id.clone();
            let job = job.clone();
            guard.queues.for_priority_mut(priority).push_back(id);
            if self.persist_locked(&guard).is_err() {
                return FinishJobResult::NotFound;
            }
            FinishJobResult::Requeued(job)
        } else {
            job.status = JobStatus::Failed;
            prepare_callback(job);
            let job = job.clone();
            if self.persist_locked(&guard).is_err() {
                return FinishJobResult::NotFound;
            }
            FinishJobResult::Failed(job)
        }
    }

    pub async fn cancel(&self, id: &str) -> CancelJobResult {
        let mut guard = self.inner.write().await;
        let Some(job) = guard.jobs.get_mut(id) else {
            return CancelJobResult::NotFound;
        };

        if job.status.is_terminal() {
            return CancelJobResult::AlreadyTerminal(job.clone());
        }

        let priority = job.priority;
        job.status = JobStatus::Cancelled;
        let now = unix_ms();
        job.updated_at_unix_ms = now;
        prepare_callback(job);
        let job = job.clone();
        guard.queues.remove(priority, id);
        if self.persist_locked(&guard).is_err() {
            return CancelJobResult::NotFound;
        }
        CancelJobResult::Cancelled(job)
    }

    fn persist_locked(&self, inner: &JobRegistryInner) -> Result<(), String> {
        if let Some(store) = &self.store {
            store
                .replace_registry_snapshot(inner)
                .map_err(|err| err.to_string())?;
        }
        Ok(())
    }
}

fn prepare_callback(job: &mut JobRecord) {
    if job.callback_url.is_none() || !job.status.is_terminal() {
        return;
    }
    if job
        .callback
        .as_ref()
        .is_some_and(|callback| callback.status == JobCallbackStatus::Delivered)
    {
        return;
    }
    job.callback.get_or_insert(JobCallbackState {
        status: JobCallbackStatus::Pending,
        attempts: 0,
        last_attempt_at_unix_ms: None,
        delivered_at_unix_ms: None,
        last_http_status: None,
        last_error: None,
    });
    if let Some(callback) = job.callback.as_mut() {
        callback.status = JobCallbackStatus::Pending;
        callback.delivered_at_unix_ms = None;
        callback.last_error = None;
        callback.last_http_status = None;
    }
}

fn requeue_expired_leases_locked(inner: &mut JobRegistryInner) -> LeaseExpiryReport {
    let now = unix_ms();
    let mut report = LeaseExpiryReport::default();
    let mut requeued_jobs = Vec::new();

    for id in inner.order.clone() {
        let Some(job) = inner.jobs.get_mut(&id) else {
            continue;
        };
        if job.status != JobStatus::Running {
            continue;
        }
        let Some(lease) = &job.lease else {
            continue;
        };
        if lease.expires_at_unix_ms > now {
            continue;
        }

        report.released_workers.push(lease.worker_id.clone());
        let retryable = job.attempts < job.max_attempts;
        job.result = None;
        job.error = Some(JobFailure {
            message: ATTEMPT_TIMEOUT_ERROR.into(),
            retryable,
        });
        job.lease = None;
        job.updated_at_unix_ms = now;

        if retryable {
            job.status = JobStatus::Queued;
            requeued_jobs.push((job.priority, id));
            report.requeued += 1;
        } else {
            job.status = JobStatus::Failed;
            prepare_callback(job);
            report.failed_jobs.push(job.clone());
            report.failed += 1;
        }
    }

    for (priority, id) in requeued_jobs {
        let queue = inner.queues.for_priority_mut(priority);
        if !queue.iter().any(|queued_id| queued_id == &id) {
            queue.push_back(id);
        }
    }

    report
}

pub enum CancelJobResult {
    Cancelled(JobRecord),
    AlreadyTerminal(JobRecord),
    NotFound,
}

pub enum RenewJobLeaseResult {
    Renewed(JobRecord),
    NotRunning(JobRecord),
    LeaseNotHeld(JobRecord),
    Expired(JobRecord),
    NotFound,
}

pub enum FinishJobResult {
    Completed(JobRecord),
    Requeued(JobRecord),
    Failed(JobRecord),
    NotRunning(JobRecord),
    LeaseNotHeld(JobRecord),
    Expired(JobRecord),
    NotFound,
}

pub async fn submit_job(
    State(state): State<AppState>,
    Json(request): Json<SubmitJobRequest>,
) -> Response {
    match state.jobs.submit(request).await {
        Ok(job) => (StatusCode::ACCEPTED, Json(JobResponse { job })).into_response(),
        Err(message) => (StatusCode::BAD_REQUEST, message).into_response(),
    }
}

pub async fn list_jobs(State(state): State<AppState>) -> Json<JobListResponse> {
    Json(JobListResponse {
        jobs: state.jobs.list().await,
    })
}

pub async fn get_job(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    match state.jobs.get(&id).await {
        Some(job) => Json(JobResponse { job }).into_response(),
        None => (StatusCode::NOT_FOUND, "job not found").into_response(),
    }
}

pub async fn cancel_job(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    match state.jobs.cancel(&id).await {
        CancelJobResult::Cancelled(job) => {
            if let Some(lease) = &job.lease {
                state.workers.release_slot(&lease.worker_id).await;
            }
            state.callback_dispatcher.dispatch(
                state.client.clone(),
                state.metrics.clone(),
                state.callback_policy,
                state.jobs.clone(),
                job.id.clone(),
            );
            Json(JobResponse { job }).into_response()
        }
        CancelJobResult::AlreadyTerminal(job) => {
            (StatusCode::CONFLICT, Json(JobResponse { job })).into_response()
        }
        CancelJobResult::NotFound => (StatusCode::NOT_FOUND, "job not found").into_response(),
    }
}

fn normalize_callback_url(callback_url: Option<String>) -> Result<Option<String>, String> {
    let Some(callback_url) = callback_url else {
        return Ok(None);
    };
    let trimmed = callback_url.trim();
    if trimmed.is_empty() {
        return Err("callback_url cannot be empty".into());
    }
    Ok(Some(trimmed.to_string()))
}

fn default_retryable_failure() -> bool {
    true
}

fn priority_rank(priority: &str) -> u8 {
    match priority {
        "realtime" => 0,
        "batch" => 1,
        "background" => 2,
        _ => 3,
    }
}

fn kind_rank(kind: &str) -> u8 {
    match kind {
        "vision_analysis" => 0,
        "embed_batch" => 1,
        "index_build" => 2,
        "cluster" => 3,
        _ => 4,
    }
}

fn status_rank(status: &str) -> u8 {
    match status {
        "queued" => 0,
        "running" => 1,
        "complete" => 2,
        "failed" => 3,
        "cancelled" => 4,
        _ => 5,
    }
}

fn unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        build_router,
        metrics::RoutingMetrics,
        priority::PriorityLimiter,
        resources::{ResourceRegistry, ResourceRoutingPolicy},
        router::SessionAffinity,
        AppState,
    };
    use axum::{
        body::Body,
        http::{Method, Request, StatusCode},
    };
    use serde_json::json;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tower::ServiceExt;

    fn test_state() -> AppState {
        test_state_with_jobs(JobRegistry::default())
    }

    fn test_state_with_jobs(jobs: JobRegistry) -> AppState {
        AppState {
            client: reqwest::Client::new(),
            backends: vec![],
            workload_pools: crate::config::WorkloadPoolPolicy::default(),
            worker_pool_ids: vec![],
            strict_worker_pools: false,
            affinity: SessionAffinity::default(),
            metrics: RoutingMetrics::default(),
            callback_policy: crate::callbacks::CallbackPolicy::default(),
            callback_dispatcher: crate::callbacks::CallbackDispatcher::default(),
            resources: ResourceRegistry::default(),
            resource_policy: ResourceRoutingPolicy::default(),
            priority_limiter: PriorityLimiter::default(),
            jobs,
            workers: crate::workers::WorkerRegistry::default(),
        }
    }

    #[tokio::test]
    async fn submit_job_returns_accepted_queued_job() {
        let app = build_router(test_state());

        let response = app
            .oneshot(
                Request::post("/v1/jobs")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "type": "vision_analysis",
                            "priority": "batch",
                            "payload": {"image_id": "image-1"},
                            "callback_url": " http://rhizome/jobs/job-1 "
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let value = response_json(response).await;
        assert_eq!(value["job"]["id"], "job-1");
        assert_eq!(value["job"]["type"], "vision_analysis");
        assert_eq!(value["job"]["priority"], "batch");
        assert_eq!(value["job"]["status"], "queued");
        assert_eq!(value["job"]["payload"], json!({"image_id": "image-1"}));
        assert_eq!(value["job"]["callback_url"], "http://rhizome/jobs/job-1");
        assert_eq!(value["job"]["attempts"], 0);
        assert_eq!(value["job"]["max_attempts"], 3);
    }

    #[tokio::test]
    async fn sqlite_registry_recovers_queued_jobs_and_next_id() {
        let path = unique_db_path("queued");
        let jobs =
            JobRegistry::with_store(crate::storage::SqliteJobStore::open(&path).unwrap()).unwrap();

        jobs.submit(SubmitJobRequest {
            kind: JobKind::IndexBuild,
            priority: Priority::Background,
            payload: json!({"index": "nightly"}),
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::EmbedBatch,
            priority: Priority::Realtime,
            payload: json!({"texts": ["a"]}),
            callback_url: Some("http://rhizome/jobs/job-2".into()),
        })
        .await
        .unwrap();

        let restored =
            JobRegistry::with_store(crate::storage::SqliteJobStore::open(&path).unwrap()).unwrap();
        let listed = restored.list().await;
        assert_eq!(
            listed.iter().map(|job| job.id.as_str()).collect::<Vec<_>>(),
            vec!["job-1", "job-2"]
        );
        assert_eq!(listed[0].payload, json!({"index": "nightly"}));
        assert_eq!(
            listed[1].callback_url.as_deref(),
            Some("http://rhizome/jobs/job-2")
        );

        let queued = restored.queued_jobs_by_priority().await;
        assert_eq!(
            queued.iter().map(|job| job.id.as_str()).collect::<Vec<_>>(),
            vec!["job-2", "job-1"]
        );

        let next = restored
            .submit(SubmitJobRequest {
                kind: JobKind::Cluster,
                priority: Priority::Batch,
                payload: Value::Null,
                callback_url: None,
            })
            .await
            .unwrap();
        assert_eq!(next.id, "job-3");
    }

    #[tokio::test]
    async fn sqlite_registry_recovers_fresh_running_cancelled_and_complete_jobs() {
        let path = unique_db_path("states");
        let jobs =
            JobRegistry::with_store(crate::storage::SqliteJobStore::open(&path).unwrap()).unwrap();

        let running = jobs
            .submit(SubmitJobRequest {
                kind: JobKind::VisionAnalysis,
                priority: Priority::Batch,
                payload: json!({"image": "rose.jpg"}),
                callback_url: None,
            })
            .await
            .unwrap();
        let cancelled = jobs
            .submit(SubmitJobRequest {
                kind: JobKind::EmbedBatch,
                priority: Priority::Realtime,
                payload: Value::Null,
                callback_url: None,
            })
            .await
            .unwrap();
        let complete = jobs
            .submit(SubmitJobRequest {
                kind: JobKind::IndexBuild,
                priority: Priority::Background,
                payload: Value::Null,
                callback_url: None,
            })
            .await
            .unwrap();

        jobs.claim_next_for_worker("worker-a", &[running.kind], 30_000)
            .await
            .unwrap();
        jobs.cancel(&cancelled.id).await;
        jobs.claim_next_for_worker("worker-b", &[complete.kind], 30_000)
            .await
            .unwrap();
        jobs.complete_lease(&complete.id, "worker-b", json!({"ok": true}))
            .await;

        let restored =
            JobRegistry::with_store(crate::storage::SqliteJobStore::open(&path).unwrap()).unwrap();
        let running = restored.get(&running.id).await.unwrap();
        assert_eq!(running.status, JobStatus::Running);
        assert_eq!(running.attempts, 1);
        assert_eq!(
            running.lease.as_ref().map(|lease| lease.worker_id.as_str()),
            Some("worker-a")
        );

        let cancelled = restored.get(&cancelled.id).await.unwrap();
        assert_eq!(cancelled.status, JobStatus::Cancelled);

        let complete = restored.get(&complete.id).await.unwrap();
        assert_eq!(complete.status, JobStatus::Complete);
        assert_eq!(complete.result, Some(json!({"ok": true})));
        assert!(complete.lease.is_none());
    }

    #[tokio::test]
    async fn sqlite_registry_requeues_expired_running_job_on_startup() {
        let path = unique_db_path("expired-requeue");
        let jobs =
            JobRegistry::with_store(crate::storage::SqliteJobStore::open(&path).unwrap()).unwrap();
        let submitted = jobs
            .submit(SubmitJobRequest {
                kind: JobKind::VisionAnalysis,
                priority: Priority::Batch,
                payload: Value::Null,
                callback_url: None,
            })
            .await
            .unwrap();
        jobs.claim_next_for_worker("worker-a", &[submitted.kind], 0)
            .await
            .unwrap();

        let restored =
            JobRegistry::with_store(crate::storage::SqliteJobStore::open(&path).unwrap()).unwrap();
        let job = restored.get(&submitted.id).await.unwrap();
        assert_eq!(job.status, JobStatus::Queued);
        assert_eq!(job.attempts, 1);
        assert!(job.lease.is_none());
        assert_eq!(
            job.error,
            Some(JobFailure {
                message: ATTEMPT_TIMEOUT_ERROR.into(),
                retryable: true,
            })
        );
        assert_eq!(restored.queued_jobs_by_priority().await[0].id, submitted.id);

        let persisted =
            JobRegistry::with_store(crate::storage::SqliteJobStore::open(&path).unwrap()).unwrap();
        assert_eq!(
            persisted.get(&submitted.id).await.unwrap().status,
            JobStatus::Queued
        );
    }

    #[tokio::test]
    async fn sqlite_registry_fails_exhausted_expired_running_job_on_startup() {
        let path = unique_db_path("expired-fail");
        let jobs =
            JobRegistry::with_store(crate::storage::SqliteJobStore::open(&path).unwrap()).unwrap();
        let submitted = jobs
            .submit(SubmitJobRequest {
                kind: JobKind::Cluster,
                priority: Priority::Background,
                payload: Value::Null,
                callback_url: None,
            })
            .await
            .unwrap();

        for _ in 0..2 {
            jobs.claim_next_for_worker("worker-a", &[submitted.kind], 0)
                .await
                .unwrap();
            jobs.requeue_expired_leases().await;
        }
        jobs.claim_next_for_worker("worker-a", &[submitted.kind], 0)
            .await
            .unwrap();

        let restored =
            JobRegistry::with_store(crate::storage::SqliteJobStore::open(&path).unwrap()).unwrap();
        let job = restored.get(&submitted.id).await.unwrap();
        assert_eq!(job.status, JobStatus::Failed);
        assert_eq!(job.attempts, 3);
        assert!(job.lease.is_none());
        assert_eq!(
            job.error,
            Some(JobFailure {
                message: ATTEMPT_TIMEOUT_ERROR.into(),
                retryable: false,
            })
        );
        assert!(restored.queued_jobs_by_priority().await.is_empty());
    }

    #[tokio::test]
    async fn sqlite_backed_endpoints_recover_jobs_after_app_rebuild() {
        let path = unique_db_path("endpoint");
        let jobs =
            JobRegistry::with_store(crate::storage::SqliteJobStore::open(&path).unwrap()).unwrap();
        let app = build_router(test_state_with_jobs(jobs));

        let submit = app
            .oneshot(
                Request::post("/v1/jobs")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "type": "embed_batch",
                            "priority": "batch",
                            "payload": {"texts": ["seed"]},
                            "callback_url": "http://rhizome/jobs/job-1"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(submit.status(), StatusCode::ACCEPTED);

        let restored_jobs =
            JobRegistry::with_store(crate::storage::SqliteJobStore::open(&path).unwrap()).unwrap();
        let restored_app = build_router(test_state_with_jobs(restored_jobs));
        let response = restored_app
            .oneshot(Request::get("/v1/jobs/job-1").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let value = response_json(response).await;
        assert_eq!(value["job"]["id"], "job-1");
        assert_eq!(value["job"]["status"], "queued");
        assert_eq!(value["job"]["payload"], json!({"texts": ["seed"]}));
        assert_eq!(value["job"]["callback_url"], "http://rhizome/jobs/job-1");
    }

    #[tokio::test]
    async fn sqlite_registry_persists_lease_renewal_after_restart() {
        let path = unique_db_path("renew");
        let jobs =
            JobRegistry::with_store(crate::storage::SqliteJobStore::open(&path).unwrap()).unwrap();
        let submitted = jobs
            .submit(SubmitJobRequest {
                kind: JobKind::VisionAnalysis,
                priority: Priority::Batch,
                payload: Value::Null,
                callback_url: None,
            })
            .await
            .unwrap();
        let claimed = jobs
            .claim_next_for_worker("worker-a", &[submitted.kind], 30_000)
            .await
            .unwrap();
        let original_expires_at = claimed.lease.unwrap().expires_at_unix_ms;

        match jobs.renew_lease(&submitted.id, "worker-a", 120_000).await {
            RenewJobLeaseResult::Renewed(job) => {
                assert!(job.lease.unwrap().expires_at_unix_ms > original_expires_at)
            }
            _ => panic!("expected lease renewal"),
        }

        let restored =
            JobRegistry::with_store(crate::storage::SqliteJobStore::open(&path).unwrap()).unwrap();
        let job = restored.get(&submitted.id).await.unwrap();
        assert_eq!(job.status, JobStatus::Running);
        assert_eq!(job.lease.as_ref().unwrap().worker_id, "worker-a");
        assert!(job.lease.unwrap().expires_at_unix_ms > original_expires_at);
    }

    #[tokio::test]
    async fn sqlite_registry_persists_worker_failures_after_restart() {
        let path = unique_db_path("failure");
        let jobs =
            JobRegistry::with_store(crate::storage::SqliteJobStore::open(&path).unwrap()).unwrap();
        let retryable = jobs
            .submit(SubmitJobRequest {
                kind: JobKind::EmbedBatch,
                priority: Priority::Batch,
                payload: Value::Null,
                callback_url: None,
            })
            .await
            .unwrap();
        let terminal = jobs
            .submit(SubmitJobRequest {
                kind: JobKind::Cluster,
                priority: Priority::Background,
                payload: Value::Null,
                callback_url: None,
            })
            .await
            .unwrap();

        jobs.claim_next_for_worker("worker-a", &[retryable.kind], 30_000)
            .await
            .unwrap();
        jobs.fail_lease(
            &retryable.id,
            "worker-a",
            JobFailure {
                message: "transient worker error".into(),
                retryable: true,
            },
        )
        .await;
        jobs.claim_next_for_worker("worker-b", &[terminal.kind], 30_000)
            .await
            .unwrap();
        jobs.fail_lease(
            &terminal.id,
            "worker-b",
            JobFailure {
                message: "bad input".into(),
                retryable: false,
            },
        )
        .await;

        let restored =
            JobRegistry::with_store(crate::storage::SqliteJobStore::open(&path).unwrap()).unwrap();
        let retryable = restored.get(&retryable.id).await.unwrap();
        assert_eq!(retryable.status, JobStatus::Queued);
        assert_eq!(
            retryable.error,
            Some(JobFailure {
                message: "transient worker error".into(),
                retryable: true,
            })
        );
        assert_eq!(
            restored
                .queued_jobs_by_priority()
                .await
                .iter()
                .map(|job| job.id.as_str())
                .collect::<Vec<_>>(),
            vec!["job-1"]
        );

        let terminal = restored.get(&terminal.id).await.unwrap();
        assert_eq!(terminal.status, JobStatus::Failed);
        assert_eq!(
            terminal.error,
            Some(JobFailure {
                message: "bad input".into(),
                retryable: false,
            })
        );
        assert!(terminal.lease.is_none());
    }

    #[tokio::test]
    async fn sqlite_backed_endpoints_claim_recovered_queue_after_app_rebuild() {
        let path = unique_db_path("endpoint-claim");
        let jobs =
            JobRegistry::with_store(crate::storage::SqliteJobStore::open(&path).unwrap()).unwrap();
        let app = build_router(test_state_with_jobs(jobs));
        let submit = app
            .oneshot(
                Request::post("/v1/jobs")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"type": "vision_analysis", "priority": "batch"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(submit.status(), StatusCode::ACCEPTED);

        let restored_jobs =
            JobRegistry::with_store(crate::storage::SqliteJobStore::open(&path).unwrap()).unwrap();
        let restored_app = build_router(test_state_with_jobs(restored_jobs));
        let register = restored_app
            .clone()
            .oneshot(
                Request::post("/v1/workers/register")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "id": "vision-worker",
                            "endpoint_url": "http://vision-worker:9000",
                            "job_types": ["vision_analysis"],
                            "max_concurrent_jobs": 1
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(register.status(), StatusCode::OK);

        let claim = restored_app
            .oneshot(
                Request::post("/v1/workers/vision-worker/claim")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(claim.status(), StatusCode::OK);
        let value = response_json(claim).await;
        assert_eq!(value["job"]["id"], "job-1");
        assert_eq!(value["job"]["status"], "running");
        assert_eq!(value["job"]["lease"]["worker_id"], "vision-worker");
    }

    #[tokio::test]
    async fn submitted_job_can_be_fetched() {
        let app = build_router(test_state());

        let submit = app
            .clone()
            .oneshot(
                Request::post("/v1/jobs")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"type": "embed_batch", "payload": {"texts": ["a", "b"]}})
                            .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(submit.status(), StatusCode::ACCEPTED);

        let response = app
            .oneshot(Request::get("/v1/jobs/job-1").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let value = response_json(response).await;
        assert_eq!(value["job"]["id"], "job-1");
        assert_eq!(value["job"]["type"], "embed_batch");
        assert_eq!(value["job"]["priority"], "realtime");
    }

    #[tokio::test]
    async fn list_jobs_returns_jobs_in_submission_order() {
        let app = build_router(test_state());

        app.clone()
            .oneshot(
                Request::post("/v1/jobs")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({"type": "vision_analysis"}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        app.clone()
            .oneshot(
                Request::post("/v1/jobs")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({"type": "embed_batch"}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        let response = app
            .oneshot(Request::get("/v1/jobs").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let value = response_json(response).await;
        assert_eq!(value["jobs"][0]["id"], "job-1");
        assert_eq!(value["jobs"][0]["type"], "vision_analysis");
        assert_eq!(value["jobs"][1]["id"], "job-2");
        assert_eq!(value["jobs"][1]["type"], "embed_batch");
    }

    #[tokio::test]
    async fn queue_snapshots_count_queued_jobs_by_priority_and_type() {
        let jobs = JobRegistry::default();

        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Realtime,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Realtime,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::EmbedBatch,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();

        assert_eq!(
            jobs.queue_snapshots().await,
            vec![
                JobQueueSnapshot {
                    priority: "realtime",
                    kind: "vision_analysis",
                    depth: 2,
                },
                JobQueueSnapshot {
                    priority: "batch",
                    kind: "embed_batch",
                    depth: 1,
                },
            ]
        );
    }

    #[tokio::test]
    async fn cancel_job_marks_queued_job_cancelled() {
        let app = build_router(test_state());

        let submit = app
            .clone()
            .oneshot(
                Request::post("/v1/jobs")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({"type": "index_build"}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(submit.status(), StatusCode::ACCEPTED);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri("/v1/jobs/job-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let value = response_json(response).await;
        assert_eq!(value["job"]["status"], "cancelled");

        let response = app
            .oneshot(Request::get("/v1/jobs/job-1").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let value = response_json(response).await;
        assert_eq!(value["job"]["status"], "cancelled");
    }

    #[tokio::test]
    async fn cancelling_job_removes_it_from_queue_depth() {
        let jobs = JobRegistry::default();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::IndexBuild,
            priority: Priority::Background,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::IndexBuild,
            priority: Priority::Background,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();

        jobs.cancel("job-1").await;

        assert_eq!(
            jobs.queue_snapshots().await,
            vec![JobQueueSnapshot {
                priority: "background",
                kind: "index_build",
                depth: 1,
            }]
        );
    }

    #[tokio::test]
    async fn queued_jobs_by_priority_returns_priority_order_and_skips_cancelled_jobs() {
        let jobs = JobRegistry::default();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::IndexBuild,
            priority: Priority::Background,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::EmbedBatch,
            priority: Priority::Realtime,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();

        jobs.cancel("job-2").await;

        let queued = jobs.queued_jobs_by_priority().await;
        assert_eq!(
            queued.iter().map(|job| job.id.as_str()).collect::<Vec<_>>(),
            vec!["job-3", "job-1"]
        );
    }

    #[tokio::test]
    async fn claim_next_for_worker_marks_job_running_and_removes_it_from_queue() {
        let jobs = JobRegistry::default();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();

        let claimed = jobs
            .claim_next_for_worker("worker-a", &[JobKind::VisionAnalysis], 30_000)
            .await
            .unwrap();

        assert_eq!(claimed.id, "job-1");
        assert_eq!(claimed.status, JobStatus::Running);
        assert_eq!(claimed.attempts, 1);
        let lease = claimed.lease.unwrap();
        assert_eq!(lease.worker_id, "worker-a");
        assert_eq!(lease.attempt, 1);
        assert!(lease.expires_at_unix_ms >= lease.claimed_at_unix_ms + 30_000);
        assert!(jobs.queue_snapshots().await.is_empty());
        assert!(jobs.queued_jobs_by_priority().await.is_empty());
    }

    #[tokio::test]
    async fn claim_next_for_worker_uses_priority_then_fifo_order() {
        let jobs = JobRegistry::default();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Background,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();

        let first = jobs
            .claim_next_for_worker("worker-a", &[JobKind::VisionAnalysis], 30_000)
            .await
            .unwrap();
        let second = jobs
            .claim_next_for_worker("worker-a", &[JobKind::VisionAnalysis], 30_000)
            .await
            .unwrap();

        assert_eq!(first.id, "job-2");
        assert_eq!(second.id, "job-3");
    }

    #[tokio::test]
    async fn cancelling_running_job_marks_it_cancelled() {
        let jobs = JobRegistry::default();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::EmbedBatch,
            priority: Priority::Realtime,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.claim_next_for_worker("worker-a", &[JobKind::EmbedBatch], 30_000)
            .await
            .unwrap();

        let result = jobs.cancel("job-1").await;
        let CancelJobResult::Cancelled(job) = result else {
            panic!("expected running job cancellation");
        };
        assert_eq!(job.status, JobStatus::Cancelled);
        assert!(job.lease.is_some());
    }

    #[tokio::test]
    async fn cancelling_running_job_prevents_later_lease_renewal() {
        let jobs = JobRegistry::default();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.claim_next_for_worker("worker-a", &[JobKind::VisionAnalysis], 30_000)
            .await
            .unwrap();

        jobs.cancel("job-1").await;

        let result = jobs.renew_lease("job-1", "worker-a", 30_000).await;
        let RenewJobLeaseResult::NotRunning(job) = result else {
            panic!("expected cancelled job to reject renewal");
        };
        assert_eq!(job.status, JobStatus::Cancelled);
        assert!(job.lease.is_some());
    }

    #[tokio::test]
    async fn requeue_expired_leases_requeues_running_job_when_attempts_remain() {
        let jobs = JobRegistry::default();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();

        jobs.claim_next_for_worker("worker-a", &[JobKind::VisionAnalysis], 0)
            .await
            .unwrap();

        let report = jobs.requeue_expired_leases().await;
        assert_eq!(report.requeued, 1);
        assert_eq!(report.failed, 0);
        assert_eq!(report.released_workers, vec!["worker-a"]);
        assert!(report.failed_jobs.is_empty());

        let job = jobs.get("job-1").await.unwrap();
        assert_eq!(job.status, JobStatus::Queued);
        assert_eq!(job.attempts, 1);
        assert_eq!(
            job.error,
            Some(JobFailure {
                message: ATTEMPT_TIMEOUT_ERROR.into(),
                retryable: true,
            })
        );
        assert!(job.lease.is_none());
        assert_eq!(
            jobs.queued_jobs_by_priority()
                .await
                .iter()
                .map(|job| job.id.as_str())
                .collect::<Vec<_>>(),
            vec!["job-1"]
        );

        let reclaimed = jobs
            .claim_next_for_worker("worker-b", &[JobKind::VisionAnalysis], 30_000)
            .await
            .unwrap();
        assert_eq!(reclaimed.status, JobStatus::Running);
        assert_eq!(reclaimed.attempts, 2);
        assert_eq!(reclaimed.lease.unwrap().worker_id, "worker-b");
    }

    #[tokio::test]
    async fn requeue_expired_leases_fails_job_when_attempts_are_exhausted() {
        let jobs = JobRegistry::default();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::Cluster,
            priority: Priority::Background,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();

        for _ in 0..2 {
            jobs.claim_next_for_worker("worker-a", &[JobKind::Cluster], 0)
                .await
                .unwrap();
            let report = jobs.requeue_expired_leases().await;
            assert_eq!(report.requeued, 1);
            assert_eq!(report.failed, 0);
        }

        jobs.claim_next_for_worker("worker-a", &[JobKind::Cluster], 0)
            .await
            .unwrap();
        let report = jobs.requeue_expired_leases().await;
        assert_eq!(report.requeued, 0);
        assert_eq!(report.failed, 1);
        assert_eq!(report.released_workers, vec!["worker-a"]);
        assert_eq!(report.failed_jobs.len(), 1);
        assert_eq!(report.failed_jobs[0].id, "job-1");

        let job = jobs.get("job-1").await.unwrap();
        assert_eq!(job.status, JobStatus::Failed);
        assert_eq!(job.attempts, job.max_attempts);
        assert_eq!(
            job.error,
            Some(JobFailure {
                message: ATTEMPT_TIMEOUT_ERROR.into(),
                retryable: false,
            })
        );
        assert!(job.lease.is_none());
        assert!(jobs.queued_jobs_by_priority().await.is_empty());
    }

    #[tokio::test]
    async fn requeue_expired_leases_ignores_fresh_and_terminal_jobs() {
        let jobs = JobRegistry::default();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::EmbedBatch,
            priority: Priority::Realtime,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::IndexBuild,
            priority: Priority::Background,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();

        jobs.claim_next_for_worker("worker-a", &[JobKind::EmbedBatch], 30_000)
            .await
            .unwrap();
        jobs.claim_next_for_worker("worker-a", &[JobKind::IndexBuild], 0)
            .await
            .unwrap();
        jobs.cancel("job-2").await;

        let report = jobs.requeue_expired_leases().await;
        assert_eq!(report.requeued, 0);
        assert_eq!(report.failed, 0);
        assert!(report.released_workers.is_empty());
        assert!(report.failed_jobs.is_empty());

        assert_eq!(jobs.get("job-1").await.unwrap().status, JobStatus::Running);
        assert_eq!(
            jobs.get("job-2").await.unwrap().status,
            JobStatus::Cancelled
        );
        assert!(jobs.queued_jobs_by_priority().await.is_empty());
    }

    #[tokio::test]
    async fn cancelling_requeued_expired_job_prevents_future_claim() {
        let jobs = JobRegistry::default();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::Cluster,
            priority: Priority::Background,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();

        jobs.claim_next_for_worker("worker-a", &[JobKind::Cluster], 0)
            .await
            .unwrap();
        let report = jobs.requeue_expired_leases().await;
        assert_eq!(report.requeued, 1);

        jobs.cancel("job-1").await;

        assert!(jobs
            .claim_next_for_worker("worker-b", &[JobKind::Cluster], 30_000)
            .await
            .is_none());
        assert!(jobs.queue_snapshots().await.is_empty());
        assert_eq!(
            jobs.get("job-1").await.unwrap().status,
            JobStatus::Cancelled
        );
    }

    #[tokio::test]
    async fn renew_lease_extends_current_workers_running_job() {
        let jobs = JobRegistry::default();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();

        let claimed = jobs
            .claim_next_for_worker("worker-a", &[JobKind::VisionAnalysis], 30_000)
            .await
            .unwrap();
        let original_lease = claimed.lease.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        let result = jobs.renew_lease("job-1", "worker-a", 30_000).await;
        let RenewJobLeaseResult::Renewed(job) = result else {
            panic!("expected lease renewal");
        };
        let renewed_lease = job.lease.unwrap();

        assert_eq!(job.status, JobStatus::Running);
        assert_eq!(job.attempts, 1);
        assert_eq!(renewed_lease.worker_id, "worker-a");
        assert_eq!(renewed_lease.attempt, 1);
        assert_eq!(
            renewed_lease.claimed_at_unix_ms,
            original_lease.claimed_at_unix_ms
        );
        assert!(renewed_lease.expires_at_unix_ms > original_lease.expires_at_unix_ms);
    }

    #[tokio::test]
    async fn renew_lease_rejects_expired_lease() {
        let jobs = JobRegistry::default();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();

        jobs.claim_next_for_worker("worker-a", &[JobKind::VisionAnalysis], 0)
            .await
            .unwrap();

        let result = jobs.renew_lease("job-1", "worker-a", 30_000).await;
        let RenewJobLeaseResult::Expired(job) = result else {
            panic!("expected expired lease rejection");
        };
        assert_eq!(job.status, JobStatus::Running);
        assert_eq!(job.lease.unwrap().worker_id, "worker-a");
    }

    #[tokio::test]
    async fn renew_lease_rejects_worker_that_does_not_hold_lease() {
        let jobs = JobRegistry::default();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::EmbedBatch,
            priority: Priority::Realtime,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();

        jobs.claim_next_for_worker("worker-a", &[JobKind::EmbedBatch], 30_000)
            .await
            .unwrap();

        let result = jobs.renew_lease("job-1", "worker-b", 30_000).await;
        let RenewJobLeaseResult::LeaseNotHeld(job) = result else {
            panic!("expected lease ownership rejection");
        };
        assert_eq!(job.status, JobStatus::Running);
        assert_eq!(job.lease.unwrap().worker_id, "worker-a");
    }

    #[tokio::test]
    async fn renew_lease_rejects_non_running_job() {
        let jobs = JobRegistry::default();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::IndexBuild,
            priority: Priority::Background,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();

        let queued_result = jobs.renew_lease("job-1", "worker-a", 30_000).await;
        let RenewJobLeaseResult::NotRunning(queued_job) = queued_result else {
            panic!("expected queued job rejection");
        };
        assert_eq!(queued_job.status, JobStatus::Queued);

        jobs.cancel("job-1").await;
        let cancelled_result = jobs.renew_lease("job-1", "worker-a", 30_000).await;
        let RenewJobLeaseResult::NotRunning(cancelled_job) = cancelled_result else {
            panic!("expected terminal job rejection");
        };
        assert_eq!(cancelled_job.status, JobStatus::Cancelled);
    }

    #[tokio::test]
    async fn renew_lease_reports_missing_job() {
        let jobs = JobRegistry::default();

        let result = jobs.renew_lease("missing", "worker-a", 30_000).await;
        assert!(matches!(result, RenewJobLeaseResult::NotFound));
    }

    #[tokio::test]
    async fn complete_lease_marks_held_running_job_complete() {
        let jobs = JobRegistry::default();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.claim_next_for_worker("worker-a", &[JobKind::VisionAnalysis], 30_000)
            .await
            .unwrap();

        let result = jobs
            .complete_lease("job-1", "worker-a", json!({"ok": true}))
            .await;
        let FinishJobResult::Completed(job) = result else {
            panic!("expected completion");
        };

        assert_eq!(job.status, JobStatus::Complete);
        assert_eq!(job.result, Some(json!({"ok": true})));
        assert!(job.error.is_none());
        assert!(job.lease.is_none());
        assert!(jobs.queue_snapshots().await.is_empty());
    }

    #[tokio::test]
    async fn complete_lease_rejects_wrong_worker_and_expired_lease() {
        let jobs = JobRegistry::default();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::EmbedBatch,
            priority: Priority::Realtime,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.claim_next_for_worker("worker-a", &[JobKind::EmbedBatch], 30_000)
            .await
            .unwrap();

        let wrong_worker = jobs.complete_lease("job-1", "worker-b", Value::Null).await;
        assert!(matches!(wrong_worker, FinishJobResult::LeaseNotHeld(_)));

        jobs.submit(SubmitJobRequest {
            kind: JobKind::Cluster,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.claim_next_for_worker("worker-a", &[JobKind::Cluster], 0)
            .await
            .unwrap();

        let expired = jobs.complete_lease("job-2", "worker-a", Value::Null).await;
        assert!(matches!(expired, FinishJobResult::Expired(_)));
    }

    #[tokio::test]
    async fn fail_lease_requeues_retryable_failure_when_attempts_remain() {
        let jobs = JobRegistry::default();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::IndexBuild,
            priority: Priority::Background,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.claim_next_for_worker("worker-a", &[JobKind::IndexBuild], 30_000)
            .await
            .unwrap();

        let result = jobs
            .fail_lease(
                "job-1",
                "worker-a",
                JobFailure {
                    message: "transient oom".into(),
                    retryable: true,
                },
            )
            .await;
        let FinishJobResult::Requeued(job) = result else {
            panic!("expected retryable failure to requeue");
        };

        assert_eq!(job.status, JobStatus::Queued);
        assert_eq!(job.attempts, 1);
        assert!(job.lease.is_none());
        assert_eq!(
            job.error,
            Some(JobFailure {
                message: "transient oom".into(),
                retryable: true,
            })
        );
        assert_eq!(
            jobs.queued_jobs_by_priority()
                .await
                .iter()
                .map(|job| job.id.as_str())
                .collect::<Vec<_>>(),
            vec!["job-1"]
        );
    }

    #[tokio::test]
    async fn fail_lease_marks_non_retryable_or_exhausted_failure_failed() {
        let jobs = JobRegistry::default();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::Cluster,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.claim_next_for_worker("worker-a", &[JobKind::Cluster], 30_000)
            .await
            .unwrap();

        let non_retryable = jobs
            .fail_lease(
                "job-1",
                "worker-a",
                JobFailure {
                    message: "bad input".into(),
                    retryable: false,
                },
            )
            .await;
        let FinishJobResult::Failed(job) = non_retryable else {
            panic!("expected non-retryable failure");
        };
        assert_eq!(job.status, JobStatus::Failed);
        assert!(job.lease.is_none());

        jobs.submit(SubmitJobRequest {
            kind: JobKind::EmbedBatch,
            priority: Priority::Realtime,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        for _ in 0..2 {
            jobs.claim_next_for_worker("worker-a", &[JobKind::EmbedBatch], 30_000)
                .await
                .unwrap();
            let result = jobs
                .fail_lease(
                    "job-2",
                    "worker-a",
                    JobFailure {
                        message: "temporary failure".into(),
                        retryable: true,
                    },
                )
                .await;
            assert!(matches!(result, FinishJobResult::Requeued(_)));
        }
        jobs.claim_next_for_worker("worker-a", &[JobKind::EmbedBatch], 30_000)
            .await
            .unwrap();
        let exhausted = jobs
            .fail_lease(
                "job-2",
                "worker-a",
                JobFailure {
                    message: "temporary failure".into(),
                    retryable: true,
                },
            )
            .await;
        let FinishJobResult::Failed(job) = exhausted else {
            panic!("expected retry exhaustion");
        };
        assert_eq!(job.status, JobStatus::Failed);
        assert_eq!(job.attempts, job.max_attempts);
        assert!(jobs.queued_jobs_by_priority().await.is_empty());
    }

    #[tokio::test]
    async fn queue_wait_snapshots_measure_queued_job_age_by_priority_and_type() {
        let jobs = JobRegistry::default();

        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let snapshots = jobs.queue_wait_snapshots().await;
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].priority, "batch");
        assert_eq!(snapshots[0].kind, "vision_analysis");
        assert_eq!(snapshots[0].count, 2);
        assert!(snapshots[0].wait_seconds_sum > 0.0);
        assert!(snapshots[0].wait_seconds_max > 0.0);
    }

    #[tokio::test]
    async fn terminal_duration_snapshots_group_terminal_jobs() {
        let jobs = JobRegistry::default();

        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::EmbedBatch,
            priority: Priority::Realtime,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::Cluster,
            priority: Priority::Background,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        jobs.claim_next_for_worker("worker-a", &[JobKind::VisionAnalysis], 30_000)
            .await
            .unwrap();
        jobs.complete_lease("job-1", "worker-a", json!({"ok": true}))
            .await;
        jobs.cancel("job-2").await;

        let snapshots = jobs.terminal_duration_snapshots().await;

        assert_eq!(snapshots.len(), 2);
        assert_eq!(snapshots[0].priority, "realtime");
        assert_eq!(snapshots[0].kind, "embed_batch");
        assert_eq!(snapshots[0].status, "cancelled");
        assert_eq!(snapshots[0].count, 1);
        assert_eq!(snapshots[1].priority, "batch");
        assert_eq!(snapshots[1].kind, "vision_analysis");
        assert_eq!(snapshots[1].status, "complete");
        assert_eq!(snapshots[1].count, 1);
        assert!(snapshots[1].duration_seconds_sum > 0.0);
        assert!(snapshots[1].duration_seconds_max > 0.0);
    }

    #[tokio::test]
    async fn cancelling_job_removes_it_from_queue_wait_snapshots() {
        let jobs = JobRegistry::default();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::IndexBuild,
            priority: Priority::Background,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::IndexBuild,
            priority: Priority::Background,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();

        jobs.cancel("job-1").await;

        let snapshots = jobs.queue_wait_snapshots().await;
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].priority, "background");
        assert_eq!(snapshots[0].kind, "index_build");
        assert_eq!(snapshots[0].count, 1);
    }

    #[tokio::test]
    async fn metrics_reports_queue_depth_and_wait_time_for_queued_jobs() {
        let app = build_router(test_state());

        app.clone()
            .oneshot(
                Request::post("/v1/jobs")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"type": "vision_analysis", "priority": "batch"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        app.clone()
            .oneshot(
                Request::post("/v1/jobs")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"type": "vision_analysis", "priority": "batch"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        app.clone()
            .oneshot(
                Request::post("/v1/jobs")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"type": "cluster", "priority": "background"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        app.clone()
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri("/v1/jobs/job-3")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let response = app
            .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
            .await
            .unwrap();

        let metrics = response_text(response).await;
        assert!(metrics
            .contains("fairlead_job_queue_depth{priority=\"batch\",type=\"vision_analysis\"} 2"));
        assert!(metrics.contains(
            "fairlead_job_queue_wait_seconds_sum{priority=\"batch\",type=\"vision_analysis\"} "
        ));
        assert!(metrics.contains(
            "fairlead_job_queue_wait_seconds_max{priority=\"batch\",type=\"vision_analysis\"} "
        ));
        assert!(
            !metrics.contains("fairlead_job_queue_depth{priority=\"background\",type=\"cluster\"}")
        );
        assert!(!metrics.contains(
            "fairlead_job_queue_wait_seconds_sum{priority=\"background\",type=\"cluster\"}"
        ));
        assert!(!metrics.contains(
            "fairlead_job_queue_wait_seconds_max{priority=\"background\",type=\"cluster\"}"
        ));
    }

    #[tokio::test]
    async fn metrics_reports_terminal_job_duration() {
        let state = test_state();
        let jobs = state.jobs.clone();
        let app = build_router(state);

        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::Cluster,
            priority: Priority::Background,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::EmbedBatch,
            priority: Priority::Realtime,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        jobs.claim_next_for_worker("worker-a", &[JobKind::VisionAnalysis], 30_000)
            .await
            .unwrap();
        jobs.complete_lease("job-1", "worker-a", json!({"ok": true}))
            .await;
        jobs.claim_next_for_worker("worker-a", &[JobKind::Cluster], 30_000)
            .await
            .unwrap();
        jobs.fail_lease(
            "job-2",
            "worker-a",
            JobFailure {
                message: "bad input".into(),
                retryable: false,
            },
        )
        .await;

        let response = app
            .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let metrics = response_text(response).await;

        assert!(metrics.contains(
            "fairlead_job_duration_seconds_count{priority=\"batch\",type=\"vision_analysis\",status=\"complete\"} 1"
        ));
        assert!(metrics.contains(
            "fairlead_job_duration_seconds_sum{priority=\"batch\",type=\"vision_analysis\",status=\"complete\"} "
        ));
        assert!(metrics.contains(
            "fairlead_job_duration_seconds_max{priority=\"batch\",type=\"vision_analysis\",status=\"complete\"} "
        ));
        assert!(metrics.contains(
            "fairlead_job_duration_seconds_count{priority=\"background\",type=\"cluster\",status=\"failed\"} 1"
        ));
        assert!(!metrics.contains(
            "fairlead_job_duration_seconds_count{priority=\"realtime\",type=\"embed_batch\""
        ));
    }

    #[tokio::test]
    async fn cancel_terminal_job_returns_conflict() {
        let app = build_router(test_state());

        app.clone()
            .oneshot(
                Request::post("/v1/jobs")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({"type": "cluster"}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        app.clone()
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri("/v1/jobs/job-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri("/v1/jobs/job-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CONFLICT);
        let value = response_json(response).await;
        assert_eq!(value["job"]["status"], "cancelled");
    }

    #[tokio::test]
    async fn get_unknown_job_returns_404() {
        let app = build_router(test_state());

        let response = app
            .oneshot(
                Request::get("/v1/jobs/missing")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn empty_callback_url_returns_400() {
        let app = build_router(test_state());

        let response = app
            .oneshot(
                Request::post("/v1/jobs")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"type": "vision_analysis", "callback_url": "   "}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn invalid_submit_job_payloads_return_client_errors() {
        let app = build_router(test_state());

        for body in [
            json!({"priority": "batch"}).to_string(),
            json!({"type": "unknown"}).to_string(),
            json!({"type": "vision_analysis", "priority": "urgent"}).to_string(),
            "{not-json".to_string(),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::post("/v1/jobs")
                        .header("content-type", "application/json")
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();

            assert!(
                response.status().is_client_error(),
                "expected client error, got {}",
                response.status()
            );
        }
    }

    async fn response_json(response: axum::response::Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    fn unique_db_path(prefix: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("fairlead-jobs-{prefix}-{unique}.sqlite3"))
    }

    async fn response_text(response: axum::response::Response) -> String {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }
}
