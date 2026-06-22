use axum::{routing::post, Json, Router};
use reqwest::StatusCode;
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use std::{
    collections::VecDeque,
    fs::{self, File},
    net::TcpListener,
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::mpsc;

struct FairleadProcess {
    child: Child,
    base_url: String,
    port: u16,
    temp_dir: PathBuf,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
    env: Vec<(String, String)>,
}

impl FairleadProcess {
    fn spawn(extra_env: &[(&str, &str)]) -> Self {
        let port = reserve_port();
        let temp_dir = unique_temp_dir("fairlead-process-harness");
        let env = process_env(port, extra_env);
        Self::spawn_with(port, temp_dir, env)
    }

    fn spawn_with(port: u16, temp_dir: PathBuf, env: Vec<(String, String)>) -> Self {
        fs::create_dir_all(&temp_dir).expect("create process harness temp dir");
        let stdout_path = temp_dir.join("fairlead.stdout.log");
        let stderr_path = temp_dir.join("fairlead.stderr.log");
        let stdout = File::create(&stdout_path).expect("create Fairlead stdout log");
        let stderr = File::create(&stderr_path).expect("create Fairlead stderr log");

        let mut command = Command::new(env!("CARGO_BIN_EXE_fairlead"));
        command
            .env_clear()
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));
        for (key, value) in &env {
            command.env(key, value);
        }

        let child = command.spawn().expect("spawn Fairlead process");
        Self {
            child,
            base_url: format!("http://127.0.0.1:{port}"),
            port,
            temp_dir,
            stdout_path,
            stderr_path,
            env,
        }
    }

    async fn wait_for_health(&mut self) -> Value {
        let client = reqwest::Client::new();
        let health_url = format!("{}/health", self.base_url);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);

        loop {
            if let Some(status) = self.child.try_wait().expect("poll Fairlead process") {
                panic!(
                    "Fairlead exited before health check passed: {status}; stderr: {}",
                    self.stderr()
                );
            }

            let last_error = match client.get(&health_url).send().await {
                Ok(response) if response.status() == StatusCode::OK => {
                    return response.json().await.expect("parse health response");
                }
                Ok(response) => format!("unexpected health status {}", response.status()),
                Err(err) => err.to_string(),
            };

            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "Fairlead did not become healthy: {last_error}; stderr: {}",
                    self.stderr()
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn stderr(&self) -> String {
        fs::read_to_string(&self.stderr_path).unwrap_or_default()
    }

    async fn get_json(&self, path: &str) -> (StatusCode, Value) {
        let response = reqwest::Client::new()
            .get(format!("{}{}", self.base_url, path))
            .send()
            .await
            .expect("send GET request to Fairlead process");
        json_response(response).await
    }

    async fn post_json(&self, path: &str, body: Value) -> (StatusCode, Value) {
        let response = reqwest::Client::new()
            .post(format!("{}{}", self.base_url, path))
            .json(&body)
            .send()
            .await
            .expect("send POST request to Fairlead process");
        json_response(response).await
    }

    async fn delete_json(&self, path: &str) -> (StatusCode, Value) {
        let response = reqwest::Client::new()
            .delete(format!("{}{}", self.base_url, path))
            .send()
            .await
            .expect("send DELETE request to Fairlead process");
        json_response(response).await
    }

    async fn get_text(&self, path: &str) -> (StatusCode, String) {
        let response = reqwest::Client::new()
            .get(format!("{}{}", self.base_url, path))
            .send()
            .await
            .expect("send GET request to Fairlead process");
        let status = response.status();
        let body = response.text().await.expect("read Fairlead response body");
        (status, body)
    }

    async fn submit_job(&self, body: Value) -> (StatusCode, Value) {
        self.post_json("/v1/jobs", body).await
    }

    async fn prune_jobs(&self) -> (StatusCode, Value) {
        self.post_json("/v1/jobs/prune", json!({})).await
    }

    async fn register_worker(&self, body: Value) -> (StatusCode, Value) {
        self.post_json("/v1/workers/register", body).await
    }

    async fn drain_worker(&self, worker_id: &str) -> (StatusCode, Value) {
        self.post_json(&format!("/v1/workers/{worker_id}/drain"), json!({}))
            .await
    }

    async fn reactivate_worker(&self, worker_id: &str) -> (StatusCode, Value) {
        self.post_json(&format!("/v1/workers/{worker_id}/reactivate"), json!({}))
            .await
    }

    async fn heartbeat_worker(&self, worker_id: &str) -> (StatusCode, Value) {
        self.post_json(&format!("/v1/workers/{worker_id}/heartbeat"), json!({}))
            .await
    }

    async fn deregister_worker(&self, worker_id: &str) -> (StatusCode, Value) {
        self.delete_json(&format!("/v1/workers/{worker_id}")).await
    }

    async fn claim_worker_job(&self, worker_id: &str) -> (StatusCode, Value) {
        self.post_json(&format!("/v1/workers/{worker_id}/claim"), json!({}))
            .await
    }

    async fn complete_worker_job(
        &self,
        worker_id: &str,
        job_id: &str,
        body: Value,
    ) -> (StatusCode, Value) {
        self.post_json(
            &format!("/v1/workers/{worker_id}/jobs/{job_id}/complete"),
            body,
        )
        .await
    }

    async fn renew_worker_job(&self, worker_id: &str, job_id: &str) -> (StatusCode, Value) {
        self.post_json(
            &format!("/v1/workers/{worker_id}/jobs/{job_id}/renew"),
            json!({}),
        )
        .await
    }

    async fn fail_worker_job(
        &self,
        worker_id: &str,
        job_id: &str,
        body: Value,
    ) -> (StatusCode, Value) {
        self.post_json(&format!("/v1/workers/{worker_id}/jobs/{job_id}/fail"), body)
            .await
    }

    async fn shutdown(&mut self) {
        if self
            .child
            .try_wait()
            .expect("poll Fairlead process")
            .is_none()
        {
            self.child.kill().expect("kill Fairlead process");
        }
        self.child.wait().expect("wait for Fairlead process");
    }

    async fn restart(&mut self) {
        self.shutdown().await;
        let port = self.port;
        let temp_dir = self.temp_dir.clone();
        let env = self.env.clone();
        let restarted = Self::spawn_with(port, temp_dir, env);
        *self = restarted;
    }
}

impl Drop for FairleadProcess {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        let _ = fs::remove_dir_all(&self.temp_dir);
    }
}

fn reserve_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve local port");
    listener.local_addr().expect("read local port").port()
}

fn process_env(port: u16, extra_env: &[(&str, &str)]) -> Vec<(String, String)> {
    let mut env = vec![
        ("PORT".to_string(), port.to_string()),
        ("LOG_LEVEL".to_string(), "info".to_string()),
        ("JOB_STORE".to_string(), "memory".to_string()),
    ];
    for (key, value) in extra_env {
        env.retain(|(existing_key, _)| existing_key != key);
        env.push((key.to_string(), value.to_string()));
    }
    env
}

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()))
}

fn assert_process_startup_fails(extra_env: &[(&str, &str)], expected_stderr: &str) {
    let port = reserve_port();
    let temp_dir = unique_temp_dir("fairlead-process-startup-failure");
    fs::create_dir_all(&temp_dir).expect("create startup failure temp dir");
    let stdout_path = temp_dir.join("fairlead.stdout.log");
    let stderr_path = temp_dir.join("fairlead.stderr.log");
    let stdout = File::create(&stdout_path).expect("create Fairlead stdout log");
    let stderr = File::create(&stderr_path).expect("create Fairlead stderr log");

    let mut command = Command::new(env!("CARGO_BIN_EXE_fairlead"));
    command
        .env_clear()
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    for (key, value) in process_env(port, extra_env) {
        command.env(key, value);
    }

    let mut child = command.spawn().expect("spawn Fairlead process");
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let status = loop {
        if let Some(status) = child.try_wait().expect("poll Fairlead process") {
            break status;
        }
        if std::time::Instant::now() >= deadline {
            child.kill().expect("kill hung Fairlead process");
            child.wait().expect("wait for killed Fairlead process");
            panic!("Fairlead did not exit for invalid startup configuration");
        }
        std::thread::sleep(Duration::from_millis(25));
    };

    let stderr = fs::read_to_string(&stderr_path).unwrap_or_default();
    assert!(
        !status.success(),
        "Fairlead unexpectedly started successfully"
    );
    assert!(
        stderr.contains(expected_stderr),
        "stderr did not contain {expected_stderr:?}; stderr: {stderr}"
    );

    fs::remove_dir_all(temp_dir).expect("remove startup failure temp dir");
}

async fn json_response(response: reqwest::Response) -> (StatusCode, Value) {
    let status = response.status();
    let body = response.text().await.expect("read Fairlead response body");
    let value = serde_json::from_str(&body).unwrap_or_else(|_| json!({ "raw": body }));
    (status, value)
}

async fn start_callback_target(status: StatusCode) -> (String, mpsc::Receiver<Value>) {
    let (tx, rx) = mpsc::channel(8);
    let app = Router::new().route(
        "/callback",
        post(move |Json(value): Json<Value>| {
            let tx = tx.clone();
            async move {
                tx.send(value).await.expect("record callback request");
                status
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind callback target");
    let addr = listener.local_addr().expect("read callback target address");
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("serve callback target");
    });
    (format!("http://{addr}/callback"), rx)
}

async fn start_sequence_callback_target(
    statuses: Vec<StatusCode>,
) -> (String, mpsc::Receiver<Value>) {
    let (tx, rx) = mpsc::channel(8);
    let statuses = Arc::new(Mutex::new(VecDeque::from(statuses)));
    let app = Router::new().route(
        "/callback",
        post(move |Json(value): Json<Value>| {
            let tx = tx.clone();
            let statuses = statuses.clone();
            async move {
                tx.send(value).await.expect("record callback request");
                statuses
                    .lock()
                    .expect("callback statuses mutex poisoned")
                    .pop_front()
                    .unwrap_or(StatusCode::OK)
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind callback target");
    let addr = listener.local_addr().expect("read callback target address");
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("serve callback target");
    });
    (format!("http://{addr}/callback"), rx)
}

async fn wait_for_callback_status(
    fairlead: &FairleadProcess,
    job_id: &str,
    expected_status: &str,
) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let (status, fetched) = fairlead.get_json(&format!("/v1/jobs/{job_id}")).await;
        assert_eq!(status, StatusCode::OK);
        if fetched["job"]["callback"]["status"] == expected_status {
            return fetched;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "callback status {expected_status} was not observed for {job_id}; last job: {fetched}"
            );
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_callback_attempt(
    fairlead: &FairleadProcess,
    job_id: &str,
    expected_status: &str,
    expected_attempts: u64,
    expected_http_status: u64,
) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let (status, fetched) = fairlead.get_json(&format!("/v1/jobs/{job_id}")).await;
        assert_eq!(status, StatusCode::OK);
        let callback = &fetched["job"]["callback"];
        if callback["status"] == expected_status
            && callback["attempts"] == expected_attempts
            && callback["last_http_status"] == expected_http_status
        {
            return fetched;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "callback attempt {expected_attempts}/{expected_http_status} was not observed for {job_id}; last job: {fetched}"
            );
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn expire_sqlite_job_lease(db_path: &str, job_id: &str) {
    let connection = Connection::open(db_path).expect("open SQLite job store for lease edit");
    let raw_lease: String = connection
        .query_row(
            "SELECT lease_json FROM jobs WHERE id = ?1",
            params![job_id],
            |row| row.get(0),
        )
        .expect("read persisted lease JSON");
    let mut lease: Value = serde_json::from_str(&raw_lease).expect("parse persisted lease JSON");
    let expired_at = current_unix_ms().saturating_sub(1);
    lease["expires_at_unix_ms"] = json!(expired_at);
    connection
        .execute(
            "UPDATE jobs SET lease_json = ?1, updated_at_unix_ms = ?2 WHERE id = ?3",
            params![lease.to_string(), expired_at as i64, job_id],
        )
        .expect("persist expired lease JSON");
}

fn current_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_millis()
}

async fn wait_for_job_status(
    fairlead: &FairleadProcess,
    job_id: &str,
    expected_status: &str,
) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let (status, fetched) = fairlead.get_json(&format!("/v1/jobs/{job_id}")).await;
        assert_eq!(status, StatusCode::OK);
        if fetched["job"]["status"] == expected_status {
            return fetched;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "job status {expected_status} was not observed for {job_id}; last job: {fetched}"
            );
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_job_not_found(fairlead: &FairleadProcess, job_id: &str) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let (status, fetched) = fairlead.get_json(&format!("/v1/jobs/{job_id}")).await;
        if status == StatusCode::NOT_FOUND {
            return fetched;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("job {job_id} was not pruned; last status: {status}; last body: {fetched}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn assert_job_remains_present_for(
    fairlead: &FairleadProcess,
    job_id: &str,
    duration: Duration,
) {
    let deadline = tokio::time::Instant::now() + duration;
    loop {
        let (status, fetched) = fairlead.get_json(&format!("/v1/jobs/{job_id}")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(fetched["job"]["id"], job_id);
        if tokio::time::Instant::now() >= deadline {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_metrics_contains(fairlead: &FairleadProcess, expected: &str) -> String {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let (status, metrics) = fairlead.get_text("/metrics").await;
        assert_eq!(status, StatusCode::OK);
        if metrics.contains(expected) {
            return metrics;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("metrics did not contain {expected:?}; last metrics: {metrics}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn assert_metrics_contain(metrics: &str, expected: &[&str]) {
    for sample in expected {
        assert!(
            metrics.contains(sample),
            "metrics did not contain {sample:?}; metrics: {metrics}"
        );
    }
}

#[tokio::test]
async fn fairlead_process_starts_serves_health_and_shuts_down() {
    let mut fairlead = FairleadProcess::spawn(&[]);

    let health = fairlead.wait_for_health().await;
    assert_eq!(health["status"], "ok");
    assert!(fairlead.stdout_path.exists());
    assert!(fairlead.stderr_path.exists());

    fairlead.shutdown().await;
}

#[tokio::test]
async fn invalid_scheduler_env_exits_before_serving_health() {
    for (key, value) in [
        ("JOB_RETENTION_SECS", "abc"),
        ("JOB_PRUNE_LIMIT", "0"),
        ("JOB_LEASE_DURATION_MS", "0"),
        ("JOB_MAINTENANCE_INTERVAL_SECS", "abc"),
        ("JOB_PRUNE_INTERVAL_SECS", "0"),
    ] {
        assert_process_startup_fails(&[(key, value)], key);
    }
}

#[tokio::test]
async fn invalid_callback_env_exits_before_serving_health() {
    for (key, value) in [
        ("CALLBACK_MAX_ATTEMPTS", "0"),
        ("CALLBACK_TIMEOUT_SECS", "0"),
        ("CALLBACK_RETRY_DELAY_MS", "abc"),
    ] {
        assert_process_startup_fails(&[(key, value)], key);
    }
}

#[tokio::test]
async fn sqlite_job_state_survives_process_restart() {
    let db_dir = unique_temp_dir("fairlead-process-db");
    fs::create_dir_all(&db_dir).expect("create SQLite process test temp dir");
    let db_path = db_dir.join("jobs.sqlite3");
    let db_path = db_path.to_string_lossy().to_string();
    let mut fairlead =
        FairleadProcess::spawn(&[("JOB_STORE", "sqlite"), ("JOB_DB_PATH", &db_path)]);
    fairlead.wait_for_health().await;

    let (status, submitted) = fairlead
        .submit_job(json!({
            "type": "vision_analysis",
            "priority": "batch",
            "payload": { "image": "rose.jpg" },
            "idempotency_key": "rose-1"
        }))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(submitted["job"]["id"], "job-1");
    assert_eq!(submitted["job"]["status"], "queued");

    fairlead.restart().await;
    fairlead.wait_for_health().await;

    let (status, fetched) = fairlead.get_json("/v1/jobs/job-1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(fetched["job"]["id"], "job-1");
    assert_eq!(fetched["job"]["status"], "queued");
    assert_eq!(fetched["job"]["idempotency_key"], "rose-1");

    let (status, duplicate) = fairlead
        .submit_job(json!({
            "type": "vision_analysis",
            "priority": "batch",
            "payload": { "image": "rose.jpg" },
            "idempotency_key": "rose-1"
        }))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(duplicate["job"]["id"], "job-1");

    fairlead.shutdown().await;
    fs::remove_dir_all(db_dir).expect("remove SQLite process test temp dir");
}

#[tokio::test]
async fn sqlite_idempotency_keys_survive_restart_and_release_after_prune() {
    let db_dir = unique_temp_dir("fairlead-process-idempotency-db");
    fs::create_dir_all(&db_dir).expect("create idempotency SQLite process test temp dir");
    let db_path = db_dir.join("jobs.sqlite3");
    let db_path = db_path.to_string_lossy().to_string();
    let mut fairlead = FairleadProcess::spawn(&[
        ("JOB_STORE", "sqlite"),
        ("JOB_DB_PATH", &db_path),
        ("JOB_RETENTION_SECS", "1"),
        ("JOB_PRUNE_LIMIT", "10"),
    ]);
    fairlead.wait_for_health().await;

    for (body, expected_error) in [
        (
            json!({
                "type": "vision_analysis",
                "idempotency_key": "   "
            }),
            "idempotency_key cannot be empty",
        ),
        (
            json!({
                "type": "vision_analysis",
                "idempotency_key": "x".repeat(257)
            }),
            "idempotency_key cannot exceed 256 bytes",
        ),
    ] {
        let (status, rejected) = fairlead.submit_job(body).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(rejected["raw"], expected_error);
    }

    let original_body = json!({
        "type": "vision_analysis",
        "priority": "batch",
        "payload": { "image": "rose.jpg" },
        "idempotency_key": "rose-image"
    });
    let (status, submitted) = fairlead.submit_job(original_body.clone()).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(submitted["job"]["id"], "job-1");
    assert_eq!(submitted["job"]["idempotency_key"], "rose-image");

    let (status, duplicate) = fairlead.submit_job(original_body.clone()).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(duplicate["job"]["id"], "job-1");

    let (status, conflict) = fairlead
        .submit_job(json!({
            "type": "vision_analysis",
            "priority": "batch",
            "payload": { "image": "lily.jpg" },
            "idempotency_key": "rose-image"
        }))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        conflict["raw"],
        "idempotency_key already used for a different job request"
    );

    let (status, listed) = fairlead.get_json("/v1/jobs").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        listed["jobs"]
            .as_array()
            .expect("jobs response is an array")
            .len(),
        1
    );

    fairlead.restart().await;
    fairlead.wait_for_health().await;

    let (status, after_restart) = fairlead.submit_job(original_body.clone()).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(after_restart["job"]["id"], "job-1");
    assert_eq!(after_restart["job"]["status"], "queued");

    let (status, _) = fairlead
        .register_worker(json!({
            "id": "idempotency-worker",
            "endpoint_url": "http://idempotency-worker.local",
            "node_id": "spark-a",
            "pool": "default",
            "job_types": ["vision_analysis"],
            "max_concurrent_jobs": 1,
            "available_vram_mb": 4096
        }))
        .await;
    assert_eq!(status, StatusCode::OK);

    let (status, claim) = fairlead.claim_worker_job("idempotency-worker").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(claim["job"]["id"], "job-1");

    let (status, completed) = fairlead
        .complete_worker_job(
            "idempotency-worker",
            "job-1",
            json!({
                "attempt": 1,
                "result": { "classification": "healthy" }
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(completed["job"]["status"], "complete");

    let (status, retained_terminal) = fairlead.submit_job(original_body).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(retained_terminal["job"]["id"], "job-1");
    assert_eq!(retained_terminal["job"]["status"], "complete");

    tokio::time::sleep(Duration::from_millis(1_200)).await;
    let (status, pruned) = fairlead.prune_jobs().await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(pruned["pruned"]["removed"], 1);

    let body = wait_for_job_not_found(&fairlead, "job-1").await;
    assert_eq!(body["raw"], "job not found");

    let (status, new_job) = fairlead
        .submit_job(json!({
            "type": "vision_analysis",
            "priority": "batch",
            "payload": { "image": "lily.jpg" },
            "idempotency_key": "rose-image"
        }))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(new_job["job"]["id"], "job-2");
    assert_eq!(new_job["job"]["idempotency_key"], "rose-image");

    fairlead.shutdown().await;
    fs::remove_dir_all(db_dir).expect("remove idempotency SQLite process test temp dir");
}

#[tokio::test]
async fn sqlite_cancelled_job_stays_idempotent_after_process_restart() {
    let db_dir = unique_temp_dir("fairlead-process-cancel-idempotency-db");
    fs::create_dir_all(&db_dir).expect("create cancellation SQLite process test temp dir");
    let db_path = db_dir.join("jobs.sqlite3");
    let db_path = db_path.to_string_lossy().to_string();
    let mut fairlead = FairleadProcess::spawn(&[
        ("JOB_STORE", "sqlite"),
        ("JOB_DB_PATH", &db_path),
        ("CALLBACK_RETRY_DELAY_MS", "50"),
    ]);
    fairlead.wait_for_health().await;
    let (callback_url, mut callbacks) = start_callback_target(StatusCode::OK).await;

    let body = json!({
        "type": "vision_analysis",
        "priority": "batch",
        "payload": { "image": "cancel-me.jpg" },
        "callback_url": callback_url,
        "idempotency_key": "cancel-image"
    });
    let (status, submitted) = fairlead.submit_job(body.clone()).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(submitted["job"]["id"], "job-1");
    assert_eq!(submitted["job"]["status"], "queued");

    let (status, cancelled) = fairlead.delete_json("/v1/jobs/job-1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(cancelled["job"]["status"], "cancelled");
    let callback = callbacks
        .recv()
        .await
        .expect("cancellation callback was delivered");
    assert_eq!(callback["job"]["id"], "job-1");
    assert_eq!(callback["job"]["status"], "cancelled");
    let delivered = wait_for_callback_status(&fairlead, "job-1", "delivered").await;
    assert_eq!(delivered["job"]["callback"]["attempts"], 1);

    fairlead.restart().await;
    fairlead.wait_for_health().await;

    let (status, duplicate_cancel) = fairlead.delete_json("/v1/jobs/job-1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(duplicate_cancel["job"]["status"], "cancelled");
    assert_eq!(duplicate_cancel["job"]["callback"]["status"], "delivered");
    assert_eq!(duplicate_cancel["job"]["callback"]["attempts"], 1);

    let (status, duplicate_submit) = fairlead.submit_job(body).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(duplicate_submit["job"]["id"], "job-1");
    assert_eq!(duplicate_submit["job"]["status"], "cancelled");
    assert_eq!(duplicate_submit["job"]["callback"]["attempts"], 1);

    let duplicate_callback =
        tokio::time::timeout(Duration::from_millis(150), callbacks.recv()).await;
    assert!(
        duplicate_callback.is_err(),
        "duplicate cancellation retry delivered another callback"
    );

    fairlead.shutdown().await;
    fs::remove_dir_all(db_dir).expect("remove cancellation SQLite process test temp dir");
}

#[tokio::test]
async fn sqlite_terminal_worker_results_stay_idempotent_after_process_restart() {
    let db_dir = unique_temp_dir("fairlead-process-terminal-idempotency-db");
    fs::create_dir_all(&db_dir).expect("create terminal idempotency SQLite process test temp dir");
    let db_path = db_dir.join("jobs.sqlite3");
    let db_path = db_path.to_string_lossy().to_string();
    let mut fairlead = FairleadProcess::spawn(&[
        ("JOB_STORE", "sqlite"),
        ("JOB_DB_PATH", &db_path),
        ("CALLBACK_RETRY_DELAY_MS", "50"),
    ]);
    fairlead.wait_for_health().await;
    let (callback_url, mut callbacks) = start_callback_target(StatusCode::OK).await;

    let (status, _) = fairlead
        .register_worker(json!({
            "id": "terminal-worker",
            "endpoint_url": "http://terminal-worker.local",
            "node_id": "spark-a",
            "pool": "default",
            "job_types": ["vision_analysis", "embed_batch"],
            "max_concurrent_jobs": 1,
            "available_vram_mb": 4096
        }))
        .await;
    assert_eq!(status, StatusCode::OK);

    let (status, complete_job) = fairlead
        .submit_job(json!({
            "type": "vision_analysis",
            "priority": "batch",
            "payload": { "image": "complete-me.jpg" },
            "callback_url": callback_url
        }))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(complete_job["job"]["id"], "job-1");

    let (status, claim) = fairlead.claim_worker_job("terminal-worker").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(claim["job"]["id"], "job-1");

    let completion_body = json!({
        "attempt": 1,
        "result": { "classification": "healthy" }
    });
    let (status, completed) = fairlead
        .complete_worker_job("terminal-worker", "job-1", completion_body.clone())
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(completed["job"]["status"], "complete");
    let callback = callbacks
        .recv()
        .await
        .expect("completion callback was delivered");
    assert_eq!(callback["job"]["id"], "job-1");
    let delivered = wait_for_callback_status(&fairlead, "job-1", "delivered").await;
    assert_eq!(delivered["job"]["callback"]["attempts"], 1);

    let (status, failed_job) = fairlead
        .submit_job(json!({
            "type": "embed_batch",
            "priority": "batch",
            "payload": { "texts": ["bad"] }
        }))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(failed_job["job"]["id"], "job-2");

    let (status, claim) = fairlead.claim_worker_job("terminal-worker").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(claim["job"]["id"], "job-2");

    let failure_body = json!({
        "attempt": 1,
        "error": "model rejected input",
        "retryable": false
    });
    let (status, failed) = fairlead
        .fail_worker_job("terminal-worker", "job-2", failure_body.clone())
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(failed["job"]["status"], "failed");

    fairlead.restart().await;
    fairlead.wait_for_health().await;

    let (status, _) = fairlead
        .register_worker(json!({
            "id": "terminal-worker",
            "endpoint_url": "http://terminal-worker.local",
            "node_id": "spark-a",
            "pool": "default",
            "job_types": ["vision_analysis", "embed_batch"],
            "max_concurrent_jobs": 1,
            "available_vram_mb": 4096
        }))
        .await;
    assert_eq!(status, StatusCode::OK);

    let (status, duplicate_complete) = fairlead
        .complete_worker_job("terminal-worker", "job-1", completion_body)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(duplicate_complete["job"]["status"], "complete");
    assert_eq!(duplicate_complete["job"]["callback"]["attempts"], 1);

    let (status, conflicting_complete) = fairlead
        .complete_worker_job(
            "terminal-worker",
            "job-1",
            json!({
                "attempt": 1,
                "result": { "classification": "diseased" }
            }),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(conflicting_complete["job"]["status"], "complete");
    assert_eq!(
        conflicting_complete["job"]["result"],
        json!({ "classification": "healthy" })
    );

    let (status, duplicate_fail) = fairlead
        .fail_worker_job("terminal-worker", "job-2", failure_body)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(duplicate_fail["job"]["status"], "failed");

    let (status, conflicting_fail) = fairlead
        .fail_worker_job(
            "terminal-worker",
            "job-2",
            json!({
                "attempt": 1,
                "error": "different error",
                "retryable": false
            }),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(conflicting_fail["job"]["status"], "failed");
    assert_eq!(
        conflicting_fail["job"]["error"]["message"],
        "model rejected input"
    );

    let duplicate_callback =
        tokio::time::timeout(Duration::from_millis(150), callbacks.recv()).await;
    assert!(
        duplicate_callback.is_err(),
        "duplicate terminal completion replay delivered another callback"
    );

    fairlead.shutdown().await;
    fs::remove_dir_all(db_dir).expect("remove terminal idempotency SQLite process test temp dir");
}

#[tokio::test]
async fn worker_result_endpoints_reject_mismatched_attempts_over_real_http() {
    let mut fairlead = FairleadProcess::spawn(&[]);
    fairlead.wait_for_health().await;

    let (status, _) = fairlead
        .register_worker(json!({
            "id": "attempt-worker",
            "endpoint_url": "http://attempt-worker.local",
            "node_id": "spark-a",
            "pool": "default",
            "job_types": ["vision_analysis", "embed_batch"],
            "max_concurrent_jobs": 1,
            "available_vram_mb": 4096
        }))
        .await;
    assert_eq!(status, StatusCode::OK);

    let (status, complete_job) = fairlead
        .submit_job(json!({
            "type": "vision_analysis",
            "priority": "batch",
            "payload": { "image": "attempt-complete.jpg" }
        }))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(complete_job["job"]["id"], "job-1");

    let (status, claim) = fairlead.claim_worker_job("attempt-worker").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(claim["job"]["lease"]["attempt"], 1);

    let (status, rejected_complete) = fairlead
        .complete_worker_job(
            "attempt-worker",
            "job-1",
            json!({
                "attempt": 2,
                "result": { "ok": true }
            }),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(rejected_complete["job"]["status"], "running");
    assert_eq!(rejected_complete["job"]["lease"]["attempt"], 1);

    let (status, completed) = fairlead
        .complete_worker_job(
            "attempt-worker",
            "job-1",
            json!({
                "attempt": 1,
                "result": { "ok": true }
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(completed["job"]["status"], "complete");

    let (status, fail_job) = fairlead
        .submit_job(json!({
            "type": "embed_batch",
            "priority": "batch",
            "payload": { "texts": ["attempt-fail"] }
        }))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(fail_job["job"]["id"], "job-2");

    let (status, claim) = fairlead.claim_worker_job("attempt-worker").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(claim["job"]["lease"]["attempt"], 1);

    let (status, rejected_fail) = fairlead
        .fail_worker_job(
            "attempt-worker",
            "job-2",
            json!({
                "attempt": 2,
                "error": "wrong attempt",
                "retryable": false
            }),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(rejected_fail["job"]["status"], "running");
    assert_eq!(rejected_fail["job"]["lease"]["attempt"], 1);

    let (status, failed) = fairlead
        .fail_worker_job(
            "attempt-worker",
            "job-2",
            json!({
                "attempt": 1,
                "error": "right attempt",
                "retryable": false
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(failed["job"]["status"], "failed");

    fairlead.shutdown().await;
}

#[tokio::test]
async fn worker_can_claim_and_complete_job_over_http() {
    let mut fairlead = FairleadProcess::spawn(&[]);
    fairlead.wait_for_health().await;

    let (status, worker) = fairlead
        .register_worker(json!({
            "id": "vision-worker",
            "endpoint_url": "http://vision-worker.local",
            "node_id": "spark-a",
            "pool": "default",
            "job_types": ["vision_analysis"],
            "max_concurrent_jobs": 1,
            "available_vram_mb": 4096
        }))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(worker["worker"]["id"], "vision-worker");
    assert_eq!(worker["worker"]["in_flight_jobs"], 0);

    let (status, submitted) = fairlead
        .submit_job(json!({
            "type": "vision_analysis",
            "priority": "batch",
            "payload": { "image": "rose.jpg" }
        }))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(submitted["job"]["id"], "job-1");

    let (status, claim) = fairlead.claim_worker_job("vision-worker").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(claim["job"]["id"], "job-1");
    assert_eq!(claim["job"]["status"], "running");
    assert_eq!(claim["job"]["lease"]["worker_id"], "vision-worker");
    assert_eq!(claim["job"]["attempts"], 1);

    let (status, completed) = fairlead
        .complete_worker_job(
            "vision-worker",
            "job-1",
            json!({
                "attempt": 1,
                "result": { "classification": "healthy" }
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(completed["job"]["status"], "complete");
    assert_eq!(
        completed["job"]["result"],
        json!({ "classification": "healthy" })
    );

    let (status, fetched) = fairlead.get_json("/v1/jobs/job-1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(fetched["job"]["status"], "complete");
    assert_eq!(
        fetched["job"]["terminal_attempt"]["worker_id"],
        "vision-worker"
    );
    assert_eq!(fetched["job"]["terminal_attempt"]["attempt"], 1);

    let (status, workers) = fairlead.get_json("/v1/workers").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(workers["workers"][0]["id"], "vision-worker");
    assert_eq!(workers["workers"][0]["in_flight_jobs"], 0);

    let (status, metrics) = fairlead.get_text("/metrics").await;
    assert_eq!(status, StatusCode::OK);
    assert!(metrics
        .contains("fairlead_worker_in_flight_jobs{worker=\"vision-worker\",node=\"spark-a\"} 0"));
    assert!(metrics.contains("fairlead_job_duration_seconds_count{priority=\"batch\",type=\"vision_analysis\",status=\"complete\"} 1"));

    fairlead.shutdown().await;
}

#[tokio::test]
async fn metrics_stay_consistent_across_process_scheduler_workflow() {
    let (callback_url, mut callbacks) = start_callback_target(StatusCode::OK).await;
    let mut fairlead = FairleadProcess::spawn(&[
        ("JOB_LEASE_DURATION_MS", "50"),
        ("JOB_MAINTENANCE_INTERVAL_SECS", "1"),
        ("JOB_RETENTION_SECS", "1"),
        ("JOB_PRUNE_LIMIT", "10"),
        ("CALLBACK_MAX_ATTEMPTS", "1"),
        ("CALLBACK_RETRY_DELAY_MS", "25"),
        ("CALLBACK_TIMEOUT_SECS", "1"),
    ]);
    fairlead.wait_for_health().await;

    let (status, _) = fairlead
        .register_worker(json!({
            "id": "metrics-worker",
            "endpoint_url": "http://metrics-worker.local",
            "node_id": "spark-a",
            "pool": "default",
            "job_types": ["vision_analysis"],
            "max_concurrent_jobs": 1,
            "available_vram_mb": 4096
        }))
        .await;
    assert_eq!(status, StatusCode::OK);

    let (status, submitted) = fairlead
        .submit_job(json!({
            "type": "vision_analysis",
            "priority": "batch",
            "payload": { "image": "metrics.jpg" },
            "callback_url": callback_url
        }))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(submitted["job"]["id"], "job-1");

    let (status, metrics) = fairlead.get_text("/metrics").await;
    assert_eq!(status, StatusCode::OK);
    assert_metrics_contain(
        &metrics,
        &[
            "fairlead_workers{type=\"vision_analysis\",status=\"available\"} 1",
            "fairlead_worker_in_flight_jobs{worker=\"metrics-worker\",node=\"spark-a\"} 0",
            "fairlead_worker_available_job_slots{worker=\"metrics-worker\",node=\"spark-a\"} 1",
            "fairlead_job_queue_depth{priority=\"batch\",type=\"vision_analysis\"} 1",
            "fairlead_job_queue_wait_seconds_sum{priority=\"batch\",type=\"vision_analysis\"}",
            "fairlead_job_queue_wait_seconds_max{priority=\"batch\",type=\"vision_analysis\"}",
        ],
    );

    let (status, claim) = fairlead.claim_worker_job("metrics-worker").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(claim["job"]["id"], "job-1");
    assert_eq!(claim["job"]["status"], "running");

    let (status, metrics) = fairlead.get_text("/metrics").await;
    assert_eq!(status, StatusCode::OK);
    assert_metrics_contain(
        &metrics,
        &[
            "fairlead_worker_in_flight_jobs{worker=\"metrics-worker\",node=\"spark-a\"} 1",
            "fairlead_worker_available_job_slots{worker=\"metrics-worker\",node=\"spark-a\"} 0",
        ],
    );
    assert!(!metrics
        .contains("fairlead_job_queue_depth{priority=\"batch\",type=\"vision_analysis\"} 1"));

    let recovered = wait_for_job_status(&fairlead, "job-1", "queued").await;
    assert_eq!(recovered["job"]["error"]["message"], "attempt timed out");
    assert_eq!(recovered["job"]["error"]["retryable"], true);
    let (status, metrics) = fairlead.get_text("/metrics").await;
    assert_eq!(status, StatusCode::OK);
    assert_metrics_contain(
        &metrics,
        &[
            "fairlead_worker_in_flight_jobs{worker=\"metrics-worker\",node=\"spark-a\"} 0",
            "fairlead_worker_available_job_slots{worker=\"metrics-worker\",node=\"spark-a\"} 1",
            "fairlead_job_queue_depth{priority=\"batch\",type=\"vision_analysis\"} 1",
        ],
    );

    let (status, reclaimed) = fairlead.claim_worker_job("metrics-worker").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(reclaimed["job"]["id"], "job-1");
    assert_eq!(reclaimed["job"]["attempts"], 2);

    let (status, completed) = fairlead
        .complete_worker_job(
            "metrics-worker",
            "job-1",
            json!({
                "attempt": 2,
                "result": { "classification": "healthy" }
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(completed["job"]["status"], "complete");

    tokio::time::timeout(Duration::from_secs(2), callbacks.recv())
        .await
        .expect("metrics callback was delivered")
        .expect("callback receiver stayed open");
    wait_for_callback_status(&fairlead, "job-1", "delivered").await;

    let (status, metrics) = fairlead.get_text("/metrics").await;
    assert_eq!(status, StatusCode::OK);
    assert_metrics_contain(
        &metrics,
        &[
            "fairlead_worker_in_flight_jobs{worker=\"metrics-worker\",node=\"spark-a\"} 0",
            "fairlead_worker_available_job_slots{worker=\"metrics-worker\",node=\"spark-a\"} 1",
            "fairlead_job_duration_seconds_count{priority=\"batch\",type=\"vision_analysis\",status=\"complete\"} 1",
            "fairlead_job_callbacks_total{type=\"vision_analysis\",status=\"complete\",outcome=\"success\",http_status=\"200\"} 1",
        ],
    );

    tokio::time::sleep(Duration::from_millis(1_200)).await;
    let (status, pruned) = fairlead.prune_jobs().await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(pruned["pruned"]["removed"], 1);

    let body = wait_for_job_not_found(&fairlead, "job-1").await;
    assert_eq!(body["raw"], "job not found");
    let (status, metrics) = fairlead.get_text("/metrics").await;
    assert_eq!(status, StatusCode::OK);
    assert_metrics_contain(
        &metrics,
        &[
            "fairlead_job_prunes_total{status=\"complete\"} 1",
            "fairlead_workers{type=\"vision_analysis\",status=\"available\"} 1",
            "fairlead_worker_in_flight_jobs{worker=\"metrics-worker\",node=\"spark-a\"} 0",
        ],
    );

    fairlead.shutdown().await;
}

#[tokio::test]
async fn complete_job_delivers_callback_over_real_http() {
    let (callback_url, mut callbacks) = start_callback_target(StatusCode::OK).await;
    let mut fairlead = FairleadProcess::spawn(&[
        ("CALLBACK_MAX_ATTEMPTS", "1"),
        ("CALLBACK_RETRY_DELAY_MS", "25"),
        ("CALLBACK_TIMEOUT_SECS", "1"),
    ]);
    fairlead.wait_for_health().await;

    let (status, _) = fairlead
        .register_worker(json!({
            "id": "callback-worker",
            "endpoint_url": "http://callback-worker.local",
            "node_id": "spark-a",
            "pool": "default",
            "job_types": ["vision_analysis"],
            "max_concurrent_jobs": 1,
            "available_vram_mb": 4096
        }))
        .await;
    assert_eq!(status, StatusCode::OK);

    let (status, submitted) = fairlead
        .submit_job(json!({
            "type": "vision_analysis",
            "priority": "batch",
            "payload": { "image": "rose.jpg" },
            "callback_url": callback_url
        }))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(submitted["job"]["id"], "job-1");
    assert_eq!(submitted["job"]["callback_url"], callback_url);

    let (status, claim) = fairlead.claim_worker_job("callback-worker").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(claim["job"]["id"], "job-1");

    let (status, completed) = fairlead
        .complete_worker_job(
            "callback-worker",
            "job-1",
            json!({
                "attempt": 1,
                "result": { "classification": "healthy" }
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(completed["job"]["status"], "complete");
    assert_eq!(completed["job"]["callback"]["status"], "pending");

    let callback = tokio::time::timeout(Duration::from_secs(2), callbacks.recv())
        .await
        .expect("callback was delivered")
        .expect("callback receiver stayed open");
    assert_eq!(callback["job"]["id"], "job-1");
    assert_eq!(callback["job"]["type"], "vision_analysis");
    assert_eq!(callback["job"]["status"], "complete");
    assert_eq!(
        callback["job"]["result"],
        json!({ "classification": "healthy" })
    );
    assert_eq!(callback["job"]["callback"]["status"], "pending");
    assert_eq!(callback["job"]["callback"]["attempts"], 1);

    let fetched = wait_for_callback_status(&fairlead, "job-1", "delivered").await;
    assert_eq!(fetched["job"]["callback"]["attempts"], 1);
    assert_eq!(fetched["job"]["callback"]["last_http_status"], 200);
    assert!(fetched["job"]["callback"]["last_error"].is_null());

    let (status, metrics) = fairlead.get_text("/metrics").await;
    assert_eq!(status, StatusCode::OK);
    assert!(metrics.contains("fairlead_job_callbacks_total{type=\"vision_analysis\",status=\"complete\",outcome=\"success\",http_status=\"200\"} 1"));

    fairlead.shutdown().await;
}

#[tokio::test]
async fn pending_callback_retries_after_process_restart() {
    let (callback_url, mut callbacks) =
        start_sequence_callback_target(vec![StatusCode::INTERNAL_SERVER_ERROR, StatusCode::OK])
            .await;
    let db_dir = unique_temp_dir("fairlead-process-callback-db");
    fs::create_dir_all(&db_dir).expect("create callback SQLite process test temp dir");
    let db_path = db_dir.join("jobs.sqlite3");
    let db_path = db_path.to_string_lossy().to_string();
    let mut fairlead = FairleadProcess::spawn(&[
        ("JOB_STORE", "sqlite"),
        ("JOB_DB_PATH", &db_path),
        ("CALLBACK_MAX_ATTEMPTS", "1"),
        ("CALLBACK_RETRY_DELAY_MS", "10000"),
        ("CALLBACK_TIMEOUT_SECS", "1"),
    ]);
    fairlead.wait_for_health().await;

    let (status, _) = fairlead
        .register_worker(json!({
            "id": "restart-callback-worker",
            "endpoint_url": "http://restart-callback-worker.local",
            "node_id": "spark-a",
            "pool": "default",
            "job_types": ["vision_analysis"],
            "max_concurrent_jobs": 1,
            "available_vram_mb": 4096
        }))
        .await;
    assert_eq!(status, StatusCode::OK);

    let (status, submitted) = fairlead
        .submit_job(json!({
            "type": "vision_analysis",
            "priority": "batch",
            "payload": { "image": "rose.jpg" },
            "callback_url": callback_url
        }))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(submitted["job"]["id"], "job-1");

    let (status, claim) = fairlead.claim_worker_job("restart-callback-worker").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(claim["job"]["id"], "job-1");

    let (status, completed) = fairlead
        .complete_worker_job(
            "restart-callback-worker",
            "job-1",
            json!({
                "attempt": 1,
                "result": { "classification": "healthy" }
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(completed["job"]["status"], "complete");

    let first_callback = tokio::time::timeout(Duration::from_secs(2), callbacks.recv())
        .await
        .expect("first callback was delivered")
        .expect("callback receiver stayed open");
    assert_eq!(first_callback["job"]["id"], "job-1");
    assert_eq!(first_callback["job"]["callback"]["attempts"], 1);

    let failed_callback = wait_for_callback_attempt(&fairlead, "job-1", "pending", 1, 500).await;
    assert_eq!(
        failed_callback["job"]["callback"]["last_error"],
        Value::Null
    );

    fairlead.restart().await;
    fairlead.wait_for_health().await;

    let second_callback = tokio::time::timeout(Duration::from_secs(2), callbacks.recv())
        .await
        .expect("pending callback was retried after restart")
        .expect("callback receiver stayed open");
    assert_eq!(second_callback["job"]["id"], "job-1");
    assert_eq!(second_callback["job"]["callback"]["status"], "pending");
    assert_eq!(second_callback["job"]["callback"]["attempts"], 2);
    assert_eq!(second_callback["job"]["callback"]["last_http_status"], 500);

    let delivered = wait_for_callback_attempt(&fairlead, "job-1", "delivered", 2, 200).await;
    assert_eq!(delivered["job"]["status"], "complete");
    assert!(delivered["job"]["callback"]["last_error"].is_null());

    let (status, metrics) = fairlead.get_text("/metrics").await;
    assert_eq!(status, StatusCode::OK);
    assert!(metrics.contains("fairlead_job_callbacks_total{type=\"vision_analysis\",status=\"complete\",outcome=\"success\",http_status=\"200\"} 1"));

    fairlead.shutdown().await;
    fs::remove_dir_all(db_dir).expect("remove callback SQLite process test temp dir");
}

#[tokio::test]
async fn expired_lease_requeues_after_process_restart() {
    let db_dir = unique_temp_dir("fairlead-process-lease-db");
    fs::create_dir_all(&db_dir).expect("create lease SQLite process test temp dir");
    let db_path = db_dir.join("jobs.sqlite3");
    let db_path = db_path.to_string_lossy().to_string();
    let mut fairlead = FairleadProcess::spawn(&[
        ("JOB_STORE", "sqlite"),
        ("JOB_DB_PATH", &db_path),
        ("JOB_MAINTENANCE_INTERVAL_SECS", "30"),
    ]);
    fairlead.wait_for_health().await;

    let (status, _) = fairlead
        .register_worker(json!({
            "id": "lease-worker",
            "endpoint_url": "http://lease-worker.local",
            "node_id": "spark-a",
            "pool": "default",
            "job_types": ["vision_analysis"],
            "max_concurrent_jobs": 1,
            "available_vram_mb": 4096
        }))
        .await;
    assert_eq!(status, StatusCode::OK);

    let (status, submitted) = fairlead
        .submit_job(json!({
            "type": "vision_analysis",
            "priority": "batch",
            "payload": { "image": "rose.jpg" }
        }))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(submitted["job"]["id"], "job-1");

    let (status, claim) = fairlead.claim_worker_job("lease-worker").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(claim["job"]["id"], "job-1");
    assert_eq!(claim["job"]["status"], "running");
    assert_eq!(claim["job"]["attempts"], 1);
    assert_eq!(claim["job"]["lease"]["worker_id"], "lease-worker");
    assert_eq!(claim["job"]["lease"]["attempt"], 1);

    fairlead.shutdown().await;
    expire_sqlite_job_lease(&db_path, "job-1");

    fairlead.restart().await;
    fairlead.wait_for_health().await;

    let recovered = wait_for_job_status(&fairlead, "job-1", "queued").await;
    assert_eq!(recovered["job"]["attempts"], 1);
    assert!(recovered["job"]["lease"].is_null());
    assert_eq!(recovered["job"]["error"]["message"], "attempt timed out");
    assert_eq!(recovered["job"]["error"]["retryable"], true);

    let (status, workers) = fairlead.get_json("/v1/workers").await;
    assert_eq!(status, StatusCode::OK);
    assert!(workers["workers"]
        .as_array()
        .expect("workers response is an array")
        .is_empty());

    let (status, _) = fairlead
        .register_worker(json!({
            "id": "replacement-worker",
            "endpoint_url": "http://replacement-worker.local",
            "node_id": "spark-b",
            "pool": "default",
            "job_types": ["vision_analysis"],
            "max_concurrent_jobs": 1,
            "available_vram_mb": 4096
        }))
        .await;
    assert_eq!(status, StatusCode::OK);

    let (status, reclaimed) = fairlead.claim_worker_job("replacement-worker").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(reclaimed["job"]["id"], "job-1");
    assert_eq!(reclaimed["job"]["status"], "running");
    assert_eq!(reclaimed["job"]["attempts"], 2);
    assert_eq!(reclaimed["job"]["lease"]["worker_id"], "replacement-worker");
    assert_eq!(reclaimed["job"]["lease"]["attempt"], 2);

    fairlead.shutdown().await;
    fs::remove_dir_all(db_dir).expect("remove lease SQLite process test temp dir");
}

#[tokio::test]
async fn background_maintenance_requeues_expired_lease() {
    let mut fairlead = FairleadProcess::spawn(&[
        ("JOB_LEASE_DURATION_MS", "50"),
        ("JOB_MAINTENANCE_INTERVAL_SECS", "1"),
    ]);
    fairlead.wait_for_health().await;

    let (status, _) = fairlead
        .register_worker(json!({
            "id": "maintenance-worker",
            "endpoint_url": "http://maintenance-worker.local",
            "node_id": "spark-a",
            "pool": "default",
            "job_types": ["vision_analysis"],
            "max_concurrent_jobs": 1,
            "available_vram_mb": 4096
        }))
        .await;
    assert_eq!(status, StatusCode::OK);

    let (status, submitted) = fairlead
        .submit_job(json!({
            "type": "vision_analysis",
            "priority": "batch",
            "payload": { "image": "rose.jpg" }
        }))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(submitted["job"]["id"], "job-1");

    let (status, claim) = fairlead.claim_worker_job("maintenance-worker").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(claim["job"]["status"], "running");
    assert_eq!(claim["job"]["attempts"], 1);
    assert_eq!(claim["job"]["lease"]["worker_id"], "maintenance-worker");

    let recovered = wait_for_job_status(&fairlead, "job-1", "queued").await;
    assert_eq!(recovered["job"]["attempts"], 1);
    assert!(recovered["job"]["lease"].is_null());
    assert_eq!(recovered["job"]["error"]["message"], "attempt timed out");
    assert_eq!(recovered["job"]["error"]["retryable"], true);

    let (status, workers) = fairlead.get_json("/v1/workers").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(workers["workers"][0]["id"], "maintenance-worker");
    assert_eq!(workers["workers"][0]["in_flight_jobs"], 0);

    let (status, reclaimed) = fairlead.claim_worker_job("maintenance-worker").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(reclaimed["job"]["id"], "job-1");
    assert_eq!(reclaimed["job"]["status"], "running");
    assert_eq!(reclaimed["job"]["attempts"], 2);
    assert_eq!(reclaimed["job"]["lease"]["worker_id"], "maintenance-worker");
    assert_eq!(reclaimed["job"]["lease"]["attempt"], 2);

    fairlead.shutdown().await;
}

#[tokio::test]
async fn background_maintenance_fails_exhausted_expired_lease_and_delivers_callback() {
    let (callback_url, mut callbacks) = start_callback_target(StatusCode::OK).await;
    let mut fairlead = FairleadProcess::spawn(&[
        ("JOB_LEASE_DURATION_MS", "50"),
        ("JOB_MAINTENANCE_INTERVAL_SECS", "1"),
        ("CALLBACK_MAX_ATTEMPTS", "1"),
        ("CALLBACK_RETRY_DELAY_MS", "25"),
        ("CALLBACK_TIMEOUT_SECS", "1"),
    ]);
    fairlead.wait_for_health().await;

    let (status, _) = fairlead
        .register_worker(json!({
            "id": "exhaustion-worker",
            "endpoint_url": "http://exhaustion-worker.local",
            "node_id": "spark-a",
            "pool": "default",
            "job_types": ["vision_analysis"],
            "max_concurrent_jobs": 1,
            "available_vram_mb": 4096
        }))
        .await;
    assert_eq!(status, StatusCode::OK);

    let (status, submitted) = fairlead
        .submit_job(json!({
            "type": "vision_analysis",
            "priority": "batch",
            "payload": { "image": "rose.jpg" },
            "callback_url": callback_url
        }))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(submitted["job"]["id"], "job-1");
    assert_eq!(submitted["job"]["max_attempts"], 3);

    for expected_attempt in 1..=2 {
        let (status, claim) = fairlead.claim_worker_job("exhaustion-worker").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(claim["job"]["id"], "job-1");
        assert_eq!(claim["job"]["status"], "running");
        assert_eq!(claim["job"]["attempts"], expected_attempt);
        assert_eq!(claim["job"]["lease"]["worker_id"], "exhaustion-worker");

        let recovered = wait_for_job_status(&fairlead, "job-1", "queued").await;
        assert_eq!(recovered["job"]["attempts"], expected_attempt);
        assert_eq!(recovered["job"]["error"]["message"], "attempt timed out");
        assert_eq!(recovered["job"]["error"]["retryable"], true);
        assert!(recovered["job"]["lease"].is_null());
    }

    let (status, final_claim) = fairlead.claim_worker_job("exhaustion-worker").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(final_claim["job"]["id"], "job-1");
    assert_eq!(final_claim["job"]["status"], "running");
    assert_eq!(final_claim["job"]["attempts"], 3);
    assert_eq!(
        final_claim["job"]["lease"]["worker_id"],
        "exhaustion-worker"
    );

    let failed = wait_for_job_status(&fairlead, "job-1", "failed").await;
    assert_eq!(failed["job"]["attempts"], 3);
    assert_eq!(failed["job"]["error"]["message"], "attempt timed out");
    assert_eq!(failed["job"]["error"]["retryable"], false);
    assert!(failed["job"]["lease"].is_null());
    assert!(matches!(
        failed["job"]["callback"]["status"].as_str(),
        Some("pending" | "delivered")
    ));

    let callback = tokio::time::timeout(Duration::from_secs(2), callbacks.recv())
        .await
        .expect("failure callback was delivered")
        .expect("callback receiver stayed open");
    assert_eq!(callback["job"]["id"], "job-1");
    assert_eq!(callback["job"]["status"], "failed");
    assert_eq!(callback["job"]["error"]["message"], "attempt timed out");

    let delivered = wait_for_callback_status(&fairlead, "job-1", "delivered").await;
    assert_eq!(delivered["job"]["status"], "failed");
    assert_eq!(delivered["job"]["callback"]["last_http_status"], 200);

    let (status, workers) = fairlead.get_json("/v1/workers").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(workers["workers"][0]["id"], "exhaustion-worker");
    assert_eq!(workers["workers"][0]["in_flight_jobs"], 0);

    let (status, no_work) = fairlead.claim_worker_job("exhaustion-worker").await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(no_work["raw"], "");

    fairlead.shutdown().await;
}

#[tokio::test]
async fn exhausted_expired_lease_dispatches_callback_after_process_restart() {
    let (callback_url, mut callbacks) = start_callback_target(StatusCode::OK).await;
    let db_dir = unique_temp_dir("fairlead-process-exhausted-restart-db");
    fs::create_dir_all(&db_dir).expect("create exhausted restart SQLite process test temp dir");
    let db_path = db_dir.join("jobs.sqlite").to_string_lossy().to_string();
    let mut fairlead = FairleadProcess::spawn(&[
        ("JOB_STORE", "sqlite"),
        ("JOB_DB_PATH", &db_path),
        ("JOB_LEASE_DURATION_MS", "50"),
        ("JOB_MAINTENANCE_INTERVAL_SECS", "1"),
        ("CALLBACK_MAX_ATTEMPTS", "1"),
        ("CALLBACK_RETRY_DELAY_MS", "25"),
        ("CALLBACK_TIMEOUT_SECS", "1"),
    ]);
    fairlead.wait_for_health().await;

    let (status, _) = fairlead
        .register_worker(json!({
            "id": "restart-exhaustion-worker",
            "endpoint_url": "http://restart-exhaustion-worker.local",
            "node_id": "spark-a",
            "pool": "default",
            "job_types": ["vision_analysis"],
            "max_concurrent_jobs": 1,
            "available_vram_mb": 4096
        }))
        .await;
    assert_eq!(status, StatusCode::OK);

    let (status, submitted) = fairlead
        .submit_job(json!({
            "type": "vision_analysis",
            "priority": "batch",
            "payload": { "image": "rose.jpg" },
            "callback_url": callback_url
        }))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(submitted["job"]["id"], "job-1");
    assert_eq!(submitted["job"]["max_attempts"], 3);

    for expected_attempt in 1..=2 {
        let (status, claim) = fairlead.claim_worker_job("restart-exhaustion-worker").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(claim["job"]["id"], "job-1");
        assert_eq!(claim["job"]["attempts"], expected_attempt);

        let recovered = wait_for_job_status(&fairlead, "job-1", "queued").await;
        assert_eq!(recovered["job"]["attempts"], expected_attempt);
        assert_eq!(recovered["job"]["error"]["retryable"], true);
    }

    let (status, final_claim) = fairlead.claim_worker_job("restart-exhaustion-worker").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(final_claim["job"]["id"], "job-1");
    assert_eq!(final_claim["job"]["status"], "running");
    assert_eq!(final_claim["job"]["attempts"], 3);
    assert_eq!(
        final_claim["job"]["lease"]["worker_id"],
        "restart-exhaustion-worker"
    );

    fairlead.shutdown().await;
    expire_sqlite_job_lease(&db_path, "job-1");

    fairlead.restart().await;
    fairlead.wait_for_health().await;

    let failed = wait_for_job_status(&fairlead, "job-1", "failed").await;
    assert_eq!(failed["job"]["attempts"], 3);
    assert_eq!(failed["job"]["error"]["message"], "attempt timed out");
    assert_eq!(failed["job"]["error"]["retryable"], false);
    assert!(failed["job"]["lease"].is_null());

    let callback = tokio::time::timeout(Duration::from_secs(2), callbacks.recv())
        .await
        .expect("restart failure callback was delivered")
        .expect("callback receiver stayed open");
    assert_eq!(callback["job"]["id"], "job-1");
    assert_eq!(callback["job"]["status"], "failed");
    assert_eq!(callback["job"]["error"]["message"], "attempt timed out");

    let delivered = wait_for_callback_status(&fairlead, "job-1", "delivered").await;
    assert_eq!(delivered["job"]["status"], "failed");
    assert_eq!(delivered["job"]["callback"]["last_http_status"], 200);

    let (status, workers) = fairlead.get_json("/v1/workers").await;
    assert_eq!(status, StatusCode::OK);
    assert!(workers["workers"]
        .as_array()
        .expect("workers response is an array")
        .is_empty());

    fairlead.shutdown().await;
    fs::remove_dir_all(db_dir).expect("remove exhausted restart SQLite process test temp dir");
}

#[tokio::test]
async fn worker_lifecycle_controls_work_over_real_http() {
    let mut fairlead = FairleadProcess::spawn(&[]);
    fairlead.wait_for_health().await;

    for worker_id in ["worker-a", "worker-b"] {
        let (status, worker) = fairlead
            .register_worker(json!({
                "id": worker_id,
                "endpoint_url": format!("http://{worker_id}.local"),
                "node_id": "spark-a",
                "pool": "default",
                "job_types": ["vision_analysis"],
                "max_concurrent_jobs": 1,
                "available_vram_mb": 4096
            }))
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(worker["worker"]["id"], worker_id);
        assert_eq!(worker["worker"]["draining"], false);
    }

    let (status, drained) = fairlead.drain_worker("worker-a").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(drained["worker"]["id"], "worker-a");
    assert_eq!(drained["worker"]["draining"], true);
    assert_eq!(drained["worker"]["available_job_slots"], 0);

    let (status, heartbeat) = fairlead.heartbeat_worker("worker-a").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(heartbeat["worker"]["draining"], true);

    let (status, re_registered) = fairlead
        .register_worker(json!({
            "id": "worker-a",
            "endpoint_url": "http://worker-a-restarted.local",
            "node_id": "spark-a",
            "pool": "default",
            "job_types": ["vision_analysis"],
            "max_concurrent_jobs": 1,
            "available_vram_mb": 4096
        }))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        re_registered["worker"]["endpoint_url"],
        "http://worker-a-restarted.local"
    );
    assert_eq!(re_registered["worker"]["draining"], true);

    let (status, submitted) = fairlead
        .submit_job(json!({
            "type": "vision_analysis",
            "priority": "batch",
            "payload": { "image": "rose-a.jpg" }
        }))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(submitted["job"]["id"], "job-1");

    let (status, blocked_claim) = fairlead.claim_worker_job("worker-a").await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(blocked_claim["raw"], "worker is draining");

    let (status, worker_b_claim) = fairlead.claim_worker_job("worker-b").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(worker_b_claim["job"]["id"], "job-1");
    assert_eq!(worker_b_claim["job"]["lease"]["worker_id"], "worker-b");

    let (status, completed) = fairlead
        .complete_worker_job(
            "worker-b",
            "job-1",
            json!({
                "attempt": 1,
                "result": { "classification": "healthy" }
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(completed["job"]["status"], "complete");

    let (status, reactivated) = fairlead.reactivate_worker("worker-a").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(reactivated["worker"]["draining"], false);
    assert_eq!(reactivated["worker"]["available_job_slots"], 1);

    let (status, second_job) = fairlead
        .submit_job(json!({
            "type": "vision_analysis",
            "priority": "batch",
            "payload": { "image": "rose-b.jpg" }
        }))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(second_job["job"]["id"], "job-2");

    let (status, worker_a_claim) = fairlead.claim_worker_job("worker-a").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(worker_a_claim["job"]["id"], "job-2");
    assert_eq!(worker_a_claim["job"]["lease"]["worker_id"], "worker-a");

    let (status, busy_deregister) = fairlead.deregister_worker("worker-a").await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(busy_deregister["worker"]["id"], "worker-a");
    assert_eq!(busy_deregister["worker"]["draining"], true);
    assert_eq!(busy_deregister["worker"]["in_flight_jobs"], 1);

    let (status, completed) = fairlead
        .complete_worker_job(
            "worker-a",
            "job-2",
            json!({
                "attempt": 1,
                "result": { "classification": "healthy" }
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(completed["job"]["status"], "complete");

    let (status, removed_worker_a) = fairlead.deregister_worker("worker-a").await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(removed_worker_a["raw"], "");

    let (status, removed_worker_b) = fairlead.deregister_worker("worker-b").await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(removed_worker_b["raw"], "");

    let (status, workers) = fairlead.get_json("/v1/workers").await;
    assert_eq!(status, StatusCode::OK);
    assert!(workers["workers"]
        .as_array()
        .expect("workers response is an array")
        .is_empty());

    fairlead.shutdown().await;
}

#[tokio::test]
async fn worker_renew_and_retryable_fail_requeues_over_real_http() {
    let mut fairlead = FairleadProcess::spawn(&[("JOB_LEASE_DURATION_MS", "5000")]);
    fairlead.wait_for_health().await;

    for worker_id in ["primary-worker", "backup-worker"] {
        let (status, worker) = fairlead
            .register_worker(json!({
                "id": worker_id,
                "endpoint_url": format!("http://{worker_id}.local"),
                "node_id": "spark-a",
                "pool": "default",
                "job_types": ["vision_analysis"],
                "max_concurrent_jobs": 1,
                "available_vram_mb": 4096
            }))
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(worker["worker"]["id"], worker_id);
    }

    let (status, submitted) = fairlead
        .submit_job(json!({
            "type": "vision_analysis",
            "priority": "batch",
            "payload": { "image": "rose.jpg" }
        }))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(submitted["job"]["id"], "job-1");

    let (status, claim) = fairlead.claim_worker_job("primary-worker").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(claim["job"]["id"], "job-1");
    assert_eq!(claim["job"]["status"], "running");
    assert_eq!(claim["job"]["attempts"], 1);
    assert_eq!(claim["job"]["lease"]["worker_id"], "primary-worker");
    let original_expires_at = claim["job"]["lease"]["expires_at_unix_ms"]
        .as_u64()
        .expect("lease expiry is a u64");

    let (status, drained) = fairlead.drain_worker("primary-worker").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(drained["worker"]["draining"], true);
    assert_eq!(drained["worker"]["in_flight_jobs"], 1);

    let (status, renewed) = fairlead.renew_worker_job("primary-worker", "job-1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(renewed["job"]["id"], "job-1");
    assert_eq!(renewed["job"]["status"], "running");
    assert_eq!(renewed["job"]["lease"]["worker_id"], "primary-worker");
    let renewed_expires_at = renewed["job"]["lease"]["expires_at_unix_ms"]
        .as_u64()
        .expect("renewed lease expiry is a u64");
    assert!(renewed_expires_at >= original_expires_at);

    let (status, failed) = fairlead
        .fail_worker_job(
            "primary-worker",
            "job-1",
            json!({
                "attempt": 1,
                "error": "worker process restarted",
                "retryable": true
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(failed["job"]["id"], "job-1");
    assert_eq!(failed["job"]["status"], "queued");
    assert!(failed["job"]["lease"].is_null());
    assert_eq!(
        failed["job"]["error"]["message"],
        "worker process restarted"
    );
    assert_eq!(failed["job"]["error"]["retryable"], true);

    let (status, workers) = fairlead.get_json("/v1/workers").await;
    assert_eq!(status, StatusCode::OK);
    let primary = workers["workers"]
        .as_array()
        .expect("workers response is an array")
        .iter()
        .find(|worker| worker["id"] == "primary-worker")
        .expect("primary worker is listed");
    assert_eq!(primary["draining"], true);
    assert_eq!(primary["in_flight_jobs"], 0);
    assert_eq!(primary["available_job_slots"], 0);

    let (status, reclaimed) = fairlead.claim_worker_job("backup-worker").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(reclaimed["job"]["id"], "job-1");
    assert_eq!(reclaimed["job"]["status"], "running");
    assert_eq!(reclaimed["job"]["attempts"], 2);
    assert_eq!(reclaimed["job"]["lease"]["worker_id"], "backup-worker");
    assert_eq!(reclaimed["job"]["lease"]["attempt"], 2);

    let (status, completed) = fairlead
        .complete_worker_job(
            "backup-worker",
            "job-1",
            json!({
                "attempt": 2,
                "result": { "classification": "healthy" }
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(completed["job"]["status"], "complete");
    assert_eq!(
        completed["job"]["terminal_attempt"]["worker_id"],
        "backup-worker"
    );
    assert_eq!(completed["job"]["terminal_attempt"]["attempt"], 2);

    fairlead.shutdown().await;
}

#[tokio::test]
async fn prune_endpoint_removes_only_eligible_terminal_jobs_over_real_http() {
    let (delivered_callback_url, mut delivered_callbacks) =
        start_callback_target(StatusCode::OK).await;
    let mut fairlead = FairleadProcess::spawn(&[
        ("JOB_RETENTION_SECS", "1"),
        ("JOB_PRUNE_LIMIT", "10"),
        ("CALLBACK_MAX_ATTEMPTS", "1"),
        ("CALLBACK_RETRY_DELAY_MS", "25"),
        ("CALLBACK_TIMEOUT_SECS", "1"),
    ]);
    fairlead.wait_for_health().await;

    let (status, _) = fairlead
        .register_worker(json!({
            "id": "prune-worker",
            "endpoint_url": "http://prune-worker.local",
            "node_id": "spark-a",
            "pool": "default",
            "job_types": ["vision_analysis"],
            "max_concurrent_jobs": 1,
            "available_vram_mb": 4096
        }))
        .await;
    assert_eq!(status, StatusCode::OK);

    let (status, complete_job) = fairlead
        .submit_job(json!({
            "type": "vision_analysis",
            "priority": "batch",
            "payload": { "image": "complete.jpg" }
        }))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(complete_job["job"]["id"], "job-1");

    let (status, claim) = fairlead.claim_worker_job("prune-worker").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(claim["job"]["id"], "job-1");
    let (status, completed) = fairlead
        .complete_worker_job(
            "prune-worker",
            "job-1",
            json!({
                "attempt": 1,
                "result": { "classification": "healthy" }
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(completed["job"]["status"], "complete");

    let (status, delivered_job) = fairlead
        .submit_job(json!({
            "type": "vision_analysis",
            "priority": "batch",
            "payload": { "image": "delivered-callback.jpg" },
            "callback_url": delivered_callback_url
        }))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(delivered_job["job"]["id"], "job-2");
    let (status, claim) = fairlead.claim_worker_job("prune-worker").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(claim["job"]["id"], "job-2");
    let (status, completed) = fairlead
        .complete_worker_job(
            "prune-worker",
            "job-2",
            json!({
                "attempt": 1,
                "result": { "classification": "healthy" }
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(completed["job"]["status"], "complete");
    tokio::time::timeout(Duration::from_secs(2), delivered_callbacks.recv())
        .await
        .expect("delivered callback was sent")
        .expect("delivered callback receiver stayed open");
    wait_for_callback_status(&fairlead, "job-2", "delivered").await;

    let (status, pending_job) = fairlead
        .submit_job(json!({
            "type": "vision_analysis",
            "priority": "batch",
            "payload": { "image": "pending-callback.jpg" },
            "callback_url": "http://127.0.0.1:9/callback"
        }))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(pending_job["job"]["id"], "job-3");
    let (status, claim) = fairlead.claim_worker_job("prune-worker").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(claim["job"]["id"], "job-3");
    let (status, completed) = fairlead
        .complete_worker_job(
            "prune-worker",
            "job-3",
            json!({
                "attempt": 1,
                "result": { "classification": "healthy" }
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(completed["job"]["status"], "complete");
    wait_for_callback_status(&fairlead, "job-3", "pending").await;

    let (status, queued_job) = fairlead
        .submit_job(json!({
            "type": "vision_analysis",
            "priority": "batch",
            "payload": { "image": "queued.jpg" }
        }))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(queued_job["job"]["id"], "job-4");

    let (status, running_job) = fairlead
        .submit_job(json!({
            "type": "vision_analysis",
            "priority": "batch",
            "payload": { "image": "running.jpg" }
        }))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(running_job["job"]["id"], "job-5");
    let (status, claim) = fairlead.claim_worker_job("prune-worker").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(claim["job"]["id"], "job-4");

    tokio::time::sleep(Duration::from_millis(1_200)).await;

    let (status, pruned) = fairlead.prune_jobs().await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(pruned["pruned"]["removed"], 2);
    assert_eq!(pruned["pruned"]["retained_pending_callbacks"], 1);
    assert_eq!(pruned["pruned"]["by_status"][0]["status"], "complete");
    assert_eq!(pruned["pruned"]["by_status"][0]["removed"], 2);

    for removed_id in ["job-1", "job-2"] {
        let (status, body) = fairlead.get_json(&format!("/v1/jobs/{removed_id}")).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["raw"], "job not found");
    }

    let (status, pending) = fairlead.get_json("/v1/jobs/job-3").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(pending["job"]["status"], "complete");
    assert_eq!(pending["job"]["callback"]["status"], "pending");

    let (status, running) = fairlead.get_json("/v1/jobs/job-4").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(running["job"]["status"], "running");

    let (status, queued) = fairlead.get_json("/v1/jobs/job-5").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(queued["job"]["status"], "queued");

    let (status, metrics) = fairlead.get_text("/metrics").await;
    assert_eq!(status, StatusCode::OK);
    assert!(metrics.contains("fairlead_job_prunes_total{status=\"complete\"} 2"));

    fairlead.shutdown().await;
}

#[tokio::test]
async fn background_pruning_removes_only_eligible_terminal_jobs_over_real_http() {
    let (delivered_callback_url, mut delivered_callbacks) =
        start_callback_target(StatusCode::OK).await;
    let db_dir = unique_temp_dir("fairlead-process-background-prune-db");
    fs::create_dir_all(&db_dir).expect("create background prune SQLite process test temp dir");
    let db_path = db_dir.join("jobs.sqlite").to_string_lossy().to_string();
    let mut fairlead = FairleadProcess::spawn(&[
        ("JOB_STORE", "sqlite"),
        ("JOB_DB_PATH", &db_path),
        ("JOB_RETENTION_SECS", "1"),
        ("JOB_PRUNE_LIMIT", "10"),
        ("JOB_PRUNE_INTERVAL_SECS", "1"),
        ("CALLBACK_MAX_ATTEMPTS", "1"),
        ("CALLBACK_RETRY_DELAY_MS", "25"),
        ("CALLBACK_TIMEOUT_SECS", "1"),
    ]);
    fairlead.wait_for_health().await;

    let (status, _) = fairlead
        .register_worker(json!({
            "id": "background-prune-worker",
            "endpoint_url": "http://background-prune-worker.local",
            "node_id": "spark-a",
            "pool": "default",
            "job_types": ["vision_analysis"],
            "max_concurrent_jobs": 1,
            "available_vram_mb": 4096
        }))
        .await;
    assert_eq!(status, StatusCode::OK);

    let (status, complete_job) = fairlead
        .submit_job(json!({
            "type": "vision_analysis",
            "priority": "batch",
            "payload": { "image": "complete.jpg" }
        }))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(complete_job["job"]["id"], "job-1");

    let (status, claim) = fairlead.claim_worker_job("background-prune-worker").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(claim["job"]["id"], "job-1");
    let (status, completed) = fairlead
        .complete_worker_job(
            "background-prune-worker",
            "job-1",
            json!({
                "attempt": 1,
                "result": { "classification": "healthy" }
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(completed["job"]["status"], "complete");

    let (status, delivered_job) = fairlead
        .submit_job(json!({
            "type": "vision_analysis",
            "priority": "batch",
            "payload": { "image": "delivered-callback.jpg" },
            "callback_url": delivered_callback_url
        }))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(delivered_job["job"]["id"], "job-2");
    let (status, claim) = fairlead.claim_worker_job("background-prune-worker").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(claim["job"]["id"], "job-2");
    let (status, completed) = fairlead
        .complete_worker_job(
            "background-prune-worker",
            "job-2",
            json!({
                "attempt": 1,
                "result": { "classification": "healthy" }
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(completed["job"]["status"], "complete");
    tokio::time::timeout(Duration::from_secs(2), delivered_callbacks.recv())
        .await
        .expect("delivered callback was sent")
        .expect("delivered callback receiver stayed open");
    wait_for_callback_status(&fairlead, "job-2", "delivered").await;

    let (status, pending_job) = fairlead
        .submit_job(json!({
            "type": "vision_analysis",
            "priority": "batch",
            "payload": { "image": "pending-callback.jpg" },
            "callback_url": "http://127.0.0.1:9/callback"
        }))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(pending_job["job"]["id"], "job-3");
    let (status, claim) = fairlead.claim_worker_job("background-prune-worker").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(claim["job"]["id"], "job-3");
    let (status, completed) = fairlead
        .complete_worker_job(
            "background-prune-worker",
            "job-3",
            json!({
                "attempt": 1,
                "result": { "classification": "healthy" }
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(completed["job"]["status"], "complete");
    wait_for_callback_status(&fairlead, "job-3", "pending").await;

    let (status, queued_job) = fairlead
        .submit_job(json!({
            "type": "vision_analysis",
            "priority": "batch",
            "payload": { "image": "queued.jpg" }
        }))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(queued_job["job"]["id"], "job-4");

    let (status, running_job) = fairlead
        .submit_job(json!({
            "type": "vision_analysis",
            "priority": "batch",
            "payload": { "image": "running.jpg" }
        }))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(running_job["job"]["id"], "job-5");
    let (status, claim) = fairlead.claim_worker_job("background-prune-worker").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(claim["job"]["id"], "job-4");

    for removed_id in ["job-1", "job-2"] {
        let body = wait_for_job_not_found(&fairlead, removed_id).await;
        assert_eq!(body["raw"], "job not found");
    }

    let (status, pending) = fairlead.get_json("/v1/jobs/job-3").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(pending["job"]["status"], "complete");
    assert_eq!(pending["job"]["callback"]["status"], "pending");

    let (status, running) = fairlead.get_json("/v1/jobs/job-4").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(running["job"]["status"], "running");

    let (status, queued) = fairlead.get_json("/v1/jobs/job-5").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(queued["job"]["status"], "queued");

    let (status, metrics) = fairlead.get_text("/metrics").await;
    assert_eq!(status, StatusCode::OK);
    assert!(metrics.contains("fairlead_job_prunes_total{status=\"complete\"} 2"));

    let (status, pruned) = fairlead.prune_jobs().await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(pruned["pruned"]["removed"], 0);
    assert_eq!(pruned["pruned"]["retained_pending_callbacks"], 1);

    fairlead.shutdown().await;
    fs::remove_dir_all(db_dir).expect("remove background prune SQLite process test temp dir");
}

#[tokio::test]
async fn background_pruning_respects_limit_and_progresses_across_intervals() {
    let db_dir = unique_temp_dir("fairlead-process-background-prune-limit-db");
    fs::create_dir_all(&db_dir)
        .expect("create background prune limit SQLite process test temp dir");
    let db_path = db_dir.join("jobs.sqlite").to_string_lossy().to_string();
    let mut fairlead = FairleadProcess::spawn(&[
        ("JOB_STORE", "sqlite"),
        ("JOB_DB_PATH", &db_path),
        ("JOB_RETENTION_SECS", "1"),
        ("JOB_PRUNE_LIMIT", "1"),
        ("JOB_PRUNE_INTERVAL_SECS", "1"),
    ]);
    fairlead.wait_for_health().await;

    let (status, _) = fairlead
        .register_worker(json!({
            "id": "prune-limit-worker",
            "endpoint_url": "http://prune-limit-worker.local",
            "node_id": "spark-a",
            "pool": "default",
            "job_types": ["vision_analysis"],
            "max_concurrent_jobs": 1,
            "available_vram_mb": 4096
        }))
        .await;
    assert_eq!(status, StatusCode::OK);

    for expected_id in ["job-1", "job-2", "job-3"] {
        let (status, submitted) = fairlead
            .submit_job(json!({
                "type": "vision_analysis",
                "priority": "batch",
                "payload": { "image": format!("{expected_id}.jpg") }
            }))
            .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(submitted["job"]["id"], expected_id);

        let (status, claim) = fairlead.claim_worker_job("prune-limit-worker").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(claim["job"]["id"], expected_id);

        let (status, completed) = fairlead
            .complete_worker_job(
                "prune-limit-worker",
                expected_id,
                json!({
                    "attempt": 1,
                    "result": { "classification": "healthy" }
                }),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(completed["job"]["status"], "complete");
    }

    wait_for_metrics_contains(
        &fairlead,
        "fairlead_job_prunes_total{status=\"complete\"} 1",
    )
    .await;
    let (status, job_2) = fairlead.get_json("/v1/jobs/job-2").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(job_2["job"]["status"], "complete");
    let (status, job_3) = fairlead.get_json("/v1/jobs/job-3").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(job_3["job"]["status"], "complete");

    for removed_id in ["job-1", "job-2", "job-3"] {
        let body = wait_for_job_not_found(&fairlead, removed_id).await;
        assert_eq!(body["raw"], "job not found");
    }
    wait_for_metrics_contains(
        &fairlead,
        "fairlead_job_prunes_total{status=\"complete\"} 3",
    )
    .await;

    fairlead.shutdown().await;
    fs::remove_dir_all(db_dir).expect("remove background prune limit SQLite process test temp dir");
}

#[tokio::test]
async fn omitted_background_prune_interval_keeps_manual_pruning_enabled() {
    let db_dir = unique_temp_dir("fairlead-process-manual-only-prune-db");
    fs::create_dir_all(&db_dir).expect("create manual-only prune SQLite process test temp dir");
    let db_path = db_dir.join("jobs.sqlite").to_string_lossy().to_string();
    let mut fairlead = FairleadProcess::spawn(&[
        ("JOB_STORE", "sqlite"),
        ("JOB_DB_PATH", &db_path),
        ("JOB_RETENTION_SECS", "1"),
        ("JOB_PRUNE_LIMIT", "10"),
    ]);
    fairlead.wait_for_health().await;

    let (status, _) = fairlead
        .register_worker(json!({
            "id": "manual-only-prune-worker",
            "endpoint_url": "http://manual-only-prune-worker.local",
            "node_id": "spark-a",
            "pool": "default",
            "job_types": ["vision_analysis"],
            "max_concurrent_jobs": 1,
            "available_vram_mb": 4096
        }))
        .await;
    assert_eq!(status, StatusCode::OK);

    let (status, submitted) = fairlead
        .submit_job(json!({
            "type": "vision_analysis",
            "priority": "batch",
            "payload": { "image": "manual-only.jpg" }
        }))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(submitted["job"]["id"], "job-1");

    let (status, claim) = fairlead.claim_worker_job("manual-only-prune-worker").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(claim["job"]["id"], "job-1");

    let (status, completed) = fairlead
        .complete_worker_job(
            "manual-only-prune-worker",
            "job-1",
            json!({
                "attempt": 1,
                "result": { "classification": "healthy" }
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(completed["job"]["status"], "complete");

    tokio::time::sleep(Duration::from_millis(1_200)).await;
    assert_job_remains_present_for(&fairlead, "job-1", Duration::from_millis(1_300)).await;

    let (status, metrics) = fairlead.get_text("/metrics").await;
    assert_eq!(status, StatusCode::OK);
    assert!(!metrics.contains("fairlead_job_prunes_total{"));

    let (status, pruned) = fairlead.prune_jobs().await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(pruned["pruned"]["removed"], 1);
    assert_eq!(pruned["pruned"]["by_status"][0]["status"], "complete");
    assert_eq!(pruned["pruned"]["by_status"][0]["removed"], 1);

    let body = wait_for_job_not_found(&fairlead, "job-1").await;
    assert_eq!(body["raw"], "job not found");
    wait_for_metrics_contains(
        &fairlead,
        "fairlead_job_prunes_total{status=\"complete\"} 1",
    )
    .await;

    fairlead.shutdown().await;
    fs::remove_dir_all(db_dir).expect("remove manual-only prune SQLite process test temp dir");
}
