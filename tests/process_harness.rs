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

    async fn register_worker(&self, body: Value) -> (StatusCode, Value) {
        self.post_json("/v1/workers/register", body).await
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
