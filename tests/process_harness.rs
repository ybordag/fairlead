use reqwest::StatusCode;
use serde_json::{json, Value};
use std::{
    fs::{self, File},
    net::TcpListener,
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

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
        .post_json(
            "/v1/jobs",
            json!({
                "type": "vision_analysis",
                "priority": "batch",
                "payload": { "image": "rose.jpg" },
                "idempotency_key": "rose-1"
            }),
        )
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
        .post_json(
            "/v1/jobs",
            json!({
                "type": "vision_analysis",
                "priority": "batch",
                "payload": { "image": "rose.jpg" },
                "idempotency_key": "rose-1"
            }),
        )
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(duplicate["job"]["id"], "job-1");

    fairlead.shutdown().await;
    fs::remove_dir_all(db_dir).expect("remove SQLite process test temp dir");
}
