use reqwest::StatusCode;
use serde_json::Value;
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
    temp_dir: PathBuf,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
}

impl FairleadProcess {
    fn spawn(extra_env: &[(&str, &str)]) -> Self {
        let port = reserve_port();
        let temp_dir = unique_temp_dir("fairlead-process-harness");
        fs::create_dir_all(&temp_dir).expect("create process harness temp dir");
        let stdout_path = temp_dir.join("fairlead.stdout.log");
        let stderr_path = temp_dir.join("fairlead.stderr.log");
        let stdout = File::create(&stdout_path).expect("create Fairlead stdout log");
        let stderr = File::create(&stderr_path).expect("create Fairlead stderr log");

        let mut command = Command::new(env!("CARGO_BIN_EXE_fairlead"));
        command
            .env_clear()
            .env("PORT", port.to_string())
            .env("LOG_LEVEL", "info")
            .env("JOB_STORE", "memory")
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));
        for (key, value) in extra_env {
            command.env(key, value);
        }

        let child = command.spawn().expect("spawn Fairlead process");
        Self {
            child,
            base_url: format!("http://127.0.0.1:{port}"),
            temp_dir,
            stdout_path,
            stderr_path,
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

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()))
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
