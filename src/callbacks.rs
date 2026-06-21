use crate::{
    jobs::{JobRecord, JobResponse},
    metrics::{CallbackLabels, RoutingMetrics},
};

pub fn dispatch_job_callback(client: reqwest::Client, metrics: RoutingMetrics, job: JobRecord) {
    let Some(callback_url) = job.callback_url.clone() else {
        return;
    };

    tokio::spawn(async move {
        let status = job.status.as_str().to_string();
        let kind = job.kind.as_str().to_string();
        let result = client
            .post(callback_url)
            .json(&JobResponse { job })
            .send()
            .await;

        match result {
            Ok(response) if response.status().is_success() => {
                metrics.record_callback(CallbackLabels {
                    kind,
                    status,
                    outcome: "success".into(),
                    http_status: response.status().as_u16(),
                });
            }
            Ok(response) => {
                metrics.record_callback(CallbackLabels {
                    kind,
                    status,
                    outcome: "failure".into(),
                    http_status: response.status().as_u16(),
                });
            }
            Err(_) => {
                metrics.record_callback(CallbackLabels {
                    kind,
                    status,
                    outcome: "failure".into(),
                    http_status: 0,
                });
            }
        }
    });
}
