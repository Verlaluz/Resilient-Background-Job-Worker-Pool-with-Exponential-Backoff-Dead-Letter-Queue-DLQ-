use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};

/// Represents an individual job envelope submitted to the queue
#[derive(Debug, Clone)]
struct BackgroundJobEnvelope {
    job_identifier: String,
    task_payload_description: String,
    current_retry_count: u32,
    maximum_retry_limit: u32,
    should_simulate_failure: bool,
}

/// The Dead-Letter Queue holds jobs that permanently failed for auditing
type SharedDeadLetterQueue = Arc<Mutex<Vec<BackgroundJobEnvelope>>>;

#[tokio::main]
async fn main() {
    println!("🚀 Initializing Resilient Background Worker Pool...\n");

    // 1. Channel for incoming tasks (Buffer capacity = 100)
    let (job_submission_transmitter, mut job_distribution_receiver) = mpsc::channel::<BackgroundJobEnvelope>(100);

    // 2. Dead-Letter Queue storage for permanently failed jobs
    let dead_letter_vault: SharedDeadLetterQueue = Arc::new(Mutex::new(Vec::new()));

    // 3. Dedicated channel to route jobs from workers back into the queue for retries
    let (retry_feedback_transmitter, mut retry_feedback_receiver) = mpsc::channel::<BackgroundJobEnvelope>(100);

    // Spawn a multiplexer task to feed both new jobs and retried jobs into the workers
    let (worker_intake_transmitter, worker_intake_receiver) = async_channel::unbounded::<BackgroundJobEnvelope>();

    // Route fresh submissions to workers
    let fresh_intake_forwarder = worker_intake_transmitter.clone();
    tokio::spawn(async move {
        while let Some(fresh_job) = job_distribution_receiver.recv().await {
            let _ = fresh_intake_forwarder.send(fresh_job).await;
        }
    });

    // Route retried jobs back to workers
    let retry_intake_forwarder = worker_intake_transmitter.clone();
    tokio::spawn(async move {
        while let Some(retried_job) = retry_feedback_receiver.recv().await {
            let _ = retry_intake_forwarder.send(retried_job).await;
        }
    });

    // 4. Spawn a pool of 3 concurrent worker tasks
    let total_workers_count = 3;
    for worker_index in 1..=total_workers_count {
        let current_worker_id = worker_index;
        let worker_queue_receiver = worker_intake_receiver.clone();
        let worker_retry_transmitter = retry_feedback_transmitter.clone();
        let worker_dlq_handle = dead_letter_vault.clone();

        tokio::spawn(async move {
            println!("  👷 Worker #{} is online and ready for tasks.", current_worker_id);

            while let Ok(mut active_job) = worker_queue_receiver.recv().await {
                println!(
                    "⚙️  [Worker #{}] Started job '{}' (Payload: '{}')",
                    current_worker_id, active_job.job_identifier, active_job.task_payload_description
                );

                // Simulate processing time
                tokio::time::sleep(Duration::from_millis(300)).await;

                // Simulate an execution failure if flagged
                if active_job.should_simulate_failure {
                    active_job.current_retry_count += 1;

                    if active_job.current_retry_count <= active_job.maximum_retry_limit {
                        // Calculate exponential backoff: 500ms * 2^(retry_count - 1)
                        let exponent_factor = 2_u64.pow(active_job.current_retry_count - 1);
                        let backoff_duration_milliseconds = 500 * exponent_factor;

                        println!(
                            "⚠️  [Worker #{}] Job '{}' failed! Scheduling Retry #{} after {}ms backoff...",
                            current_worker_id,
                            active_job.job_identifier,
                            active_job.current_retry_count,
                            backoff_duration_milliseconds
                        );

                        let delayed_retry_transmitter = worker_retry_transmitter.clone();
                        let job_to_retry_later = active_job;

                        // Sleep asynchronously without blocking the worker thread
                        tokio::spawn(async move {
                            tokio::time::sleep(Duration::from_millis(backoff_duration_milliseconds)).await;
                            let _ = delayed_retry_transmitter.send(job_to_retry_later).await;
                        });
                    } else {
                        // Max retries exceeded -> Escalate to Dead-Letter Queue
                        println!(
                            "💀 [Worker #{}] CRITICAL: Job '{}' exceeded max retries! Moving to Dead-Letter Queue.",
                            current_worker_id, active_job.job_identifier
                        );

                        let mut dlq_guard = worker_dlq_handle.lock().await;
                        dlq_guard.push(active_job);
                    }
                } else {
                    println!(
                        "✅ [Worker #{}] Completed job '{}' successfully!",
                        current_worker_id, active_job.job_identifier
                    );
                }
            }
        });
    }

    // Give workers a brief moment to boot
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 5. Submit a batch of test jobs
    println!("\n📥 Submitting job batch to queue...\n");

    // Job 1: Normal task that will succeed immediately
    job_submission_transmitter.send(BackgroundJobEnvelope {
        job_identifier: String::from("JOB_101_SEND_WELCOME_EMAIL"),
        task_payload_description: String::from("Send email to user_alice@example.com"),
        current_retry_count: 0,
        maximum_retry_limit: 3,
        should_simulate_failure: false,
    }).await.unwrap();

    // Job 2: Flaky task that permanently fails and will trigger retries and move to DLQ
    job_submission_transmitter.send(BackgroundJobEnvelope {
        job_identifier: String::from("JOB_102_BILLING_INVOICE_CHARGE"),
        task_payload_description: String::from("Process $49.00 subscription charge"),
        current_retry_count: 0,
        maximum_retry_limit: 2, // Allow 2 retries before giving up
        should_simulate_failure: true,
    }).await.unwrap();

    // Job 3: Another normal task
    job_submission_transmitter.send(BackgroundJobEnvelope {
        job_identifier: String::from("JOB_103_GENERATE_REPORT_PDF"),
        task_payload_description: String::from("Generate quarterly balance sheet"),
        current_retry_count: 0,
        maximum_retry_limit: 3,
        should_simulate_failure: false,
    }).await.unwrap();

    // Wait 5 seconds to let retries and backoffs process
    tokio::time::sleep(Duration::from_secs(5)).await;

    // 6. Inspect Dead-Letter Queue
    let finalized_dlq = dead_letter_vault.lock().await;
    println!("\n========================================");
    println!("📋 DEAD-LETTER QUEUE AUDIT REPORT");
    println!("========================================");
    println!("Total permanently quarantined jobs: {}", finalized_dlq.len());
    for quarantined_job in finalized_dlq.iter() {
        println!(
            "  -> Job ID: '{}' | Failed Retries: {}/{} | Payload: '{}'",
            quarantined_job.job_identifier,
            quarantined_job.current_retry_count,
            quarantined_job.maximum_retry_limit,
            quarantined_job.task_payload_description
        );
    }
}