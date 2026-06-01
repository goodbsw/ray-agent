use tokio::sync::mpsc::Receiver;
use tokio::time::Instant;
use crate::metric::SharedMetrics;

pub async fn run_log_parser(mut rx: Receiver<String>, parser_metric: SharedMetrics) {
    println!("📡 [Consumer] Log parser task initialized.");

    let start_time = Instant::now();
    
    // Thread-local variables for ultra-fast arithmetic processing without lock contention
    let mut total_processed = 0;
    let mut error_count = 0;
    let mut total_latency: u64 = 0;

    while let Some(line) = rx.recv().await {
        // [Task 1] Detect server-side errors and extract tracing Ray IDs
        if line.contains("Status: 403") || line.contains("Status: 500") || line.contains("Status: 504") {
            error_count += 1;
            if let Some((_, ray_id)) = line.split_once("CF-RayID: ") {
                if let Some((pure_ray_id, _)) = ray_id.split_once(" ") {
                    println!("Error found in Ray id: {}\n{}", pure_ray_id, line);
                }                
            }
        }

        // [Task 2] Extract latency indicators and trigger alerts for spikes
        if let Some((_, duration_part)) = line.split_once("Latency: ") {
            if let Some((latency_str, _)) = duration_part.split_once("ms") {
                if let Ok(latency_num) = latency_str.parse::<u32>() {
                    total_latency += latency_num as u64;
                    if latency_num > 100 {
                        println!("[Spike-Warning] {}", line);
                    }
                }
            }
        }

        total_processed += 1;

        // Periodic batch synchronization (every 200k lines) to update the shared memory state
        if total_processed % 200_000 == 0 {
            let elapsed = start_time.elapsed().as_secs_f64();
            let lps = total_processed as f64 / elapsed;
            println!(
                "📊 [Metrics] Processed: {} lines | Errors: {} | Throughput: {:.2} lines/sec",
                total_processed, error_count, lps
            );

            // Open temporary scope to minimize Mutex guard lifetime (Early Drop)
            {
                let mut data = parser_metric.lock().unwrap();
                data.total_latency = total_latency;
                data.error_count = error_count;
                data.total_processed = total_processed;
            } 
        }
    }

    let total_elapsed = start_time.elapsed().as_secs_f64();
    
    // Final state synchronization upon channel closing
    {
        let mut data = parser_metric.lock().unwrap();
        data.total_latency = total_latency;
        data.error_count = error_count;
        data.total_processed = total_processed;
    }

    // Capture the final snapshot for local CLI reporting
    let data = parser_metric.lock().unwrap();
    let avg_latency = match data.total_processed {
        0 => 0.0,
        _ => data.total_latency as f64 / data.total_processed as f64,
    };

    println!("=== Final Parser Report ===");
    println!("Total Lines Processed : {}", data.total_processed);
    println!("Total Server Errors     : {}", data.error_count);
    println!("Total Execution Time    : {:.4}s", total_elapsed);
    println!("Avg Latency             : {:.4}ms", avg_latency);

    println!("\n=== 🎯 Prometheus Exposition Format ===");
    let prometheus_string = format_prometheus_metrics(data.total_processed, data.error_count, avg_latency);
    println!("{}", prometheus_string);
}

// Formatter mapping state data into Prometheus standard exposition exposition string format
fn format_prometheus_metrics(total: u64, errors: u64, avg_latency: f64) -> String {
    format!(
r#"# HELP log_lines_total Total number of processed log lines.
# TYPE log_lines_total counter
log_lines_total {}

# HELP log_errors_total Total number of server errors (403, 500, 504).
# TYPE log_errors_total counter
log_errors_total {}

# HELP log_avg_latency_ms Average processing latency in milliseconds.
# TYPE log_avg_latency_ms gauge
log_avg_latency_ms {:.2}
"#, 
        total, errors, avg_latency
    )
}