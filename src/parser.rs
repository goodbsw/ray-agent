use tokio::sync::mpsc::Receiver;
use tokio::time::{Instant};
pub async fn run_log_parser(mut rx: Receiver<String>) {
    println!("📡 [Consumer] Log parser task initialized.");

    let start_time = Instant::now();
    let mut total_processed = 0;
    let mut error_count = 0;


    while let Some(line) = rx.recv().await {
        if line.contains("Status: 403") || line.contains("Status: 500") || line.contains("Status: 504") {
            error_count += 1;
            if let Some((_, ray_id)) = line.split_once("CF-RayID: ") {
                if let Some((pure_ray_id, _)) = ray_id.split_once(" ") {
                    println!("Error found in Ray id: {}\n{}", pure_ray_id, line);
                }                
            }
        }

        total_processed += 1;

        if total_processed % 200_000 == 0 {
            let elapsed = start_time.elapsed().as_secs_f64();
            let lps = total_processed as f64 / elapsed;
            println!(
                "📊 [Metrics] Processed: {} lines | Errors: {} | Throughput: {:.2} lines/sec",
                total_processed, error_count, lps
            );
        }
    }

    let total_elapsed = start_time.elapsed().as_secs_f64();
    println!("=== Final Parser Report ===");
    println!("Total Lines Processed : {}", total_processed);
    println!("Total Server Errors     : {}", error_count);
    println!("Total Execution Time    : {:.4}s", total_elapsed);
}