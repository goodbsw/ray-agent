mod generator;
mod metric;
mod parser;

use axum::{extract::State, routing::get, Router};
use metric::{LogMetrics, SharedMetrics};
use parser::run_log_parser;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Allocate the metric monitor storage into Heap memory guarded via Arc+Mutex
    let shared_metrics: SharedMetrics = Arc::new(Mutex::new(LogMetrics::new()));

    // Clone atomic pointers for individual runtime ownership requirements
    let parser_metrics = Arc::clone(&shared_metrics);
    let web_metrics = Arc::clone(&shared_metrics);

    // Initialize cross-thread async communication MPSC channel pipeline
    let (tx, rx) = mpsc::channel::<String>(10_000);

    // Spawn logging infrastructure routines concurrently onto Tokio worker threads
    let parse_handle = tokio::spawn(run_log_parser(rx, parser_metrics));
    let produce_handle = tokio::spawn(generator::generate_logs_to_channel(tx, 1_000_000));

    // Configure Axum Web Framework router and inject shared metrics state 
    let app = Router::new()
        .route("/metrics", get(handle_metrics))
        .with_state(web_metrics);

    // Bind non-blocking TCP socket listener onto local port 8080
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080").await?;
    println!("🚀 [Web Server] Prometheus exporter metric server running on http://127.0.0.1:8080/metrics");
    
    // Run the HTTP server daemon concurrently
    let server_handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Await all parallel async operations
    let _ = produce_handle.await;
    let _ = parse_handle.await;
    let _ = server_handle.await;

    Ok(())
}

// GET /metrics handler responding with Prometheus formatted system stats
async fn handle_metrics(State(metrics): State<SharedMetrics>) -> String {
    // Acquire resource lock briefly to generate string data snapshot
    let data = metrics.lock().unwrap();

    let avg_latency = match data.total_processed {
        0 => 0.0,
        _ => data.total_latency as f64 / data.total_processed as f64,
    };

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
        data.total_processed, data.error_count, avg_latency
    )
}