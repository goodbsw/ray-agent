mod generator;
mod parser;
mod metric;
use parser::run_log_parser;
use metric::LogMetrics;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (tx, rx) = mpsc::channel::<String>(10_000);
    let shared_metric = Arc::new(Mutex::new(LogMetrics::new()));
    let parser_metric = Arc::clone(&shared_metric);
    let parse_handle = tokio::spawn(run_log_parser(rx, parser_metric));
    let produce_handle = tokio::spawn(generator::generate_logs_to_channel(tx, 1_000_000));

    let _ = produce_handle.await.expect("Failed to run producer");
    let _ = parse_handle.await.expect("Failed to run tasks");
    
    Ok(())
}