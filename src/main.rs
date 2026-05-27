mod generator;
mod parser;
use parser::run_log_parser;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::fs::File;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (tx, rx) = mpsc::channel::<String>(10_000);
    let file_path = "cloudflare_huge.log";
    let file = File::open(file_path).await.expect("Failed to open file");
    let mut reader = BufReader::new(file);
    let parse_handle = tokio::spawn(run_log_parser(rx));

    loop {
        // producer
        let mut line = String::new();
        let byte_read = reader.read_line(&mut line).await.expect("Failed to read bytes from buffer");


        if byte_read == 0 {
            break
        }

        if let Err(e) = tx.send(line).await {
            eprintln!("Non-critical: Failed to send log line: {}", e);
            continue;
        }
    }
    drop(tx);
    parse_handle.await.expect("Failed to run tasks");
    
    Ok(())
}