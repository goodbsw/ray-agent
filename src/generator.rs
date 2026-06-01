use rand::Rng;
use tokio::sync::mpsc::Sender;

pub async fn generate_logs_to_channel(tx: Sender<String>, counts: usize) {
    println!("⏳ Generating {} logs", counts);

    let methods = ["GET", "POST", "PUT", "DELETE"];
    let statuses = [200, 200, 200, 302, 403, 500, 504]; 
    let paths = ["/api/v1/user", "/api/v1/auth", "/index.html", "/checkout", "/api/v2/bao"];

    for i in 0..counts {
        // Isolate non-Send Rng within an anonymous scope to prevent across-await thread panic
        let log_line = {
            let mut rng = rand::thread_rng();
            let method = methods[rng.gen_range(0..methods.len())];
            let status = statuses[rng.gen_range(0..statuses.len())];
            let path = paths[rng.gen_range(0..paths.len())];
            let latency = rng.gen_range(15..2500);
            let ray_id: u64 = rng.r#gen();
            
            // Format simulation logs resembling Cloudflare CDN standards
            format!(
                "CF-RayID: {:x} | [{}] {} -> {} | Status: {} | Latency: {}ms\n",
                ray_id, method, path, if status >= 500 { "upstream_timeout" } else { "ok" }, status, latency
            )
        };

        // Push formatted log to the in-memory buffer channel
        if let Err(e) = tx.send(log_line).await {
            eprintln!("Failed to send logs to the channel: {}", e);
            continue;
        }

        if i % 100_000 == 0 {
            println!("  > {}0k lines generated...", i / 100_000);
        }
    }

    // Explicitly drop transmitter to notify the receiver task about EOF
    drop(tx);
    println!("✅ Log generation is complete");
}