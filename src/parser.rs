use tokio::sync::mpsc::Receiver;
use tokio::time::Instant;

pub async fn run_log_parser(mut rx: Receiver<String>) {
    println!("📡 [Consumer] Log parser task initialized.");

    let start_time = Instant::now();
    let mut total_processed = 0;
    let mut error_count = 0;
    let mut total_latency: u64 = 0;

    while let Some(line) = rx.recv().await {
        // [Task 1] 에러 로그 감지 및 Ray ID 추출
        if line.contains("Status: 403") || line.contains("Status: 500") || line.contains("Status: 504") {
            error_count += 1;
            if let Some((_, ray_id)) = line.split_once("CF-RayID: ") {
                if let Some((pure_ray_id, _)) = ray_id.split_once(" ") {
                    println!("Error found in Ray id: {}\n{}", pure_ray_id, line);
                }                
            }
        }

        // [Task 2] 레이턴시 추출 및 누적 연산
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

        // 20만 줄마다 인터벌 메트릭 출력
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
    
    // 🛡️ Zero-division 방벽 (match 표현식)
    let avg_latency = match total_processed {
        0 => 0.0,
        _ => total_latency as f64 / total_processed as f64,
    };

    println!("=== Final Parser Report ===");
    println!("Total Lines Processed : {}", total_processed);
    println!("Total Server Errors     : {}", error_count);
    println!("Total Execution Time    : {:.4}s", total_elapsed);
    println!("Avg Latency             : {:.4}ms", avg_latency);

    // -------------------------------------------------------------
    // 🔥 Phase 4 핵심: 프로메테우스 텍스트 포맷 출력
    // -------------------------------------------------------------
    println!("\n=== 🎯 Prometheus Exposition Format ===");
    let prometheus_string = format_prometheus_metrics(total_processed, error_count, avg_latency);
    println!("{}", prometheus_string);
}

// 💡 원시 문자열 리터럴 r#""# 을 활용한 프로메테우스 규격 포맷터 함수
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