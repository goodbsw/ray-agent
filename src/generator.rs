use rand::Rng;
use tokio::sync::mpsc::Sender;

pub async fn generate_logs_to_channel(tx: Sender<String>, counts: usize) {
    println!("⏳ Generating {} logs", counts);
    
    // 가상의 데이터 풀 (메모리 주소만 참조하여 무작위 로그 생성)
    let methods = ["GET", "POST", "PUT", "DELETE"];
    let statuses = [200, 200, 200, 302, 403, 500, 504]; // 500대 에러 비율 조정
    let paths = ["/api/v1/user", "/api/v1/auth", "/index.html", "/checkout", "/api/v2/bao"];

    for i in 0.. counts {
        // 무작위 데이터 매칭
        let log_line = {
            let mut rng = rand::thread_rng();
            let method = methods[rng.gen_range(0..methods.len())];
            let status = statuses[rng.gen_range(0..statuses.len())];
            let path = paths[rng.gen_range(0..paths.len())];
            let latency = rng.gen_range(15..2500);
            let ray_id: u64 = rng.r#gen();
            
            // 이 블록의 최종 결과물로 log_line 문자열을 반환합니다 (세미콜론 없음)
            format!(
                "CF-RayID: {:x} | [{}] {} -> {} | Status: {} | Latency: {}ms\n",
                ray_id, method, path, if status >= 500 { "upstream_timeout" } else { "ok" }, status, latency
            )
        };

        // 3. 비동기 버퍼에 쏟아붓기 (.await로 디스크 커널 이벤트 대기)
        if let Err(e) = tx.send(log_line).await {
            eprintln!("Failed to send logs to the channel: {}", e);
            continue
        }

        // 10만 줄마다 진행 상황 체크
        if i % 100000 == 0 {
            println!("  > {}만 줄 돌파...", i / 10000);
        }
    }

    // 4. 버퍼에 남아있는 찌꺼기 데이터를 디스크에 완전히 밀어 넣고 마칩니다.
    drop(tx);
    println!("✅ Log generation is complete");
}