use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use rand::Rng;

pub async fn generate_huge_log(file_path: &str, total_lines: usize) {
    println!("⏳ 딛스크에 {}줄짜리 대용량 테스트 로그 생성 중...", total_lines);
    
    // 1. 비동기 쓰기 모드로 파일을 생성하고, 대용량 쓰기에 필수적인 BufWriter를 붙입니다.
    let file = File::create(file_path).await.unwrap();
    let mut writer = BufWriter::new(file);

    let mut rng = rand::thread_rng();
    
    // 가상의 데이터 풀 (메모리 주소만 참조하여 무작위 로그 생성)
    let methods = ["GET", "POST", "PUT", "DELETE"];
    let statuses = [200, 200, 200, 302, 403, 500, 504]; // 500대 에러 비율 조정
    let paths = ["/api/v1/user", "/api/v1/auth", "/index.html", "/checkout", "/api/v2/bao"];

    for i in 1..=total_lines {
        // 무작위 데이터 매칭
        let method = methods[rng.gen_range(0..methods.len())];
        let status = statuses[rng.gen_range(0..statuses.len())];
        let path = paths[rng.gen_range(0..paths.len())];
        let latency = rng.gen_range(15..2500); // 밀리초 단위 레이턴시

        // 💡 Cloudflare Ray ID 시뮬레이션 (16진수 무작위 문자열 생성)
        let ray_id: u64 = rng.r#gen();
        
        // 2. 하나의 완성된 대규모 분산 환경 규격의 로그 포맷 생성
        let log_line = format!(
            "CF-RayID: {:x} | [{}] {} -> {} | Status: {} | Latency: {}ms\n",
            ray_id, method, path, if status >= 500 { "upstream_timeout" } else { "ok" }, status, latency
        );

        // 3. 비동기 버퍼에 쏟아붓기 (.await로 디스크 커널 이벤트 대기)
        writer.write_all(log_line.as_bytes()).await.unwrap();

        // 10만 줄마다 진행 상황 체크
        if i % 100000 == 0 {
            println!("  > {}만 줄 돌파...", i / 10000);
        }
    }

    // 4. 버퍼에 남아있는 찌꺼기 데이터를 디스크에 완전히 밀어 넣고 마칩니다.
    writer.flush().await.unwrap();
    println!("✅ 로그 생성 완료: {}", file_path);
}