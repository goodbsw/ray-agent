use std::sync::{Arc, Mutex};

pub struct LogMetrics {
    pub total_processed: u64,
    pub error_count: u64,
    pub total_latency: u64
}

impl LogMetrics {
    pub fn new() -> Self {
        Self {
            total_processed: 0,
            total_latency: 0,
            error_count: 0
        }
    }
}

pub type SharedMetrics = Arc<Mutex<LogMetrics>>;