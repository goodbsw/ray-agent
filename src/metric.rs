use std::sync::{Arc, Mutex};

pub struct LogMetrics {
    total_processed: u64,
    error_count: u64,
    total_latency: u64
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