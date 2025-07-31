use peak_alloc::PeakAlloc;

#[global_allocator]
pub static PEAK_ALLOC: PeakAlloc = PeakAlloc;

use std::time::{Duration, Instant};

pub struct PerfLogger {
    checkpoint_count: usize,
    start: Option<Instant>,
    last: Option<Duration>,
}

impl PerfLogger {
    pub fn start(&mut self) {
        self.start = Some(Instant::now());
    }
    pub fn new() -> Self {
        PerfLogger {
            checkpoint_count: 0,
            start: None,
            last: Some(Duration::new(0, 0)),
        }
    }

    pub fn new_started() -> Self {
        let mut metadata = PerfLogger {
            checkpoint_count: 0,
            start: None,
            last: Some(Duration::new(0, 0)),
        };
        metadata.start();
        metadata
    }

    pub fn log_checkpoint(&mut self, message: &str, with_memory: bool) {
        let memory = if with_memory {
            let string = format!("{}Mb RAM", PEAK_ALLOC.current_usage_as_mb());
            string
        } else {
            String::new()
        };

        log::debug!(
            "Checkpoint[{new_checkpoint}]: {message}, since Checkpoint[{last_checkpoint}]: {delta:?} | {memory}",
            new_checkpoint=self.checkpoint_count + 1,
            last_checkpoint=self.checkpoint_count,
            delta=self.start.unwrap().elapsed() - self.last.unwrap(),
            memory=memory
        );
        self.last = Some(self.start.unwrap().elapsed());
        self.checkpoint_count += 1;
    }

    pub fn log_elapsed(&self) {
        match self.start {
            None => panic!("Cannot print total duration without calling .start() first"),
            Some(instant) => log::debug!("total_elapsed_data_loading: {:.2?}", instant.elapsed()),
        }
    }

    pub fn log_peak_memory(&self) {
        log::debug!("peak_mem_usage: {}MB", PEAK_ALLOC.peak_usage_as_mb())
    }
    pub fn elapsed(&self) -> Duration {
        self.start.unwrap().elapsed()
    }
}

pub fn log_memory() {
    log::debug!(
        "[DEBUG] Current memory usage: {}MB",
        PEAK_ALLOC.current_usage_as_mb()
    )
}

pub fn log_memory_with_message(message: &str) {
    log::debug!(
        "[DEBUG] {message} | Current memory usage: {}MB",
        PEAK_ALLOC.current_usage_as_mb()
    )
}

pub fn log_peak_memory() {
    log::debug!(
        "[DEBUG] Peak memory usage: {}MB",
        PEAK_ALLOC.peak_usage_as_mb()
    )
}
