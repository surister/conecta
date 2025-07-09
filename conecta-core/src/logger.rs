use peak_alloc::PeakAlloc;

#[global_allocator]
static PEAK_ALLOC: PeakAlloc = PeakAlloc;

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
