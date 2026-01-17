//! Utility functions for robocodec.

/// Detect the number of available CPU cores with proper fallback.
///
/// This function attempts to detect the number of logical CPU cores available
/// for parallel processing. If detection fails, it returns a sensible default.
///
/// # Returns
///
/// * `u32` - Number of available CPU cores (minimum 1)
///
/// # Examples
///
/// ```
/// use robocodec::core::detect_cpu_count;
///
/// let cpus = detect_cpu_count();
/// println!("Available CPUs: {}", cpus);
/// ```
pub fn detect_cpu_count() -> u32 {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or_else(|_| {
            eprintln!("Warning: Failed to detect CPU count, defaulting to 1");
            1
        }) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_cpu_count() {
        let cpus = detect_cpu_count();
        assert!(cpus >= 1);
    }
}
