//! Hardware detection for auto-configuration.
//!
//! Provides system capability detection including CPU cores, memory size,
//! and CPU cache information for intelligent performance tuning.

use std::sync::OnceLock;
use tracing::info;

/// Detected hardware information.
#[derive(Debug, Clone, Copy)]
pub struct HardwareInfo {
    /// Total number of logical CPU cores available.
    pub cpu_cores: usize,
    /// Total system memory in bytes.
    pub total_memory_bytes: u64,
    /// L3 cache size in bytes (if detectable).
    pub l3_cache_bytes: Option<u64>,
    /// Whether this is an Apple Silicon (ARM) processor.
    pub is_apple_silicon: bool,
}

impl HardwareInfo {
    /// Detect hardware information.
    ///
    /// This function caches the result since hardware doesn't change at runtime.
    pub fn detect() -> Self {
        static DETECTED: OnceLock<HardwareInfo> = OnceLock::new();
        *DETECTED.get_or_init(Self::detect_impl)
    }

    #[cfg(all(target_arch = "x86_64", feature = "cpuid"))]
    fn detect_impl() -> Self {
        use raw_cpuid::CpuId;

        let cpu_cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);

        // Detect L3 cache
        let l3_cache_bytes = CpuId::new().get_cache_parameters().and_then(|cparams| {
            for cache in cparams {
                if cache.level() == 3 {
                    let cache_size = cache.sets() as u64
                        * cache.associativity() as u64
                        * cache.coherency_line_size() as u64;
                    return Some(cache_size);
                }
            }
            None
        });

        // Detect system memory (platform-specific)
        #[cfg(target_os = "macos")]
        let total_memory_bytes = detect_memory_macos();
        #[cfg(target_os = "linux")]
        let total_memory_bytes = detect_memory_linux();
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        let total_memory_bytes = detect_memory_fallback();

        info!(
            cpu_cores,
            memory_gb = total_memory_bytes / (1024 * 1024 * 1024),
            l3_cache_mb = l3_cache_bytes.map(|b| b / (1024 * 1024)),
            "Detected hardware (x86_64 with cpuid)"
        );

        Self {
            cpu_cores,
            total_memory_bytes,
            l3_cache_bytes,
            is_apple_silicon: false,
        }
    }

    #[cfg(all(target_arch = "x86_64", not(feature = "cpuid")))]
    fn detect_impl() -> Self {
        let cpu_cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);

        #[cfg(target_os = "macos")]
        let total_memory_bytes = detect_memory_macos();
        #[cfg(target_os = "linux")]
        let total_memory_bytes = detect_memory_linux();
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        let total_memory_bytes = detect_memory_fallback();

        info!(
            cpu_cores,
            memory_gb = total_memory_bytes / (1024 * 1024 * 1024),
            "Detected hardware (x86_64 without cpuid)"
        );

        Self {
            cpu_cores,
            total_memory_bytes,
            l3_cache_bytes: None,
            is_apple_silicon: false,
        }
    }

    #[cfg(target_arch = "aarch64")]
    fn detect_impl() -> Self {
        let cpu_cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);

        // Detect if this is Apple Silicon
        #[cfg(target_os = "macos")]
        let is_apple_silicon = true;
        #[cfg(not(target_os = "macos"))]
        let is_apple_silicon = false;

        #[cfg(target_os = "macos")]
        let total_memory_bytes = detect_memory_macos();
        #[cfg(target_os = "linux")]
        let total_memory_bytes = detect_memory_linux();
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        let total_memory_bytes = detect_memory_fallback();

        info!(
            cpu_cores,
            memory_gb = total_memory_bytes / (1024 * 1024 * 1024),
            is_apple_silicon,
            "Detected hardware (aarch64)"
        );

        Self {
            cpu_cores,
            total_memory_bytes,
            l3_cache_bytes: None, // ARM cache detection is complex
            is_apple_silicon,
        }
    }

    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    fn detect_impl() -> Self {
        let cpu_cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);

        #[cfg(target_os = "macos")]
        let total_memory_bytes = detect_memory_macos();
        #[cfg(target_os = "linux")]
        let total_memory_bytes = detect_memory_linux();
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        let total_memory_bytes = detect_memory_fallback();

        info!(
            cpu_cores,
            memory_gb = total_memory_bytes / (1024 * 1024 * 1024),
            "Detected hardware (generic)"
        );

        Self {
            cpu_cores,
            total_memory_bytes,
            l3_cache_bytes: None,
            is_apple_silicon: false,
        }
    }

    /// Get total memory in gigabytes.
    pub fn total_memory_gb(&self) -> f64 {
        self.total_memory_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
    }

    /// Get L3 cache size in megabytes (if available).
    pub fn l3_cache_mb(&self) -> Option<f64> {
        self.l3_cache_bytes
            .map(|bytes| bytes as f64 / (1024.0 * 1024.0))
    }

    /// Get a reasonable default batch size based on cache size.
    ///
    /// Uses L3 cache if available, otherwise scales with total memory.
    pub fn suggested_batch_size(&self) -> usize {
        if let Some(l3_bytes) = self.l3_cache_bytes {
            // Use half of L3 cache as batch size (aggressive)
            (l3_bytes / 2).clamp(4 * 1024 * 1024, 64 * 1024 * 1024) as usize
        } else {
            // Scale with total memory: 1MB per GB, clamped to reasonable range
            let mem_mb = (self.total_memory_bytes / (1024 * 1024)) as usize;
            (mem_mb).clamp(8, 32) * 1024 * 1024
        }
    }

    /// Get suggested compression thread count.
    ///
    /// Reserves some cores for other pipeline stages.
    pub fn suggested_compression_threads(&self) -> usize {
        // Reserve 4 cores for other stages (prefetch, parser, batcher, packetizer)
        // Minimum 2 threads for compression
        (self.cpu_cores.saturating_sub(4)).max(2)
    }

    /// Get suggested per-stage thread count (parser, batcher, etc.).
    ///
    /// Uses a small fraction of available cores.
    pub fn suggested_stage_threads(&self) -> usize {
        // Use 1/8 of cores for lightweight stages, minimum 2
        (self.cpu_cores / 8).max(2)
    }

    /// Get suggested channel capacity (scales with memory).
    pub fn suggested_channel_capacity(&self) -> usize {
        // Scale with memory: 4 channels per GB of RAM, minimum 16
        let mem_gb = (self.total_memory_bytes / (1024 * 1024 * 1024)) as usize;
        (mem_gb * 4).max(16)
    }
}

/// Detect system memory on macOS using sysctl.
#[cfg(target_os = "macos")]
fn detect_memory_macos() -> u64 {
    unsafe {
        let mut len: std::os::raw::c_uint = 0;
        let name = c"hw.memsize".as_ptr();

        // First call to get the length
        if libc::sysctlbyname(
            name,
            std::ptr::null_mut(),
            &mut len as *mut _ as *mut _,
            std::ptr::null_mut(),
            0,
        ) != 0
        {
            return 8 * 1024 * 1024 * 1024; // 8GB default
        }

        let mut memory: u64 = 0;
        if libc::sysctlbyname(
            name,
            &mut memory as *mut _ as *mut _,
            &mut len as *mut _ as *mut _,
            std::ptr::null_mut(),
            0,
        ) != 0
        {
            return 8 * 1024 * 1024 * 1024;
        }

        memory
    }
}

/// Detect system memory on Linux by reading /proc/meminfo.
#[cfg(target_os = "linux")]
fn detect_memory_linux() -> u64 {
    use std::fs;

    // Try /proc/meminfo first
    if let Ok(meminfo) = fs::read_to_string("/proc/meminfo") {
        for line in meminfo.lines() {
            if line.starts_with("MemTotal:") {
                // Format: "MemTotal:       16384000 kB"
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    if let Ok(kb) = parts[1].parse::<u64>() {
                        return kb * 1024;
                    }
                }
            }
        }
    }

    // Fallback
    8 * 1024 * 1024 * 1024
}

/// Fallback memory detection using a reasonable default.
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn detect_memory_fallback() -> u64 {
    // Conservative 8GB default for unknown platforms
    8 * 1024 * 1024 * 1024
}

impl Default for HardwareInfo {
    fn default() -> Self {
        Self::detect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hardware_detection() {
        let hw = HardwareInfo::detect();
        assert!(hw.cpu_cores >= 1);
        assert!(hw.total_memory_bytes >= 1024 * 1024 * 1024); // At least 1GB
    }

    #[test]
    fn test_suggested_compression_threads() {
        let hw = HardwareInfo::detect();
        let threads = hw.suggested_compression_threads();
        assert!(threads >= 2);
        assert!(threads <= hw.cpu_cores);
    }

    #[test]
    fn test_suggested_batch_size() {
        let hw = HardwareInfo::detect();
        let batch = hw.suggested_batch_size();
        assert!(batch >= 4 * 1024 * 1024); // At least 4MB
        assert!(batch <= 64 * 1024 * 1024); // At most 64MB
    }

    #[test]
    fn test_suggested_stage_threads() {
        let hw = HardwareInfo::detect();
        let threads = hw.suggested_stage_threads();
        assert!(threads >= 2);
    }

    #[test]
    fn test_suggested_channel_capacity() {
        let hw = HardwareInfo::detect();
        let capacity = hw.suggested_channel_capacity();
        assert!(capacity >= 16);
    }

    #[test]
    fn test_total_memory_gb() {
        let hw = HardwareInfo::detect();
        let gb = hw.total_memory_gb();
        assert!(gb >= 1.0);
    }
}
