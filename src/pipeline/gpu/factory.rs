//! Factory for creating compression backends.
//!
//! Provides automatic backend selection and GPU initialization with fallback.

use super::{
    backend::{CompressorBackend, CpuCompressor},
    config::GpuCompressionConfig,
    BackendType, GpuResult,
};

#[cfg(all(feature = "gpu", target_os = "macos"))]
use super::apple;

#[cfg(all(
    feature = "gpu",
    any(
        all(
            target_os = "linux",
            any(target_arch = "x86_64", target_arch = "aarch64")
        ),
        not(all(
            target_os = "linux",
            any(target_arch = "x86_64", target_arch = "aarch64")
        ))
    )
))]
use super::nvcomp;

/// Factory for creating compression backends with automatic fallback.
pub struct GpuCompressorFactory;

impl GpuCompressorFactory {
    /// Create a compressor backend based on the configuration.
    ///
    /// This method will:
    /// 1. Attempt to use the requested backend
    /// 2. Fall back to CPU if GPU is unavailable and auto_fallback is enabled
    /// 3. Return an error if the requested backend is unavailable
    pub fn create(config: &GpuCompressionConfig) -> GpuResult<Box<dyn CompressorBackend>> {
        config.validate()?;

        match config.backend {
            BackendType::Cpu => Ok(Box::new(CpuCompressor::new(
                config.compression_level,
                config.cpu_threads,
            ))),
            #[cfg(feature = "gpu")]
            BackendType::NvComp => {
                // Try nvcomp, fall back to CPU if enabled
                match nvcomp::NvComCompressor::try_new(
                    config.compression_level,
                    config.gpu_device.unwrap_or(0),
                    config.max_chunk_size,
                ) {
                    Ok(compressor) => Ok(Box::new(compressor)),
                    Err(e) if config.auto_fallback => {
                        eprintln!("GPU compression unavailable: {}. Falling back to CPU.", e);
                        Ok(Box::new(CpuCompressor::new(
                            config.compression_level,
                            config.cpu_threads,
                        )))
                    }
                    Err(e) => Err(e),
                }
            }
            BackendType::Apple => {
                // Try Apple compression, fall back to CPU if enabled
                #[cfg(target_os = "macos")]
                {
                    match apple::AppleCompressor::try_new(
                        config.compression_level,
                        config.cpu_threads,
                        apple::AppleCompressionAlgorithm::Auto,
                    ) {
                        Ok(compressor) => {
                            eprintln!(
                                "Using Apple hardware-accelerated compression (libcompression)"
                            );
                            Ok(Box::new(compressor))
                        }
                        Err(e) if config.auto_fallback => {
                            eprintln!("Apple compression unavailable: {}. Falling back to CPU.", e);
                            Ok(Box::new(CpuCompressor::new(
                                config.compression_level,
                                config.cpu_threads,
                            )))
                        }
                        Err(e) => Err(e),
                    }
                }
                #[cfg(not(target_os = "macos"))]
                {
                    if config.auto_fallback {
                        eprintln!("Apple compression not available on this platform. Falling back to CPU.");
                        Ok(Box::new(CpuCompressor::new(
                            config.compression_level,
                            config.cpu_threads,
                        )))
                    } else {
                        Err(super::GpuCompressionError::DeviceNotFound)
                    }
                }
            }
            BackendType::Auto => {
                // Auto-detect: prioritize Apple on macOS, then GPU, then CPU
                #[cfg(all(feature = "gpu", target_os = "macos"))]
                {
                    // On macOS, try Apple compression first
                    match apple::AppleCompressor::try_new(
                        config.compression_level,
                        config.cpu_threads,
                        apple::AppleCompressionAlgorithm::Auto,
                    ) {
                        Ok(compressor) => {
                            eprintln!("Using Apple hardware-accelerated compression");
                            return Ok(Box::new(compressor));
                        }
                        Err(e) => {
                            eprintln!("Apple compression unavailable: {}", e);
                        }
                    }
                }

                // Try GPU (nvcomp) on Linux or if Apple failed
                #[cfg(feature = "gpu")]
                {
                    match nvcomp::NvComCompressor::try_new(
                        config.compression_level,
                        config.gpu_device.unwrap_or(0),
                        config.max_chunk_size,
                    ) {
                        Ok(compressor) => {
                            eprintln!("Using GPU compression (nvCOMP)");
                            return Ok(Box::new(compressor));
                        }
                        Err(e) => {
                            if config.auto_fallback {
                                eprintln!(
                                    "GPU compression unavailable: {}. Using CPU compression.",
                                    e
                                );
                            } else {
                                return Err(e);
                            }
                        }
                    }
                }

                #[cfg(not(feature = "gpu"))]
                {
                    eprintln!("GPU feature not enabled.");
                }

                // Fallback to CPU
                eprintln!("Using CPU compression");
                Ok(Box::new(CpuCompressor::new(
                    config.compression_level,
                    config.cpu_threads,
                )))
            }
        }
    }

    /// Check if GPU compression is available on this system.
    pub fn is_gpu_available() -> bool {
        #[cfg(feature = "gpu")]
        {
            nvcomp::NvComCompressor::is_available()
        }
        #[cfg(not(feature = "gpu"))]
        {
            false
        }
    }

    /// Get information about available GPU devices.
    pub fn gpu_device_info() -> Vec<GpuDeviceInfo> {
        #[cfg(feature = "gpu")]
        {
            nvcomp::NvComCompressor::device_info()
        }
        #[cfg(not(feature = "gpu"))]
        {
            Vec::new()
        }
    }
}

/// Information about a GPU device.
#[derive(Debug, Clone)]
pub struct GpuDeviceInfo {
    /// Device ID
    pub device_id: u32,
    /// Device name
    pub name: String,
    /// Total memory in bytes
    pub total_memory: usize,
    /// Available memory in bytes
    pub available_memory: usize,
    /// Compute capability major version
    pub compute_capability_major: u32,
    /// Compute capability minor version
    pub compute_capability_minor: u32,
}

/// Compression statistics for monitoring.
#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub struct CompressionStats {
    /// Number of chunks compressed
    pub chunks_compressed: u64,
    /// Total bytes processed (uncompressed)
    pub total_input_bytes: u64,
    /// Total bytes output (compressed)
    pub total_output_bytes: u64,
    /// Compression ratio
    pub compression_ratio: f64,
    /// Average throughput in MB/s
    pub average_throughput_mb_s: f64,
    /// Whether GPU was used
    pub gpu_used: bool,
}

impl CompressionStats {
    /// Calculate compression ratio from input/output bytes.
    #[allow(dead_code)]
    pub fn calculate_ratio(input: u64, output: u64) -> f64 {
        if input == 0 {
            1.0
        } else {
            output as f64 / input as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::gpu::backend::CompressorType;

    #[test]
    fn test_factory_cpu_backend() {
        let config = GpuCompressionConfig::new().with_backend(BackendType::Cpu);
        let compressor = GpuCompressorFactory::create(&config).unwrap();
        assert_eq!(compressor.compressor_type(), CompressorType::Cpu);
    }

    #[test]
    fn test_factory_auto_backend() {
        let config = GpuCompressionConfig::new().with_backend(BackendType::Auto);
        let compressor = GpuCompressorFactory::create(&config).unwrap();
        // Should fall back to CPU if GPU not available
        assert!(compressor.is_available());
    }

    #[test]
    fn test_compression_ratio() {
        let ratio = CompressionStats::calculate_ratio(1000, 350);
        assert!((ratio - 0.35).abs() < 0.01);
    }
}
