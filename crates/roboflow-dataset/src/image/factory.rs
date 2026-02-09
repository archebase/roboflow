// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Factory for creating image decoder backends with automatic fallback.
//!
//! Provides automatic backend selection and GPU initialization with fallback,
//! similar to `GpuCompressorFactory` in `roboflow-pipeline/gpu/factory.rs`.
//!
use super::{
    ImageError, Result,
    backend::{CpuImageDecoder, ImageDecoderBackend},
    config::{DecoderBackendType, ImageDecoderConfig},
    memory::MemoryStrategy,
};

/// Information about an available GPU device.
///
/// Similar to `GpuDeviceInfo` in `roboflow-pipeline/gpu/factory.rs`.
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

/// Factory for creating image decoder backends with automatic fallback.
///
/// This factory mirrors the design of `GpuCompressorFactory`:
/// - Auto-detects available GPU decoding
/// - Provides automatic CPU fallback
/// - Caches decoder instances to avoid re-initialization
pub struct ImageDecoderFactory {
    config: ImageDecoderConfig,
    cached_decoder: Option<Box<dyn ImageDecoderBackend>>,
}

impl ImageDecoderFactory {
    /// Create a new factory with the given configuration.
    pub fn new(config: &ImageDecoderConfig) -> Self {
        config.validate().expect("Invalid ImageDecoderConfig");
        Self {
            config: config.clone(),
            cached_decoder: None,
        }
    }

    /// Create a decoder backend based on the configuration.
    ///
    /// This method will:
    /// 1. Attempt to use the requested backend
    /// 2. Fall back to CPU if GPU is unavailable and auto_fallback is enabled
    /// 3. Return an error if the requested backend is unavailable
    pub fn create_decoder(&mut self) -> Result<Box<dyn ImageDecoderBackend>> {
        match self.config.backend {
            DecoderBackendType::Cpu => Ok(Box::new(CpuImageDecoder::new(
                self.config.memory_strategy,
                self.config.cpu_threads,
            ))),

            DecoderBackendType::Gpu => {
                #[cfg(target_os = "linux")]
                {
                    use super::gpu::GpuImageDecoder;

                    match GpuImageDecoder::try_new(
                        self.config.gpu_device.unwrap_or(0),
                        self.config.memory_strategy,
                    ) {
                        Ok(decoder) => {
                            tracing::info!("Using GPU decoder (nvJPEG)");
                            return Ok(Box::new(decoder));
                        }
                        Err(e) if self.config.auto_fallback => {
                            tracing::warn!(
                                error = %e,
                                "GPU decoder unavailable. Falling back to CPU."
                            );
                            return Ok(Box::new(CpuImageDecoder::new(
                                self.config.memory_strategy,
                                self.config.cpu_threads,
                            )));
                        }
                        Err(e) => Err(e),
                    }
                }
                #[cfg(not(target_os = "linux"))]
                {
                    if self.config.auto_fallback {
                        tracing::warn!("GPU decoding not supported on this platform. Using CPU.");
                        Ok(Box::new(CpuImageDecoder::new(
                            self.config.memory_strategy,
                            self.config.cpu_threads,
                        )))
                    } else {
                        Err(ImageError::GpuUnavailable(
                            "GPU decoding is supported on Linux only".to_string(),
                        ))
                    }
                }
            }

            DecoderBackendType::Apple => {
                #[cfg(target_os = "macos")]
                {
                    use super::apple::AppleImageDecoder;

                    match AppleImageDecoder::try_new(self.config.memory_strategy) {
                        Ok(decoder) => {
                            tracing::info!("Using Apple hardware-accelerated decoder");
                            return Ok(Box::new(decoder));
                        }
                        Err(e) if self.config.auto_fallback => {
                            tracing::warn!(
                                error = %e,
                                "Apple decoder unavailable. Falling back to CPU."
                            );
                            return Ok(Box::new(CpuImageDecoder::new(
                                self.config.memory_strategy,
                                self.config.cpu_threads,
                            )));
                        }
                        Err(e) => Err(e),
                    }
                }
                #[cfg(not(target_os = "macos"))]
                {
                    if self.config.auto_fallback {
                        tracing::warn!("Apple decoding not supported on this platform. Using CPU.");
                        Ok(Box::new(CpuImageDecoder::new(
                            self.config.memory_strategy,
                            self.config.cpu_threads,
                        )))
                    } else {
                        Err(ImageError::GpuUnavailable(
                            "Apple hardware decoding is supported on macOS only".to_string(),
                        ))
                    }
                }
            }

            DecoderBackendType::Auto => {
                // Auto-detect: prioritize GPU, then CPU

                // Try Apple first on macOS
                #[cfg(target_os = "macos")]
                {
                    use super::apple::AppleImageDecoder;

                    if let Ok(decoder) = AppleImageDecoder::try_new(self.config.memory_strategy) {
                        tracing::debug!("Auto-detected Apple hardware decoder");
                        return Ok(Box::new(decoder));
                    }
                }

                // Try GPU on Linux
                #[cfg(target_os = "linux")]
                {
                    use super::gpu::GpuImageDecoder;

                    if let Ok(decoder) = GpuImageDecoder::try_new(
                        self.config.gpu_device.unwrap_or(0),
                        self.config.memory_strategy,
                    ) {
                        tracing::debug!("Auto-detected GPU decoder (nvJPEG)");
                        return Ok(Box::new(decoder));
                    }
                }

                // Fallback to CPU
                tracing::debug!("Using CPU decoder (image crate)");
                Ok(Box::new(CpuImageDecoder::new(
                    self.config.memory_strategy,
                    self.config.cpu_threads,
                )))
            }
        }
    }

    /// Get or create a decoder (cached).
    ///
    /// Returns a reference to the cached decoder if available,
    /// otherwise creates and caches a new one. The backend is chosen once
    /// at first use and does not change for the lifetime of this factory.
    ///
    /// This is useful for maintaining decoder state (e.g., CUDA context)
    /// across multiple decode operations.
    pub fn get_decoder(&mut self) -> &dyn ImageDecoderBackend {
        if self.cached_decoder.is_none() {
            match self.create_decoder() {
                Ok(decoder) => self.cached_decoder = Some(decoder),
                Err(e) => {
                    tracing::error!("Failed to create decoder: {}. Using fallback.", e);
                    // Create a basic CPU decoder as fallback
                    self.cached_decoder =
                        Some(Box::new(CpuImageDecoder::new(MemoryStrategy::Heap, 1)));
                }
            }
        }

        // SAFETY: We just ensured cached_decoder is Some
        self.cached_decoder
            .as_deref()
            .expect("Decoder should be cached")
    }

    /// Check if GPU decoding is available on this system.
    pub fn is_gpu_available() -> bool {
        #[cfg(target_os = "linux")]
        {
            super::gpu::GpuImageDecoder::is_available()
        }
        #[cfg(not(target_os = "linux"))]
        {
            false
        }
    }

    /// Check if Apple hardware decoding is available on this system.
    pub fn is_apple_available() -> bool {
        #[cfg(target_os = "macos")]
        {
            super::apple::AppleImageDecoder::is_available()
        }
        #[cfg(not(target_os = "macos"))]
        {
            false
        }
    }

    /// Get information about available GPU devices.
    pub fn gpu_device_info() -> Vec<GpuDeviceInfo> {
        #[cfg(target_os = "linux")]
        {
            super::gpu::GpuImageDecoder::device_info()
        }
        #[cfg(not(target_os = "linux"))]
        {
            Vec::new()
        }
    }
}

/// Decode statistics for monitoring.
///
/// Similar to `CompressionStats` in `roboflow-pipeline/gpu/factory.rs`.
#[derive(Debug, Clone, Default)]
pub struct DecodeStats {
    /// Number of images decoded
    pub images_decoded: u64,
    /// Total bytes processed (compressed input)
    pub total_input_bytes: u64,
    /// Total bytes output (RGB decoded)
    pub total_output_bytes: u64,
    /// Compression ratio (output / input)
    pub decompression_ratio: f64,
    /// Average throughput in MPixels/sec
    pub average_throughput_mpx_s: f64,
    /// Whether GPU was used
    pub gpu_used: bool,
}

impl DecodeStats {
    /// Calculate decompression ratio.
    pub fn calculate_ratio(input: u64, output: u64) -> f64 {
        if input == 0 {
            1.0
        } else {
            output as f64 / input as f64
        }
    }

    /// Record a decode operation.
    pub fn record_decode(&mut self, input_size: u64, output_size: u64, gpu_used: bool) {
        self.images_decoded += 1;
        self.total_input_bytes += input_size;
        self.total_output_bytes += output_size;

        if gpu_used {
            self.gpu_used = true;
        }

        self.decompression_ratio =
            Self::calculate_ratio(self.total_input_bytes, self.total_output_bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::image::backend::DecoderType;

    #[test]
    fn test_factory_cpu_backend() {
        let config = ImageDecoderConfig::new().with_backend(DecoderBackendType::Cpu);
        let mut factory = ImageDecoderFactory::new(&config);
        let decoder = factory.create_decoder().unwrap();
        assert_eq!(decoder.decoder_type(), DecoderType::Cpu);
    }

    #[test]
    fn test_factory_auto_backend() {
        let config = ImageDecoderConfig::new().with_backend(DecoderBackendType::Auto);
        let mut factory = ImageDecoderFactory::new(&config);
        let decoder = factory.get_decoder();
        assert!(decoder.is_available());
        assert_eq!(decoder.decoder_type(), DecoderType::Cpu); // Falls back to CPU
    }

    #[test]
    fn test_decode_stats() {
        let mut stats = DecodeStats::default();
        stats.record_decode(1000, 3000, false);

        assert_eq!(stats.images_decoded, 1);
        assert_eq!(stats.total_input_bytes, 1000);
        assert_eq!(stats.total_output_bytes, 3000);
        assert_eq!(stats.decompression_ratio, 3.0);
        assert!(!stats.gpu_used);
    }
}
