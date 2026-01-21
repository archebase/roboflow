//! NVIDIA nvCOMP GPU compression backend.
//!
//! This module provides FFI bindings and a Rust wrapper around NVIDIA's
//! nvCOMP library for GPU-accelerated lossless compression.
//!
//! # Experimental
//!
//! This module is **experimental** and requires:
//! - NVIDIA GPU with compute capability 7.0+
//! - CUDA toolkit 11.0+
//! - nvCOMP library installed
//!
//! # Platform Support
//!
//! Currently only supported on:
//! - Linux x86_64
//! - Linux aarch64

pub mod sys;

use super::backend::{CompressedChunk, CompressorBackend, CompressorType, CpuCompressor};
use super::{GpuCompressionError, GpuResult};

/// nvCOMP compression backend.
///
/// Wraps NVIDIA's nvCOMP library for GPU-accelerated compression.
pub struct NvComCompressor {
    compression_level: u32,
    device_id: u32,
    max_chunk_size: usize,
    is_available: bool,
}

impl NvComCompressor {
    /// Try to create a new nvCOMP compressor.
    ///
    /// Returns an error if nvCOMP is not available or initialization fails.
    pub fn try_new(
        compression_level: u32,
        device_id: u32,
        max_chunk_size: usize,
    ) -> GpuResult<Self> {
        // Try to load and initialize nvCOMP
        let available = Self::check_nvcomp_available();

        if !available {
            return Err(GpuCompressionError::NvcompNotFound);
        }

        // Validate device
        Self::validate_device(device_id)?;

        Ok(Self {
            compression_level,
            device_id,
            max_chunk_size,
            is_available: true,
        })
    }

    /// Check if nvCOMP is available on the system.
    fn check_nvcomp_available() -> bool {
        // Try to dlopen nvcomp library
        // For now, we'll check for CUDA first
        Self::check_cuda_available()
    }

    /// Check if CUDA is available.
    fn check_cuda_available() -> bool {
        // Try to initialize CUDA
        // This is a simplified check - in production, use proper CUDA initialization
        false // Placeholder - CUDA not linked yet
    }

    /// Validate that the specified GPU device is available.
    fn validate_device(device_id: u32) -> GpuResult<()> {
        // Check device exists and has required capabilities
        // This would use CUDA calls in production
        if device_id > 16 {
            // Sanity check
            return Err(GpuCompressionError::DeviceNotFound);
        }
        Ok(())
    }

    /// Get information about available GPU devices.
    pub fn device_info() -> Vec<super::factory::GpuDeviceInfo> {
        // Query CUDA devices
        // This would use CUDA driver API in production
        Vec::new()
    }

    /// Check if nvCOMP is available.
    pub fn is_available() -> bool {
        Self::check_nvcomp_available()
    }
}

impl CompressorBackend for NvComCompressor {
    fn compress_chunk(
        &self,
        chunk: &super::backend::ChunkToCompress,
    ) -> GpuResult<CompressedChunk> {
        if !self.is_available {
            return Err(GpuCompressionError::CompressionFailed(
                "nvCOMP not available".to_string(),
            ));
        }

        // For now, fall back to CPU compression
        // In production, this would:
        // 1. Allocate GPU memory
        // 2. Copy data to GPU
        // 3. Launch nvCOMP compression kernel
        // 4. Copy compressed data back
        let cpu_compressor = CpuCompressor::new(self.compression_level, 1);
        cpu_compressor.compress_chunk(chunk)
    }

    fn compress_parallel(
        &self,
        chunks: &[super::backend::ChunkToCompress],
    ) -> GpuResult<Vec<CompressedChunk>> {
        if !self.is_available {
            return Err(GpuCompressionError::CompressionFailed(
                "nvCOMP not available".to_string(),
            ));
        }

        // For now, fall back to CPU parallel compression
        let cpu_compressor = CpuCompressor::new(self.compression_level, 8);
        cpu_compressor.compress_parallel(chunks)
    }

    fn compressor_type(&self) -> CompressorType {
        CompressorType::Gpu
    }

    fn compression_level(&self) -> u32 {
        self.compression_level
    }

    fn estimate_memory(&self, data_size: usize) -> usize {
        // nvCOMP uses GPU memory for compression
        // Estimate based on chunk size and compression algorithm
        // LZ4/ZSTD typically need 2-3x the data size
        data_size * 3
    }

    fn is_available(&self) -> bool {
        self.is_available
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nvcomp_unavailable() {
        // nvCOMP should not be available without CUDA
        assert!(!NvComCompressor::is_available());
    }

    #[test]
    fn test_try_new_fails_without_cuda() {
        let result = NvComCompressor::try_new(3, 0, 1024 * 1024);
        assert!(result.is_err());
    }
}
