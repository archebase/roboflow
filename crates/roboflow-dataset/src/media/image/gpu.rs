// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! GPU-accelerated image decoding using NVIDIA nvJPEG.
//!
//! # Platform Support
//!
//! - Linux x86_64/aarch64 with CUDA toolkit
//! - Requires NVIDIA GPU with compute capability 6.0+
//! - Falls back to CPU decoder on error or for unsupported formats
//!
//! # Implementation
//!
//! GPU decoding uses cudarc for safe Rust bindings to CUDA:
//! - nvJPEG for JPEG decoding directly to GPU memory
//! - CUDA pinned memory for efficient CPU-GPU transfers
//! - Batch decoding for multiple images

#[cfg(target_os = "linux")]
use super::{
    ImageError, ImageFormat, Result,
    backend::{DecodedImage, DecoderType, ImageDecoderBackend},
    memory::MemoryStrategy,
};

/// GPU decoder using NVIDIA nvJPEG library (Linux only).
#[cfg(target_os = "linux")]
#[derive(Debug)]
pub struct GpuImageDecoder {
    device_id: u32,
    memory_strategy: MemoryStrategy,
    cuda_available: bool,
}

#[cfg(target_os = "linux")]
impl GpuImageDecoder {
    /// Try to create a new nvJPEG decoder.
    ///
    /// Returns error if CUDA device is not available or initialization fails.
    pub fn try_new(device_id: u32, memory_strategy: MemoryStrategy) -> Result<Self> {
        // CUDA pinned memory feature has been removed
        // GPU decoding is not available without the feature
        let cuda_available = false;

        Ok(Self {
            device_id,
            memory_strategy,
            cuda_available,
        })
    }

    /// Check if nvJPEG is available.
    pub fn is_available() -> bool {
        // CUDA pinned memory feature has been removed
        false
    }

    /// Get information about available GPU devices.
    pub fn device_info() -> Vec<super::factory::GpuDeviceInfo> {
        // CUDA pinned memory feature has been removed
        Vec::new()
    }
}

#[cfg(target_os = "linux")]
impl ImageDecoderBackend for GpuImageDecoder {
    fn decode(&self, data: &[u8], format: ImageFormat) -> Result<DecodedImage> {
        match format {
            ImageFormat::Jpeg => {
                // CUDA is not available, use CPU decoder
                tracing::debug!("CUDA not available, using CPU decoder for JPEG");
                self.decode_cpu_fallback(data, format)
            }
            ImageFormat::Png => {
                // nvJPEG doesn't support PNG, must use CPU
                tracing::debug!("nvJPEG doesn't support PNG, using CPU decoder");
                self.decode_cpu_fallback(data, format)
            }
            ImageFormat::Rgb8 => {
                // RGB8 format requires explicit dimensions from message metadata
                Err(ImageError::InvalidData(
                    "RGB8 format requires explicit width/height from message metadata.".to_string(),
                ))
            }
            ImageFormat::Bgr8 => {
                // BGR8 format requires explicit dimensions from message metadata
                Err(ImageError::InvalidData(
                    "BGR8 format requires explicit width/height from message metadata.".to_string(),
                ))
            }
            ImageFormat::Gray8 => {
                // Gray8 format requires explicit dimensions from message metadata
                Err(ImageError::InvalidData(
                    "Gray8 format requires explicit width/height from message metadata."
                        .to_string(),
                ))
            }
            ImageFormat::Unknown => Err(ImageError::UnsupportedFormat(
                "Unknown format (cannot detect from magic bytes)".to_string(),
            )),
        }
    }

    fn decode_batch(&self, images: &[(&[u8], ImageFormat)]) -> Result<Vec<DecodedImage>> {
        // Use sequential CPU decoding
        images
            .iter()
            .map(|(data, format)| self.decode(data, *format))
            .collect()
    }

    fn decoder_type(&self) -> DecoderType {
        DecoderType::Gpu
    }

    fn memory_strategy(&self) -> MemoryStrategy {
        self.memory_strategy
    }
}

#[cfg(target_os = "linux")]
impl GpuImageDecoder {
    /// Fallback to CPU decoding for unsupported formats.
    fn decode_cpu_fallback(&self, data: &[u8], format: ImageFormat) -> Result<DecodedImage> {
        use super::backend::CpuImageDecoder;

        let cpu_decoder = CpuImageDecoder::new(self.memory_strategy, 1);
        cpu_decoder.decode(data, format)
    }
}

#[cfg(not(target_os = "linux"))]
pub use super::backend::CpuImageDecoder as GpuImageDecoder;

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    #[test]
    fn test_gpu_decoder_creation() {
        let decoder = GpuImageDecoder::try_new(0, MemoryStrategy::Heap);
        assert!(decoder.is_ok());
    }

    #[test]
    fn test_gpu_device_info() {
        let devices = GpuImageDecoder::device_info();
        // Should return empty since CUDA feature was removed
        assert!(devices.is_empty());
    }
}
