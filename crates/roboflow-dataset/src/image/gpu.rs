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
//! # Implementation Status
//!
//! GPU decoding is a planned enhancement. The stub implementation provides:
//! - Type definitions for future integration with cudarc crate
//! - Interface compatibility with existing decoder traits
//! - Clear error messages when GPU decoding is attempted
//!
//! Full implementation will require:
//! - cudarc dependency integration
//! - CUDA context initialization
//! - nvJPEG handle creation and management
//! - Batch decoding optimization for multiple images

use super::{
    ImageError, ImageFormat, Result,
    backend::{DecoderType, ImageDecoderBackend},
    memory::MemoryStrategy,
};

/// GPU decoder using NVIDIA nvJPEG library.
#[allow(dead_code)]
pub struct GpuImageDecoder {
    device_id: u32, // For CUDA context initialization
    memory_strategy: MemoryStrategy, // For CUDA pinned memory allocation
                    // Future fields (when cudarc is integrated):
                    // cuda_ctx: cudarc::driver::CudaDevice,
                    // nvjpeg_handle: cudarc::nvjpeg::NvJpeg,
}

impl GpuImageDecoder {
    /// Try to create a new nvJPEG decoder.
    ///
    /// This is a stub implementation. Full GPU decoding requires:
    /// - cudarc dependency integration
    /// - CUDA context initialization
    /// - nvJPEG handle creation and management
    pub fn try_new(_device_id: u32, _memory_strategy: MemoryStrategy) -> Result<Self> {
        #[cfg(all(feature = "gpu-decode", target_os = "linux"))]
        {
            // GPU decoding is not yet implemented.
            // See module-level documentation for implementation plan.
            Err(ImageError::GpuUnavailable(
                "GPU decoding not yet implemented. See image::gpu module docs.".to_string(),
            ))
        }
        #[cfg(not(all(feature = "gpu-decode", target_os = "linux")))]
        {
            Err(ImageError::GpuUnavailable(
                "GPU decoding requires 'gpu-decode' feature on Linux".to_string(),
            ))
        }
    }

    /// Check if nvJPEG is available.
    ///
    /// Returns false until GPU decoding is fully implemented.
    pub fn is_available() -> bool {
        false
    }

    /// Get information about available GPU devices.
    ///
    /// Returns empty list until CUDA integration is complete.
    pub fn device_info() -> Vec<super::factory::GpuDeviceInfo> {
        Vec::new()
    }
}

impl ImageDecoderBackend for GpuImageDecoder {
    fn decode(&self, data: &[u8], format: ImageFormat) -> Result<super::backend::DecodedImage> {
        match format {
            ImageFormat::Jpeg => {
                // GPU JPEG decoding not yet implemented, fall back to CPU
                tracing::info!("GPU JPEG decoding not yet implemented, using CPU decoder");
                self.decode_cpu_fallback(data, format)
            }
            ImageFormat::Png => {
                // nvJPEG doesn't support PNG, must use CPU
                tracing::info!("nvJPEG doesn't support PNG, using CPU decoder");
                self.decode_cpu_fallback(data, format)
            }
            ImageFormat::Rgb8 => {
                // RGB8 format requires explicit dimensions from message metadata.
                // The sqrt() approach was incorrect for non-square images.
                Err(ImageError::InvalidData(
                    "RGB8 format requires explicit width/height from message metadata.".to_string(),
                ))
            }
            ImageFormat::Unknown => Err(ImageError::UnsupportedFormat(
                "Unknown format (cannot detect from magic bytes)".to_string(),
            )),
        }
    }

    fn decode_batch(
        &self,
        images: &[(&[u8], ImageFormat)],
    ) -> Result<Vec<super::backend::DecodedImage>> {
        // GPU batch decoding not yet implemented, use sequential processing
        tracing::debug!("GPU batch decoding not yet implemented, using sequential");
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

impl GpuImageDecoder {
    /// Fallback to CPU decoding for unsupported formats.
    fn decode_cpu_fallback(
        &self,
        data: &[u8],
        format: ImageFormat,
    ) -> Result<super::backend::DecodedImage> {
        use super::backend::CpuImageDecoder;

        let cpu_decoder = CpuImageDecoder::new(self.memory_strategy, 1);
        cpu_decoder.decode(data, format)
    }
}

#[cfg(all(
    feature = "gpu-decode",
    not(target_os = "linux"),
    not(all(
        target_os = "macos",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))
))]
pub use super::backend::CpuImageDecoder as GpuImageDecoder;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gpu_decoder_not_available() {
        assert!(!GpuImageDecoder::is_available());
        assert!(GpuImageDecoder::device_info().is_empty());
    }
}
