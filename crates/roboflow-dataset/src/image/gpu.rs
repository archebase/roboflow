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
//! # TODO
//!
//! - [ ] Implement nvJPEG integration with cudarc
//! - [ ] Add batch decoding optimization
//! - [ ] Implement CUDA pinned memory allocation
//! - [ ] Add GPU memory pooling for performance

use super::{
    ImageError, ImageFormat, Result,
    backend::{DecoderType, ImageDecoderBackend},
    memory::MemoryStrategy,
};

/// GPU decoder using NVIDIA nvJPEG library.
#[allow(dead_code)]
pub struct GpuImageDecoder {
    device_id: u32, // TODO: will be used for CUDA context initialization
    memory_strategy: MemoryStrategy, // TODO: will be used for CUDA pinned memory
                    // TODO: Add CUDA context and nvJPEG handle when cudarc is integrated
                    // cuda_ctx: cudarc::driver::CudaDevice,
                    // nvjpeg_handle: cudarc::nvjpeg::NvJpeg,
}

impl GpuImageDecoder {
    /// Try to create a new nvJPEG decoder.
    ///
    /// # TODO
    ///
    /// This is a stub implementation. Full implementation requires:
    /// - cudarc dependency in Cargo.toml
    /// - CUDA context initialization
    /// - nvJPEG handle creation
    pub fn try_new(_device_id: u32, _memory_strategy: MemoryStrategy) -> Result<Self> {
        #[cfg(all(feature = "gpu-decode", target_os = "linux"))]
        {
            // TODO: Implement CUDA/nvJPEG initialization
            // For now, return an error indicating not yet implemented
            Err(ImageError::GpuUnavailable(
                "GPU decoding not yet implemented. See TODO in image/gpu.rs".to_string(),
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
    pub fn is_available() -> bool {
        // TODO: Check for CUDA runtime and nvJPEG library
        false
    }

    /// Get information about available GPU devices.
    pub fn device_info() -> Vec<super::factory::GpuDeviceInfo> {
        // TODO: Query CUDA devices and return their info
        Vec::new()
    }
}

impl ImageDecoderBackend for GpuImageDecoder {
    fn decode(&self, data: &[u8], format: ImageFormat) -> Result<super::backend::DecodedImage> {
        match format {
            ImageFormat::Jpeg => {
                // TODO: Implement nvJPEG decoding
                tracing::warn!("GPU JPEG decoding not yet implemented, falling back to CPU");
                self.decode_cpu_fallback(data, format)
            }
            ImageFormat::Png => {
                // nvJPEG doesn't support PNG, must use CPU
                tracing::debug!("nvJPEG doesn't support PNG, using CPU decoder");
                self.decode_cpu_fallback(data, format)
            }
            ImageFormat::Rgb8 => {
                // Already RGB, just wrap it
                let pixel_count = data.len() / 3;
                let width = (pixel_count as f32).sqrt().round() as u32;
                let height = pixel_count as u32 / width.max(1);
                Ok(super::backend::DecodedImage::new(
                    width,
                    height,
                    data.to_vec(),
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
        // TODO: Implement nvJPEG batch decoding for maximum throughput
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
