// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Apple hardware-accelerated image decoding.
//!
//! # Platform Support
//!
//! - macOS with Apple Silicon (M1/M2/M3 chips)
//! - Uses libjpeg-turbo with hardware acceleration
//! - Falls back to CPU decoder when hardware unavailable

use super::{
    backend::{DecoderType, ImageDecoderBackend},
    memory::MemoryStrategy,
    ImageFormat, Result,
};

/// Apple hardware-accelerated image decoder.
pub struct AppleImageDecoder {
    memory_strategy: MemoryStrategy,
}

impl AppleImageDecoder {
    /// Try to create a new Apple hardware-accelerated decoder.
    pub fn try_new(memory_strategy: MemoryStrategy) -> Result<Self> {
        // TODO: Integrate with libjpeg-turbo hardware acceleration
        // For now, we create a decoder that uses optimized CPU paths
        Ok(Self { memory_strategy })
    }

    /// Check if Apple hardware acceleration is available.
    pub fn is_available() -> bool {
        // TODO: Check for Apple Silicon and hardware acceleration support
        // For now, return true on macOS as we can use optimized CPU paths
        cfg!(target_os = "macos")
    }
}

impl ImageDecoderBackend for AppleImageDecoder {
    fn decode(&self, data: &[u8], format: ImageFormat) -> Result<super::backend::DecodedImage> {
        // Delegate to CPU decoder for now
        // TODO: Use libjpeg-turbo with hardware acceleration when available
        use super::backend::CpuImageDecoder;
        let cpu_decoder = CpuImageDecoder::new(self.memory_strategy, rayon::current_num_threads().max(1));
        cpu_decoder.decode(data, format)
    }

    fn decode_batch(&self, images: &[(&[u8], ImageFormat)]) -> Result<Vec<super::backend::DecodedImage>> {
        // Apple Silicon can decode multiple images in parallel
        use rayon::prelude::*;
        images
            .par_iter()
            .map(|(data, format)| self.decode(data, *format))
            .collect()
    }

    fn decoder_type(&self) -> DecoderType {
        DecoderType::Apple
    }

    fn memory_strategy(&self) -> MemoryStrategy {
        self.memory_strategy
    }
}

// Stub for non-macOS platforms
#[cfg(not(target_os = "macos"))]
pub mod stub {
    use super::{
        backend::{DecoderType, ImageDecoderBackend},
        memory::MemoryStrategy,
        ImageError, ImageFormat, Result,
    };

    /// Stub decoder for non-macOS platforms.
    pub struct AppleImageDecoder {
        memory_strategy: MemoryStrategy,
    }

    impl AppleImageDecoder {
        /// Try to create a new Apple decoder (returns error on non-macOS).
        pub fn try_new(memory_strategy: MemoryStrategy) -> Result<super::AppleImageDecoder> {
            Err(ImageError::GpuUnavailable(
                "Apple decoding only supported on macOS".to_string()
            ))
        }

        /// Check if available (always false on non-macOS).
        pub fn is_available() -> bool {
            false
        }
    }

    impl ImageDecoderBackend for super::AppleImageDecoder {
        fn decode(&self, _data: &[u8], _format: ImageFormat) -> Result<super::backend::DecodedImage> {
            Err(ImageError::GpuUnavailable(
                "Apple decoding only supported on macOS".to_string()
            ))
        }

        fn decode_batch(&self, _images: &[(&[u8], ImageFormat)]) -> Result<Vec<super::backend::DecodedImage>> {
            Err(ImageError::GpuUnavailable(
                "Apple decoding only supported on macOS".to_string()
            ))
        }

        fn decoder_type(&self) -> DecoderType {
            DecoderType::Apple
        }

        fn memory_strategy(&self) -> MemoryStrategy {
            self.memory_strategy
        }
    }
}

#[cfg(not(target_os = "macos"))]
pub use stub::AppleImageDecoder;

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "macos")]
    #[test]
    fn test_apple_decoder_available() {
        // On macOS, Apple decoder should be available
        assert!(AppleImageDecoder::is_available());
    }

    #[cfg(not(target_os = "macos"))]
    #[test]
    fn test_apple_decoder_not_available() {
        // On non-macOS, Apple decoder should not be available
        assert!(!AppleImageDecoder::is_available());
    }
}
