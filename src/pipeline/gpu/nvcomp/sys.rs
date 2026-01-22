// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Raw FFI bindings to NVIDIA nvCOMP library.
//!
//! This module contains the low-level foreign function interface bindings
//! to the nvCOMP C library.
//!
//! # Experimental
//!
//! These bindings are **experimental** and may not cover all nvCOMP functionality.
//! They require the nvCOMP library to be installed on the system.

use std::ffi::{c_char, c_int, c_void};

/// nvCOMP compression algorithms supported.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub enum nvcompCompressionAlgorithm {
    /// No compression
    nvcompNoCompression = 0,
    /// LZ4 compression
    nvcompLZ4 = 1,
    /// Snappy compression
    nvcompSnappy = 2,
    /// ZSTD compression
    nvcompZSTD = 3,
    /// Deflate compression
    nvcompDeflate = 4,
}

/// nvCOMP status codes.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub enum nvcompStatus_t {
    /// Success
    nvcompSuccess = 0,
    /// Error
    nvcompErrorGeneric = 1,
    /// Error: Invalid parameter
    nvcompErrorInvalidParameter = 2,
    /// Error: Insufficient GPU memory
    nvcompErrorInsufficientGPU_MEMORY = 3,
    /// Error: CUDA error
    nvcompErrorCuda = 4,
    /// Error: Internal error
    nvcompErrorInternal = 5,
    /// Error: Not supported
    nvcompErrorNotSupported = 6,
}

/// nvCOMP compression configuration.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct nvcompCompressionConfig {
    /// Compression algorithm to use
    pub algorithm: nvcompCompressionAlgorithm,
    /// Compression level (algorithm-specific)
    pub level: c_int,
    /// Chunk size for compression
    pub chunk_size: usize,
    /// Reserved for future use
    _reserved: [usize; 8],
}

/// nvCOMP compressor handle (opaque).
#[repr(C)]
pub struct nvcompCompressor_t(c_void);

/// nvCOMP decompressor handle (opaque).
#[repr(C)]
pub struct nvcompDecompressor_t(c_void);

// External function declarations
//
// Note: These are placeholder declarations. In production, these would
// be generated using bindgen or manually maintained to match the
// nvCOMP C API.

extern "C" {
    /// Create a new compressor.
    ///
    /// # Arguments
    ///
    /// * `config` - Compression configuration
    /// * `compressor` - Output pointer to compressor handle
    ///
    /// # Returns
    ///
    /// nvcompStatus_t indicating success or failure
    pub fn nvcompCompressorCreate(
        config: *const nvcompCompressionConfig,
        compressor: *mut *mut nvcompCompressor_t,
    ) -> nvcompStatus_t;

    /// Destroy a compressor.
    ///
    /// # Arguments
    ///
    /// * `compressor` - Compressor handle to destroy
    pub fn nvcompCompressorDestroy(compressor: *mut nvcompCompressor_t);

    /// Compress data on GPU.
    ///
    /// # Arguments
    ///
    /// * `compressor` - Compressor handle
    /// * `input_ptr` - Pointer to input data on GPU
    /// * `input_size` - Size of input data in bytes
    /// * `output_ptr` - Pointer to output buffer on GPU
    /// * `output_size_ptr` - Pointer to output size, will be filled with actual size
    ///
    /// # Returns
    ///
    /// nvcompStatus_t indicating success or failure
    pub fn nvcompCompress(
        compressor: *mut nvcompCompressor_t,
        input_ptr: *const c_void,
        input_size: usize,
        output_ptr: *mut c_void,
        output_size_ptr: *mut usize,
    ) -> nvcompStatus_t;

    /// Get maximum compressed size for given input size.
    ///
    /// # Arguments
    ///
    /// * `compressor` - Compressor handle
    /// * `input_size` - Input data size in bytes
    /// * `max_compressed_size_ptr` - Output pointer to maximum compressed size
    ///
    /// # Returns
    ///
    /// nvcompStatus_t indicating success or failure
    pub fn nvcompGetMaxCompressedSize(
        compressor: *const nvcompCompressor_t,
        input_size: usize,
        max_compressed_size_ptr: *mut usize,
    ) -> nvcompStatus_t;

    /// Get last error message.
    ///
    /// # Returns
    ///
    /// Pointer to null-terminated error message string
    pub fn nvcompGetLastError() -> *const c_char;

    /// Initialize nvCOMP library.
    ///
    /// # Returns
    ///
    /// nvcompStatus_t indicating success or failure
    pub fn nvcompInit() -> nvcompStatus_t;

    /// Shutdown nvCOMP library.
    pub fn nvcompShutdown();
}

// Helper functions

/// Convert nvcompStatus_t to Result.
pub fn check_status(status: nvcompStatus_t) -> Result<(), nvcompStatus_t> {
    match status {
        nvcompStatus_t::nvcompSuccess => Ok(()),
        _ => Err(status),
    }
}

/// Get error message from status code.
pub fn status_to_message(status: nvcompStatus_t) -> &'static str {
    match status {
        nvcompStatus_t::nvcompSuccess => "Success",
        nvcompStatus_t::nvcompErrorGeneric => "Generic error",
        nvcompStatus_t::nvcompErrorInvalidParameter => "Invalid parameter",
        nvcompStatus_t::nvcompErrorInsufficientGPU_MEMORY => "Insufficient GPU memory",
        nvcompStatus_t::nvcompErrorCuda => "CUDA error",
        nvcompStatus_t::nvcompErrorInternal => "Internal error",
        nvcompStatus_t::nvcompErrorNotSupported => "Not supported",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_algorithm_values() {
        assert_eq!(nvcompCompressionAlgorithm::nvcompNoCompression as i32, 0);
        assert_eq!(nvcompCompressionAlgorithm::nvcompLZ4 as i32, 1);
        assert_eq!(nvcompCompressionAlgorithm::nvcompZSTD as i32, 3);
    }

    #[test]
    fn test_status_conversion() {
        assert!(check_status(nvcompStatus_t::nvcompSuccess).is_ok());
        assert!(check_status(nvcompStatus_t::nvcompErrorGeneric).is_err());
    }

    #[test]
    fn test_status_messages() {
        assert_eq!(status_to_message(nvcompStatus_t::nvcompSuccess), "Success");
        assert_eq!(
            status_to_message(nvcompStatus_t::nvcompErrorInsufficientGPU_MEMORY),
            "Insufficient GPU memory"
        );
    }
}
