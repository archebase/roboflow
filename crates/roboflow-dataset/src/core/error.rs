// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Unified error types for the roboflow-pipeline crate.
//!
//! This module provides a comprehensive error type hierarchy that can be used
//! across all dataset formats and pipeline components.

use std::path::PathBuf;
use thiserror::Error;

/// Result type alias for pipeline operations.
pub type Result<T> = std::result::Result<T, PipelineError>;

/// Unified error type for all pipeline operations.
///
/// This error type wraps format-specific errors and provides
/// additional context for debugging.
#[derive(Debug, Error)]
pub enum PipelineError {
    /// Dataset writer error.
    #[error("Dataset writer error: {0}")]
    Writer(#[from] DatasetWriterError),

    /// Format not supported or registered.
    #[error("Format not supported: {format}")]
    FormatNotSupported {
        /// Format name that was requested
        format: String,
    },

    /// Configuration error with structured context.
    #[error("Configuration error in {context}: {message}")]
    Config {
        /// Configuration context (e.g., field name)
        context: String,
        /// Error message
        message: String,
    },

    /// I/O error during operation.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Storage backend error with path context.
    #[error("Storage error at {path}: {message}")]
    Storage {
        /// Path where the error occurred
        path: PathBuf,
        /// Error message
        message: String,
    },

    /// Video encoding error (preserves original error).
    #[error("Video encoding error: {0}")]
    VideoEncoding(#[from] VideoError),

    /// Image processing error (preserves original error).
    #[error("Image processing error: {0}")]
    ImageProcessing(#[from] ImageDataError),

    /// Pipeline execution error.
    #[error("Pipeline error: {0}")]
    Pipeline(String),

    /// Episode management error.
    #[error("Episode error: {0}")]
    Episode(String),

    /// Invalid or malformed data.
    #[error("Invalid data: {0}")]
    InvalidData(String),

    /// Required resource not found.
    #[error("Resource not found: {resource_type} at {location}")]
    NotFound {
        /// Type of resource (e.g., "file", "topic")
        resource_type: String,
        /// Location where resource was expected
        location: String,
    },

    /// Operation not supported.
    #[error("Operation not supported: {operation}")]
    NotSupported {
        /// The unsupported operation
        operation: String,
    },

    /// Internal error (should not happen in normal operation).
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Error type for dataset writer operations.
///
/// This is a general error type that can be used by any dataset format.
/// Format-specific writers may add their own error variants.
#[derive(Debug, Error)]
pub enum DatasetWriterError {
    /// I/O error during write operation.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// HDF5 library error.
    /// Note: This variant will be enabled with the dataset-hdf5 feature.
    #[error("HDF5 error: {0}")]
    Hdf5(String),

    /// Parquet encoding error.
    #[error("Parquet error: {0}")]
    Parquet(String),

    /// Video/image encoding error.
    #[error("Encoding error: {0}")]
    Encoding(String),

    /// Invalid or malformed message data.
    #[error("Invalid message data: {0}")]
    InvalidData(String),

    /// Required channel/topic not found.
    #[error("Channel not found: {0}")]
    ChannelNotFound(String),

    /// Feature not mapped in configuration.
    #[error("Feature not mapped: {0}")]
    FeatureNotMapped(String),

    /// Writer was used before initialization.
    #[error("Writer not initialized")]
    NotInitialized,

    /// Writer was used after finalization.
    #[error("Writer already finalized")]
    AlreadyFinalized,

    /// Episode management error.
    #[error("Episode error: {0}")]
    Episode(String),

    /// Storage operation failed.
    #[error("Storage error at {path}: {message}")]
    Storage {
        /// Path where the error occurred.
        path: PathBuf,
        /// Error message.
        message: String,
    },

    /// Format-specific error.
    #[error("{format} error: {message}")]
    Format {
        /// Format name (e.g., "LeRobot", "HDF5").
        format: &'static str,
        /// Error message.
        message: String,
    },
}

/// Error type for image data operations.
#[derive(Debug, Error)]
pub enum ImageDataError {
    /// Image dimensions are invalid.
    #[error("Invalid image dimensions: {width}x{height}")]
    InvalidDimensions {
        /// Image width.
        width: usize,
        /// Image height.
        height: usize,
    },

    /// Data size doesn't match expected size.
    #[error("Data size mismatch: expected {expected}, got {actual}")]
    SizeMismatch {
        /// Expected size in bytes.
        expected: usize,
        /// Actual size in bytes.
        actual: usize,
    },

    /// Invalid pixel format.
    #[error("Invalid pixel format: {0}")]
    InvalidPixelFormat(String),

    /// Depth image specific error.
    #[error("Depth image error: {0}")]
    Depth(String),
}

/// Error type for video operations.
#[derive(Debug, Error)]
pub enum VideoError {
    /// Encoder initialization failed.
    #[error("Failed to initialize encoder: {0}")]
    EncoderInit(String),

    /// Encoding failed.
    #[error("Encoding failed: {0}")]
    EncodingFailed(String),

    /// Invalid video configuration.
    #[error("Invalid video configuration: {0}")]
    InvalidConfig(String),

    /// Frame processing error.
    #[error("Frame error: {0}")]
    FrameError(String),

    /// Upload failed.
    #[error("Upload failed: {0}")]
    UploadFailed(String),

    /// Fragment error.
    #[error("Fragment error: {0}")]
    FragmentError(String),
}

// Note: DatasetWriterError, ImageDataError, and VideoError -> PipelineError conversions
// are provided by the #[from] attribute on their respective variants.
