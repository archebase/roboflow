// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Format detection using magic numbers and file analysis.
//!
//! This module provides robust format detection that goes beyond
//! simple file extension checking. It uses magic numbers (file signatures)
//! to identify the actual format of robotics data files.
//!
//! # Supported Formats
//!
//! - **MCAP**: Identified by magic number at start or end of file
//! - **ROS1 Bag**: Identified by file header structure
//!
//! # Example
//!
//! ```rust,no_run
//! use robocodec::io::detection::detect_format;
//! use robocodec::io::metadata::FileFormat;
//!
//! let format = detect_format("data.mcap")?;
//! assert_eq!(format, FileFormat::Mcap);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use std::fs::File;
use std::io::Read;
use std::path::Path;

use crate::CodecError;

use super::metadata::FileFormat;

/// MCAP magic number placeholder.
///
/// MCAP files don't have a simple magic string - they have a structured header.
/// We detect MCAP by checking for the MCAP record structure.
/// Try to detect the file format from the file content.
///
/// This function reads the file header and checks for magic numbers
/// to identify the format, falling back to file extension if needed.
///
/// # Arguments
///
/// * `path` - Path to the file to analyze
///
/// # Returns
///
/// The detected format, or `FileFormat::Unknown` if the format cannot be determined.
///
/// # Example
///
/// ```rust,no_run
/// use robocodec::io::detection::detect_format;
/// use robocodec::io::metadata::FileFormat;
///
/// let format = detect_format("data.mcap")?;
/// match format {
///     FileFormat::Mcap => println!("MCAP file detected"),
///     FileFormat::Bag => println!("ROS1 bag file detected"),
///     FileFormat::Unknown => println!("Unknown format"),
/// }
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn detect_format<P: AsRef<Path>>(path: P) -> Result<FileFormat, CodecError> {
    let path_ref = path.as_ref();

    // First try magic number detection
    match detect_from_magic(path_ref) {
        Ok(FileFormat::Unknown) => {
            // Magic detection didn't find anything, fall back to extension
        }
        Ok(format) => return Ok(format),
        Err(_) => {
            // Error reading file, fall back to extension
        }
    }

    // Fall back to extension detection
    let format = detect_from_extension(path_ref);
    Ok(format)
}

/// Detect format by reading file magic numbers.
fn detect_from_magic(path: &Path) -> Result<FileFormat, CodecError> {
    let mut file = File::open(path)
        .map_err(|e| CodecError::encode("FormatDetection", format!("Failed to open file: {e}")))?;

    let mut header = [0u8; 1024];
    let n = file.read(&mut header).map_err(|e| {
        CodecError::encode("FormatDetection", format!("Failed to read header: {e}"))
    })?;

    if n < 8 {
        return Ok(FileFormat::Unknown);
    }

    // Check for MCAP magic number
    // MCAP files start with a magic number and end with one
    // The magic is not a simple string, so we check the MCAP record structure
    if is_mcap_magic(&header[..n]) {
        return Ok(FileFormat::Mcap);
    }

    // Check for ROS1 bag format
    // Bag files start with "#ROSBAH" (old format) or have a specific structure
    if is_rosbag_magic(&header[..n]) {
        return Ok(FileFormat::Bag);
    }

    Ok(FileFormat::Unknown)
}

/// Check if the header starts with MCAP magic.
fn is_mcap_magic(header: &[u8]) -> bool {
    if header.len() < 8 {
        return false;
    }

    // MCAP files start with the MCAP record magic
    // The magic is the bytes of "MCAP" followed by version info
    // We check for common patterns:
    // 1. MCAP little-endian magic (0x1C, 0xC1, 0x41, 0x50, 0x43, 0x41, 0x4D, 0x00...)
    //    This is "MCAP\x00" with some prefix bytes
    if header.len() >= 8 {
        // Check for MCAP signature (appears as "MCAP" starting at byte 4)
        if &header[4..8] == b"MCAP" {
            return true;
        }
        // Also check at the start (some variants)
        if &header[0..4] == b"MCAP" {
            return true;
        }
    }

    false
}

/// Check if the header starts with ROS1 bag magic.
fn is_rosbag_magic(header: &[u8]) -> bool {
    if header.len() < 8 {
        return false;
    }

    // Old format ROS1 bags start with "#ROSBAH"
    if header.starts_with(b"#ROSBAH") {
        return true;
    }

    // New format bags have a version string like "#ROSBAG"
    if header.starts_with(b"#ROSBAG") {
        return true;
    }

    // Check for bag file structure (starts with version record)
    // The bag format begins with a file header record
    if header.len() >= 13 {
        // Check for version string like "1.2" or "2.0"
        let header_str = std::str::from_utf8(&header[..header.len().min(100)]);
        if let Ok(s) = header_str {
            if s.starts_with("#ROSBAG") || s.contains("VERSION") {
                return true;
            }
        }
    }

    false
}

/// Detect format from file extension (fallback).
fn detect_from_extension(path: &Path) -> FileFormat {
    path.extension()
        .and_then(|e| e.to_str())
        .map(|ext| match ext.to_lowercase().as_str() {
            "mcap" => FileFormat::Mcap,
            "bag" => FileFormat::Bag,
            _ => FileFormat::Unknown,
        })
        .unwrap_or(FileFormat::Unknown)
}

/// Format detector with caching capabilities.
///
/// This trait can be implemented for custom format detection logic.
pub trait FormatDetector: Send + Sync {
    /// Detect the format of a file.
    fn detect(&self, path: &Path) -> Result<FileFormat, CodecError>;
}

/// Default format detector implementation.
#[derive(Debug, Clone, Copy)]
pub struct DefaultFormatDetector;

impl FormatDetector for DefaultFormatDetector {
    fn detect(&self, path: &Path) -> Result<FileFormat, CodecError> {
        detect_format(path)
    }
}

/// Check if a file is likely an MCAP file.
///
/// This is a convenience function that only checks for MCAP format.
pub fn is_mcap_file<P: AsRef<Path>>(path: P) -> bool {
    matches!(detect_format(path), Ok(FileFormat::Mcap))
}

/// Check if a file is likely a ROS1 bag file.
///
/// This is a convenience function that only checks for bag format.
pub fn is_bag_file<P: AsRef<Path>>(path: P) -> bool {
    matches!(detect_format(path), Ok(FileFormat::Bag))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;

    fn create_temp_file(name: &str, ext: &str, data: &[u8]) -> String {
        let path = format!(
            "/tmp/robocodec_test_detect_{}_{}.{}",
            std::process::id(),
            name,
            ext
        );
        {
            let mut temp_file = File::create(&path).unwrap();
            temp_file.write_all(data).unwrap();
            temp_file.flush().unwrap();
        }
        path
    }

    #[test]
    fn test_detect_from_extension_mcap() {
        let path = create_temp_file("ext_mcap", "mcap", b"dummy content");

        let format = detect_format(&path).unwrap();
        assert_eq!(format, FileFormat::Mcap);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_detect_from_extension_bag() {
        let path = create_temp_file("ext_bag", "bag", b"dummy content");

        let format = detect_format(&path).unwrap();
        assert_eq!(format, FileFormat::Bag);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_is_mcap_file() {
        let path = create_temp_file("is_mcap", "mcap", b"dummy content");

        assert!(is_mcap_file(&path));
        assert!(!is_bag_file(&path));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_is_bag_file() {
        let path = create_temp_file("is_bag", "bag", b"#ROSBAG");

        assert!(is_bag_file(&path));
        assert!(!is_mcap_file(&path));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_detect_from_magic_mcap() {
        let path = create_temp_file("magic_mcap", "bin", b"\x1C\xC1\x41\x50MCAP");

        let format = detect_from_magic(std::path::Path::new(&path)).unwrap();
        assert_eq!(format, FileFormat::Mcap);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_detect_from_magic_rosbag() {
        let path = create_temp_file("magic_bag", "bin", b"#ROSBAG V2.0");

        let format = detect_from_magic(std::path::Path::new(&path)).unwrap();
        assert_eq!(format, FileFormat::Bag);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_unknown_format() {
        let path = create_temp_file("unknown", "xyz", b"unknown content");

        let format = detect_format(&path).unwrap();
        assert_eq!(format, FileFormat::Unknown);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_format_detector_trait() {
        let detector = DefaultFormatDetector;
        let path = create_temp_file("detector", "mcap", b"dummy");

        let format = detector.detect(std::path::Path::new(&path)).unwrap();
        assert_eq!(format, FileFormat::Mcap);

        let _ = std::fs::remove_file(&path);
    }
}
