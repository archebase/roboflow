// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Compression presets for the fluent pipeline API.
//!
//! Provides user-friendly compression level presets instead of raw ZSTD levels.

/// Compression preset for the pipeline.
///
/// Maps user-friendly names to ZSTD compression levels.
///
/// # Examples
///
/// ```no_run
/// use roboflow::pipeline::fluent::CompressionPreset;
///
/// let preset = CompressionPreset::Balanced; // Level 3
/// assert_eq!(preset.compression_level(), 3);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompressionPreset {
    /// Fast compression (ZSTD level 1).
    ///
    /// Best for:
    /// - Real-time processing
    /// - Large files where speed matters
    /// - Temporary conversions
    Fast,

    /// Balanced compression (ZSTD level 3).
    ///
    /// Best for:
    /// - General-purpose use
    /// - Good balance of speed and size
    /// - Most common scenarios
    #[default]
    Balanced,

    /// Slow compression (ZSTD level 9).
    ///
    /// Best for:
    /// - Archival storage
    /// - Network transfer where bandwidth is limited
    /// - Final deliverables
    Slow,
}

impl CompressionPreset {
    /// Get the ZSTD compression level for this preset.
    #[inline]
    pub fn compression_level(&self) -> i32 {
        match self {
            CompressionPreset::Fast => 1,
            CompressionPreset::Balanced => 3,
            CompressionPreset::Slow => 9,
        }
    }

    /// Get the default chunk size for this preset.
    ///
    /// Fast mode uses larger chunks to reduce compression overhead.
    /// Slow mode uses standard chunks for better seek performance.
    #[inline]
    pub fn default_chunk_size(&self) -> usize {
        match self {
            CompressionPreset::Fast => 32 * 1024 * 1024,     // 32MB
            CompressionPreset::Balanced => 16 * 1024 * 1024, // 16MB
            CompressionPreset::Slow => 16 * 1024 * 1024,     // 16MB
        }
    }
}

impl std::fmt::Display for CompressionPreset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompressionPreset::Fast => write!(f, "Fast (level 1)"),
            CompressionPreset::Balanced => write!(f, "Balanced (level 3)"),
            CompressionPreset::Slow => write!(f, "Slow (level 9)"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_levels() {
        assert_eq!(CompressionPreset::Fast.compression_level(), 1);
        assert_eq!(CompressionPreset::Balanced.compression_level(), 3);
        assert_eq!(CompressionPreset::Slow.compression_level(), 9);
    }

    #[test]
    fn test_chunk_sizes() {
        assert_eq!(
            CompressionPreset::Fast.default_chunk_size(),
            32 * 1024 * 1024
        );
        assert_eq!(
            CompressionPreset::Balanced.default_chunk_size(),
            16 * 1024 * 1024
        );
        assert_eq!(
            CompressionPreset::Slow.default_chunk_size(),
            16 * 1024 * 1024
        );
    }

    #[test]
    fn test_default() {
        assert_eq!(CompressionPreset::default(), CompressionPreset::Balanced);
    }
}
