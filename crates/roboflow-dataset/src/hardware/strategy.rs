// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Pipeline strategy selection.
//!
//! Selects the optimal processing strategy based on input format
//! and available hardware capabilities.

use crate::common::ImageFormat;
use crate::hardware::HardwareCapabilities;

/// Processing pipeline strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineStrategy {
    /// GPU zero-copy: CUDA decode → NVENC encode (NVIDIA Linux)
    GpuZeroCopy,
    /// Apple hybrid: ImageDecode → VideoToolbox (macOS)
    AppleHybrid,
    /// CPU optimized: JPEG passthrough + parallel decode
    CpuOptimized,
    /// Direct passthrough: Already encoded video
    Passthrough,
}

impl PipelineStrategy {
    /// Select the optimal strategy based on input format and hardware.
    pub fn select_optimal(input_format: ImageFormat) -> Self {
        let hw = HardwareCapabilities::get();

        // Passthrough for already-encoded formats
        if input_format == ImageFormat::Jpeg && hw.has_nvenc {
            // Can use GPU acceleration for JPEG → H.264
            if hw.has_cuda {
                return Self::GpuZeroCopy;
            }
        }

        // Platform-specific optimizations
        #[cfg(target_os = "macos")]
        {
            if hw.has_apple_video_toolbox {
                return Self::AppleHybrid;
            }
        }

        #[allow(unexpected_cfgs)]
        {
            if cfg!(all(target_os = "linux", feature = "gpu")) && hw.has_cuda && hw.has_nvenc {
                return Self::GpuZeroCopy;
            }
        }

        // Fallback to CPU-optimized path
        Self::CpuOptimized
    }

    /// Get a human-readable description of this strategy.
    pub fn description(&self) -> &'static str {
        match self {
            Self::GpuZeroCopy => "GPU zero-copy (CUDA decode → NVENC encode)",
            Self::AppleHybrid => "Apple hybrid (ImageDecode → VideoToolbox)",
            Self::CpuOptimized => "CPU optimized (parallel decode + JPEG passthrough)",
            Self::Passthrough => "Direct passthrough (no transcoding)",
        }
    }

    /// Check if this strategy uses GPU acceleration.
    pub fn uses_gpu(&self) -> bool {
        matches!(self, Self::GpuZeroCopy)
    }

    /// Check if this strategy uses hardware video encoding.
    pub fn uses_hw_encode(&self) -> bool {
        matches!(self, Self::GpuZeroCopy | Self::AppleHybrid)
    }
}

/// Strategy selection context with additional parameters.
pub struct StrategySelection {
    /// Selected strategy
    pub strategy: PipelineStrategy,
    /// Input format
    pub input_format: ImageFormat,
    /// Available hardware
    pub hardware: HardwareCapabilities,
}

impl StrategySelection {
    /// Create a new strategy selection.
    pub fn new(input_format: ImageFormat) -> Self {
        let strategy = PipelineStrategy::select_optimal(input_format);
        let hardware = *HardwareCapabilities::get();

        Self {
            strategy,
            input_format,
            hardware,
        }
    }

    /// Get a detailed description of the selection.
    pub fn description(&self) -> String {
        format!(
            "Strategy: {} | Input: {:?} | Hardware: {}",
            self.strategy.description(),
            self.input_format,
            self.hardware.description()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strategy_description() {
        assert!(!PipelineStrategy::GpuZeroCopy.description().is_empty());
        assert!(!PipelineStrategy::AppleHybrid.description().is_empty());
        assert!(!PipelineStrategy::CpuOptimized.description().is_empty());
        assert!(!PipelineStrategy::Passthrough.description().is_empty());
    }

    #[test]
    fn test_strategy_uses_gpu() {
        assert!(PipelineStrategy::GpuZeroCopy.uses_gpu());
        assert!(!PipelineStrategy::AppleHybrid.uses_gpu());
        assert!(!PipelineStrategy::CpuOptimized.uses_gpu());
        assert!(!PipelineStrategy::Passthrough.uses_gpu());
    }

    #[test]
    fn test_strategy_uses_hw_encode() {
        assert!(PipelineStrategy::GpuZeroCopy.uses_hw_encode());
        assert!(PipelineStrategy::AppleHybrid.uses_hw_encode());
        assert!(!PipelineStrategy::CpuOptimized.uses_hw_encode());
        assert!(!PipelineStrategy::Passthrough.uses_hw_encode());
    }

    #[test]
    fn test_select_optimal() {
        // Should always return a valid strategy
        let strategy = PipelineStrategy::select_optimal(ImageFormat::Jpeg);
        assert!(!matches!(strategy, PipelineStrategy::Passthrough));

        let strategy = PipelineStrategy::select_optimal(ImageFormat::RawRgb8);
        // Raw RGB will be handled by some strategy
        let _ = strategy;
    }

    #[test]
    fn test_strategy_selection_description() {
        let selection = StrategySelection::new(ImageFormat::Jpeg);
        let desc = selection.description();
        assert!(!desc.is_empty());
        assert!(desc.contains("Strategy:"));
    }
}
