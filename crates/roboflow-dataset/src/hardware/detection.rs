// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Hardware capability detection.
//!
//! Detects available hardware acceleration features at runtime.

use std::sync::OnceLock;

/// Hardware capabilities detected at runtime.
#[derive(Debug, Clone, Copy)]
pub struct HardwareCapabilities {
    /// CUDA is available (NVIDIA GPU)
    pub has_cuda: bool,
    /// NVENC encoder is available (via ffmpeg)
    pub has_nvenc: bool,
    /// Apple VideoToolbox is available (macOS)
    pub has_apple_video_toolbox: bool,
    /// Intel Quick Sync Video is available (Linux/Windows)
    pub has_intel_qsv: bool,
    /// VAAPI is available (Linux)
    pub has_vaapi: bool,
    /// Number of CPU cores available
    pub cpu_cores: usize,
}

impl HardwareCapabilities {
    /// Detect hardware capabilities (cached).
    pub fn get() -> &'static Self {
        static CAPABILITIES: OnceLock<HardwareCapabilities> = OnceLock::new();
        CAPABILITIES.get_or_init(Self::detect)
    }

    /// Detect hardware capabilities.
    fn detect() -> Self {
        // Check for gpu feature (allow for future use)
        #[allow(unexpected_cfgs)]
        let has_gpu = cfg!(feature = "gpu");

        Self {
            has_cuda: has_gpu && Self::detect_cuda(),
            has_nvenc: has_gpu && Self::detect_nvenc(),
            has_apple_video_toolbox: cfg!(target_os = "macos"),
            has_intel_qsv: cfg!(any(target_os = "linux", target_os = "windows"))
                && Self::detect_qsv(),
            has_vaapi: cfg!(target_os = "linux") && Self::detect_vaapi(),
            cpu_cores: Self::detect_cpu_cores(),
        }
    }

    /// Check if NVIDIA GPU is available via nvidia-smi.
    fn detect_cuda() -> bool {
        std::process::Command::new("nvidia-smi")
            .arg("--query-gpu=name")
            .arg("--format=csv,noheader")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    /// Check if NVENC encoder is available via ffmpeg.
    fn detect_nvenc() -> bool {
        std::process::Command::new("ffmpeg")
            .args(["-hide_banner", "-encoders"])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .output()
            .map(|o| {
                let output = String::from_utf8_lossy(&o.stdout);
                output.contains("h264_nvenc") || output.contains("hevc_nvenc")
            })
            .unwrap_or(false)
    }

    /// Check if Intel QSV is available via ffmpeg.
    fn detect_qsv() -> bool {
        std::process::Command::new("ffmpeg")
            .args(["-hide_banner", "-encoders"])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .output()
            .map(|o| {
                let output = String::from_utf8_lossy(&o.stdout);
                output.contains("h264_qsv") || output.contains("hevc_qsv")
            })
            .unwrap_or(false)
    }

    /// Check if VAAPI is available via ffmpeg.
    fn detect_vaapi() -> bool {
        std::process::Command::new("ffmpeg")
            .args(["-hide_banner", "-encoders"])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .output()
            .map(|o| {
                let output = String::from_utf8_lossy(&o.stdout);
                output.contains("h264_vaapi") || output.contains("hevc_vaapi")
            })
            .unwrap_or(false)
    }

    /// Detect the number of available CPU cores.
    fn detect_cpu_cores() -> usize {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
    }

    /// Get a human-readable description of available hardware.
    pub fn description(&self) -> String {
        let mut parts = Vec::new();

        if self.has_cuda {
            parts.push("CUDA".to_string());
        }
        if self.has_nvenc {
            parts.push("NVENC".to_string());
        }
        if self.has_apple_video_toolbox {
            parts.push("VideoToolbox".to_string());
        }
        if self.has_intel_qsv {
            parts.push("QSV".to_string());
        }
        if self.has_vaapi {
            parts.push("VAAPI".to_string());
        }

        if parts.is_empty() {
            format!("CPU only ({} cores)", self.cpu_cores)
        } else {
            format!(
                "{} + CPU ({} cores)",
                parts.join(" + "),
                self.cpu_cores
            )
        }
    }

    /// Check if any hardware acceleration is available.
    pub fn has_hw_acceleration(&self) -> bool {
        self.has_cuda
            || self.has_nvenc
            || self.has_apple_video_toolbox
            || self.has_intel_qsv
            || self.has_vaapi
    }
}

impl Default for HardwareCapabilities {
    fn default() -> Self {
        *Self::get()
    }
}

/// Detect hardware capabilities.
pub fn detect_hardware() -> HardwareCapabilities {
    *HardwareCapabilities::get()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_cpu_cores() {
        let cores = HardwareCapabilities::get().cpu_cores;
        assert!(cores > 0);
        assert!(cores <= 256); // Reasonable upper bound
    }

    #[test]
    fn test_hardware_capabilities_description() {
        let hw = HardwareCapabilities::get();
        let desc = hw.description();
        assert!(!desc.is_empty());
        assert!(desc.contains("CPU"));
    }

    #[test]
    fn test_has_hw_acceleration() {
        let hw = HardwareCapabilities::get();
        // This should always return a valid bool
        let _ = hw.has_hw_acceleration();
    }
}
