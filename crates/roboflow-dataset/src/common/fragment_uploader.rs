// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Upload commands for video fragment uploading.
//!
//! This module provides the `UploadCommand` enum used for communication
//! between camera pipelines and upload threads.

use crate::common::fragment_encoder::FragmentInfo;

/// Command for upload threads.
#[derive(Debug)]
pub enum UploadCommand {
    /// Upload a fragment.
    UploadFragment {
        camera: String,
        fragment: FragmentInfo,
    },
    /// Finish upload for a camera.
    Finish { camera: String },
    /// Abort all uploads.
    AbortAll,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_upload_command_debug() {
        let cmd = UploadCommand::Finish {
            camera: "cam0".to_string(),
        };
        assert!(format!("{:?}", cmd).contains("Finish"));
    }
}
