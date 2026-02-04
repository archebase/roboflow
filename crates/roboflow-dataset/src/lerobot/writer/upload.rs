// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Cloud storage upload functionality for LeRobot datasets.

use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use roboflow_core::Result;

/// Upload a Parquet file to cloud storage.
pub fn upload_parquet_file(
    storage: &dyn roboflow_storage::Storage,
    local_path: &Path,
    output_prefix: &str,
) -> Result<()> {
    let filename = local_path
        .file_name()
        .ok_or_else(|| roboflow_core::RoboflowError::parse("Path", "Invalid file name"))?;

    let remote_path = if output_prefix.is_empty() {
        Path::new("data/chunk-000").join(filename)
    } else {
        Path::new(output_prefix)
            .join("data/chunk-000")
            .join(filename)
    };

    upload_file(storage, local_path, &remote_path)?;

    tracing::info!(
        local = %local_path.display(),
        remote = %remote_path.display(),
        "Uploaded Parquet file to cloud storage"
    );

    Ok(())
}

/// Upload a video file to cloud storage.
pub fn upload_video_file(
    storage: &dyn roboflow_storage::Storage,
    local_path: &Path,
    camera: &str,
    output_prefix: &str,
) -> Result<()> {
    let filename = local_path
        .file_name()
        .ok_or_else(|| roboflow_core::RoboflowError::parse("Path", "Invalid file name"))?;

    // camera key already contains the full feature path
    let remote_path = if output_prefix.is_empty() {
        Path::new("videos/chunk-000").join(camera).join(filename)
    } else {
        Path::new(output_prefix)
            .join("videos/chunk-000")
            .join(camera)
            .join(filename)
    };

    upload_file(storage, local_path, &remote_path)?;

    tracing::info!(
        local = %local_path.display(),
        remote = %remote_path.display(),
        camera = %camera,
        "Uploaded video file to cloud storage"
    );

    Ok(())
}

/// Upload multiple video files to cloud storage in parallel.
pub fn upload_videos_parallel(
    storage: &dyn roboflow_storage::Storage,
    video_files: Vec<(PathBuf, String)>,
) -> Result<()> {
    use rayon::prelude::*;

    let results: Vec<Result<()>> = video_files
        .par_iter()
        .map(|(path, camera)| upload_video_file(storage, path, camera, ""))
        .collect();

    // Check for any errors
    for result in results {
        result?;
    }

    Ok(())
}

/// Generic file upload helper.
fn upload_file(
    storage: &dyn roboflow_storage::Storage,
    local_path: &Path,
    remote_path: &Path,
) -> Result<usize> {
    // Read local file
    let mut file = fs::File::open(local_path).map_err(|e| {
        roboflow_core::RoboflowError::encode(
            "Storage",
            format!("Failed to open local file: {}", e),
        )
    })?;

    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).map_err(|e| {
        roboflow_core::RoboflowError::encode(
            "Storage",
            format!("Failed to read local file: {}", e),
        )
    })?;

    let size = buffer.len();

    // Write to storage
    let mut writer = storage.writer(remote_path).map_err(|e| {
        roboflow_core::RoboflowError::encode(
            "Storage",
            format!("Failed to create storage writer: {}", e),
        )
    })?;

    writer.write_all(&buffer).map_err(|e| {
        roboflow_core::RoboflowError::encode(
            "Storage",
            format!("Failed to write to storage: {}", e),
        )
    })?;

    writer.flush().map_err(|e| {
        roboflow_core::RoboflowError::encode(
            "Storage",
            format!("Failed to flush to storage: {}", e),
        )
    })?;

    // Delete local file after successful upload
    if let Err(e) = fs::remove_file(local_path) {
        tracing::error!(
            path = %local_path.display(),
            error = %e,
            "Failed to delete local file after upload - disk space may leak"
        );
    } else {
        tracing::debug!(path = %local_path.display(), "Deleted local file after upload");
    }

    Ok(size)
}
