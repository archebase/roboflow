// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Download utility for streaming input files from cloud storage.

use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use roboflow_storage::{Storage, StorageError};

/// Download a file from storage to local path with optional progress tracking.
///
/// This function streams the download in chunks to avoid loading the entire
/// file into memory. It's suitable for large files (multi-GB MCAP files).
///
/// # Arguments
///
/// * `storage` - Storage backend to download from
/// * `remote_path` - Path to the remote file
/// * `local_path` - Destination path for the downloaded file
/// * `progress` - Optional progress callback (bytes_downloaded, total_bytes)
///
/// # Returns
///
/// The total number of bytes downloaded.
///
/// # Errors
///
/// Returns `StorageError` if the download fails.
pub fn download_with_progress(
    storage: &dyn Storage,
    remote_path: &Path,
    local_path: &Path,
    progress: Option<&dyn Fn(u64, u64)>,
) -> Result<u64, StorageError> {
    // Get file size for progress tracking
    let total_bytes = storage.size(remote_path)?;

    // Open remote reader
    let mut reader = storage.reader(remote_path)?;

    // Create local file with buffered writer
    let file = std::fs::File::create(local_path).map_err(StorageError::Io)?;
    let mut writer = BufWriter::with_capacity(1024 * 1024, file); // 1MB buffer

    // Download in chunks
    const CHUNK_SIZE: usize = 1024 * 1024; // 1MB chunks
    let mut buffer = vec![0u8; CHUNK_SIZE];
    let mut bytes_downloaded = 0u64;

    loop {
        let bytes_read = reader.read(&mut buffer).map_err(StorageError::Io)?;
        if bytes_read == 0 {
            break;
        }

        writer
            .write_all(&buffer[..bytes_read])
            .map_err(StorageError::Io)?;
        bytes_downloaded += bytes_read as u64;

        // Report progress
        if let Some(callback) = progress {
            callback(bytes_downloaded, total_bytes);
        }
    }

    writer.flush().map_err(StorageError::Io)?;

    // Verify download size
    if bytes_downloaded != total_bytes {
        return Err(StorageError::Other(format!(
            "Download size mismatch: expected {} bytes, got {} bytes",
            total_bytes, bytes_downloaded
        )));
    }

    Ok(bytes_downloaded)
}

/// Download a file from storage to a local temporary file.
///
/// This is a convenience function that creates a temp file and returns its path.
///
/// # Arguments
///
/// * `storage` - Storage backend to download from
/// * `remote_path` - Path to the remote file
/// * `temp_dir` - Directory for the temp file
///
/// # Returns
///
/// The path to the downloaded temp file.
pub fn download_to_temp(
    storage: &dyn Storage,
    remote_path: &Path,
    temp_dir: &Path,
) -> Result<PathBuf, StorageError> {
    // Ensure temp directory exists
    std::fs::create_dir_all(temp_dir).map_err(StorageError::Io)?;

    // Create temp file with unique name
    let file_name = remote_path
        .file_name()
        .ok_or_else(|| StorageError::invalid_path(remote_path.display().to_string()))?;

    // Use a unique suffix to avoid conflicts
    let unique_name = format!(
        "{}_{}",
        uuid::Uuid::new_v4().simple(),
        file_name.to_string_lossy()
    );
    let local_path = temp_dir.join(&unique_name);

    // Download
    download_with_progress(storage, remote_path, &local_path, None)?;

    Ok(local_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use roboflow_storage::LocalStorage;
    use std::fs;
    use std::io::Write;

    #[test]
    fn test_download_local_to_local() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = LocalStorage::new(temp_dir.path());

        // Create a test file
        let source_path = "test_source.txt";
        let test_content = b"Hello, World! This is a test file for download.";
        let mut writer = storage.writer(Path::new(source_path)).unwrap();
        writer.write_all(test_content).unwrap();
        writer.flush().unwrap();

        // Download to temp
        let download_dir = tempfile::tempdir().unwrap();
        let downloaded_path =
            download_to_temp(&storage, Path::new(source_path), download_dir.path()).unwrap();

        // Verify content
        let content = fs::read_to_string(&downloaded_path).unwrap();
        assert_eq!(content, String::from_utf8_lossy(test_content));

        // Cleanup
        storage.delete(Path::new(source_path)).unwrap();
    }

    #[test]
    fn test_download_with_progress() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = LocalStorage::new(temp_dir.path());

        // Create a test file
        let source_path = "test_progress.txt";
        let test_content = b"Progress test content";
        let mut writer = storage.writer(Path::new(source_path)).unwrap();
        writer.write_all(test_content).unwrap();
        writer.flush().unwrap();

        // Download with progress
        let download_dir = tempfile::tempdir().unwrap();
        let downloaded_path = download_dir.path().join("downloaded.txt");

        // Use std::sync::Mutex for thread-safe progress tracking
        let progress_calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let progress_calls_clone = progress_calls.clone();
        let result = download_with_progress(
            &storage,
            Path::new(source_path),
            &downloaded_path,
            Some(&move |downloaded, total| {
                progress_calls_clone
                    .lock()
                    .unwrap()
                    .push((downloaded, total));
            }),
        );

        assert!(result.is_ok());
        let progress_calls = progress_calls.lock().unwrap();
        assert!(!progress_calls.is_empty());

        // Verify final progress report
        let last_call = progress_calls.last().unwrap();
        assert_eq!(last_call.0, test_content.len() as u64);
        assert_eq!(last_call.1, test_content.len() as u64);

        // Cleanup
        storage.delete(Path::new(source_path)).unwrap();
    }
}
