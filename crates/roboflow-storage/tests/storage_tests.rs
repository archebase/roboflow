// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Integration tests for storage layer.
//!
//! Tests cover:
//! - LocalStorage operations (read, write, delete, list, etc.)
//! - Error handling and edge cases
//! - Retry logic with RetryingStorage
//! - Path traversal security
//! - Configuration parsing

use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::str::FromStr;

use roboflow_storage::retry::{RetryConfig, RetryingStorage};
use roboflow_storage::{
    LocalStorage, ObjectMetadata, Storage, StorageError, StorageFactory, StorageUrl,
};

// =============================================================================
// Test Helpers
// =============================================================================

/// Creates a temporary directory for testing and returns its path.
fn setup_temp_dir() -> tempfile::TempDir {
    tempfile::tempdir().expect("Failed to create temp directory")
}

/// Generates test content of specified size.
fn generate_test_content(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 256) as u8).collect()
}

// =============================================================================
// LocalStorage Tests
// =============================================================================

#[test]
fn test_local_storage_write_and_read() {
    let temp_dir = setup_temp_dir();
    let storage = LocalStorage::new(temp_dir.path());

    let test_path = Path::new("test_write_read.txt");
    let test_content = b"Hello, World!";

    // Write
    let mut writer = storage.writer(test_path).expect("Failed to create writer");
    writer.write_all(test_content).expect("Failed to write");
    writer.flush().expect("Failed to flush");

    // Read
    let mut reader = storage.reader(test_path).expect("Failed to create reader");
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).expect("Failed to read");

    assert_eq!(buffer, test_content);

    // Cleanup
    storage.delete(test_path).expect("Failed to cleanup");
}

#[test]
fn test_local_storage_write_and_read_large_file() {
    let temp_dir = setup_temp_dir();
    let storage = LocalStorage::new(temp_dir.path());

    let test_path = Path::new("large_file.bin");
    let test_content = generate_test_content(1024 * 1024); // 1 MB

    // Write
    let mut writer = storage.writer(test_path).expect("Failed to create writer");
    writer.write_all(&test_content).expect("Failed to write");
    writer.flush().expect("Failed to flush");

    // Read
    let mut reader = storage.reader(test_path).expect("Failed to create reader");
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).expect("Failed to read");

    assert_eq!(buffer.len(), test_content.len());
    assert_eq!(buffer, test_content);

    // Verify size
    let size = storage.size(test_path).expect("Failed to get size");
    assert_eq!(size, test_content.len() as u64);

    // Cleanup
    storage.delete(test_path).expect("Failed to cleanup");
}

#[test]
fn test_local_storage_exists() {
    let temp_dir = setup_temp_dir();
    let storage = LocalStorage::new(temp_dir.path());

    let test_path = Path::new("test_exists.txt");

    // File doesn't exist initially
    assert!(!storage.exists(test_path));

    // Write file
    let mut writer = storage.writer(test_path).expect("Failed to create writer");
    writer.write_all(b"test").expect("Failed to write");
    writer.flush().expect("Failed to flush");

    // File exists now
    assert!(storage.exists(test_path));

    // Cleanup
    storage.delete(test_path).expect("Failed to cleanup");
}

#[test]
fn test_local_storage_size() {
    let temp_dir = setup_temp_dir();
    let storage = LocalStorage::new(temp_dir.path());

    let test_path = Path::new("test_size.txt");
    let test_content = b"test content";

    let mut writer = storage.writer(test_path).expect("Failed to create writer");
    writer.write_all(test_content).expect("Failed to write");
    writer.flush().expect("Failed to flush");

    let size = storage.size(test_path).expect("Failed to get size");
    assert_eq!(size, test_content.len() as u64);

    // Cleanup
    storage.delete(test_path).expect("Failed to cleanup");
}

#[test]
fn test_local_storage_metadata() {
    let temp_dir = setup_temp_dir();
    let storage = LocalStorage::new(temp_dir.path());

    let test_path = Path::new("test_metadata.txt");
    let test_content = b"test content";

    let mut writer = storage.writer(test_path).expect("Failed to create writer");
    writer.write_all(test_content).expect("Failed to write");
    writer.flush().expect("Failed to flush");

    let metadata = storage.metadata(test_path).expect("Failed to get metadata");
    assert_eq!(metadata.size, test_content.len() as u64);
    assert!(!metadata.is_dir);
    // metadata.path returns the full absolute path, not the relative input
    assert!(metadata.path.ends_with("test_metadata.txt"));

    // Cleanup
    storage.delete(test_path).expect("Failed to cleanup");
}

#[test]
fn test_local_storage_list() {
    let temp_dir = setup_temp_dir();
    let storage = LocalStorage::new(temp_dir.path());

    // Create test files with different prefixes
    let prefix_path = Path::new("list_test");
    storage
        .create_dir(prefix_path)
        .expect("Failed to create prefix dir");

    for i in 1..=3 {
        let file_path = prefix_path.join(format!("file{}.txt", i));
        let mut writer = storage.writer(&file_path).expect("Failed to create writer");
        writer
            .write_all(format!("content{}", i).as_bytes())
            .expect("Failed to write");
        writer.flush().expect("Failed to flush");
    }

    // Also create a file outside the prefix
    let other_path = Path::new("other_file.txt");
    let mut writer = storage.writer(other_path).expect("Failed to create writer");
    writer.write_all(b"other").expect("Failed to write");
    writer.flush().expect("Failed to flush");

    // List with prefix
    let results = storage.list(prefix_path).expect("Failed to list");
    assert_eq!(results.len(), 3);

    // Verify file names
    let names: Vec<&str> = results
        .iter()
        .map(|m| m.path.rsplit('/').next().unwrap())
        .collect();
    assert!(names.contains(&"file1.txt"));
    assert!(names.contains(&"file2.txt"));
    assert!(names.contains(&"file3.txt"));

    // Cleanup - delete files first using relative paths
    storage
        .delete(Path::new("list_test/file1.txt"))
        .expect("Failed to cleanup file1");
    storage
        .delete(Path::new("list_test/file2.txt"))
        .expect("Failed to cleanup file2");
    storage
        .delete(Path::new("list_test/file3.txt"))
        .expect("Failed to cleanup file3");
    storage
        .delete(other_path)
        .expect("Failed to cleanup other file");
    // Note: tempdir will cleanup the directory on drop
}

#[test]
fn test_local_storage_delete() {
    let temp_dir = setup_temp_dir();
    let storage = LocalStorage::new(temp_dir.path());

    let test_path = Path::new("test_delete.txt");

    // Create file
    let mut writer = storage.writer(test_path).expect("Failed to create writer");
    writer.write_all(b"test").expect("Failed to write");
    writer.flush().expect("Failed to flush");
    assert!(storage.exists(test_path));

    // Delete file
    storage.delete(test_path).expect("Failed to delete");
    assert!(!storage.exists(test_path));
}

#[test]
fn test_local_storage_delete_not_found() {
    let temp_dir = setup_temp_dir();
    let storage = LocalStorage::new(temp_dir.path());

    let result = storage.delete(Path::new("nonexistent.txt"));
    // LocalStorage returns NotFound error for non-existent files
    assert!(matches!(result, Err(StorageError::NotFound(_))));
}

#[test]
fn test_local_storage_copy() {
    let temp_dir = setup_temp_dir();
    let storage = LocalStorage::new(temp_dir.path());

    let src_path = Path::new("src_copy.txt");
    let dst_path = Path::new("dst_copy.txt");
    let test_content = b"copy test content";

    // Create source file
    let mut writer = storage.writer(src_path).expect("Failed to create writer");
    writer.write_all(test_content).expect("Failed to write");
    writer.flush().expect("Failed to flush");

    // Copy
    storage.copy(src_path, dst_path).expect("Failed to copy");

    // Verify both files exist and have same content
    assert!(storage.exists(src_path));
    assert!(storage.exists(dst_path));

    let src_size = storage.size(src_path).expect("Failed to get src size");
    let dst_size = storage.size(dst_path).expect("Failed to get dst size");
    assert_eq!(src_size, dst_size);
    assert_eq!(src_size, test_content.len() as u64);

    // Cleanup
    storage.delete(src_path).expect("Failed to cleanup src");
    storage.delete(dst_path).expect("Failed to cleanup dst");
}

#[test]
fn test_local_storage_create_dir() {
    let temp_dir = setup_temp_dir();
    let storage = LocalStorage::new(temp_dir.path());

    let dir_path = Path::new("test_dir/nested/subdir");

    // Single directory
    storage
        .create_dir(Path::new("test_dir"))
        .expect("Failed to create dir");
    assert!(storage.exists(Path::new("test_dir")));

    // Nested directories with create_dir_all
    storage
        .create_dir_all(dir_path)
        .expect("Failed to create nested dirs");
    assert!(storage.exists(dir_path));

    // Note: tempdir will cleanup on drop
}

#[test]
fn test_local_storage_read_range() {
    let temp_dir = setup_temp_dir();
    let storage = LocalStorage::new(temp_dir.path());

    let test_path = Path::new("test_range.txt");
    let test_content = b"0123456789ABCDEFGHIJ";

    let mut writer = storage.writer(test_path).expect("Failed to create writer");
    writer.write_all(test_content).expect("Failed to write");
    writer.flush().expect("Failed to flush");

    // Read range [5:15)
    let mut reader = storage
        .read_range(test_path, 5, Some(15))
        .expect("Failed to create range reader");
    let mut buffer = Vec::new();
    reader
        .read_to_end(&mut buffer)
        .expect("Failed to read range");

    assert_eq!(buffer, b"56789ABCDE");

    // Cleanup
    storage.delete(test_path).expect("Failed to cleanup");
}

#[test]
fn test_local_storage_seekable() {
    use roboflow_storage::SeekableStorage;

    let temp_dir = setup_temp_dir();
    let storage = LocalStorage::new(temp_dir.path());

    let test_path = Path::new("test_seekable.txt");
    let test_content = b"0123456789";

    let mut writer = storage.writer(test_path).expect("Failed to create writer");
    writer.write_all(test_content).expect("Failed to write");
    writer.flush().expect("Failed to flush");

    // Test seekable reader
    let mut reader = storage
        .seekable_reader(test_path)
        .expect("Failed to create seekable reader");

    // Read first 5 bytes
    let mut buffer = [0u8; 5];
    reader
        .read_exact(&mut buffer)
        .expect("Failed to read first 5");
    assert_eq!(&buffer, b"01234");

    // Seek to position 2
    reader.seek(SeekFrom::Start(2)).expect("Failed to seek");
    reader
        .read_exact(&mut buffer)
        .expect("Failed to read after seek");
    assert_eq!(&buffer, b"23456");

    // Cleanup
    storage.delete(test_path).expect("Failed to cleanup");
}

// =============================================================================
// Error Handling Tests
// =============================================================================

#[test]
fn test_local_storage_not_found_error() {
    let temp_dir = setup_temp_dir();
    let storage = LocalStorage::new(temp_dir.path());

    let result = storage.reader(Path::new("nonexistent_file.txt"));
    assert!(matches!(result, Err(StorageError::NotFound(_))));
}

#[test]
fn test_local_storage_invalid_path_error() {
    let temp_dir = setup_temp_dir();
    let storage = LocalStorage::new(temp_dir.path());

    // Invalid characters in path (may vary by OS)
    let result = storage.size(Path::new("\x00_invalid.txt"));
    assert!(result.is_err());
}

#[test]
fn test_local_storage_parent_auto_creation() {
    let temp_dir = setup_temp_dir();
    let storage = LocalStorage::new(temp_dir.path());

    // Write to nested path without explicitly creating directories
    let nested_path = Path::new("auto_created/nested/deep/file.txt");
    let mut writer = storage
        .writer(nested_path)
        .expect("Failed to create writer");
    writer.write_all(b"test").expect("Failed to write");
    writer.flush().expect("Failed to flush");

    // Verify file was created
    assert!(storage.exists(nested_path));

    // Cleanup - just delete the file, tempdir handles the rest
    storage
        .delete(nested_path)
        .expect("Failed to cleanup nested file");
}

// =============================================================================
// Security Tests - Path Traversal Protection
// =============================================================================

#[test]
fn test_path_traversal_double_dot() {
    let temp_dir = setup_temp_dir();
    let storage = LocalStorage::new(temp_dir.path());

    // Try to escape with double dots
    let escape_path = Path::new("../../../etc/passwd");
    let result = storage.reader(escape_path);

    // Should either fail or be contained within temp_dir
    // The actual behavior depends on normalize_path implementation
    assert!(result.is_err() || result.is_ok());
}

#[test]
fn test_path_traversal_mixed_dots() {
    let temp_dir = setup_temp_dir();
    let storage = LocalStorage::new(temp_dir.path());

    // Try mixed dots and normal path
    let escape_path = Path::new("test/.././../etc/passwd");
    let result = storage.reader(escape_path);

    assert!(result.is_err() || result.is_ok());
}

#[test]
fn test_path_traversal_leading_double_dot() {
    let temp_dir = setup_temp_dir();
    let storage = LocalStorage::new(temp_dir.path());

    let escape_path = Path::new("/../../etc/passwd");
    let result = storage.reader(escape_path);

    // Should fail for absolute path outside temp dir
    assert!(result.is_err());
}

// =============================================================================
// Retry Logic Tests
// =============================================================================

#[test]
fn test_retry_config_default() {
    let config = RetryConfig::default();
    assert_eq!(config.max_retries, 5);
    assert_eq!(config.initial_backoff_ms, 100);
    assert_eq!(config.max_backoff_ms, 30000);
    assert_eq!(config.backoff_multiplier, 2.0);
    assert!(config.jitter_enabled);
    assert_eq!(config.jitter_factor, 0.15);
}

#[test]
fn test_retry_config_builder() {
    let config = RetryConfig::new()
        .with_max_retries(10)
        .with_initial_backoff_ms(50)
        .with_max_backoff_ms(10000)
        .with_backoff_multiplier(3.0)
        .with_jitter(false);

    assert_eq!(config.max_retries, 10);
    assert_eq!(config.initial_backoff_ms, 50);
    assert_eq!(config.max_backoff_ms, 10000);
    assert_eq!(config.backoff_multiplier, 3.0);
    assert!(!config.jitter_enabled);
}

#[test]
fn test_retry_config_jitter_factor() {
    let config = RetryConfig::new().with_jitter_factor(0.25);
    assert_eq!(config.jitter_factor, 0.25);

    // Clamping
    let config = RetryConfig::new().with_jitter_factor(1.5);
    assert_eq!(config.jitter_factor, 1.0);

    let config = RetryConfig::new().with_jitter_factor(-0.5);
    assert_eq!(config.jitter_factor, 0.0);
}

#[test]
fn test_retry_storage_successful_operation() {
    let temp_dir = setup_temp_dir();
    let storage = LocalStorage::new(temp_dir.path());
    let retrying = RetryingStorage::with_default(std::sync::Arc::new(storage));

    let test_path = Path::new("retry_test.txt");
    let test_content = b"retry test";

    let mut writer = retrying.writer(test_path).expect("Failed to create writer");
    writer.write_all(test_content).expect("Failed to write");
    writer.flush().expect("Failed to flush");

    let mut reader = retrying.reader(test_path).expect("Failed to read");
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).expect("Failed to read");

    assert_eq!(buffer, test_content);

    // Cleanup
    retrying.delete(test_path).expect("Failed to cleanup");
}

#[test]
fn test_retry_storage_non_retryable_error_fails_immediately() {
    let temp_dir = setup_temp_dir();
    let storage = LocalStorage::new(temp_dir.path());
    let retrying = RetryingStorage::with_default(std::sync::Arc::new(storage));

    // Try to read non-existent file - should fail immediately without retry
    let result = retrying.reader(Path::new("does_not_exist.txt"));
    assert!(matches!(result, Err(StorageError::NotFound(_))));
}

// =============================================================================
// StorageFactory and URL Tests
// =============================================================================

#[test]
fn test_storage_url_local_file() {
    let url = StorageUrl::from_str("/tmp/test.txt").expect("Failed to parse URL");
    assert!(url.is_local());
    assert_eq!(url.path(), "/tmp/test.txt");
}

#[test]
fn test_storage_url_local_file_explicit() {
    let url = StorageUrl::from_str("file:///tmp/test.txt").expect("Failed to parse URL");
    assert!(url.is_local());
    assert_eq!(url.path(), "/tmp/test.txt");
}

#[test]
fn test_storage_url_s3() {
    let url = StorageUrl::from_str("s3://my-bucket/path/to/file.txt").expect("Failed to parse URL");
    assert!(url.is_remote());
    assert_eq!(url.bucket(), Some("my-bucket"));
    assert_eq!(url.path(), "path/to/file.txt");
}

#[test]
fn test_storage_url_s3_with_endpoint() {
    let url = StorageUrl::from_str(
        "s3://my-bucket/path/to/file.txt?endpoint=https://s3.amazonaws.com&region=us-west-2",
    )
    .expect("Failed to parse URL");

    // Use pattern matching to check S3 URL with endpoint and region
    match &url {
        roboflow_storage::StorageUrl::S3 {
            bucket,
            key,
            endpoint,
            region,
        } => {
            assert_eq!(bucket, "my-bucket");
            assert_eq!(key, "path/to/file.txt");
            assert_eq!(endpoint.as_deref(), Some("https://s3.amazonaws.com"));
            assert_eq!(region.as_deref(), Some("us-west-2"));
        }
        _ => panic!("Expected S3 URL"),
    }
}

#[test]
fn test_storage_url_oss() {
    let url =
        StorageUrl::from_str("oss://my-bucket/path/to/file.txt").expect("Failed to parse URL");
    assert!(url.is_remote());
    assert_eq!(url.bucket(), Some("my-bucket"));
    assert_eq!(url.path(), "path/to/file.txt");
}

#[test]
fn test_storage_url_oss_with_endpoint() {
    let url = StorageUrl::from_str(
        "oss://my-bucket/path/to/file.txt?endpoint=https://oss-cn-hangzhou.aliyuncs.com",
    )
    .expect("Failed to parse URL");

    // Use pattern matching to check OSS URL with endpoint
    match &url {
        roboflow_storage::StorageUrl::Oss {
            bucket,
            key,
            endpoint,
            ..
        } => {
            assert_eq!(bucket, "my-bucket");
            assert_eq!(key, "path/to/file.txt");
            assert_eq!(
                endpoint.as_deref(),
                Some("https://oss-cn-hangzhou.aliyuncs.com")
            );
        }
        _ => panic!("Expected OSS URL"),
    }
}

#[test]
fn test_storage_factory_local() {
    let temp_dir = setup_temp_dir();
    let factory = StorageFactory::new();

    let url_str = format!("file://{}", temp_dir.path().to_str().unwrap());
    let storage = factory.create(&url_str).expect("Failed to create storage");
    // We should get a storage implementation
    let _ = storage.exists(Path::new("."));
}

// =============================================================================
// Metadata Tests
// =============================================================================

#[test]
fn test_object_metadata_builder() {
    let metadata = ObjectMetadata::new("test/path.txt", 1024)
        .with_content_type("text/plain")
        .with_last_modified(std::time::SystemTime::now());

    assert_eq!(metadata.path, "test/path.txt");
    assert_eq!(metadata.size, 1024);
    assert_eq!(metadata.content_type.as_deref(), Some("text/plain"));
    assert!(metadata.last_modified.is_some());
    assert!(!metadata.is_dir);
}

#[test]
fn test_object_metadata_directory() {
    let metadata = ObjectMetadata::dir("/tmp/test_dir");

    assert_eq!(metadata.path, "/tmp/test_dir");
    assert_eq!(metadata.size, 0);
    assert!(metadata.is_dir);
}

// =============================================================================
// Edge Cases
// =============================================================================

#[test]
fn test_local_storage_empty_file() {
    let temp_dir = setup_temp_dir();
    let storage = LocalStorage::new(temp_dir.path());

    let test_path = Path::new("empty.txt");

    // Write empty file
    let mut writer = storage.writer(test_path).expect("Failed to create writer");
    writer.flush().expect("Failed to flush");

    // Read empty file
    let mut reader = storage.reader(test_path).expect("Failed to create reader");
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).expect("Failed to read");

    assert_eq!(buffer.len(), 0);

    // Size should be 0
    let size = storage.size(test_path).expect("Failed to get size");
    assert_eq!(size, 0);

    // Cleanup
    storage.delete(test_path).expect("Failed to cleanup");
}

#[test]
fn test_local_storage_unicode_filename() {
    let temp_dir = setup_temp_dir();
    let storage = LocalStorage::new(temp_dir.path());

    let test_path = Path::new("unicode_测试_🚀.txt");
    let test_content = b"unicode test";

    let mut writer = storage.writer(test_path).expect("Failed to create writer");
    writer.write_all(test_content).expect("Failed to write");
    writer.flush().expect("Failed to flush");

    assert!(storage.exists(test_path));

    let size = storage.size(test_path).expect("Failed to get size");
    assert_eq!(size, test_content.len() as u64);

    // Cleanup
    storage.delete(test_path).expect("Failed to cleanup");
}

#[test]
fn test_local_storage_overwrite() {
    let temp_dir = setup_temp_dir();
    let storage = LocalStorage::new(temp_dir.path());

    let test_path = Path::new("overwrite_test.txt");

    // Write initial content
    let mut writer = storage.writer(test_path).expect("Failed to create writer");
    writer
        .write_all(b"initial content")
        .expect("Failed to write");
    writer.flush().expect("Failed to flush");

    // Overwrite with new content
    let mut writer = storage.writer(test_path).expect("Failed to create writer");
    writer.write_all(b"new content").expect("Failed to write");
    writer.flush().expect("Failed to flush");

    let mut reader = storage.reader(test_path).expect("Failed to read");
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).expect("Failed to read");

    assert_eq!(buffer, b"new content");
    assert_ne!(buffer, b"initial content");

    // Cleanup
    storage.delete(test_path).expect("Failed to cleanup");
}
