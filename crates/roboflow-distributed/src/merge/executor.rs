// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Parquet merge executor for distributed dataset conversion.
//!
//! Handles the actual merging of staged parquet files from multiple workers
//! into a single sequential LeRobot dataset.

use super::schema::MergeState;
use crate::tikv::error::TikvError;
use polars::prelude::*;
use roboflow_storage::{Storage, StorageFactory, StorageUrl};
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Factory for creating storage backends from URLs.
///
/// This trait allows dependency injection for testing.
pub trait StorageFactoryTrait: Send + Sync {
    /// Create a storage backend for the given URL.
    fn create(&self, url: &str) -> roboflow_storage::StorageResult<Arc<dyn Storage>>;
}

/// Default implementation using StorageFactory.
pub struct DefaultStorageFactory;

impl StorageFactoryTrait for DefaultStorageFactory {
    fn create(&self, url: &str) -> roboflow_storage::StorageResult<Arc<dyn Storage>> {
        let factory = StorageFactory::new();
        factory.create(url)
    }
}

/// Parquet merge executor.
///
/// Reads staged parquet files from multiple workers and merges them
/// with sequential episode_index values.
pub struct ParquetMergeExecutor {
    /// Storage backend for reading/writing files.
    storage: Arc<dyn Storage>,

    /// Output path for the merged dataset.
    output_path: String,

    /// Temporary directory for merge operations.
    temp_dir: PathBuf,

    /// Factory for creating storage backends from URLs.
    storage_factory: Box<dyn StorageFactoryTrait>,
}

impl ParquetMergeExecutor {
    /// Create a new merge executor.
    pub fn new(storage: Arc<dyn Storage>, output_path: String, temp_dir: PathBuf) -> Self {
        Self {
            storage,
            output_path,
            temp_dir,
            storage_factory: Box::new(DefaultStorageFactory),
        }
    }

    /// Create a new merge executor with a custom storage factory (for testing).
    #[cfg(test)]
    pub fn with_factory(
        storage: Arc<dyn Storage>,
        output_path: String,
        temp_dir: PathBuf,
        factory: Box<dyn StorageFactoryTrait>,
    ) -> Self {
        Self {
            storage,
            output_path,
            temp_dir,
            storage_factory: factory,
        }
    }

    /// Execute the merge operation.
    ///
    /// # Arguments
    /// * `state` - The merge state containing staging paths from all workers
    ///
    /// # Returns
    /// The total number of frames merged
    pub async fn execute(&self, state: &MergeState) -> Result<u64, TikvError> {
        info!(
            job_id = %state.job_id,
            workers = state.completed_workers,
            total_frames = state.total_frames,
            "Starting parquet merge"
        );

        // Step 1: Discover all parquet files from staging paths
        let parquet_files = self.discover_parquet_files(state).await?;

        if parquet_files.is_empty() {
            warn!("No parquet files found in staging paths");
            return Ok(0);
        }

        info!(
            files_found = parquet_files.len(),
            "Discovered parquet files for merging"
        );

        // Step 2: Read and collect all dataframes with sequential episode_index
        let merged_df = self.merge_parquet_files(&parquet_files).await?;

        let total_frames = merged_df.height() as u64;

        // Step 3: Write merged parquet to output path
        self.write_merged_parquet(&merged_df).await?;

        // Step 4: Update video paths to point to merged location
        // (This is handled by the path references in the parquet file itself)

        info!(
            job_id = %state.job_id,
            total_frames,
            output_path = %self.output_path,
            "Parquet merge completed successfully"
        );

        Ok(total_frames)
    }

    /// Discover all parquet files in the staging paths.
    ///
    /// Supports both local filesystem and cloud storage (S3, etc.) via StorageUrl parsing.
    async fn discover_parquet_files(
        &self,
        state: &MergeState,
    ) -> Result<Vec<StagedParquetFile>, TikvError> {
        use tokio::task::spawn_blocking;

        let mut files = Vec::new();

        for (worker_id, staging_path) in &state.staging_paths {
            // Parse the staging path as a StorageUrl
            let staging_url: StorageUrl =
                staging_path
                    .parse()
                    .map_err(|e: roboflow_storage::StorageError| {
                        TikvError::Serialization(format!(
                            "Failed to parse staging path '{}': {}",
                            staging_path, e
                        ))
                    })?;

            // Create storage backend for this staging path using the factory
            let storage = self.storage_factory.create(staging_path).map_err(|e| {
                TikvError::Serialization(format!(
                    "Failed to create storage for '{}': {}",
                    staging_path, e
                ))
            })?;

            // Build the prefix for listing: staging_path/data/chunk-000/
            let list_prefix = format!("{}/data/chunk-000/", staging_url.path());
            let prefix_path = Path::new(&list_prefix);

            debug!(
                worker_id = %worker_id,
                staging_path = %staging_path,
                list_prefix = %list_prefix,
                "Searching for parquet files"
            );

            // List files with prefix using blocking I/O
            let storage_clone = Arc::clone(&storage);
            let prefix_path = prefix_path.to_path_buf();
            let worker_id = worker_id.clone();

            let worker_files: Vec<StagedParquetFile> = spawn_blocking(move || {
                let mut files = Vec::new();

                match storage_clone.list(&prefix_path) {
                    Ok(objects) => {
                        for obj in objects {
                            // Filter for .parquet extension
                            if obj.path.ends_with(".parquet") {
                                // Extract episode number from filename
                                let episode_num = extract_episode_number_from_str(&obj.path);

                                files.push(StagedParquetFile {
                                    path: obj.path.clone(),
                                    worker_id: worker_id.clone(),
                                    episode_index: episode_num,
                                });

                                debug!(
                                    worker_id = %worker_id,
                                    path = %obj.path,
                                    episode_index = episode_num,
                                    "Found parquet file"
                                );
                            }
                        }
                    }
                    Err(e) => {
                        warn!(
                            worker_id = %worker_id,
                            prefix = %prefix_path.display(),
                            error = %e,
                            "Failed to list files in staging path"
                        );
                    }
                }

                files
            })
            .await
            .map_err(|e| TikvError::Serialization(format!("Task join failed: {}", e)))?;

            files.extend(worker_files);
        }

        // Sort by episode_index to maintain order
        files.sort_by_key(|f| f.episode_index);

        info!(
            total_files = files.len(),
            "Discovered parquet files from all staging paths"
        );

        Ok(files)
    }

    /// Merge parquet files from all workers.
    async fn merge_parquet_files(
        &self,
        files: &[StagedParquetFile],
    ) -> Result<DataFrame, TikvError> {
        use tokio::task::spawn_blocking;

        let mut all_dataframes: Vec<DataFrame> = Vec::new();
        let mut current_episode_index: i64 = 0;
        let mut current_frame_index: i64 = 0;
        let mut current_global_index: i64 = 0;

        for file in files {
            debug!(
                worker_id = %file.worker_id,
                path = %file.path,
                episode_index = file.episode_index,
                "Reading parquet file"
            );

            // Read parquet file using blocking I/O in a separate thread
            let df = spawn_blocking({
                let path_str = file.path.clone();
                move || {
                    let path = Path::new(&path_str);
                    let lf = LazyFrame::scan_parquet(path, Default::default()).map_err(|e| {
                        TikvError::Serialization(format!(
                            "Failed to read parquet '{}': {}",
                            path_str, e
                        ))
                    })?;
                    lf.collect().map_err(|e| {
                        TikvError::Serialization(format!("Failed to collect dataframe: {}", e))
                    })
                }
            })
            .await
            .map_err(|e| TikvError::Serialization(format!("Task join failed: {}", e)))?
            .map_err(|e| TikvError::Serialization(format!("Failed to read parquet: {}", e)))?;

            if df.height() == 0 {
                continue;
            }

            let n_rows = df.height();

            // Create new index columns with sequential values
            let new_episode_index: Vec<i64> = (0..n_rows).map(|_| current_episode_index).collect();

            let new_frame_index: Vec<i64> = (0..n_rows)
                .map(|i| current_frame_index + i as i64)
                .collect();

            let new_index: Vec<i64> = (0..n_rows)
                .map(|i| current_global_index + i as i64)
                .collect();

            // Create modified dataframe with new index columns
            let mut df_modified = df.clone();

            // Replace index columns
            let _ = df_modified.replace(
                "episode_index",
                Series::new("episode_index", new_episode_index),
            );
            let _ = df_modified.replace("frame_index", Series::new("frame_index", new_frame_index));
            let _ = df_modified.replace("index", Series::new("index", new_index));

            all_dataframes.push(df_modified);

            // Increment for next episode
            current_episode_index += 1;
            current_frame_index += n_rows as i64;
            current_global_index += n_rows as i64;
        }

        if all_dataframes.is_empty() {
            return Err(TikvError::Serialization(
                "No dataframes to merge".to_string(),
            ));
        }

        // Concatenate all dataframes vertically using diagonal concat to handle missing columns
        let merged = spawn_blocking(move || {
            let mut result = all_dataframes[0].clone();
            for df in &all_dataframes[1..] {
                // Use hstack to combine dataframes, then use vstack for vertical concatenation
                // For diagonal concat, we need to handle column alignment
                result = polars::functions::concat_df_diagonal(&[result.clone(), df.clone()])
                    .map_err(|e| {
                        TikvError::Serialization(format!("Failed to concatenate dataframes: {}", e))
                    })?;
            }
            Ok::<DataFrame, TikvError>(result)
        })
        .await
        .map_err(|e| TikvError::Serialization(format!("Task join failed: {}", e)))?
        .map_err(|e| {
            TikvError::Serialization(format!("Failed to concatenate dataframes: {}", e))
        })?;

        debug!(
            total_frames = merged.height(),
            total_columns = merged.width(),
            "Merged all parquet files"
        );

        Ok(merged)
    }

    /// Write the merged parquet file to storage.
    async fn write_merged_parquet(&self, df: &DataFrame) -> Result<(), TikvError> {
        // Parse the output URL and extract the path/key portion
        let output_url: StorageUrl =
            self.output_path
                .parse()
                .map_err(|e: roboflow_storage::StorageError| {
                    TikvError::Serialization(format!("Failed to parse output path: {}", e))
                })?;

        // Get the path prefix (key for S3, path for local)
        let output_prefix = Path::new(output_url.path());
        let data_dir = output_prefix.join("data/chunk-000");

        // Create a unique merged parquet filename
        let output_filename = format!("merged_{}.parquet", uuid::Uuid::new_v4());
        let output_path = data_dir.join(&output_filename);

        // Write to local temp file first
        let local_path = self.temp_dir.join(&output_filename);

        {
            let file = std::fs::File::create(&local_path).map_err(|e| {
                TikvError::Serialization(format!("Failed to create temp file: {}", e))
            })?;
            let mut writer = BufWriter::new(file);

            ParquetWriter::new(&mut writer)
                .finish(&mut df.clone())
                .map_err(|e| TikvError::Serialization(format!("Failed to write parquet: {}", e)))?;
        }

        // Upload to storage if using cloud storage
        if output_url.is_remote() {
            let mut reader = std::fs::File::open(&local_path).map_err(|e| {
                TikvError::Serialization(format!("Failed to open temp file: {}", e))
            })?;

            let mut storage_writer = self.storage.writer(&output_path).map_err(|e| {
                TikvError::Serialization(format!("Failed to create storage writer: {}", e))
            })?;

            let mut buffer = Vec::new();
            std::io::copy(&mut reader, &mut buffer).map_err(|e| {
                TikvError::Serialization(format!("Failed to read temp file: {}", e))
            })?;

            use std::io::Write;
            storage_writer.write_all(&buffer).map_err(|e| {
                TikvError::Serialization(format!("Failed to write to storage: {}", e))
            })?;

            storage_writer.flush().map_err(|e| {
                TikvError::Serialization(format!("Failed to flush to storage: {}", e))
            })?;

            // Clean up temp file
            let _ = std::fs::remove_file(&local_path);

            info!(
                path = %output_path.display(),
                size = buffer.len(),
                "Wrote merged parquet to storage"
            );
        } else {
            // Move to final local location
            if let Some(parent) = output_path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    TikvError::Serialization(format!("Failed to create output directory: {}", e))
                })?;
            }

            std::fs::rename(&local_path, &output_path).map_err(|e| {
                TikvError::Serialization(format!("Failed to move parquet file: {}", e))
            })?;

            info!(
                path = %output_path.display(),
                "Wrote merged parquet file"
            );
        }

        Ok(())
    }
}

/// Represents a staged parquet file from a worker.
#[derive(Debug, Clone)]
struct StagedParquetFile {
    /// Path to the parquet file (can be local path or S3 key).
    path: String,

    /// Worker ID that created this file.
    worker_id: String,

    /// Episode index from the worker.
    episode_index: i64,
}

/// Extract episode number from a parquet filename string.
/// Handles patterns like "episode_000123.parquet" or "episode_123.parquet".
fn extract_episode_number_from_str(path_str: &str) -> i64 {
    // Get the filename from the path (handle both "/" and "\" separators)
    let filename = path_str
        .rfind(['/', '\\'])
        .map_or(path_str, |idx| &path_str[idx + 1..]);

    // Extract number from episode_NNNNNN.parquet
    if let Some(rest) = filename.strip_prefix("episode_")
        && let Some(num_str) = rest.strip_suffix(".parquet")
    {
        return num_str.parse().unwrap_or(0);
    }

    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_episode_number() {
        assert_eq!(
            extract_episode_number_from_str("episode_000123.parquet"),
            123
        );
        assert_eq!(extract_episode_number_from_str("episode_0.parquet"), 0);
        assert_eq!(extract_episode_number_from_str("episode_42.parquet"), 42);
        assert_eq!(extract_episode_number_from_str("invalid.parquet"), 0);
    }

    #[test]
    fn test_extract_episode_number_large() {
        assert_eq!(
            extract_episode_number_from_str("episode_999999.parquet"),
            999999
        );
    }

    #[test]
    fn test_extract_episode_number_with_path() {
        assert_eq!(
            extract_episode_number_from_str("/some/path/episode_00123.parquet"),
            123
        );
        assert_eq!(
            extract_episode_number_from_str("relative/path/episode_42.parquet"),
            42
        );
    }

    #[test]
    fn test_extract_episode_number_edge_cases() {
        // Missing prefix
        assert_eq!(extract_episode_number_from_str("000123.parquet"), 0);
        // Missing suffix
        assert_eq!(extract_episode_number_from_str("episode_123"), 0);
        // Empty path
        assert_eq!(extract_episode_number_from_str(""), 0);
    }

    #[test]
    fn test_staged_parquet_file_debug() {
        let file = StagedParquetFile {
            path: "/path/to/file.parquet".to_string(),
            worker_id: "worker-1".to_string(),
            episode_index: 42,
        };
        let debug_str = format!("{:?}", file);
        assert!(debug_str.contains("StagedParquetFile"));
        assert!(debug_str.contains("worker-1"));
    }

    #[test]
    fn test_staged_parquet_file_clone() {
        let file = StagedParquetFile {
            path: "/path/to/file.parquet".to_string(),
            worker_id: "worker-1".to_string(),
            episode_index: 42,
        };
        let cloned = file.clone();
        assert_eq!(file.path, cloned.path);
        assert_eq!(file.worker_id, cloned.worker_id);
        assert_eq!(file.episode_index, cloned.episode_index);
    }

    #[test]
    fn test_staged_parquet_file_sorting() {
        let mut files = [
            StagedParquetFile {
                path: "episode_3.parquet".to_string(),
                worker_id: "w1".to_string(),
                episode_index: 3,
            },
            StagedParquetFile {
                path: "episode_1.parquet".to_string(),
                worker_id: "w1".to_string(),
                episode_index: 1,
            },
            StagedParquetFile {
                path: "episode_2.parquet".to_string(),
                worker_id: "w2".to_string(),
                episode_index: 2,
            },
        ];

        files.sort_by_key(|f| f.episode_index);

        assert_eq!(files[0].episode_index, 1);
        assert_eq!(files[1].episode_index, 2);
        assert_eq!(files[2].episode_index, 3);
    }

    #[test]
    fn test_extract_episode_number_various_patterns() {
        // Standard patterns
        assert_eq!(extract_episode_number_from_str("episode_000000.parquet"), 0);
        assert_eq!(extract_episode_number_from_str("episode_000001.parquet"), 1);
        assert_eq!(
            extract_episode_number_from_str("episode_999999.parquet"),
            999999
        );

        // With different padding
        assert_eq!(extract_episode_number_from_str("episode_1.parquet"), 1);
        assert_eq!(extract_episode_number_from_str("episode_42.parquet"), 42);

        // With path prefix
        assert_eq!(
            extract_episode_number_from_str("/data/chunk-000/episode_00123.parquet"),
            123
        );
        assert_eq!(
            extract_episode_number_from_str("staging/worker-1/episode_456.parquet"),
            456
        );
    }

    #[test]
    fn test_extract_episode_number_invalid_patterns() {
        // Invalid patterns
        assert_eq!(extract_episode_number_from_str("data.parquet"), 0);
        assert_eq!(extract_episode_number_from_str("episode_.parquet"), 0);
        assert_eq!(extract_episode_number_from_str("episode_.txt"), 0);
        assert_eq!(extract_episode_number_from_str("episode_abc.parquet"), 0);

        // Mixed formats
        assert_eq!(extract_episode_number_from_str("my_episode_123.parquet"), 0);
        assert_eq!(extract_episode_number_from_str("episode-123.parquet"), 0);
    }

    #[test]
    fn test_parquet_merge_executor_new() {
        use roboflow_storage::LocalStorage;

        let temp_dir = std::env::temp_dir();
        let storage = Arc::new(LocalStorage::new(temp_dir.clone()));
        let executor =
            ParquetMergeExecutor::new(storage, "s3://bucket/output".to_string(), temp_dir);

        // Just verify we can create it
        let _ = executor;
    }

    #[test]
    fn test_parquet_merge_executor_local_output() {
        use roboflow_storage::LocalStorage;

        let temp_dir = std::env::temp_dir();
        let storage = Arc::new(LocalStorage::new(temp_dir.clone()));
        let executor =
            ParquetMergeExecutor::new(storage, "file:///output/dataset".to_string(), temp_dir);

        let _ = executor;
    }

    #[test]
    fn test_parquet_merge_executor_relative_output() {
        use roboflow_storage::LocalStorage;

        let temp_dir = std::env::temp_dir();
        let storage = Arc::new(LocalStorage::new(temp_dir.clone()));
        let executor = ParquetMergeExecutor::new(storage, "./output/dataset".to_string(), temp_dir);

        let _ = executor;
    }

    #[test]
    fn test_staged_parquet_file_equality() {
        let file1 = StagedParquetFile {
            path: "episode_1.parquet".to_string(),
            worker_id: "worker-1".to_string(),
            episode_index: 1,
        };

        let file2 = StagedParquetFile {
            path: "episode_1.parquet".to_string(),
            worker_id: "worker-1".to_string(),
            episode_index: 1,
        };

        // Both files should be equal in their fields
        assert_eq!(file1.path, file2.path);
        assert_eq!(file1.worker_id, file2.worker_id);
        assert_eq!(file1.episode_index, file2.episode_index);
    }

    #[test]
    fn test_staged_parquet_file_different_workers() {
        let file1 = StagedParquetFile {
            path: "episode_1.parquet".to_string(),
            worker_id: "worker-1".to_string(),
            episode_index: 1,
        };

        let file2 = StagedParquetFile {
            path: "episode_1.parquet".to_string(),
            worker_id: "worker-2".to_string(),
            episode_index: 1,
        };

        // Same episode_index and path but different workers
        assert_eq!(file1.episode_index, file2.episode_index);
        assert_ne!(file1.worker_id, file2.worker_id);
    }

    #[test]
    fn test_extract_episode_number_unicode() {
        // Test with non-ASCII characters - should return 0
        assert_eq!(extract_episode_number_from_str("episode_一二三.parquet"), 0);
    }

    #[test]
    fn test_extract_episode_number_negative() {
        // Negative numbers are parsed as-is (the function doesn't validate)
        let result = extract_episode_number_from_str("episode_-1.parquet");
        // The function will parse "-1" as a valid i64
        assert_eq!(result, -1);
    }

    #[test]
    fn test_extract_episode_number_overflow() {
        // Very large numbers
        let result = extract_episode_number_from_str("episode_999999999999.parquet");
        // Should parse or return 0 if it overflows
        assert!(result >= 0);
    }

    #[test]
    fn test_extract_episode_number_leading_zeros() {
        assert_eq!(
            extract_episode_number_from_str("episode_0000000001.parquet"),
            1
        );
        assert_eq!(
            extract_episode_number_from_str("episode_0000000000.parquet"),
            0
        );
    }

    #[test]
    fn test_extract_episode_number_case_sensitivity() {
        // Should be case sensitive - Episode vs episode
        assert_eq!(extract_episode_number_from_str("Episode_123.parquet"), 0);
        assert_eq!(extract_episode_number_from_str("episode_123.parquet"), 123);
    }

    #[test]
    fn test_staged_parquet_file_zero_episode() {
        let file = StagedParquetFile {
            path: "episode_0.parquet".to_string(),
            worker_id: "worker-1".to_string(),
            episode_index: 0,
        };

        assert_eq!(file.episode_index, 0);
    }

    #[test]
    fn test_staged_parquet_file_large_episode() {
        let file = StagedParquetFile {
            path: "episode_999999.parquet".to_string(),
            worker_id: "worker-1".to_string(),
            episode_index: 999999,
        };

        assert_eq!(file.episode_index, 999999);
    }

    #[test]
    fn test_extract_episode_number_with_query_string() {
        // URLs with query strings
        assert_eq!(
            extract_episode_number_from_str("episode_123.parquet?version=1"),
            0
        );
    }

    #[test]
    fn test_extract_episode_number_from_str() {
        // Basic patterns
        assert_eq!(
            extract_episode_number_from_str("episode_000123.parquet"),
            123
        );
        assert_eq!(extract_episode_number_from_str("episode_0.parquet"), 0);
        assert_eq!(extract_episode_number_from_str("episode_42.parquet"), 42);
        assert_eq!(extract_episode_number_from_str("invalid.parquet"), 0);

        // With path prefixes
        assert_eq!(
            extract_episode_number_from_str("/some/path/episode_00123.parquet"),
            123
        );
        assert_eq!(
            extract_episode_number_from_str("relative/path/episode_42.parquet"),
            42
        );
        assert_eq!(
            extract_episode_number_from_str(
                "s3://bucket/staging/job-001/data/chunk-000/episode_005.parquet"
            ),
            5
        );
    }

    #[test]
    fn test_extract_episode_number_from_str_edge_cases() {
        // Missing prefix
        assert_eq!(extract_episode_number_from_str("000123.parquet"), 0);
        // Missing suffix
        assert_eq!(extract_episode_number_from_str("episode_123"), 0);
        // Empty string
        assert_eq!(extract_episode_number_from_str(""), 0);
        // Just filename
        assert_eq!(extract_episode_number_from_str("episode_999.parquet"), 999);
    }

    // Mock storage factory for testing
    struct MockStorageFactory {
        storage: Arc<dyn Storage>,
    }

    impl MockStorageFactory {
        fn new(storage: Arc<dyn Storage>) -> Self {
            Self { storage }
        }
    }

    impl StorageFactoryTrait for MockStorageFactory {
        fn create(&self, _url: &str) -> roboflow_storage::StorageResult<Arc<dyn Storage>> {
            Ok(Arc::clone(&self.storage))
        }
    }

    #[tokio::test]
    async fn test_merge_with_s3_staging() {
        use roboflow_storage::mock::MockStorage;

        // Create a mock storage with pre-populated parquet files
        let mock_storage = Arc::new(MockStorage::with_data(vec![
            (
                "staging/job-001/worker-1/data/chunk-000/episode_001.parquet",
                b"FAKE_PARQUET_1",
            ),
            (
                "staging/job-001/worker-1/data/chunk-000/episode_002.parquet",
                b"FAKE_PARQUET_2",
            ),
            (
                "staging/job-001/worker-2/data/chunk-000/episode_001.parquet",
                b"FAKE_PARQUET_3",
            ),
            (
                "staging/job-001/worker-2/data/chunk-000/episode_003.parquet",
                b"FAKE_PARQUET_4",
            ),
        ]));

        // Create merge state with S3-style staging paths
        let mut state = MergeState::new("job-001".to_string(), 2, "s3://bucket/output".to_string());
        state.add_worker(
            "worker-1".to_string(),
            "s3://bucket/staging/job-001/worker-1".to_string(),
            100,
        );
        state.add_worker(
            "worker-2".to_string(),
            "s3://bucket/staging/job-001/worker-2".to_string(),
            150,
        );

        // Create executor with mock storage factory
        let temp_dir = std::env::temp_dir();
        let base_storage = Arc::new(MockStorage::new());
        let executor = ParquetMergeExecutor::with_factory(
            base_storage,
            "s3://bucket/output/dataset".to_string(),
            temp_dir,
            Box::new(MockStorageFactory::new(mock_storage)),
        );

        // Test discover_parquet_files
        let files = executor.discover_parquet_files(&state).await.unwrap();

        // Should find 4 files total (2 from each worker)
        assert_eq!(files.len(), 4, "Expected 4 parquet files");

        // Verify files are sorted by episode_index
        // Note: Files will be sorted by episode_index, so order depends on episode numbers
        let episode_indices: Vec<i64> = files.iter().map(|f| f.episode_index).collect();
        assert_eq!(
            episode_indices,
            vec![1, 1, 2, 3],
            "Files should be sorted by episode index"
        );

        // Verify worker assignments
        let worker1_files: Vec<&StagedParquetFile> =
            files.iter().filter(|f| f.worker_id == "worker-1").collect();
        let worker2_files: Vec<&StagedParquetFile> =
            files.iter().filter(|f| f.worker_id == "worker-2").collect();

        assert_eq!(worker1_files.len(), 2, "Worker 1 should have 2 files");
        assert_eq!(worker2_files.len(), 2, "Worker 2 should have 2 files");

        // Verify paths contain S3-style keys
        for file in &files {
            assert!(
                file.path.contains("staging/job-001/"),
                "Path should contain staging prefix: {}",
                file.path
            );
            assert!(
                file.path.ends_with(".parquet"),
                "Path should end with .parquet: {}",
                file.path
            );
        }
    }

    #[tokio::test]
    async fn test_merge_with_s3_staging_empty() {
        use roboflow_storage::mock::MockStorage;

        // Create empty mock storage
        let mock_storage = Arc::new(MockStorage::new());

        // Create merge state with S3-style staging paths
        let mut state = MergeState::new("job-002".to_string(), 1, "s3://bucket/output".to_string());
        state.add_worker(
            "worker-1".to_string(),
            "s3://bucket/staging/job-002/worker-1".to_string(),
            0,
        );

        // Create executor with mock storage factory
        let temp_dir = std::env::temp_dir();
        let base_storage = Arc::new(MockStorage::new());
        let executor = ParquetMergeExecutor::with_factory(
            base_storage,
            "s3://bucket/output/dataset".to_string(),
            temp_dir,
            Box::new(MockStorageFactory::new(mock_storage)),
        );

        // Test discover_parquet_files with empty storage
        let files = executor.discover_parquet_files(&state).await.unwrap();

        // Should find no files
        assert!(
            files.is_empty(),
            "Expected no parquet files in empty storage"
        );
    }

    #[tokio::test]
    async fn test_merge_with_s3_staging_mixed_extensions() {
        use roboflow_storage::mock::MockStorage;

        // Create mock storage with mixed file types
        let mock_storage = Arc::new(MockStorage::with_data(vec![
            (
                "staging/job-003/worker-1/data/chunk-000/episode_001.parquet",
                b"FAKE_PARQUET",
            ),
            (
                "staging/job-003/worker-1/data/chunk-000/episode_002.txt",
                b"NOT_PARQUET",
            ),
            (
                "staging/job-003/worker-1/data/chunk-000/episode_003.parquet",
                b"FAKE_PARQUET_2",
            ),
            (
                "staging/job-003/worker-1/data/chunk-000/metadata.json",
                b"{}",
            ),
        ]));

        // Create merge state
        let mut state = MergeState::new("job-003".to_string(), 1, "s3://bucket/output".to_string());
        state.add_worker(
            "worker-1".to_string(),
            "s3://bucket/staging/job-003/worker-1".to_string(),
            50,
        );

        // Create executor with mock storage factory
        let temp_dir = std::env::temp_dir();
        let base_storage = Arc::new(MockStorage::new());
        let executor = ParquetMergeExecutor::with_factory(
            base_storage,
            "s3://bucket/output/dataset".to_string(),
            temp_dir,
            Box::new(MockStorageFactory::new(mock_storage)),
        );

        // Test discover_parquet_files - should only find .parquet files
        let files = executor.discover_parquet_files(&state).await.unwrap();

        // Should find only 2 parquet files
        assert_eq!(
            files.len(),
            2,
            "Expected 2 parquet files (filtered by extension)"
        );

        // Verify only parquet files were found
        for file in &files {
            assert!(
                file.path.ends_with(".parquet"),
                "Only .parquet files should be found"
            );
        }

        // Verify episode indices
        let episode_indices: Vec<i64> = files.iter().map(|f| f.episode_index).collect();
        assert_eq!(episode_indices, vec![1, 3], "Should find episodes 1 and 3");
    }
}
