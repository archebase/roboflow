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
use roboflow_dataset::formats::lerobot::config::LerobotConfig;
use roboflow_dataset::formats::lerobot::metadata::MetadataCollector;
use roboflow_storage::{Storage, StorageFactory, StorageUrl};
use std::collections::HashMap;
use std::io::{BufWriter, Read, Write};
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
        let factory = StorageFactory::from_env();
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
    /// * `config` - Optional LeRobot configuration for metadata generation
    ///
    /// # Returns
    /// The total number of frames merged
    pub async fn execute(
        &self,
        state: &MergeState,
        config: Option<&LerobotConfig>,
    ) -> Result<u64, TikvError> {
        info!(
            job_id = %state.job_id,
            workers = state.completed_workers,
            total_frames = state.total_frames,
            "Starting parquet merge"
        );

        // Step 1: Discover all parquet files from staging paths
        let parquet_files = self.discover_parquet_files(state).await?;

        if parquet_files.is_empty() {
            return Err(TikvError::Serialization(
                "No parquet files found in staging paths".to_string(),
            ));
        }

        info!(
            files_found = parquet_files.len(),
            "Discovered parquet files for merging"
        );

        // Step 2: Read and collect all dataframes with sequential episode_index.
        // Also compute staged media copy tasks with remapped episode paths.
        let (merged_df, media_copy_tasks) = self.merge_parquet_files(&parquet_files).await?;

        let total_frames = merged_df.height() as u64;

        // Step 3: Write merged parquet to output path
        self.write_merged_parquet(&merged_df).await?;

        // Step 4: Copy staged media files to final dataset output paths.
        self.copy_media_assets(&media_copy_tasks)?;

        // Step 5: Generate LeRobot v2.1 metadata files if config is provided
        if let Some(lerobot_config) = config {
            self.write_metadata(&merged_df, lerobot_config).await?;
        } else {
            warn!(
                job_id = %state.job_id,
                "No LeRobot config provided - skipping metadata generation"
            );
        }

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

            // Build the prefix for listing all staged parquet chunks.
            // Supports data/chunk-000, data/chunk-001, ...
            let list_prefix = format!("{}/data/", staging_url.path());
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

                // Storage::list is one-level listing for both local and S3 backends.
                // Traverse recursively to discover parquet files under data/chunk-*/.
                let mut dirs_to_visit = vec![prefix_path.clone()];

                while let Some(dir) = dirs_to_visit.pop() {
                    match storage_clone.list(&dir) {
                        Ok(objects) => {
                            for obj in objects {
                                if obj.is_dir {
                                    dirs_to_visit.push(PathBuf::from(&obj.path));
                                    continue;
                                }

                                if obj.path.ends_with(".parquet") {
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
                                prefix = %dir.display(),
                                error = %e,
                                "Failed to list files in staging path"
                            );
                        }
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
    ) -> Result<(DataFrame, Vec<MediaCopyTask>), TikvError> {
        use tokio::task::spawn_blocking;

        let mut all_dataframes: Vec<DataFrame> = Vec::new();
        let mut media_tasks: HashMap<String, MediaCopyTask> = HashMap::new();
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

            // Read parquet via storage backend and parse from bytes in a blocking thread.
            // This supports cloud paths (S3 keys) and local files uniformly.
            let df = spawn_blocking({
                let storage = Arc::clone(&self.storage);
                let path_str = file.path.clone();
                move || {
                    let mut reader = storage.reader(Path::new(&path_str)).map_err(|e| {
                        TikvError::Serialization(format!(
                            "Failed to read parquet '{}': {}",
                            path_str, e
                        ))
                    })?;

                    let mut parquet_bytes = Vec::new();
                    reader.read_to_end(&mut parquet_bytes).map_err(|e| {
                        TikvError::Serialization(format!(
                            "Failed to read parquet bytes '{}': {}",
                            path_str, e
                        ))
                    })?;

                    let cursor = std::io::Cursor::new(parquet_bytes);
                    ParquetReader::new(cursor).finish().map_err(|e| {
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
            let staging_prefix = staging_prefix_from_parquet_path(&file.path).ok_or_else(|| {
                TikvError::Serialization(format!(
                    "Invalid staged parquet path (missing /data/ segment): {}",
                    file.path
                ))
            })?;

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

            // Rewrite video path columns to sequential episode paths and collect
            // media copy tasks from worker staging to final output.
            let column_names: Vec<String> = df_modified
                .get_column_names()
                .into_iter()
                .map(|n| n.to_string())
                .collect();

            for col_name in column_names {
                if !col_name.ends_with("_path") {
                    continue;
                }

                let series = match df_modified.column(&col_name) {
                    Ok(s) => s.clone(),
                    Err(_) => continue,
                };

                let utf8 = match series.str() {
                    Ok(v) => v,
                    Err(_) => continue,
                };

                let remapped_values: Vec<Option<String>> = utf8
                    .into_iter()
                    .map(|opt| {
                        opt.map(|src_path| {
                            let dst_path =
                                remap_episode_video_path(src_path, current_episode_index);

                            let src_key = format!(
                                "{}/{}",
                                staging_prefix.trim_end_matches('/'),
                                src_path.trim_start_matches('/'),
                            );
                            let task_key = format!("{}|{}", src_key, dst_path);

                            media_tasks
                                .entry(task_key)
                                .or_insert_with(|| MediaCopyTask {
                                    source_key: src_key,
                                    dest_key: dst_path.clone(),
                                });

                            dst_path
                        })
                    })
                    .collect();

                let _ =
                    df_modified.replace(&col_name, Series::new(col_name.as_str(), remapped_values));
            }

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

        Ok((merged, media_tasks.into_values().collect()))
    }

    /// Copy staged media assets into final dataset output paths.
    fn copy_media_assets(&self, tasks: &[MediaCopyTask]) -> Result<(), TikvError> {
        let mut copied_files = 0usize;
        let output_prefix = self
            .output_path
            .parse::<StorageUrl>()
            .map_err(|e: roboflow_storage::StorageError| {
                TikvError::Serialization(format!("Failed to parse output path: {}", e))
            })?
            .path()
            .trim_start_matches('/')
            .to_string();

        for task in tasks {
            let mut reader = self
                .storage
                .reader(Path::new(&task.source_key))
                .map_err(|e| {
                    TikvError::Serialization(format!(
                        "Failed to read staged media '{}': {}",
                        task.source_key, e
                    ))
                })?;

            let destination_key = if output_prefix.is_empty() {
                task.dest_key.clone()
            } else {
                format!(
                    "{}/{}",
                    output_prefix.trim_end_matches('/'),
                    task.dest_key.trim_start_matches('/'),
                )
            };

            let mut writer = self
                .storage
                .writer(Path::new(&destination_key))
                .map_err(|e| {
                    TikvError::Serialization(format!(
                        "Failed to create merged media '{}': {}",
                        destination_key, e
                    ))
                })?;

            std::io::copy(&mut reader, &mut writer).map_err(|e| {
                TikvError::Serialization(format!(
                    "Failed to copy staged media '{}' to '{}': {}",
                    task.source_key, destination_key, e
                ))
            })?;
            writer.flush().map_err(|e| {
                TikvError::Serialization(format!(
                    "Failed to flush merged media '{}': {}",
                    destination_key, e
                ))
            })?;

            copied_files += 1;
        }

        info!(
            copied_files,
            "Copied staged media assets into merged dataset output"
        );
        Ok(())
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

    /// Write LeRobot v2.1 metadata files.
    ///
    /// Generates meta/info.json, meta/episodes.jsonl, and meta/episodes_stats.jsonl
    /// from the merged parquet data.
    async fn write_metadata(
        &self,
        merged_df: &DataFrame,
        config: &LerobotConfig,
    ) -> Result<(), TikvError> {
        info!("Generating LeRobot v2.1 metadata files");

        let mut collector = MetadataCollector::new();

        // Extract episode information from the merged DataFrame
        let episode_index_col = merged_df
            .column("episode_index")
            .map_err(|e| TikvError::Other(format!("Missing episode_index column: {}", e)))?;

        let episode_indices = episode_index_col
            .i64()
            .map_err(|e| TikvError::Other(format!("episode_index is not i64: {}", e)))?;

        // Group by episode_index to get frame counts per episode
        let mut episode_frame_counts: HashMap<i64, usize> = HashMap::new();
        for idx in episode_indices.into_iter().flatten() {
            *episode_frame_counts.entry(idx).or_insert(0) += 1;
        }

        // Sort episodes by index
        let mut sorted_episodes: Vec<_> = episode_frame_counts.into_iter().collect();
        sorted_episodes.sort_by_key(|(idx, _)| *idx);

        // Add episodes to collector
        for (episode_idx, frame_count) in sorted_episodes {
            collector.add_episode(episode_idx as usize, frame_count, vec![]);
        }

        // Extract feature dimensions from DataFrame schema
        for field in merged_df.schema().iter_fields() {
            let name = field.name();

            // Skip metadata columns
            if name == "episode_index"
                || name == "frame_index"
                || name == "timestamp"
                || name == "index"
            {
                continue;
            }

            // Video path columns (e.g., observation.images.cam_left_path)
            if name.ends_with("_path") {
                let feature_name = name.trim_end_matches("_path");
                // Assume standard video dimensions (will be overridden if we can extract from config)
                collector.update_image_shape(feature_name.to_string(), 640, 480);
            }
            // State columns (float arrays)
            else if matches!(field.data_type(), DataType::List(_)) {
                // Try to infer dimension from first non-null value
                if let Ok(col) = merged_df.column(name)
                    && let Ok(list_col) = col.list()
                    && let Some(series) = list_col.into_iter().flatten().next()
                {
                    let dim = series.len();
                    collector.update_state_dim(name.to_string(), dim);
                }
            }
        }

        // Parse output path to get storage prefix
        let output_url: StorageUrl =
            self.output_path
                .parse()
                .map_err(|e: roboflow_storage::StorageError| {
                    TikvError::Other(format!("Invalid output path '{}': {}", self.output_path, e))
                })?;

        let output_prefix = output_url.path().trim_start_matches('/').to_string();

        // Write metadata to storage
        collector
            .write_all_to_storage(&self.storage, &output_prefix, config)
            .map_err(|e| TikvError::Other(format!("Failed to write metadata: {}", e)))?;

        info!(
            output_path = %self.output_path,
            episodes = collector.episodes.len(),
            "LeRobot v2.1 metadata files written successfully"
        );

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

#[derive(Debug, Clone)]
struct MediaCopyTask {
    source_key: String,
    dest_key: String,
}

fn staging_prefix_from_parquet_path(path: &str) -> Option<String> {
    path.rfind("/data/").map(|idx| path[..idx].to_string())
}

fn remap_episode_video_path(path: &str, episode_index: i64) -> String {
    let Some(slash_idx) = path.rfind('/') else {
        return path.to_string();
    };

    let (dir, file) = path.split_at(slash_idx + 1);
    if file.starts_with("episode_") && file.ends_with(".mp4") {
        format!("{}episode_{:06}.mp4", dir, episode_index)
    } else {
        path.to_string()
    }
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

    /// Helper to build a test LerobotConfig.
    fn test_lerobot_config() -> LerobotConfig {
        use roboflow_dataset::formats::common::DatasetBaseConfig;
        use roboflow_dataset::formats::lerobot::config::{DatasetConfig, VideoConfig};

        LerobotConfig {
            dataset: DatasetConfig {
                base: DatasetBaseConfig {
                    name: "test_dataset".to_string(),
                    fps: 30,
                    robot_type: Some("so100".to_string()),
                },
                env_type: None,
            },
            mappings: vec![],
            video: VideoConfig::default(),
            annotation_file: None,
            flushing: Default::default(),
            streaming: Default::default(),
        }
    }

    /// Helper to build a merged DataFrame with the given episodes and frame counts.
    fn build_test_dataframe(episodes: &[(i64, usize)]) -> DataFrame {
        let mut episode_indices: Vec<i64> = Vec::new();
        let mut frame_indices: Vec<i64> = Vec::new();
        let mut timestamps: Vec<f64> = Vec::new();

        for &(ep_idx, frame_count) in episodes {
            for f in 0..frame_count {
                episode_indices.push(ep_idx);
                frame_indices.push(f as i64);
                timestamps.push(f as f64 / 30.0);
            }
        }

        DataFrame::new::<Series>(vec![
            Series::new("episode_index".into(), &episode_indices),
            Series::new("frame_index".into(), &frame_indices),
            Series::new("timestamp".into(), &timestamps),
        ])
        .unwrap()
    }

    #[tokio::test]
    async fn test_write_metadata_single_episode() {
        use roboflow_storage::mock::MockStorage;

        let mock_storage = Arc::new(MockStorage::new());
        let executor = ParquetMergeExecutor::new(
            Arc::clone(&mock_storage) as Arc<dyn Storage>,
            "s3://bucket/datasets/test001".to_string(),
            std::env::temp_dir(),
        );

        let df = build_test_dataframe(&[(0, 100)]);
        let config = test_lerobot_config();

        executor.write_metadata(&df, &config).await.unwrap();

        // Verify meta/info.json was written
        assert!(
            mock_storage.exists(Path::new("datasets/test001/meta/info.json")),
            "meta/info.json should exist"
        );

        let info_content = {
            let mut buf = Vec::new();
            mock_storage
                .reader(Path::new("datasets/test001/meta/info.json"))
                .unwrap()
                .read_to_end(&mut buf)
                .unwrap();
            String::from_utf8(buf).unwrap()
        };
        let info: serde_json::Value = serde_json::from_str(&info_content).unwrap();
        assert_eq!(info["total_episodes"], 1);
        assert_eq!(info["total_frames"], 100);
        assert_eq!(info["fps"], 30);
        assert_eq!(info["name"], "test_dataset");
        assert_eq!(info["robot_type"], "so100");

        // Verify meta/episodes.jsonl was written
        assert!(
            mock_storage.exists(Path::new("datasets/test001/meta/episodes.jsonl")),
            "meta/episodes.jsonl should exist"
        );

        let episodes_content = {
            let mut buf = Vec::new();
            mock_storage
                .reader(Path::new("datasets/test001/meta/episodes.jsonl"))
                .unwrap()
                .read_to_end(&mut buf)
                .unwrap();
            String::from_utf8(buf).unwrap()
        };
        let lines: Vec<&str> = episodes_content.lines().collect();
        assert_eq!(lines.len(), 1);

        let ep: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(ep["episode_index"], 0);
        assert_eq!(ep["length"], 100);
    }

    #[tokio::test]
    async fn test_write_metadata_multiple_episodes() {
        use roboflow_storage::mock::MockStorage;

        let mock_storage = Arc::new(MockStorage::new());
        let executor = ParquetMergeExecutor::new(
            Arc::clone(&mock_storage) as Arc<dyn Storage>,
            "s3://bucket/datasets/test002".to_string(),
            std::env::temp_dir(),
        );

        let df = build_test_dataframe(&[(0, 50), (1, 75), (2, 25)]);
        let config = test_lerobot_config();

        executor.write_metadata(&df, &config).await.unwrap();

        let info_content = {
            let mut buf = Vec::new();
            mock_storage
                .reader(Path::new("datasets/test002/meta/info.json"))
                .unwrap()
                .read_to_end(&mut buf)
                .unwrap();
            String::from_utf8(buf).unwrap()
        };
        let info: serde_json::Value = serde_json::from_str(&info_content).unwrap();
        assert_eq!(info["total_episodes"], 3);
        assert_eq!(info["total_frames"], 150);

        let episodes_content = {
            let mut buf = Vec::new();
            mock_storage
                .reader(Path::new("datasets/test002/meta/episodes.jsonl"))
                .unwrap()
                .read_to_end(&mut buf)
                .unwrap();
            String::from_utf8(buf).unwrap()
        };
        let lines: Vec<&str> = episodes_content.lines().collect();
        assert_eq!(lines.len(), 3);

        // Episodes should be sorted by index
        let ep0: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        let ep1: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        let ep2: serde_json::Value = serde_json::from_str(lines[2]).unwrap();
        assert_eq!(ep0["episode_index"], 0);
        assert_eq!(ep0["length"], 50);
        assert_eq!(ep1["episode_index"], 1);
        assert_eq!(ep1["length"], 75);
        assert_eq!(ep2["episode_index"], 2);
        assert_eq!(ep2["length"], 25);
    }

    #[tokio::test]
    async fn test_write_metadata_with_video_path_columns() {
        use roboflow_storage::mock::MockStorage;

        let mock_storage = Arc::new(MockStorage::new());
        let executor = ParquetMergeExecutor::new(
            Arc::clone(&mock_storage) as Arc<dyn Storage>,
            "s3://bucket/datasets/test003".to_string(),
            std::env::temp_dir(),
        );

        // Build a DataFrame with video path columns
        let df = DataFrame::new::<Series>(vec![
            Series::new("episode_index".into(), &[0i64, 0, 1, 1]),
            Series::new("frame_index".into(), &[0i64, 1, 0, 1]),
            Series::new("timestamp".into(), &[0.0f64, 0.033, 0.0, 0.033]),
            Series::new(
                "observation.images.cam_left_path".into(),
                &[
                    "videos/chunk-000/observation.images.cam_left/episode_000000.mp4",
                    "videos/chunk-000/observation.images.cam_left/episode_000000.mp4",
                    "videos/chunk-000/observation.images.cam_left/episode_000001.mp4",
                    "videos/chunk-000/observation.images.cam_left/episode_000001.mp4",
                ],
            ),
            Series::new(
                "observation.images.cam_right_path".into(),
                &[
                    "videos/chunk-000/observation.images.cam_right/episode_000000.mp4",
                    "videos/chunk-000/observation.images.cam_right/episode_000000.mp4",
                    "videos/chunk-000/observation.images.cam_right/episode_000001.mp4",
                    "videos/chunk-000/observation.images.cam_right/episode_000001.mp4",
                ],
            ),
        ])
        .unwrap();

        let config = test_lerobot_config();
        executor.write_metadata(&df, &config).await.unwrap();

        let info_content = {
            let mut buf = Vec::new();
            mock_storage
                .reader(Path::new("datasets/test003/meta/info.json"))
                .unwrap()
                .read_to_end(&mut buf)
                .unwrap();
            String::from_utf8(buf).unwrap()
        };
        let info: serde_json::Value = serde_json::from_str(&info_content).unwrap();

        // Should have detected both cameras as video features
        let features = info["features"].as_object().unwrap();
        assert!(
            features.contains_key("observation.images.cam_left"),
            "Should contain cam_left feature"
        );
        assert!(
            features.contains_key("observation.images.cam_right"),
            "Should contain cam_right feature"
        );

        // Check video feature structure
        let cam_left = &features["observation.images.cam_left"];
        assert_eq!(cam_left["dtype"], "video");
        assert!(cam_left["shape"].is_array());
    }

    #[tokio::test]
    async fn test_write_metadata_with_state_list_columns() {
        use roboflow_storage::mock::MockStorage;

        let mock_storage = Arc::new(MockStorage::new());
        let executor = ParquetMergeExecutor::new(
            Arc::clone(&mock_storage) as Arc<dyn Storage>,
            "s3://bucket/datasets/test004".to_string(),
            std::env::temp_dir(),
        );

        // Build a DataFrame with list columns for state
        let state_values: Series = Series::new(
            "observation.state".into(),
            vec![
                Series::new("".into(), &[1.0f32, 2.0, 3.0, 4.0, 5.0, 6.0]),
                Series::new("".into(), &[1.1f32, 2.1, 3.1, 4.1, 5.1, 6.1]),
            ],
        );
        let action_values: Series = Series::new(
            "action".into(),
            vec![
                Series::new("".into(), &[0.5f32, 0.6, 0.7]),
                Series::new("".into(), &[0.8f32, 0.9, 1.0]),
            ],
        );

        let s1: Series = Series::new("episode_index".into(), &[0i64, 0]);
        let s2: Series = Series::new("frame_index".into(), &[0i64, 1]);
        let s3: Series = Series::new("timestamp".into(), &[0.0f64, 0.033]);
        let df = DataFrame::new::<Series>(vec![s1, s2, s3, state_values, action_values]).unwrap();

        let config = test_lerobot_config();
        executor.write_metadata(&df, &config).await.unwrap();

        let info_content = {
            let mut buf = Vec::new();
            mock_storage
                .reader(Path::new("datasets/test004/meta/info.json"))
                .unwrap()
                .read_to_end(&mut buf)
                .unwrap();
            String::from_utf8(buf).unwrap()
        };
        let info: serde_json::Value = serde_json::from_str(&info_content).unwrap();

        // Should have detected state features with correct dimensions
        let features = info["features"].as_object().unwrap();
        assert!(
            features.contains_key("observation.state"),
            "Should contain observation.state"
        );
        assert!(features.contains_key("action"), "Should contain action");

        let obs_state = &features["observation.state"];
        assert_eq!(obs_state["dtype"], "float32");
        assert_eq!(obs_state["shape"], serde_json::json!([6]));

        let action = &features["action"];
        assert_eq!(action["dtype"], "float32");
        assert_eq!(action["shape"], serde_json::json!([3]));
    }

    #[tokio::test]
    async fn test_write_metadata_missing_episode_index_column() {
        use roboflow_storage::mock::MockStorage;

        let mock_storage = Arc::new(MockStorage::new());
        let executor = ParquetMergeExecutor::new(
            Arc::clone(&mock_storage) as Arc<dyn Storage>,
            "s3://bucket/datasets/test005".to_string(),
            std::env::temp_dir(),
        );

        // DataFrame without episode_index column
        let s1: Series = Series::new("frame_index".into(), &[0i64, 1]);
        let s2: Series = Series::new("timestamp".into(), &[0.0f64, 0.033]);
        let df = DataFrame::new::<Series>(vec![s1, s2]).unwrap();

        let config = test_lerobot_config();
        let result = executor.write_metadata(&df, &config).await;

        assert!(result.is_err(), "Should fail without episode_index column");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("episode_index"),
            "Error should mention episode_index: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn test_write_metadata_to_mock_storage() {
        use roboflow_storage::mock::MockStorage;

        let mock_storage = Arc::new(MockStorage::new());
        let executor = ParquetMergeExecutor::new(
            Arc::clone(&mock_storage) as Arc<dyn Storage>,
            "s3://bucket/datasets/my_dataset".to_string(),
            std::env::temp_dir(),
        );

        let df = build_test_dataframe(&[(0, 30), (1, 45)]);
        let config = test_lerobot_config();

        executor.write_metadata(&df, &config).await.unwrap();

        // Verify files were written to mock storage
        assert!(
            mock_storage.exists(Path::new("datasets/my_dataset/meta/info.json")),
            "info.json should be written to storage"
        );
        assert!(
            mock_storage.exists(Path::new("datasets/my_dataset/meta/episodes.jsonl")),
            "episodes.jsonl should be written to storage"
        );
    }
}
