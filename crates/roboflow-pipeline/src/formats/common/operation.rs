// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Write operations for dataset output.
//!
//! This module defines the `WriteOperation` enum and `Sink` trait that enable
//! separation between dataset writers (which produce operations) and storage
//! backends (which execute them).

use roboflow_core::Result;
use std::collections::HashMap;
use std::path::PathBuf;

use super::ImageData;
use crate::video::VideoEncoderConfig;

#[derive(Debug, Clone, Default)]
pub struct DatasetStats {
    pub frames: usize,
    pub episodes: usize,
    pub parquet_sizes: HashMap<PathBuf, u64>,
    pub video_sizes: HashMap<PathBuf, u64>,
    pub duration_sec: f64,
}

/// All possible write operations that a DatasetWriter can request.
///
/// Storage backends implement how to execute these operations via the `Sink` trait.
/// This separation enables:
/// - Testable writers (no storage dependency)
/// - Reusable storage logic across formats
/// - Clear boundaries between processing and I/O
#[derive(Debug, Clone)]
pub enum WriteOperation {
    /// Write raw bytes to a file
    WriteFile { path: PathBuf, data: Vec<u8> },

    /// Write parquet data (serialized frames)
    WriteParquet {
        path: PathBuf,
        /// Serialized frame data (format-specific)
        data: Vec<u8>,
    },

    /// Encode video frames and write to file
    EncodeAndWriteVideo {
        /// Camera identifier
        camera: String,
        /// Frame images to encode
        frames: Vec<ImageData>,
        /// Output file path
        output_path: PathBuf,
        /// Video encoding configuration
        config: VideoEncoderConfig,
    },

    /// Write JSON metadata file
    WriteMetadata {
        path: PathBuf,
        content: serde_json::Value,
    },

    /// Compose multiple files into one (for video segments)
    ComposeFiles {
        /// Source files to combine
        sources: Vec<PathBuf>,
        /// Destination path
        destination: PathBuf,
    },

    /// Upload a complete local dataset to cloud storage.
    ///
    /// Used in the staging pattern where:
    /// 1. Writer produces local dataset in temp directory
    /// 2. Sink uploads from local temp to cloud storage
    /// 3. Sink reports completion stats back to executor/TiKV
    UploadDataset {
        /// Local source directory containing complete dataset
        local_path: PathBuf,
        /// Cloud destination prefix (e.g., "s3://bucket/prefix/")
        cloud_prefix: String,
        /// Dataset statistics for reporting
        stats: DatasetStats,
    },
}

impl WriteOperation {
    /// Get the target path for this operation
    pub fn target_path(&self) -> &PathBuf {
        match self {
            WriteOperation::WriteFile { path, .. } => path,
            WriteOperation::WriteParquet { path, .. } => path,
            WriteOperation::EncodeAndWriteVideo { output_path, .. } => output_path,
            WriteOperation::WriteMetadata { path, .. } => path,
            WriteOperation::ComposeFiles { destination, .. } => destination,
            WriteOperation::UploadDataset { local_path, .. } => local_path,
        }
    }

    /// Get the operation type name for logging
    pub fn operation_type(&self) -> &'static str {
        match self {
            WriteOperation::WriteFile { .. } => "WriteFile",
            WriteOperation::WriteParquet { .. } => "WriteParquet",
            WriteOperation::EncodeAndWriteVideo { .. } => "EncodeAndWriteVideo",
            WriteOperation::WriteMetadata { .. } => "WriteMetadata",
            WriteOperation::ComposeFiles { .. } => "ComposeFiles",
            WriteOperation::UploadDataset { .. } => "UploadDataset",
        }
    }
}

/// Sink executes write operations.
///
/// This trait is implemented by storage backends to execute the operations
/// produced by dataset writers. It provides a clean boundary between
/// data processing (writers) and I/O (storage).
///
/// # Example
///
/// ```rust,ignore
/// use roboflow_pipeline::formats::{WriteOperation, Sink};
///
/// // Mock sink for testing
/// pub struct VecSink {
///     operations: RefCell<Vec<WriteOperation>>,
/// }
///
/// impl Sink for VecSink {
///     fn execute(&self, op: WriteOperation) -> Result<()> {
///         self.operations.borrow_mut().push(op);
///         Ok(())
///     }
/// }
/// ```
pub trait Sink: Send + Sync {
    /// Execute a single write operation
    fn execute(&self, op: WriteOperation) -> Result<()>;

    /// Execute multiple operations
    ///
    /// Default implementation calls `execute` for each operation.
    /// Storage backends can override for batch optimizations.
    fn execute_batch(&self, ops: Vec<WriteOperation>) -> Result<()> {
        for op in ops {
            self.execute(op)?;
        }
        Ok(())
    }
}

/// In-memory sink for testing.
///
/// Captures all write operations for inspection without performing I/O.
#[derive(Debug, Default)]
pub struct VecSink {
    operations: std::sync::Mutex<Vec<WriteOperation>>,
}

impl VecSink {
    /// Create a new empty VecSink
    pub fn new() -> Self {
        Self::default()
    }

    /// Get all captured operations
    pub fn operations(&self) -> Vec<WriteOperation> {
        self.operations.lock().unwrap().clone()
    }

    /// Get the number of captured operations
    pub fn len(&self) -> usize {
        self.operations.lock().unwrap().len()
    }

    /// Check if no operations were captured
    pub fn is_empty(&self) -> bool {
        self.operations.lock().unwrap().is_empty()
    }

    /// Clear all captured operations
    pub fn clear(&self) {
        self.operations.lock().unwrap().clear();
    }
}

impl Sink for VecSink {
    fn execute(&self, op: WriteOperation) -> Result<()> {
        self.operations.lock().unwrap().push(op);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_file_operation() {
        let op = WriteOperation::WriteFile {
            path: PathBuf::from("test.txt"),
            data: b"hello".to_vec(),
        };

        assert_eq!(op.target_path(), &PathBuf::from("test.txt"));
        assert_eq!(op.operation_type(), "WriteFile");
    }

    #[test]
    fn test_write_parquet_operation() {
        let op = WriteOperation::WriteParquet {
            path: PathBuf::from("data.parquet"),
            data: vec![1, 2, 3],
        };

        assert_eq!(op.target_path(), &PathBuf::from("data.parquet"));
        assert_eq!(op.operation_type(), "WriteParquet");
    }

    #[test]
    fn test_encode_video_operation() {
        use crate::video::VideoEncoderConfig;

        let op = WriteOperation::EncodeAndWriteVideo {
            camera: "cam_0".to_string(),
            frames: vec![],
            output_path: PathBuf::from("video.mp4"),
            config: VideoEncoderConfig::default(),
        };

        assert_eq!(op.target_path(), &PathBuf::from("video.mp4"));
        assert_eq!(op.operation_type(), "EncodeAndWriteVideo");
    }

    #[test]
    fn test_write_metadata_operation() {
        let op = WriteOperation::WriteMetadata {
            path: PathBuf::from("meta.json"),
            content: serde_json::json!({"version": "1.0"}),
        };

        assert_eq!(op.target_path(), &PathBuf::from("meta.json"));
        assert_eq!(op.operation_type(), "WriteMetadata");
    }

    #[test]
    fn test_compose_files_operation() {
        let op = WriteOperation::ComposeFiles {
            sources: vec![PathBuf::from("a.mp4"), PathBuf::from("b.mp4")],
            destination: PathBuf::from("merged.mp4"),
        };

        assert_eq!(op.target_path(), &PathBuf::from("merged.mp4"));
        assert_eq!(op.operation_type(), "ComposeFiles");
    }

    #[test]
    fn test_upload_dataset_operation() {
        let op = WriteOperation::UploadDataset {
            local_path: PathBuf::from("/tmp/dataset"),
            cloud_prefix: "s3://bucket/dataset".to_string(),
            stats: DatasetStats::default(),
        };

        assert_eq!(op.target_path(), &PathBuf::from("/tmp/dataset"));
        assert_eq!(op.operation_type(), "UploadDataset");
    }

    #[test]
    fn test_dataset_stats_default() {
        let stats = DatasetStats::default();
        assert_eq!(stats.frames, 0);
        assert_eq!(stats.episodes, 0);
        assert!(stats.parquet_sizes.is_empty());
        assert!(stats.video_sizes.is_empty());
        assert_eq!(stats.duration_sec, 0.0);
    }

    #[test]
    fn test_dataset_stats_with_values() {
        let mut stats = DatasetStats {
            frames: 100,
            episodes: 5,
            duration_sec: 10.5,
            ..Default::default()
        };
        stats
            .parquet_sizes
            .insert(PathBuf::from("data.parquet"), 1024);
        stats.video_sizes.insert(PathBuf::from("video.mp4"), 2048);

        assert_eq!(stats.frames, 100);
        assert_eq!(stats.episodes, 5);
        assert_eq!(stats.parquet_sizes.len(), 1);
        assert_eq!(stats.video_sizes.len(), 1);
        assert_eq!(stats.duration_sec, 10.5);
    }

    #[test]
    fn test_vec_sink_basic() {
        let sink = VecSink::new();

        assert!(sink.is_empty());
        assert_eq!(sink.len(), 0);

        let op = WriteOperation::WriteFile {
            path: PathBuf::from("test.txt"),
            data: b"hello".to_vec(),
        };

        sink.execute(op.clone()).unwrap();

        assert!(!sink.is_empty());
        assert_eq!(sink.len(), 1);

        let ops = sink.operations();
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].operation_type(), "WriteFile");
    }

    #[test]
    fn test_vec_sink_multiple_operations() {
        let sink = VecSink::new();

        let ops = vec![
            WriteOperation::WriteFile {
                path: PathBuf::from("a.txt"),
                data: b"a".to_vec(),
            },
            WriteOperation::WriteFile {
                path: PathBuf::from("b.txt"),
                data: b"b".to_vec(),
            },
            WriteOperation::WriteMetadata {
                path: PathBuf::from("meta.json"),
                content: serde_json::json!({}),
            },
        ];

        sink.execute_batch(ops).unwrap();

        assert_eq!(sink.len(), 3);

        let stored = sink.operations();
        assert_eq!(stored[0].operation_type(), "WriteFile");
        assert_eq!(stored[1].operation_type(), "WriteFile");
        assert_eq!(stored[2].operation_type(), "WriteMetadata");
    }

    #[test]
    fn test_vec_sink_clear() {
        let sink = VecSink::new();

        sink.execute(WriteOperation::WriteFile {
            path: PathBuf::from("test.txt"),
            data: b"hello".to_vec(),
        })
        .unwrap();

        assert_eq!(sink.len(), 1);

        sink.clear();

        assert!(sink.is_empty());
        assert_eq!(sink.len(), 0);
    }

    #[test]
    fn test_vec_sink_thread_safety() {
        use std::thread;

        let sink = std::sync::Arc::new(VecSink::new());
        let mut handles = vec![];

        for i in 0..10 {
            let sink_clone = sink.clone();
            let handle = thread::spawn(move || {
                sink_clone
                    .execute(WriteOperation::WriteFile {
                        path: PathBuf::from(format!("file{}.txt", i)),
                        data: vec![i as u8],
                    })
                    .unwrap();
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(sink.len(), 10);
    }

    #[test]
    fn test_all_operation_types_covered() {
        let operations = [
            WriteOperation::WriteFile {
                path: PathBuf::from("a"),
                data: vec![],
            },
            WriteOperation::WriteParquet {
                path: PathBuf::from("b"),
                data: vec![],
            },
            WriteOperation::EncodeAndWriteVideo {
                camera: "cam".to_string(),
                frames: vec![],
                output_path: PathBuf::from("c"),
                config: VideoEncoderConfig::default(),
            },
            WriteOperation::WriteMetadata {
                path: PathBuf::from("d"),
                content: serde_json::json!({}),
            },
            WriteOperation::ComposeFiles {
                sources: vec![],
                destination: PathBuf::from("e"),
            },
            WriteOperation::UploadDataset {
                local_path: PathBuf::from("f"),
                cloud_prefix: "s3".to_string(),
                stats: DatasetStats::default(),
            },
        ];

        let types: Vec<_> = operations.iter().map(|op| op.operation_type()).collect();
        assert_eq!(types.len(), 6);
        assert!(types.contains(&"WriteFile"));
        assert!(types.contains(&"WriteParquet"));
        assert!(types.contains(&"EncodeAndWriteVideo"));
        assert!(types.contains(&"WriteMetadata"));
        assert!(types.contains(&"ComposeFiles"));
        assert!(types.contains(&"UploadDataset"));
    }
}
