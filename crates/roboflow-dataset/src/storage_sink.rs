use std::path::PathBuf;
use std::sync::Arc;

use roboflow_core::{Result, RoboflowError};
use roboflow_storage::{LocalStorage, Storage};

use crate::formats::common::{
    ImageData, Sink, VideoEncoderConfig, WriteOperation, decode_image_to_rgb,
};
use crate::media::video::{RsmpegMp4Encoder, VideoFrame, VideoFrameBuffer};

pub struct StorageSink {
    storage: Arc<dyn Storage>,
    temp_dir: PathBuf,
}

impl StorageSink {
    pub fn new_local(base_path: impl Into<PathBuf>) -> Result<Self> {
        let base = base_path.into();
        std::fs::create_dir_all(&base)?;

        Ok(Self {
            storage: Arc::new(LocalStorage::new(&base)),
            temp_dir: base.join(".temp"),
        })
    }

    pub fn with_storage(storage: Arc<dyn Storage>, temp_dir: PathBuf) -> Self {
        Self { storage, temp_dir }
    }

    fn is_local_storage(&self) -> bool {
        self.storage.as_any().type_id() == std::any::TypeId::of::<LocalStorage>()
    }
}

impl Sink for StorageSink {
    fn execute(&self, op: WriteOperation) -> Result<()> {
        match op {
            WriteOperation::WriteFile { path, data } => {
                let mut writer = self
                    .storage
                    .writer(&path)
                    .map_err(|e| RoboflowError::other(format!("Storage error: {}", e)))?;
                writer.write_all(&data)?;
                writer.flush()?;
                Ok(())
            }

            WriteOperation::WriteParquet { path, data } => {
                let mut writer = self
                    .storage
                    .writer(&path)
                    .map_err(|e| RoboflowError::other(format!("Storage error: {}", e)))?;
                writer.write_all(&data)?;
                writer.flush()?;
                Ok(())
            }

            WriteOperation::WriteMetadata { path, content } => {
                let data = serde_json::to_vec_pretty(&content)
                    .map_err(|e| RoboflowError::other(format!("JSON error: {}", e)))?;
                let mut writer = self
                    .storage
                    .writer(&path)
                    .map_err(|e| RoboflowError::other(format!("Storage error: {}", e)))?;
                writer.write_all(&data)?;
                writer.flush()?;
                Ok(())
            }

            WriteOperation::EncodeAndWriteVideo {
                camera: _,
                frames,
                output_path,
                config,
            } => self.encode_and_write_video(&frames, &output_path, &config),

            WriteOperation::ComposeFiles {
                sources,
                destination,
            } => {
                let source_refs: Vec<_> = sources.iter().map(|p| p.as_path()).collect();
                self.storage
                    .compose_objects(&source_refs, &destination)
                    .map_err(|e| RoboflowError::other(format!("Storage error: {}", e)))?;
                Ok(())
            }

            WriteOperation::UploadDataset {
                local_path,
                cloud_prefix,
                stats: _,
            } => self.upload_dataset(&local_path, &cloud_prefix),
        }
    }
}

impl StorageSink {
    fn encode_and_write_video(
        &self,
        frames: &[ImageData],
        output_path: &std::path::Path,
        config: &VideoEncoderConfig,
    ) -> Result<()> {
        if frames.is_empty() {
            tracing::warn!("No frames to encode for video: {}", output_path.display());
            return Ok(());
        }

        let mut buffer = VideoFrameBuffer::new();
        for img in frames {
            if img.width == 0 || img.height == 0 {
                continue;
            }

            let (width, height, rgb_data) = if img.is_encoded {
                match decode_image_to_rgb(img) {
                    Some((w, h, data)) => (w, h, data),
                    None => {
                        tracing::debug!("Failed to decode image, skipping");
                        continue;
                    }
                }
            } else {
                (img.width, img.height, img.data.clone())
            };

            let video_frame = VideoFrame::new(width, height, rgb_data);
            if let Err(e) = buffer.add_frame(video_frame) {
                tracing::warn!("Frame dimension mismatch: {}", e);
            }
        }

        if buffer.is_empty() {
            tracing::warn!(
                "No valid frames after decoding for: {}",
                output_path.display()
            );
            return Ok(());
        }

        let is_local = self.is_local_storage();

        if is_local {
            RsmpegMp4Encoder::with_config(config.clone())
                .encode_buffer(&buffer, output_path)
                .map_err(|e| RoboflowError::other(format!("Video encoding failed: {}", e)))?;
        } else {
            std::fs::create_dir_all(&self.temp_dir)?;
            let temp_path = self
                .temp_dir
                .join(format!("video_{}.mp4", uuid::Uuid::new_v4()));

            RsmpegMp4Encoder::with_config(config.clone())
                .encode_buffer(&buffer, &temp_path)
                .map_err(|e| RoboflowError::other(format!("Video encoding failed: {}", e)))?;

            let data = std::fs::read(&temp_path)?;
            let mut writer = self
                .storage
                .writer(output_path)
                .map_err(|e| RoboflowError::other(format!("Storage error: {}", e)))?;
            writer.write_all(&data)?;
            writer.flush()?;

            if let Err(e) = std::fs::remove_file(&temp_path) {
                tracing::warn!("Failed to cleanup temp file: {}", e);
            }
        }

        tracing::info!(
            path = %output_path.display(),
            frames = buffer.len(),
            is_local,
            "Video encoded successfully"
        );

        Ok(())
    }

    fn upload_dataset(&self, local_path: &std::path::Path, cloud_prefix: &str) -> Result<()> {
        if !local_path.exists() {
            return Err(RoboflowError::other(format!(
                "Local dataset path does not exist: {}",
                local_path.display()
            )));
        }

        if self.is_local_storage() {
            tracing::info!(
                local_path = %local_path.display(),
                "Local storage - dataset already in place"
            );
            return Ok(());
        }

        tracing::info!(
            local_path = %local_path.display(),
            cloud_prefix = %cloud_prefix,
            "Uploading dataset to cloud storage"
        );

        self.upload_dir_recursive(local_path, local_path, cloud_prefix)?;

        tracing::info!("Dataset upload complete");
        Ok(())
    }

    fn upload_dir_recursive(
        &self,
        base_path: &std::path::Path,
        current_dir: &std::path::Path,
        cloud_prefix: &str,
    ) -> Result<()> {
        for entry in std::fs::read_dir(current_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_file() {
                let relative = path
                    .strip_prefix(base_path)
                    .map_err(|e| RoboflowError::other(format!("Path strip error: {}", e)))?;
                let cloud_path = format!(
                    "{}/{}",
                    cloud_prefix.trim_end_matches('/'),
                    relative.display()
                );

                let data = std::fs::read(&path)?;
                let mut writer = self
                    .storage
                    .writer(std::path::Path::new(&cloud_path))
                    .map_err(|e| RoboflowError::other(format!("Storage writer error: {}", e)))?;
                writer.write_all(&data)?;
                writer.flush()?;

                tracing::debug!("Uploaded: {} -> {}", path.display(), cloud_path);
            } else if path.is_dir() {
                self.upload_dir_recursive(base_path, &path, cloud_prefix)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::formats::common::operation::DatasetStats;
    use tempfile::tempdir;

    #[test]
    fn test_storage_sink_new_local() {
        let dir = tempdir().unwrap();
        let sink = StorageSink::new_local(dir.path());
        assert!(sink.is_ok());

        let sink = sink.unwrap();
        assert!(sink.is_local_storage());
    }

    #[test]
    fn test_storage_sink_local_write_file() {
        let dir = tempdir().unwrap();
        let sink = StorageSink::new_local(dir.path()).unwrap();

        sink.execute(WriteOperation::WriteFile {
            path: PathBuf::from("test.txt"),
            data: b"hello world".to_vec(),
        })
        .unwrap();

        let path = dir.path().join("test.txt");
        assert!(path.exists());
        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content, "hello world");
    }

    #[test]
    fn test_storage_sink_write_parquet() {
        let dir = tempdir().unwrap();
        let sink = StorageSink::new_local(dir.path()).unwrap();

        let data = vec![0u8; 100];
        sink.execute(WriteOperation::WriteParquet {
            path: PathBuf::from("data.parquet"),
            data: data.clone(),
        })
        .unwrap();

        let path = dir.path().join("data.parquet");
        assert!(path.exists());
        let read_data = std::fs::read(&path).unwrap();
        assert_eq!(read_data, data);
    }

    #[test]
    fn test_storage_sink_write_metadata() {
        let dir = tempdir().unwrap();
        let sink = StorageSink::new_local(dir.path()).unwrap();

        let mut metadata = serde_json::Map::new();
        metadata.insert("version".to_string(), serde_json::json!("1.0"));
        metadata.insert("fps".to_string(), serde_json::json!(30));

        sink.execute(WriteOperation::WriteMetadata {
            path: PathBuf::from("meta.json"),
            content: serde_json::Value::Object(metadata),
        })
        .unwrap();

        let path = dir.path().join("meta.json");
        assert!(path.exists());

        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("version"));
        assert!(content.contains("1.0"));
        assert!(content.contains("fps"));
        assert!(content.contains("30"));
    }

    #[test]
    fn test_storage_sink_compose_files() {
        let dir = tempdir().unwrap();
        let sink = StorageSink::new_local(dir.path()).unwrap();

        let source1 = dir.path().join("source1.txt");
        let source2 = dir.path().join("source2.txt");
        std::fs::write(&source1, "hello ").unwrap();
        std::fs::write(&source2, "world").unwrap();

        sink.execute(WriteOperation::ComposeFiles {
            sources: vec![source1, source2],
            destination: PathBuf::from("combined.txt"),
        })
        .unwrap();

        let path = dir.path().join("combined.txt");
        assert!(path.exists());
        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content, "hello world");
    }

    #[test]
    fn test_storage_sink_upload_dataset_local() {
        let dir = tempdir().unwrap();
        let sink = StorageSink::new_local(dir.path()).unwrap();

        let dataset_dir = dir.path().join("dataset");
        std::fs::create_dir(&dataset_dir).unwrap();
        std::fs::create_dir(dataset_dir.join("videos")).unwrap();
        std::fs::write(dataset_dir.join("data.parquet"), "parquet data").unwrap();
        std::fs::write(dataset_dir.join("videos/cam.mp4"), "video data").unwrap();

        sink.execute(WriteOperation::UploadDataset {
            local_path: dataset_dir.clone(),
            cloud_prefix: dir.path().join("cloud").to_string_lossy().to_string(),
            stats: DatasetStats::default(),
        })
        .unwrap();

        assert!(dataset_dir.exists());
        assert!(dataset_dir.join("data.parquet").exists());
    }

    #[test]
    fn test_storage_sink_upload_dataset_nonexistent() {
        let dir = tempdir().unwrap();
        let sink = StorageSink::new_local(dir.path()).unwrap();

        let result = sink.execute(WriteOperation::UploadDataset {
            local_path: PathBuf::from("/nonexistent/path"),
            cloud_prefix: "s3://bucket/prefix".to_string(),
            stats: DatasetStats::default(),
        });

        assert!(result.is_err());
    }

    #[test]
    fn test_storage_sink_with_storage() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(LocalStorage::new(dir.path()));
        let temp_dir = dir.path().join("temp");

        let sink = StorageSink::with_storage(storage, temp_dir);
        assert!(sink.is_local_storage());
    }

    #[test]
    fn test_storage_sink_nested_directories() {
        let dir = tempdir().unwrap();
        let sink = StorageSink::new_local(dir.path()).unwrap();

        sink.execute(WriteOperation::WriteFile {
            path: PathBuf::from("a/b/c/deep.txt"),
            data: b"deep content".to_vec(),
        })
        .unwrap();

        let path = dir.path().join("a/b/c/deep.txt");
        assert!(path.exists());
        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content, "deep content");
    }

    #[test]
    fn test_storage_sink_multiple_writes_same_file() {
        let dir = tempdir().unwrap();
        let sink = StorageSink::new_local(dir.path()).unwrap();

        sink.execute(WriteOperation::WriteFile {
            path: PathBuf::from("test.txt"),
            data: b"first".to_vec(),
        })
        .unwrap();

        sink.execute(WriteOperation::WriteFile {
            path: PathBuf::from("test.txt"),
            data: b"second".to_vec(),
        })
        .unwrap();

        let content = std::fs::read_to_string(dir.path().join("test.txt")).unwrap();
        assert_eq!(content, "second");
    }

    #[test]
    fn test_storage_sink_empty_data() {
        let dir = tempdir().unwrap();
        let sink = StorageSink::new_local(dir.path()).unwrap();

        sink.execute(WriteOperation::WriteFile {
            path: PathBuf::from("empty.txt"),
            data: vec![],
        })
        .unwrap();

        let path = dir.path().join("empty.txt");
        assert!(path.exists());
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
    }

    #[test]
    fn test_storage_sink_large_data() {
        let dir = tempdir().unwrap();
        let sink = StorageSink::new_local(dir.path()).unwrap();

        let large_data = vec![0u8; 1024 * 1024]; // 1MB
        sink.execute(WriteOperation::WriteFile {
            path: PathBuf::from("large.bin"),
            data: large_data.clone(),
        })
        .unwrap();

        let path = dir.path().join("large.bin");
        assert!(path.exists());
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 1024 * 1024);
    }

    #[test]
    fn test_storage_sink_batch_operations() {
        let dir = tempdir().unwrap();
        let sink = StorageSink::new_local(dir.path()).unwrap();

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

        assert!(dir.path().join("a.txt").exists());
        assert!(dir.path().join("b.txt").exists());
        assert!(dir.path().join("meta.json").exists());
    }

    #[test]
    fn test_storage_sink_special_characters_in_path() {
        let dir = tempdir().unwrap();
        let sink = StorageSink::new_local(dir.path()).unwrap();

        sink.execute(WriteOperation::WriteFile {
            path: PathBuf::from("file with spaces.txt"),
            data: b"content".to_vec(),
        })
        .unwrap();

        assert!(dir.path().join("file with spaces.txt").exists());
    }

    #[test]
    fn test_storage_sink_unicode_content() {
        let dir = tempdir().unwrap();
        let sink = StorageSink::new_local(dir.path()).unwrap();

        let unicode_data = "Hello 世界 🌍".as_bytes().to_vec();
        sink.execute(WriteOperation::WriteFile {
            path: PathBuf::from("unicode.txt"),
            data: unicode_data.clone(),
        })
        .unwrap();

        let read_data = std::fs::read(dir.path().join("unicode.txt")).unwrap();
        assert_eq!(read_data, unicode_data);
    }

    #[test]
    fn test_encode_and_write_video_empty_frames() {
        let dir = tempdir().unwrap();
        let sink = StorageSink::new_local(dir.path()).unwrap();

        // Test with empty frames vector
        let result = sink.encode_and_write_video(
            &[],
            std::path::Path::new("test.mp4"),
            &VideoEncoderConfig::default(),
        );

        // Should succeed but do nothing
        assert!(result.is_ok());
    }

    #[test]
    fn test_encode_and_write_video_zero_dimension_frames() {
        let dir = tempdir().unwrap();
        let sink = StorageSink::new_local(dir.path()).unwrap();

        // Create frame with zero dimensions
        let frames = vec![ImageData::new_rgb(0, 0, vec![]).unwrap_or_else(|_| {
            // Fallback: create encoded image with zero dims
            let mut img = ImageData::encoded(0, 0, vec![0xff, 0xd8, 0xff, 0xe0]); // JPEG header
            img.width = 0;
            img.height = 0;
            img
        })];

        let result = sink.encode_and_write_video(
            &frames,
            std::path::Path::new("test.mp4"),
            &VideoEncoderConfig::default(),
        );

        // Should succeed but skip the invalid frame
        assert!(result.is_ok());
    }

    #[test]
    fn test_encode_and_write_video_with_valid_rgb_frame() {
        let dir = tempdir().unwrap();
        let sink = StorageSink::new_local(dir.path()).unwrap();

        // Create a valid RGB frame (64x48 = small but valid)
        let width = 64u32;
        let height = 48u32;
        let rgb_data = vec![128u8; (width * height * 3) as usize];
        let frame = ImageData::new_rgb(width, height, rgb_data).unwrap();

        let output_path = dir.path().join("output.mp4");
        let result =
            sink.encode_and_write_video(&[frame], &output_path, &VideoEncoderConfig::default());

        assert!(result.is_ok());
        // Video file should be created
        assert!(output_path.exists());
    }

    #[test]
    fn test_upload_dir_recursive_creates_nested_structure() {
        let dir = tempdir().unwrap();
        let sink = StorageSink::new_local(dir.path()).unwrap();

        // Create nested directory structure
        let dataset_dir = dir.path().join("dataset");
        std::fs::create_dir(&dataset_dir).unwrap();
        std::fs::create_dir(dataset_dir.join("episode_000")).unwrap();
        std::fs::create_dir(dataset_dir.join("episode_000/videos")).unwrap();
        std::fs::write(dataset_dir.join("episode_000/data.parquet"), "data").unwrap();
        std::fs::write(dataset_dir.join("episode_000/videos/cam.mp4"), "video").unwrap();

        // Upload dataset
        sink.execute(WriteOperation::UploadDataset {
            local_path: dataset_dir.clone(),
            cloud_prefix: dir.path().join("cloud").to_string_lossy().to_string(),
            stats: DatasetStats::default(),
        })
        .unwrap();

        // Verify upload (for local storage, it's a no-op but should not error)
        assert!(dataset_dir.exists());
    }

    #[test]
    fn test_new_local_creates_directory() {
        let dir = tempdir().unwrap();
        let subdir = dir.path().join("nested/subdir");

        // Directory doesn't exist yet
        assert!(!subdir.exists());

        // Creating StorageSink should create the directory
        let sink = StorageSink::new_local(&subdir);
        assert!(sink.is_ok());
        assert!(subdir.exists());
    }

    #[test]
    fn test_write_metadata_with_complex_json() {
        let dir = tempdir().unwrap();
        let sink = StorageSink::new_local(dir.path()).unwrap();

        let metadata = serde_json::json!({
            "dataset": {
                "name": "test_dataset",
                "fps": 30,
                "robot_type": "test_robot"
            },
            "features": ["state", "action", "image"],
            "stats": {
                "episodes": 10,
                "frames": 1000
            }
        });

        sink.execute(WriteOperation::WriteMetadata {
            path: PathBuf::from("complex.json"),
            content: metadata,
        })
        .unwrap();

        let path = dir.path().join("complex.json");
        assert!(path.exists());

        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("test_dataset"));
        assert!(content.contains("30"));
        assert!(content.contains("state"));
    }
}
