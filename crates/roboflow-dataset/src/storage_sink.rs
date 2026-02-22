// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Local file sink for dataset output.
//!
//! This module provides [`LocalSink`] which writes all dataset files to the local filesystem.
//! Upload to cloud storage is handled by the executor, not the dataset crate.

use std::path::PathBuf;

use roboflow_core::{Result, RoboflowError};

use crate::formats::common::operation::{Sink, WriteOperation};
use crate::formats::common::{ImageData, decode_image_to_rgb};
use crate::media::video::{
    OutputConfig, RsmpegVideoComposer, VideoComposer, VideoEncoder, VideoEncoderConfig, VideoFrame,
    VideoFrameBuffer,
};

/// Local filesystem sink for dataset output.
///
/// Writes all files to a local base directory. Upload to cloud storage
/// should be handled by the executor after conversion completes.
///
/// Note: This is an internal implementation detail. External users should use
/// the [`convert_file`] function or the writer types directly.
#[allow(dead_code)]
pub struct LocalSink {
    base_path: PathBuf,
}

#[allow(dead_code)]
impl LocalSink {
    /// Create a new local sink writing to the given base directory.
    ///
    /// Creates the directory if it doesn't exist.
    pub fn new(base_path: impl Into<PathBuf>) -> Result<Self> {
        let base = base_path.into();
        std::fs::create_dir_all(&base)?;
        Ok(Self { base_path: base })
    }

    /// Get the base path for this sink.
    pub fn base_path(&self) -> &std::path::Path {
        &self.base_path
    }
}

impl Sink for LocalSink {
    fn execute(&self, op: WriteOperation) -> Result<()> {
        match op {
            WriteOperation::WriteFile { path, data } => {
                let full_path = self.base_path.join(&path);
                if let Some(parent) = full_path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::write(&full_path, data)?;
                Ok(())
            }

            WriteOperation::WriteParquet { path, data } => {
                let full_path = self.base_path.join(&path);
                if let Some(parent) = full_path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::write(&full_path, data)?;
                Ok(())
            }

            WriteOperation::WriteMetadata { path, content } => {
                let full_path = self.base_path.join(&path);
                if let Some(parent) = full_path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                let data = serde_json::to_vec_pretty(&content)
                    .map_err(|e| RoboflowError::other(format!("JSON error: {}", e)))?;
                std::fs::write(&full_path, data)?;
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
                let full_dest = self.base_path.join(&destination);

                // All sources are local files
                let source_paths: Vec<std::path::PathBuf> =
                    sources.iter().map(|s| self.base_path.join(s)).collect();

                // Verify all sources exist
                for src in &source_paths {
                    if !src.exists() {
                        return Err(RoboflowError::other(format!(
                            "Source file not found: {}",
                            src.display()
                        )));
                    }
                }

                // Create destination directory
                if let Some(parent) = full_dest.parent() {
                    std::fs::create_dir_all(parent)?;
                }

                // Compose using RsmpegVideoComposer
                let source_refs: Vec<_> = source_paths.iter().map(|p| p.as_path()).collect();
                let composer = RsmpegVideoComposer::new();
                composer.compose(&source_refs, &full_dest).map_err(|e| {
                    RoboflowError::other(format!("Video composition failed: {}", e))
                })?;

                tracing::info!(
                    sources = sources.len(),
                    dest = %destination.display(),
                    "Video composition complete"
                );

                Ok(())
            }
        }
    }
}

#[allow(dead_code)]
impl LocalSink {
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

        let full_path = self.base_path.join(output_path);
        if let Some(parent) = full_path.parent() {
            std::fs::create_dir_all(parent)?;
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

        // Use unified VideoEncoder for encoding
        let output = OutputConfig::file(&full_path);
        let mut encoder = VideoEncoder::new(config.clone(), output)
            .map_err(|e| RoboflowError::other(format!("Failed to create encoder: {}", e)))?;

        // Encode frames from buffer
        for frame in &buffer.frames {
            encoder
                .encode_frame(frame.data(), frame.width, frame.height)
                .map_err(|e| RoboflowError::other(format!("Failed to encode frame: {}", e)))?;
        }

        let result = encoder
            .finalize()
            .map_err(|e| RoboflowError::other(format!("Failed to finalize encoder: {}", e)))?;

        tracing::info!(
            path = %output_path.display(),
            frames = result.frames_encoded,
            bytes = result.bytes_written,
            "Video encoded successfully"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_local_sink_new() {
        let dir = tempdir().unwrap();
        let sink = LocalSink::new(dir.path());
        assert!(sink.is_ok());
    }

    #[test]
    fn test_local_sink_write_file() {
        let dir = tempdir().unwrap();
        let sink = LocalSink::new(dir.path()).unwrap();

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
    fn test_local_sink_write_parquet() {
        let dir = tempdir().unwrap();
        let sink = LocalSink::new(dir.path()).unwrap();

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
    fn test_local_sink_write_metadata() {
        let dir = tempdir().unwrap();
        let sink = LocalSink::new(dir.path()).unwrap();

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
    fn test_local_sink_nested_directories() {
        let dir = tempdir().unwrap();
        let sink = LocalSink::new(dir.path()).unwrap();

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
    fn test_local_sink_multiple_writes_same_file() {
        let dir = tempdir().unwrap();
        let sink = LocalSink::new(dir.path()).unwrap();

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
    fn test_local_sink_empty_data() {
        let dir = tempdir().unwrap();
        let sink = LocalSink::new(dir.path()).unwrap();

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
    fn test_local_sink_large_data() {
        let dir = tempdir().unwrap();
        let sink = LocalSink::new(dir.path()).unwrap();

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
    fn test_local_sink_batch_operations() {
        let dir = tempdir().unwrap();
        let sink = LocalSink::new(dir.path()).unwrap();

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
    fn test_local_sink_special_characters_in_path() {
        let dir = tempdir().unwrap();
        let sink = LocalSink::new(dir.path()).unwrap();

        sink.execute(WriteOperation::WriteFile {
            path: PathBuf::from("file with spaces.txt"),
            data: b"content".to_vec(),
        })
        .unwrap();

        assert!(dir.path().join("file with spaces.txt").exists());
    }

    #[test]
    fn test_local_sink_unicode_content() {
        let dir = tempdir().unwrap();
        let sink = LocalSink::new(dir.path()).unwrap();

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
        let sink = LocalSink::new(dir.path()).unwrap();

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
        let sink = LocalSink::new(dir.path()).unwrap();

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
        let sink = LocalSink::new(dir.path()).unwrap();

        // Create a valid RGB frame (64x48 = small but valid)
        let width = 64u32;
        let height = 48u32;
        let rgb_data = vec![128u8; (width * height * 3) as usize];
        let frame = ImageData::new_rgb(width, height, rgb_data).unwrap();

        let output_path = std::path::PathBuf::from("output.mp4");
        let result =
            sink.encode_and_write_video(&[frame], &output_path, &VideoEncoderConfig::default());

        assert!(result.is_ok());
        // Video file should be created
        assert!(dir.path().join("output.mp4").exists());
    }

    #[test]
    fn test_new_creates_directory() {
        let dir = tempdir().unwrap();
        let subdir = dir.path().join("nested/subdir");

        // Directory doesn't exist yet
        assert!(!subdir.exists());

        // Creating LocalSink should create the directory
        let sink = LocalSink::new(&subdir);
        assert!(sink.is_ok());
        assert!(subdir.exists());
    }

    #[test]
    fn test_write_metadata_with_complex_json() {
        let dir = tempdir().unwrap();
        let sink = LocalSink::new(dir.path()).unwrap();

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
