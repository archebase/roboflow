// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Dataset format definition for type-safe pipeline execution.

use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Dataset format definition with associated types.
pub trait DatasetFormat: Send + Sync + 'static + Debug {
    const NAME: &'static str;
    const VERSION: &'static str;
    type Writer: EpisodeWriter;
    type MetadataGenerator: MetadataGenerator;
    type Config: FormatConfig;
    type EpisodeMetadata: Serialize + for<'de> Deserialize<'de> + Send + Sync;
    type DatasetMetadata: Serialize + for<'de> Deserialize<'de> + Send + Sync;
}

pub trait EpisodeWriter: Send + Sync + Debug {
    fn write_frame(&mut self, frame: &Frame) -> Result<(), WriterError>;
    fn finalize(self) -> Result<EpisodeMetadata, WriterError>;
}

pub trait MetadataGenerator: Send + Sync + Debug {
    fn generate_dataset_metadata(
        &self,
        episodes: &[EpisodeMetadata],
    ) -> Result<DatasetMetadata, MetadataError>;
}

pub trait FormatConfig: Send + Sync + Debug + Clone + 'static {
    fn validate(&self) -> Result<(), ConfigError>;
}

#[derive(Debug, Clone)]
pub struct Frame {
    pub timestamp_ns: u64,
    pub observations: Vec<(String, Observation)>,
    pub state: Vec<(String, serde_json::Value)>,
    pub action: Vec<(String, serde_json::Value)>,
}

#[derive(Debug, Clone)]
pub struct Observation {
    pub data: Vec<u8>,
    pub width: u32,
    pub height: u32,
    pub is_depth: bool,
}

#[derive(Debug, Clone)]
pub struct EpisodeMetadata {
    pub episode_index: usize,
    pub frame_count: usize,
    pub duration_secs: f64,
    pub video_paths: Vec<(String, String)>,
    pub parquet_path: String,
}

#[derive(Debug, Clone)]
pub struct DatasetMetadata {
    pub name: String,
    pub total_episodes: usize,
    pub total_frames: usize,
    pub total_duration_secs: f64,
    pub features: Vec<Feature>,
}

#[derive(Debug, Clone)]
pub struct Feature {
    pub name: String,
    pub dtype: String,
    pub shape: Option<Vec<usize>>,
}

#[derive(Debug, thiserror::Error)]
pub enum WriterError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Invalid frame: {0}")]
    InvalidFrame(String),
}

#[derive(Debug, thiserror::Error)]
pub enum MetadataError {
    #[error("Invalid episode metadata: {0}")]
    InvalidEpisode(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("Invalid configuration: {0}")]
    Invalid(String),
}

// Format implementations module - defines placeholder types
mod formats {
    use super::*;

    #[derive(Debug)]
    pub struct LeRobotWriter;
    impl EpisodeWriter for LeRobotWriter {
        fn write_frame(&mut self, _frame: &Frame) -> Result<(), WriterError> {
            unimplemented!()
        }
        fn finalize(self) -> Result<EpisodeMetadata, WriterError> {
            unimplemented!()
        }
    }

    #[derive(Debug)]
    pub struct LeRobotMetadataGenerator;
    impl MetadataGenerator for LeRobotMetadataGenerator {
        fn generate_dataset_metadata(
            &self,
            _episodes: &[EpisodeMetadata],
        ) -> Result<DatasetMetadata, MetadataError> {
            unimplemented!()
        }
    }

    #[derive(Debug, Clone)]
    pub struct LeRobotConfig;
    impl FormatConfig for LeRobotConfig {
        fn validate(&self) -> Result<(), ConfigError> {
            Ok(())
        }
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    pub struct LeRobotEpisodeMeta;

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    pub struct LeRobotDatasetMeta;

    #[derive(Debug)]
    pub struct RLDSWriter;
    impl EpisodeWriter for RLDSWriter {
        fn write_frame(&mut self, _frame: &Frame) -> Result<(), WriterError> {
            unimplemented!()
        }
        fn finalize(self) -> Result<EpisodeMetadata, WriterError> {
            unimplemented!()
        }
    }

    #[derive(Debug)]
    pub struct RLDSMetadataGenerator;
    impl MetadataGenerator for RLDSMetadataGenerator {
        fn generate_dataset_metadata(
            &self,
            _episodes: &[EpisodeMetadata],
        ) -> Result<DatasetMetadata, MetadataError> {
            unimplemented!()
        }
    }

    #[derive(Debug, Clone)]
    pub struct RLDSConfig;
    impl FormatConfig for RLDSConfig {
        fn validate(&self) -> Result<(), ConfigError> {
            Ok(())
        }
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    pub struct RLDSEpisodeMeta;

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    pub struct RLDSDatasetMeta;
}

#[derive(Debug, Clone)]
pub struct LeRobotV21;

impl DatasetFormat for LeRobotV21 {
    const NAME: &'static str = "lerobot";
    const VERSION: &'static str = "v2.1";
    type Writer = formats::LeRobotWriter;
    type MetadataGenerator = formats::LeRobotMetadataGenerator;
    type Config = formats::LeRobotConfig;
    type EpisodeMetadata = formats::LeRobotEpisodeMeta;
    type DatasetMetadata = formats::LeRobotDatasetMeta;
}

#[derive(Debug, Clone)]
pub struct RLDS;

impl DatasetFormat for RLDS {
    const NAME: &'static str = "rlds";
    const VERSION: &'static str = "0.1.0";
    type Writer = formats::RLDSWriter;
    type MetadataGenerator = formats::RLDSMetadataGenerator;
    type Config = formats::RLDSConfig;
    type EpisodeMetadata = formats::RLDSEpisodeMeta;
    type DatasetMetadata = formats::RLDSDatasetMeta;
}
