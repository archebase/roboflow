pub mod common;
pub mod config;
pub mod hardware;
pub mod lerobot;
pub mod image;
pub mod streaming;
pub mod pipeline;

pub use common::{AlignedFrame, AudioData, DatasetWriter, ImageData, WriterStats};
pub use config::{OutputConfig, OutputFormat};
pub use pipeline::{PipelineConfig, PipelineExecutor, PipelineStats};
pub use image::{
    DecodedImage, ImageDecoderBackend, ImageDecoderConfig, ImageDecoderFactory, ImageError,
    ImageFormat, decode_compressed_image,
};

use roboflow_core::Result;
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatasetFormat {
    Lerobot,
}

#[derive(Debug, Clone)]
pub enum DatasetConfig {
    Lerobot(lerobot::LerobotConfig),
}

impl DatasetConfig {
    pub fn lerobot(config: lerobot::LerobotConfig) -> Self {
        Self::Lerobot(config)
    }

    pub fn from_file(path: impl AsRef<Path>, format: DatasetFormat) -> Result<Self> {
        match format {
            DatasetFormat::Lerobot => {
                let config = lerobot::LerobotConfig::from_file(path)?;
                Ok(Self::Lerobot(config))
            }
        }
    }

    pub fn from_toml(toml_str: &str, format: DatasetFormat) -> Result<Self> {
        match format {
            DatasetFormat::Lerobot => {
                let config = lerobot::LerobotConfig::from_toml(toml_str)?;
                Ok(Self::Lerobot(config))
            }
        }
    }

    pub fn new(
        format: DatasetFormat,
        name: impl Into<String>,
        fps: u32,
        robot_type: Option<String>,
    ) -> Self {
        let name = name.into();
        match format {
            DatasetFormat::Lerobot => Self::Lerobot(lerobot::LerobotConfig {
                dataset: lerobot::DatasetConfig {
                    base: common::DatasetBaseConfig {
                        name,
                        fps,
                        robot_type,
                    },
                    env_type: None,
                },
                mappings: Vec::new(),
                video: Default::default(),
                annotation_file: None,
                flushing: Default::default(),
                streaming: Default::default(),
            }),
        }
    }

    pub fn format(&self) -> DatasetFormat {
        match self {
            Self::Lerobot(_) => DatasetFormat::Lerobot,
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Self::Lerobot(c) => &c.dataset.base.name,
        }
    }

    pub fn fps(&self) -> u32 {
        match self {
            Self::Lerobot(c) => c.dataset.base.fps,
        }
    }

    pub fn robot_type(&self) -> Option<&str> {
        match self {
            Self::Lerobot(c) => c.dataset.base.robot_type.as_deref(),
        }
    }

    pub fn as_lerobot(&self) -> Option<&lerobot::LerobotConfig> {
        match self {
            Self::Lerobot(c) => Some(c),
        }
    }
}

pub fn create_writer(
    output_dir: impl AsRef<Path>,
    storage: Option<&std::sync::Arc<dyn roboflow_storage::Storage>>,
    output_prefix: Option<&str>,
    config: &DatasetConfig,
) -> Result<Box<dyn DatasetWriter>> {
    match config {
        DatasetConfig::Lerobot(lerobot_config) => {
            use crate::formats::lerobot::LerobotWriter;
            if let (Some(storage), Some(prefix)) = (storage, output_prefix) {
                let writer = LerobotWriter::new(
                    std::sync::Arc::clone(storage),
                    prefix.to_string(),
                    output_dir,
                    lerobot_config.clone(),
                )?;
                Ok(Box::new(writer))
            } else {
                let writer = LerobotWriter::new_local(output_dir, lerobot_config.clone())?;
                Ok(Box::new(writer))
            }
        }
    }
}
