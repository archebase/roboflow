pub mod alignment;
pub mod common;
pub mod config;
pub mod dataset_executor;
pub mod lerobot;

// Format modules - always available (stubs for future formats)
pub mod hdf5;
pub mod rlds;
pub mod zarr;

pub use common::{AlignedFrame, AudioData, DatasetWriter, ImageData, WriterStats};
pub use config::{OutputConfig, OutputFormat};
// Re-export image types from media module for backward compatibility
pub use dataset_executor::{
    DatasetPipelineConfig, DatasetPipelineExecutor, DatasetPipelineStats, EpisodeStrategy,
};
pub use roboflow_media::image::{
    DecodedImage, ImageDecoderBackend, ImageDecoderConfig, ImageDecoderFactory, ImageError,
    ImageFormat, decode_compressed_image,
};

use roboflow_core::Result;
use std::path::Path;
use std::sync::LazyLock;

// Auto-register LeRobot format when the registry is first accessed
fn ensure_formats_registered() {
    static REGISTERED: LazyLock<()> = LazyLock::new(|| {
        lerobot::register_lerobot_format();
        tracing::debug!("Registered built-in formats: lerobot");
    });
    LazyLock::force(&REGISTERED);
}

/// Initialize the format registry with built-in formats.
///
/// This is called automatically when needed, but can also be called
/// explicitly to ensure formats are registered early.
pub fn init_formats() {
    ensure_formats_registered();
}

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

/// Create a dataset writer using the format registry.
///
/// This function uses the registry system to create format writers dynamically.
/// The format is determined by the `DatasetConfig` variant.
///
/// # Arguments
///
/// * `output_dir` - Output directory for the dataset
/// * `storage` - Optional storage backend (for cloud storage)
/// * `output_prefix` - Optional prefix within storage (for cloud storage)
/// * `config` - Dataset configuration (format-specific)
///
/// # Returns
///
/// A boxed writer implementing `DatasetWriter`.
///
/// # Example
///
/// ```rust,ignore
/// use roboflow_dataset::formats::{create_writer, DatasetConfig, DatasetFormat};
///
/// let config = DatasetConfig::new(DatasetFormat::Lerobot, "my_dataset", 30, None);
/// let mut writer = create_writer(Path::new("./output"), None, None, &config)?;
/// writer.write_frame(&frame)?;
/// writer.finalize()?;
/// ```
#[allow(deprecated)]
pub fn create_writer(
    output_dir: impl AsRef<Path>,
    storage: Option<&std::sync::Arc<dyn roboflow_storage::Storage>>,
    output_prefix: Option<&str>,
    config: &DatasetConfig,
) -> Result<Box<dyn DatasetWriter>> {
    // Ensure formats are registered
    ensure_formats_registered();

    // Determine format name and get typed config
    let (format_name, format_config) = match config {
        DatasetConfig::Lerobot(lerobot_config) => ("lerobot", lerobot_config.clone()),
    };

    // Build output URL
    let output_url = if let (Some(_s), Some(prefix)) = (storage, output_prefix) {
        prefix.to_string()
    } else {
        output_dir.as_ref().to_string_lossy().to_string()
    };

    // Create context for the registry
    let context = crate::core::traits::FormatContext {
        output_url,
        storage: storage.map(std::sync::Arc::clone),
        base_path: output_dir.as_ref().to_path_buf(),
        num_workers: 4,
    };

    // Convert config to JSON
    let json_config = serde_json::to_value(&format_config).map_err(|e| {
        roboflow_core::RoboflowError::other(format!("Failed to serialize config: {}", e))
    })?;

    // Use the registry to create the writer
    let registry = crate::core::registry::FormatRegistry::global();
    let registry_guard = registry.read().map_err(|e| {
        roboflow_core::RoboflowError::other(format!("Failed to acquire registry lock: {}", e))
    })?;

    let writer = registry_guard.create_writer(format_name, &json_config, &context)?;

    // Wrap FormatWriter to DatasetWriter
    // Since FormatWriter implements all DatasetWriter methods, we can use a wrapper
    Ok(Box::new(FormatWriterAdapter(writer)))
}

/// Adapter to convert a FormatWriter to a DatasetWriter.
///
/// This is needed because the registry returns `Box<dyn FormatWriter>` but
/// `create_writer` returns `Box<dyn DatasetWriter>`.
struct FormatWriterAdapter(Box<dyn crate::core::traits::FormatWriter>);

impl DatasetWriter for FormatWriterAdapter {
    fn write_frame(&mut self, frame: &common::AlignedFrame) -> Result<()> {
        self.0.write_frame(frame)
    }

    fn write_batch(&mut self, frames: &[common::AlignedFrame]) -> Result<()> {
        self.0.write_batch(frames)
    }

    fn finalize(&mut self) -> Result<common::WriterStats> {
        self.0.finalize()
    }

    fn frame_count(&self) -> usize {
        self.0.frame_count()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self.0.as_any()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_dataset_format_lerobot() {
        assert_eq!(DatasetFormat::Lerobot, DatasetFormat::Lerobot);
    }

    #[test]
    fn test_dataset_config_lerobot() {
        let config = lerobot::LerobotConfig {
            dataset: lerobot::DatasetConfig {
                base: common::DatasetBaseConfig {
                    name: "test".to_string(),
                    fps: 30,
                    robot_type: Some("test_robot".to_string()),
                },
                env_type: None,
            },
            mappings: vec![],
            video: Default::default(),
            annotation_file: None,
            flushing: Default::default(),
            streaming: Default::default(),
        };

        let dataset_config = DatasetConfig::lerobot(config);
        assert!(matches!(dataset_config, DatasetConfig::Lerobot(_)));
    }

    #[test]
    fn test_dataset_config_new() {
        let config = DatasetConfig::new(
            DatasetFormat::Lerobot,
            "test_dataset",
            30,
            Some("robot".to_string()),
        );

        assert_eq!(config.format(), DatasetFormat::Lerobot);
        assert_eq!(config.name(), "test_dataset");
        assert_eq!(config.fps(), 30);
        assert_eq!(config.robot_type(), Some("robot"));
    }

    #[test]
    fn test_dataset_config_name_only() {
        let config = DatasetConfig::new(DatasetFormat::Lerobot, "test", 60, None);
        assert_eq!(config.name(), "test");
        assert_eq!(config.fps(), 60);
        assert_eq!(config.robot_type(), None);
    }

    #[test]
    fn test_dataset_config_as_lerobot() {
        let config = DatasetConfig::new(DatasetFormat::Lerobot, "test", 30, None);
        assert!(config.as_lerobot().is_some());
    }

    #[test]
    fn test_create_writer_local() {
        let temp_dir = tempdir().unwrap();
        let config = DatasetConfig::new(DatasetFormat::Lerobot, "test", 30, None);

        let writer = create_writer(temp_dir.path(), None, None, &config);
        assert!(writer.is_ok());
    }

    #[test]
    fn test_dataset_config_from_toml_valid() {
        let toml = r#"
            [dataset]
            name = "test"
            fps = 30
            robot_type = "test_robot"

            [[mappings]]
            topic = "/cam"
            feature = "observation.images.cam"
            type = "image"
        "#;

        let result = DatasetConfig::from_toml(toml, DatasetFormat::Lerobot);
        assert!(result.is_ok());

        let config = result.unwrap();
        assert_eq!(config.name(), "test");
        assert_eq!(config.fps(), 30);
    }

    #[test]
    fn test_dataset_config_from_toml_invalid() {
        let toml = "invalid toml content {{{";
        let result = DatasetConfig::from_toml(toml, DatasetFormat::Lerobot);
        assert!(result.is_err());
    }

    #[test]
    fn test_dataset_config_from_file() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("config.toml");

        std::fs::write(
            &config_path,
            r#"
            [dataset]
            name = "test"
            fps = 30
        "#,
        )
        .unwrap();

        let result = DatasetConfig::from_file(&config_path, DatasetFormat::Lerobot);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().name(), "test");
    }

    #[test]
    fn test_dataset_config_from_file_not_found() {
        let result = DatasetConfig::from_file("/nonexistent/path.toml", DatasetFormat::Lerobot);
        assert!(result.is_err());
    }

    #[test]
    fn test_exports_exist() {
        // Test that all public exports are accessible
        let _: AlignedFrame = AlignedFrame::new(0, 0);
        let _: ImageData = ImageData::new(640, 480, vec![0u8; 640 * 480 * 3]);
    }
}
