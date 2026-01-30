// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Python bindings for dataset conversion.
//!
//! Provides a Python API for converting robotics data (MCAP, ROS bags)
//! to ML dataset formats (KPS, LeRobot).

use pyo3::exceptions::{PyIOError, PyNotImplementedError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use std::path::{Path, PathBuf};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::core::Result;
use crate::dataset::{
    DatasetFormat, kps::config::KpsConfig, lerobot::config::LerobotConfig,
};
use crate::dataset::common::{
    ProgressReceiver, ProgressSender, ProgressUpdate, WriterStats,
};

// =============================================================================
// Error conversion
// =============================================================================

fn dataset_error_to_py(error: crate::RoboflowError) -> PyErr {
    // Log the error for audit trail with structured fields
    tracing::error!(
        error_code = error.code(),
        error_category = error.category().as_str(),
        error_message = %error,
        "Dataset conversion error"
    );

    // Classify errors by variant for proper Python exception types
    match &error {
        // I/O and file-related errors -> PyIOError
        crate::RoboflowError::CodecError { message }
            if message.contains("No such file")
                || message.contains("not found")
                || message.contains("Failed to open")
                || message.contains("cannot open")
                || message.contains("does not exist") =>
        {
            PyIOError::new_err(error.to_string())
        }

        // Parse, schema, and validation errors -> PyValueError
        crate::RoboflowError::ParseError { .. }
        | crate::RoboflowError::InvalidSchema { .. }
        | crate::RoboflowError::TypeNotFound { .. }
        | crate::RoboflowError::FieldDecodeError { .. }
        | crate::RoboflowError::Unsupported { .. } =>
        {
            PyValueError::new_err(error.to_string())
        }

        // Codec errors (including I/O from formats) -> PyIOError
        crate::RoboflowError::CodecError { .. } =>
        {
            PyIOError::new_err(error.to_string())
        }

        // Encoding/transform errors -> PyRuntimeError
        crate::RoboflowError::EncodeError { .. }
        | crate::RoboflowError::TransformError { .. }
        | crate::RoboflowError::InvariantViolation { .. } =>
        {
            PyRuntimeError::new_err(error.to_string())
        }

        // Other errors -> check message content
        _ => {
            let msg = error.to_string();
            if msg.contains("Failed to open") || msg.contains("No such file") || msg.contains("not found") {
                PyIOError::new_err(msg)
            } else if msg.contains("Invalid") || msg.contains("parse") {
                PyValueError::new_err(msg)
            } else {
                PyRuntimeError::new_err(msg)
            }
        }
    }
}

// =============================================================================
// Dataset Statistics
// =============================================================================

/// Statistics from a dataset conversion operation.
///
/// Returned after conversion completes to provide metrics about the operation.
#[pyclass(name = "DatasetStats")]
#[derive(Debug, Clone)]
pub struct PyDatasetStats {
    pub(crate) stats: WriterStats,
    /// Number of warnings during conversion
    pub(crate) warning_count: usize,
    /// Number of errors during conversion
    pub(crate) error_count: usize,
}

#[pymethods]
impl PyDatasetStats {
    /// Number of frames written to the dataset.
    #[getter]
    fn frames_written(&self) -> usize {
        self.stats.frames_written
    }

    /// Number of images encoded/written.
    #[getter]
    fn images_encoded(&self) -> usize {
        self.stats.images_encoded
    }

    /// Number of state/action records written.
    #[getter]
    fn state_records(&self) -> usize {
        self.stats.state_records
    }

    /// Size of output data in bytes.
    #[getter]
    fn output_bytes(&self) -> u64 {
        self.stats.output_bytes
    }

    /// Processing duration in seconds.
    #[getter]
    fn duration_seconds(&self) -> f64 {
        self.stats.duration_sec
    }

    /// Number of warnings during conversion.
    #[getter]
    fn warning_count(&self) -> usize {
        self.warning_count
    }

    /// Number of errors during conversion.
    #[getter]
    fn error_count(&self) -> usize {
        self.error_count
    }

    /// Throughput in frames per second.
    #[getter]
    fn fps(&self) -> f64 {
        self.stats.fps()
    }

    /// Throughput in MB/s.
    #[getter]
    fn mb_per_sec(&self) -> f64 {
        self.stats.mb_per_sec()
    }

    fn __repr__(&self) -> String {
        format!(
            "DatasetStats(frames_written={}, images_encoded={}, duration_sec={:.2})",
            self.stats.frames_written,
            self.stats.images_encoded,
            self.stats.duration_sec
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

// =============================================================================
// Progress Update
// =============================================================================

/// Progress update from a running conversion.
#[pyclass(name = "ProgressUpdate")]
#[derive(Debug, Clone)]
pub struct PyProgressUpdate {
    pub(crate) update: ProgressUpdate,
}

#[pymethods]
impl PyProgressUpdate {
    /// Progress percentage (0-100), if available.
    #[getter]
    fn percent_complete(&self) -> Option<f64> {
        self.update.percent_complete()
    }

    /// Number of frames processed so far.
    #[getter]
    fn frames_processed(&self) -> Option<u64> {
        self.update.frames_processed()
    }

    /// Estimated total frames, if known.
    #[getter]
    fn estimated_total(&self) -> Option<u64> {
        self.update.estimated_total()
    }

    /// Processing speed in frames per second.
    #[getter]
    fn fps(&self) -> Option<f64> {
        match &self.update {
            ProgressUpdate::FrameProgress { fps, .. } => Some(*fps),
            _ => None,
        }
    }

    /// Estimated time remaining in seconds.
    #[getter]
    fn eta_seconds(&self) -> Option<u64> {
        match &self.update {
            ProgressUpdate::FrameProgress { eta, .. } => Some(eta.as_secs()),
            _ => None,
        }
    }

    /// Human-readable ETA string.
    #[getter]
    fn eta(&self) -> Option<String> {
        match &self.update {
            ProgressUpdate::FrameProgress { eta, .. } => {
                let secs = eta.as_secs();
                if secs > 3600 {
                    Some(format!("{}h {}m", secs / 3600, (secs % 3600) / 60))
                } else if secs > 60 {
                    Some(format!("{}m {}s", secs / 60, secs % 60))
                } else {
                    Some(format!("{}s", secs))
                }
            }
            _ => None,
        }
    }

    /// Whether the conversion is complete.
    #[getter]
    fn is_complete(&self) -> bool {
        self.update.is_complete()
    }

    /// Whether this update contains an error.
    #[getter]
    fn is_error(&self) -> bool {
        self.update.is_error()
    }

    /// Whether this update contains a warning.
    #[getter]
    fn is_warning(&self) -> bool {
        self.update.is_warning()
    }

    /// Error message, if this is an error update.
    #[getter]
    fn error_message(&self) -> Option<String> {
        match &self.update {
            ProgressUpdate::Error { message, .. } => Some(message.clone()),
            _ => None,
        }
    }

    /// Warning message, if this is a warning update.
    #[getter]
    fn warning_message(&self) -> Option<String> {
        match &self.update {
            ProgressUpdate::Warning { message, .. } => Some(message.clone()),
            _ => None,
        }
    }

    fn __repr__(&self) -> String {
        match &self.update {
            ProgressUpdate::Started { input_file, .. } => {
                format!("ProgressUpdate(Started, file={})", input_file)
            }
            ProgressUpdate::FrameProgress {
                frames_processed,
                estimated_total,
                fps,
                ..
            } => {
                format!(
                    "ProgressUpdate(Frame: {}/{}, {:.1} fps)",
                    frames_processed, estimated_total, fps
                )
            }
            ProgressUpdate::VideoProgress { camera, frame, total } => {
                format!("ProgressUpdate(Video: {} - {}/{})", camera, frame, total)
            }
            ProgressUpdate::Completed { stats } => {
                format!("ProgressUpdate(Completed, {} frames)", stats.frames_written)
            }
            ProgressUpdate::Error { message, .. } => {
                format!("ProgressUpdate(Error: {})", message)
            }
            ProgressUpdate::Warning { message, .. } => {
                format!("ProgressUpdate(Warning: {})", message)
            }
            ProgressUpdate::ParquetProgress { .. } => {
                "ProgressUpdate(Parquet)".to_string()
            }
        }
    }
}

// =============================================================================
// Conversion Job
// =============================================================================

/// A running dataset conversion job.
///
/// Use this to monitor progress and wait for completion.
#[pyclass(name = "ConversionJob")]
pub struct PyConversionJob {
    receiver: ProgressReceiver,
    thread: Option<JoinHandle<Result<WriterStats>>>,
    /// Cached stats after completion
    stats: Option<WriterStats>,
    /// Number of warnings received
    warning_count: usize,
    /// Number of errors received
    error_count: usize,
    /// Cancellation flag
    cancelled: bool,
}

impl PyConversionJob {
    fn new(
        receiver: ProgressReceiver,
        thread: JoinHandle<Result<WriterStats>>,
    ) -> Self {
        Self {
            receiver,
            thread: Some(thread),
            stats: None,
            warning_count: 0,
            error_count: 0,
            cancelled: false,
        }
    }
}

#[pymethods]
impl PyConversionJob {
    /// Check if the conversion is complete.
    ///
    /// Returns true if the job finished successfully, failed, or was cancelled.
    #[getter]
    fn is_complete(&self) -> bool {
        // Job is complete if we have cached stats OR thread is gone
        self.stats.is_some() || self.thread.is_none()
    }

    /// Check if the job was cancelled.
    #[getter]
    fn is_cancelled(&self) -> bool {
        self.cancelled
    }

    /// Get the number of warnings received.
    #[getter]
    fn warning_count(&self) -> usize {
        self.warning_count
    }

    /// Get the number of errors received.
    #[getter]
    fn error_count(&self) -> usize {
        self.error_count
    }

    /// Get the latest progress update without blocking.
    ///
    /// Returns None if no update is available.
    fn get_progress(&mut self) -> Option<PyProgressUpdate> {
        if let Some(update) = self.receiver.latest() {
            // Track warnings/errors for final stats
            match &update {
                ProgressUpdate::Warning { .. } => {
                    self.warning_count += 1;
                }
                ProgressUpdate::Error { .. } => {
                    self.error_count += 1;
                }
                _ => {}
            }
            Some(PyProgressUpdate { update })
        } else {
            None
        }
    }

    /// Cancel the running conversion.
    ///
    /// This sets a cancellation flag that the conversion thread should check.
    /// Note: The conversion may not stop immediately if it's in the middle
    /// of processing a frame.
    fn cancel(&mut self) {
        self.cancelled = true;
    }

    /// Wait for the conversion to complete.
    ///
    /// Blocks until the conversion finishes or fails.
    /// Returns the final statistics.
    fn wait(&mut self, py: Python<'_>) -> PyResult<PyDatasetStats> {
        // If we already have stats, return them
        if let Some(stats) = &self.stats {
            return Ok(PyDatasetStats {
                stats: stats.clone(),
                warning_count: self.warning_count,
                error_count: self.error_count,
            });
        }

        // Release GIL while waiting
        let thread = self
            .thread
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("Job already consumed"))?;

        py.allow_threads(|| {
            thread
                .join()
                .map_err(|e| {
                    // Provide better panic information
                    if let Some(msg) = e.downcast_ref::<String>() {
                        PyRuntimeError::new_err(format!("Thread panic: {}", msg))
                    } else if let Some(msg) = e.downcast_ref::<&str>() {
                        PyRuntimeError::new_err(format!("Thread panic: {}", msg))
                    } else {
                        PyRuntimeError::new_err(format!("Thread panic: {:?}", e))
                    }
                })?
                .map_err(dataset_error_to_py)
        })
        .map(|stats| {
            self.stats = Some(stats.clone());
            PyDatasetStats {
                stats,
                warning_count: self.warning_count,
                error_count: self.error_count,
            }
        })
    }

    /// Wait for the conversion with progress updates.
    ///
    /// Polls for progress while waiting. Returns when complete.
    fn wait_with_progress(&mut self, py: Python<'_>) -> PyResult<PyDatasetStats> {
        // Release GIL during polling loop
        py.allow_threads(|| {
            loop {
                if self.is_complete() {
                    break;
                }

                if let Some(update) = self.receiver.latest() {
                    if update.is_complete() {
                        break;
                    }
                    // Print progress to stderr
                    match &update {
                        ProgressUpdate::FrameProgress { eta, .. } => {
                            if let Some(pct) = update.percent_complete() {
                                let secs = eta.as_secs();
                                let eta_str = if secs > 3600 {
                                    format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
                                } else if secs > 60 {
                                    format!("{}m {}s", secs / 60, secs % 60)
                                } else {
                                    format!("{}s", secs)
                                };
                                eprintln!("Progress: {:.1}% - ETA: {}", pct, eta_str);
                            }
                        }
                        _ => {}
                    }
                }

                thread::sleep(Duration::from_millis(250));
            }
        });

        self.wait(py)
    }

    fn __repr__(&self) -> String {
        if self.is_complete() {
            "ConversionJob(complete)".to_string()
        } else if self.cancelled {
            "ConversionJob(cancelled)".to_string()
        } else {
            "ConversionJob(running)".to_string()
        }
    }
}

// =============================================================================
// Dataset Config (Python-friendly wrapper)
// =============================================================================

/// Base dataset configuration.
///
/// This is a Python-friendly wrapper that can represent either KPS or LeRobot config.
#[pyclass(name = "DatasetConfig")]
#[derive(Debug)]
pub struct PyDatasetConfig {
    /// Dataset name
    pub(crate) name: String,

    /// Frames per second
    pub(crate) fps: u32,

    /// Robot type (optional)
    pub(crate) robot_type: Option<String>,

    /// Output format
    pub(crate) format: String,
}

#[pymethods]
impl PyDatasetConfig {
    #[new]
    fn new(
        name: String,
        fps: u32,
        robot_type: Option<String>,
    ) -> Self {
        Self {
            name,
            fps,
            robot_type,
            format: "kps".to_string(),  // Default to KPS as it's the only supported format
        }
    }

    /// Add a topic mapping.
    ///
    /// This method is not currently implemented. Use TOML config files instead.
    fn add_mapping(&self, _topic: String, _feature: String, _mapping_type: String) -> PyResult<()> {
        Err(PyNotImplementedError::new_err(
            "Topic mapping configuration through Python is not yet implemented. \
             Please use a TOML config file with LerobotConfig::from_file() or KpsConfig::from_file()."
        ))
    }

    /// Get the format.
    #[getter]
    fn format(&self) -> String {
        self.format.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "DatasetConfig(name={}, fps={}, format={})",
            self.name, self.fps, self.format
        )
    }
}

// =============================================================================
// Dataset Converter
// =============================================================================

/// Dataset converter for creating ML training datasets from robotics data.
///
/// Supports both KPS and LeRobot output formats.
///
/// # Example
///
/// ```python
/// import roboflow
///
/// config = roboflow.dataset.LerobotConfig.from_file("config.toml")
/// converter = roboflow.dataset.DatasetConverter.create("/output", config)
/// stats = converter.convert("input.bag")
/// ```
#[pyclass(name = "DatasetConverter")]
pub struct PyDatasetConverter {
    /// Output directory
    output_dir: PathBuf,

    /// Dataset format
    format: DatasetFormat,

    /// Lerobot config (if LeRobot format)
    lerobot_config: Option<LerobotConfig>,

    /// KPS config (if KPS format)
    kps_config: Option<KpsConfig>,

    /// Maximum frames to process (None = unlimited)
    max_frames: Option<usize>,
}

#[pymethods]
impl PyDatasetConverter {
    /// Create a LeRobot dataset converter.
    #[staticmethod]
    fn lerobot(
        output_dir: String,
        config: &PyLerobotConfig,
    ) -> PyResult<Self> {
        Ok(Self {
            output_dir: PathBuf::from(output_dir),
            format: DatasetFormat::Lerobot,
            lerobot_config: Some(config.config.clone()),
            kps_config: None,
            max_frames: config.max_frames,
        })
    }

    /// Create a KPS dataset converter.
    #[staticmethod]
    fn kps(
        output_dir: String,
        config: &PyKpsConfig,
    ) -> PyResult<Self> {
        Ok(Self {
            output_dir: PathBuf::from(output_dir),
            format: DatasetFormat::Kps,
            lerobot_config: None,
            kps_config: Some(config.config.clone()),
            max_frames: config.max_frames,
        })
    }

    /// Get the output directory.
    #[getter]
    fn output_dir(&self) -> String {
        self.output_dir.to_string_lossy().to_string()
    }

    /// Convert a single input file to dataset format.
    ///
    /// This is a synchronous operation that blocks until complete.
    /// For progress monitoring during conversion, use `convert_async`.
    fn convert(&self, py: Python<'_>, input_path: String) -> PyResult<PyDatasetStats> {
        let input_path = PathBuf::from(&input_path);
        if !input_path.exists() {
            return Err(PyIOError::new_err(format!(
                "Input file not found: {}",
                input_path.display()
            )));
        }

        let stats = py.allow_threads(|| {
            self.convert_internal(&input_path)
                .map_err(dataset_error_to_py)
        })?;

        Ok(PyDatasetStats {
            stats,
            warning_count: 0,
            error_count: 0,
        })
    }

    /// Start an async conversion job.
    ///
    /// Returns a ConversionJob that can be monitored for progress.
    fn convert_async(
        &mut self,
        _py: Python<'_>,
        input_path: String,
    ) -> PyResult<PyConversionJob> {
        let input_path = PathBuf::from(&input_path);
        if !input_path.exists() {
            return Err(PyIOError::new_err(format!(
                "Input file not found: {}",
                input_path.display()
            )));
        }

        // Create progress channel with larger capacity for TB-scale conversions
        let (sender, receiver) = ProgressSender::new(1000);

        // Clone the data we need for the thread
        let output_dir = self.output_dir.clone();
        let format = self.format;
        let lerobot_config = self.lerobot_config.clone();
        let kps_config = self.kps_config.clone();
        let max_frames = self.max_frames;

        // Spawn conversion thread
        let thread = thread::spawn(move || {
            Self::convert_with_progress(
                input_path,
                output_dir,
                format,
                lerobot_config,
                kps_config,
                max_frames,
                sender,
            )
        });

        Ok(PyConversionJob::new(receiver, thread))
    }

    fn __repr__(&self) -> String {
        format!(
            "DatasetConverter(output_dir={}, format={:?})",
            self.output_dir.display(),
            self.format
        )
    }
}

impl PyDatasetConverter {
    /// Create a LeRobot dataset converter (Rust-side).
    pub fn lerobot_rust(
        output_dir: String,
        config: &PyLerobotConfig,
    ) -> PyResult<Self> {
        Ok(Self {
            output_dir: PathBuf::from(output_dir),
            format: DatasetFormat::Lerobot,
            lerobot_config: Some(config.config.clone()),
            kps_config: None,
            max_frames: config.max_frames,
        })
    }

    /// Create a KPS dataset converter (Rust-side).
    pub fn kps_rust(
        output_dir: String,
        config: &PyKpsConfig,
    ) -> PyResult<Self> {
        Ok(Self {
            output_dir: PathBuf::from(output_dir),
            format: DatasetFormat::Kps,
            lerobot_config: None,
            kps_config: Some(config.config.clone()),
            max_frames: config.max_frames,
        })
    }

    /// Convert a single input file (Rust-side).
    pub fn convert_rust(&self, py: Python<'_>, input_path: String) -> PyResult<PyDatasetStats> {
        let input_path = PathBuf::from(&input_path);
        if !input_path.exists() {
            return Err(PyIOError::new_err(format!(
                "Input file not found: {}",
                input_path.display()
            )));
        }

        let stats = py.allow_threads(|| {
            self.convert_internal(&input_path)
                .map_err(dataset_error_to_py)
        })?;

        Ok(PyDatasetStats {
            stats,
            warning_count: 0,
            error_count: 0,
        })
    }

    fn convert_internal(&self, input_path: &Path) -> Result<WriterStats> {
        // This is a simplified synchronous conversion
        // In production, this would use the progress-aware version
        use crate::pipeline::dataset_converter::DatasetConverter;

        let converter = match (&self.kps_config, &self.lerobot_config) {
            (Some(kps_config), None) => {
                DatasetConverter::new_kps(&self.output_dir, kps_config.clone())
            }
            (None, Some(lerobot_config)) => {
                return Err(crate::RoboflowError::unsupported(
                    "LeRobot dataset conversion",
                ));
            }
            _ => {
                return Err(crate::RoboflowError::parse(
                    "DatasetConverter",
                    "Either KPS or LeRobot config must be set",
                ));
            }
        };

        converter.convert(input_path)
            .map(|s| s.into_writer_stats())
    }

    fn convert_with_progress(
        input_path: PathBuf,
        output_dir: PathBuf,
        format: DatasetFormat,
        lerobot_config: Option<LerobotConfig>,
        kps_config: Option<KpsConfig>,
        max_frames: Option<usize>,
        progress: ProgressSender,
    ) -> Result<WriterStats> {
        // Send started notification
        progress.started(input_path.to_string_lossy().to_string(), None);

        // For now, delegate to the internal conversion
        // TODO: Integrate progress throughout the conversion pipeline

        let stats = match (format, kps_config, lerobot_config) {
            (DatasetFormat::Kps, Some(kps_config), None) => {
                use crate::pipeline::dataset_converter::DatasetConverter;
                let mut converter = DatasetConverter::new_kps(&output_dir, kps_config.clone());
                if let Some(max) = max_frames {
                    converter = converter.with_max_frames(max);
                }
                converter.convert(&input_path)?
                    .into_writer_stats()
            }
            (DatasetFormat::Lerobot, None, Some(_lerobot_config)) => {
                progress.error(
                    "LeRobot dataset conversion is not yet supported. Please use KPS format.".to_string(),
                    false,
                );
                return Err(crate::RoboflowError::unsupported(
                    "LeRobot dataset conversion",
                ));
            }
            _ => {
                return Err(crate::RoboflowError::parse(
                    "DatasetConverter",
                    "Invalid config combination",
                ));
            }
        };

        progress.completed(stats.clone());
        Ok(stats)
    }
}

// Helper extension to convert converter stats to writer stats
trait IntoWriterStats {
    fn into_writer_stats(self) -> WriterStats;
}

impl IntoWriterStats for crate::pipeline::dataset_converter::DatasetConverterStats {
    fn into_writer_stats(self) -> WriterStats {
        WriterStats {
            frames_written: self.frames_written,
            images_encoded: self.images_encoded,
            state_records: 0, // Not tracked
            output_bytes: self.output_bytes,
            duration_sec: self.duration_sec,
        }
    }
}

impl PyLerobotConfig {
    /// Create a new Lerobot config (Rust-side).
    pub fn new_rust(
        name: String,
        fps: u32,
        robot_type: Option<String>,
        env_type: Option<String>,
    ) -> Self {
        use crate::dataset::lerobot::config::{DatasetConfig, LerobotConfig};

        let config = LerobotConfig {
            dataset: DatasetConfig {
                name,
                fps,
                robot_type,
                env_type,
            },
            mappings: Vec::new(),
            video: Default::default(),
            annotation_file: None,
        };

        Self {
            config,
            max_frames: None,
        }
    }
}

// =============================================================================
// KPS Config Wrapper
// =============================================================================

/// LeRobot dataset configuration.
///
/// Load from TOML file or create programmatically.
#[pyclass(name = "LerobotConfig")]
#[derive(Debug, Clone)]
pub struct PyLerobotConfig {
    pub(crate) config: LerobotConfig,
    /// Maximum frames to process (None = unlimited)
    pub(crate) max_frames: Option<usize>,
}

#[pymethods]
impl PyLerobotConfig {
    /// Load configuration from a TOML file.
    #[staticmethod]
    fn from_file(path: String) -> PyResult<Self> {
        LerobotConfig::from_file(&path)
            .map(|config| Self {
                config,
                max_frames: None,
            })
            .map_err(|e| PyIOError::new_err(format!("Failed to load config: {}", e)))
    }

    /// Create a new Lerobot config.
    #[new]
    fn new(
        name: String,
        fps: u32,
        robot_type: Option<String>,
        env_type: Option<String>,
    ) -> Self {
        use crate::dataset::lerobot::config::{DatasetConfig, LerobotConfig};

        let config = LerobotConfig {
            dataset: DatasetConfig {
                name,
                fps,
                robot_type,
                env_type,
            },
            mappings: Vec::new(),
            video: Default::default(),
            annotation_file: None,
        };

        Self {
            config,
            max_frames: None,
        }
    }

    /// Set the maximum frames to process.
    ///
    /// # Errors
    ///
    /// Returns PyValueError if max_frames is 0.
    fn with_max_frames(mut slf: PyRefMut<'_, Self>, max_frames: usize) -> PyResult<()> {
        if max_frames == 0 {
            return Err(PyValueError::new_err(
                "max_frames must be greater than 0 (use None for unlimited)"
            ));
        }
        if max_frames > 1_000_000_000 {
            tracing::warn!(
                max_frames,
                "Unusually large max_frames value - this may take a very long time"
            );
        }
        slf.max_frames = Some(max_frames);
        Ok(())
    }

    /// Get the dataset name.
    #[getter]
    fn name(&self) -> String {
        self.config.dataset.name.clone()
    }

    /// Get the FPS.
    #[getter]
    fn fps(&self) -> u32 {
        self.config.dataset.fps
    }

    /// Get the robot type.
    #[getter]
    fn robot_type(&self) -> Option<String> {
        self.config.dataset.robot_type.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "LerobotConfig(name={}, fps={})",
            self.config.dataset.name,
            self.config.dataset.fps
        )
    }
}

// =============================================================================
// KPS Config Wrapper
// =============================================================================

/// KPS dataset configuration.
///
/// Load from TOML file or create programmatically.
#[pyclass(name = "KpsConfig")]
#[derive(Debug, Clone)]
pub struct PyKpsConfig {
    pub(crate) config: KpsConfig,
    /// Maximum frames to process (None = unlimited)
    pub(crate) max_frames: Option<usize>,
}

#[pymethods]
impl PyKpsConfig {
    /// Load configuration from a TOML file.
    #[staticmethod]
    fn from_file(path: String) -> PyResult<Self> {
        KpsConfig::from_file(&path)
            .map(|config| Self {
                config,
                max_frames: None,
            })
            .map_err(|e| PyIOError::new_err(format!("Failed to load config: {}", e)))
    }

    /// Create a new KPS config.
    #[new]
    fn new(
        name: String,
        fps: u32,
        robot_type: Option<String>,
    ) -> Self {
        use crate::dataset::kps::config::{DatasetConfig, KpsConfig, OutputConfig};

        let config = KpsConfig {
            dataset: DatasetConfig {
                name,
                fps,
                robot_type,
            },
            mappings: Vec::new(),
            output: OutputConfig::default(),
        };

        Self {
            config,
            max_frames: None,
        }
    }

    /// Set the maximum frames to process.
    ///
    /// # Errors
    ///
    /// Returns PyValueError if max_frames is 0.
    fn with_max_frames(mut slf: PyRefMut<'_, Self>, max_frames: usize) -> PyResult<()> {
        if max_frames == 0 {
            return Err(PyValueError::new_err(
                "max_frames must be greater than 0 (use None for unlimited)"
            ));
        }
        if max_frames > 1_000_000_000 {
            tracing::warn!(
                max_frames,
                "Unusually large max_frames value - this may take a very long time"
            );
        }
        slf.max_frames = Some(max_frames);
        Ok(())
    }

    /// Get the dataset name.
    #[getter]
    fn name(&self) -> String {
        self.config.dataset.name.clone()
    }

    /// Get the FPS.
    #[getter]
    fn fps(&self) -> u32 {
        self.config.dataset.fps
    }

    fn __repr__(&self) -> String {
        format!(
            "KpsConfig(name={}, fps={})",
            self.config.dataset.name,
            self.config.dataset.fps
        )
    }
}

impl PyKpsConfig {
    /// Create a new KPS config (Rust-side).
    pub fn new_rust(
        name: String,
        fps: u32,
        robot_type: Option<String>,
    ) -> Self {
        use crate::dataset::kps::config::{DatasetConfig, KpsConfig, OutputConfig};

        let config = KpsConfig {
            dataset: DatasetConfig {
                name,
                fps,
                robot_type,
            },
            mappings: Vec::new(),
            output: OutputConfig::default(),
        };

        Self {
            config,
            max_frames: None,
        }
    }
}

// =============================================================================
// Module exports
// =============================================================================
