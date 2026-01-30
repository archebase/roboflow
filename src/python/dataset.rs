// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Python bindings for dataset conversion.
//!
//! Provides a Python API for converting robotics data (MCAP, ROS bags)
//! to ML dataset formats (KPS, LeRobot).

use pyo3::exceptions::{PyIOError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use std::path::{Path, PathBuf};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::core::Result;
use crate::dataset::common::{ProgressReceiver, ProgressSender, ProgressUpdate, WriterStats};
use crate::dataset::{DatasetConfig as RustDatasetConfig, DatasetFormat};

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
        | crate::RoboflowError::Unsupported { .. } => PyValueError::new_err(error.to_string()),

        // Codec errors (including I/O from formats) -> PyIOError
        crate::RoboflowError::CodecError { .. } => PyIOError::new_err(error.to_string()),

        // Encoding/transform errors -> PyRuntimeError
        crate::RoboflowError::EncodeError { .. }
        | crate::RoboflowError::TransformError { .. }
        | crate::RoboflowError::InvariantViolation { .. } => {
            PyRuntimeError::new_err(error.to_string())
        }

        // Other errors -> check message content
        _ => {
            let msg = error.to_string();
            if msg.contains("Failed to open")
                || msg.contains("No such file")
                || msg.contains("not found")
            {
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
            self.stats.frames_written, self.stats.images_encoded, self.stats.duration_sec
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
    /// The type of progress update variant.
    ///
    /// One of: "started", "frame_progress", "video_progress", "parquet_progress",
    /// "warning", "error", "completed"
    #[getter]
    fn variant_type(&self) -> &'static str {
        match &self.update {
            ProgressUpdate::Started { .. } => "started",
            ProgressUpdate::FrameProgress { .. } => "frame_progress",
            ProgressUpdate::VideoProgress { .. } => "video_progress",
            ProgressUpdate::ParquetProgress { .. } => "parquet_progress",
            ProgressUpdate::Warning { .. } => "warning",
            ProgressUpdate::Error { .. } => "error",
            ProgressUpdate::Completed { .. } => "completed",
        }
    }

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
            ProgressUpdate::VideoProgress {
                camera,
                frame,
                total,
            } => {
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
            ProgressUpdate::ParquetProgress { .. } => "ProgressUpdate(Parquet)".to_string(),
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
    fn new(receiver: ProgressReceiver, thread: JoinHandle<Result<WriterStats>>) -> Self {
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
    fn is_complete(&self) -> bool {
        // Job is complete if we have cached stats OR thread is gone
        self.stats.is_some() || self.thread.is_none()
    }

    /// Check if the conversion is still running.
    fn is_running(&self) -> bool {
        !self.is_complete() && !self.cancelled
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
    /// Returns True if cancellation was requested, False if already complete.
    fn cancel(&mut self) -> bool {
        if self.is_complete() {
            return false;
        }
        self.cancelled = true;
        true
    }

    /// Wait for the conversion to complete.
    ///
    /// Blocks until the conversion finishes or fails.
    /// If timeout is provided (in seconds), returns None if the timeout expires.
    /// Returns the final statistics.
    #[pyo3(signature = (timeout=None))]
    fn wait(&mut self, py: Python<'_>, timeout: Option<f64>) -> PyResult<Option<PyDatasetStats>> {
        // If we already have stats, return them
        if let Some(stats) = &self.stats {
            return Ok(Some(PyDatasetStats {
                stats: stats.clone(),
                warning_count: self.warning_count,
                error_count: self.error_count,
            }));
        }

        // If timeout specified, poll until complete or timeout
        if let Some(timeout_secs) = timeout {
            let start = std::time::Instant::now();
            let timeout_duration = Duration::from_secs_f64(timeout_secs);

            py.allow_threads(|| {
                while start.elapsed() < timeout_duration {
                    if self.stats.is_some() || self.thread.is_none() {
                        break;
                    }
                    thread::sleep(Duration::from_millis(50));
                }
            });

            // Check if complete
            if self.stats.is_some() {
                return Ok(Some(PyDatasetStats {
                    stats: self.stats.clone().unwrap(),
                    warning_count: self.warning_count,
                    error_count: self.error_count,
                }));
            }

            // If thread is done, get result
            if self
                .thread
                .as_ref()
                .map(|t| t.is_finished())
                .unwrap_or(true)
            {
                if let Some(thread) = self.thread.take() {
                    let result = py.allow_threads(|| {
                        thread
                            .join()
                            .map_err(|e| {
                                if let Some(msg) = e.downcast_ref::<String>() {
                                    PyRuntimeError::new_err(format!("Thread panic: {}", msg))
                                } else if let Some(msg) = e.downcast_ref::<&str>() {
                                    PyRuntimeError::new_err(format!("Thread panic: {}", msg))
                                } else {
                                    PyRuntimeError::new_err(format!("Thread panic: {:?}", e))
                                }
                            })?
                            .map_err(dataset_error_to_py)
                    })?;
                    self.stats = Some(result.clone());
                    return Ok(Some(PyDatasetStats {
                        stats: result,
                        warning_count: self.warning_count,
                        error_count: self.error_count,
                    }));
                }
            }

            // Timeout expired
            return Ok(None);
        }

        // No timeout - block until complete
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
            Some(PyDatasetStats {
                stats,
                warning_count: self.warning_count,
                error_count: self.error_count,
            })
        })
    }

    /// Wait for the conversion with progress updates.
    ///
    /// Polls for progress while waiting. Returns when complete.
    fn wait_with_progress(&mut self, py: Python<'_>) -> PyResult<Option<PyDatasetStats>> {
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

        self.wait(py, None)
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
// Dataset Config (Unified Python wrapper)
// =============================================================================

/// Unified dataset configuration for converting MCAP/ROS bag to KPS or LeRobot format.
///
/// # Example
///
/// ```python
/// import roboflow
///
/// # Create programmatically
/// config = roboflow.DatasetConfig("kps", fps=30, name="my_dataset")
///
/// # Load from TOML file
/// config = roboflow.DatasetConfig.from_file("config.toml", format="kps")
///
/// # Create converter and convert
/// converter = roboflow.DatasetConverter.create("/output", config)
/// stats = converter.convert("input.mcap")
/// ```
#[pyclass(name = "DatasetConfig")]
#[derive(Debug, Clone)]
pub struct PyDatasetConfig {
    /// Internal unified Rust config
    pub(crate) config: RustDatasetConfig,

    /// Maximum frames to process (None = unlimited)
    pub(crate) max_frames: Option<usize>,
}

#[pymethods]
impl PyDatasetConfig {
    /// Create a new DatasetConfig.
    ///
    /// Args:
    ///     format: Output format ("lerobot" or "kps")
    ///     fps: Frames per second
    ///     name: Dataset name
    ///     robot_type: Robot type (optional)
    #[new]
    #[pyo3(signature = (format, *, fps, name, robot_type=None))]
    fn new(format: String, fps: u32, name: String, robot_type: Option<String>) -> PyResult<Self> {
        let ds_format = match format.as_str() {
            "kps" => DatasetFormat::Kps,
            "lerobot" => DatasetFormat::Lerobot,
            _ => {
                return Err(PyValueError::new_err(format!(
                    "Invalid format '{}'. Must be 'lerobot' or 'kps'",
                    format
                )));
            }
        };

        Ok(Self {
            config: RustDatasetConfig::new(ds_format, name, fps, robot_type),
            max_frames: None,
        })
    }

    /// Load configuration from a TOML file.
    ///
    /// Args:
    ///     path: Path to the TOML config file
    ///     format: Output format ("lerobot" or "kps")
    #[staticmethod]
    #[pyo3(signature = (path, format="kps"))]
    fn from_file(path: String, format: &str) -> PyResult<Self> {
        let ds_format = match format {
            "kps" => DatasetFormat::Kps,
            "lerobot" => DatasetFormat::Lerobot,
            _ => {
                return Err(PyValueError::new_err(format!(
                    "Invalid format '{}'. Must be 'lerobot' or 'kps'",
                    format
                )));
            }
        };

        RustDatasetConfig::from_file(&path, ds_format)
            .map(|config| Self {
                config,
                max_frames: None,
            })
            .map_err(|e| PyIOError::new_err(format!("Failed to load config: {}", e)))
    }

    /// Load configuration from a TOML string.
    ///
    /// Args:
    ///     toml_str: TOML configuration string
    ///     format: Output format ("lerobot" or "kps")
    #[staticmethod]
    #[pyo3(signature = (toml_str, format="kps"))]
    fn from_toml(toml_str: String, format: &str) -> PyResult<Self> {
        let ds_format = match format {
            "kps" => DatasetFormat::Kps,
            "lerobot" => DatasetFormat::Lerobot,
            _ => {
                return Err(PyValueError::new_err(format!(
                    "Invalid format '{}'. Must be 'lerobot' or 'kps'",
                    format
                )));
            }
        };

        RustDatasetConfig::from_toml(&toml_str, ds_format)
            .map(|config| Self {
                config,
                max_frames: None,
            })
            .map_err(|e| PyValueError::new_err(format!("Failed to parse TOML: {}", e)))
    }

    /// Set the maximum frames to process.
    fn with_max_frames(mut slf: PyRefMut<'_, Self>, max_frames: usize) -> PyResult<()> {
        if max_frames == 0 {
            return Err(PyValueError::new_err(
                "max_frames must be greater than 0 (use None for unlimited)",
            ));
        }
        slf.max_frames = Some(max_frames);
        Ok(())
    }

    /// Get the dataset name.
    #[getter]
    fn name(&self) -> String {
        self.config.name().to_string()
    }

    /// Get the FPS.
    #[getter]
    fn fps(&self) -> u32 {
        self.config.fps()
    }

    /// Get the robot type.
    #[getter]
    fn robot_type(&self) -> Option<String> {
        self.config.robot_type().map(|s| s.to_string())
    }

    /// Get the format.
    #[getter]
    fn format(&self) -> String {
        match self.config.format() {
            DatasetFormat::Kps => "kps".to_string(),
            DatasetFormat::Lerobot => "lerobot".to_string(),
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "DatasetConfig(format={}, name={}, fps={})",
            self.format(),
            self.name(),
            self.fps()
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
/// config = roboflow.DatasetConfig.from_file("config.toml", format="kps")
/// converter = roboflow.DatasetConverter.create("/output", config)
/// stats = converter.convert("input.mcap")
/// ```
#[pyclass(name = "DatasetConverter")]
pub struct PyDatasetConverter {
    /// Output directory
    output_dir: PathBuf,

    /// Unified dataset config
    config: RustDatasetConfig,

    /// Maximum frames to process (None = unlimited)
    max_frames: Option<usize>,
}

#[pymethods]
impl PyDatasetConverter {
    /// Create a dataset converter from a DatasetConfig.
    ///
    /// Args:
    ///     output_dir: Directory to write output files
    ///     config: DatasetConfig with format and settings
    #[staticmethod]
    pub fn create(output_dir: String, config: &PyDatasetConfig) -> PyResult<Self> {
        Ok(Self {
            output_dir: PathBuf::from(output_dir),
            config: config.config.clone(),
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
    pub fn convert(&self, py: Python<'_>, input_path: String) -> PyResult<PyDatasetStats> {
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
    /// Note: File existence is not checked here - errors will be reported through
    /// the progress channel so the job can be properly monitored.
    fn convert_async(&mut self, _py: Python<'_>, input_path: String) -> PyResult<PyConversionJob> {
        let input_path = PathBuf::from(&input_path);

        // Create progress channel with larger capacity for TB-scale conversions
        let (sender, receiver) = ProgressSender::new(1000);

        // Clone the data we need for the thread
        let output_dir = self.output_dir.clone();
        let config = self.config.clone();
        let max_frames = self.max_frames;

        // Spawn conversion thread
        let thread = thread::spawn(move || {
            Self::convert_with_progress(input_path, output_dir, config, max_frames, sender)
        });

        Ok(PyConversionJob::new(receiver, thread))
    }

    fn __repr__(&self) -> String {
        format!(
            "DatasetConverter(output_dir={}, format={:?})",
            self.output_dir.display(),
            self.config.format()
        )
    }
}

impl PyDatasetConverter {
    fn convert_internal(&self, input_path: &Path) -> Result<WriterStats> {
        // This is a simplified synchronous conversion
        // In production, this would use the progress-aware version
        use crate::pipeline::dataset_converter::DatasetConverter;

        match &self.config {
            RustDatasetConfig::Kps(kps_config) => {
                let converter = DatasetConverter::new_kps(&self.output_dir, kps_config.clone());
                converter.convert(input_path).map(|s| s.into_writer_stats())
            }
            RustDatasetConfig::Lerobot(_) => Err(crate::RoboflowError::unsupported(
                "LeRobot dataset conversion",
            )),
        }
    }

    fn convert_with_progress(
        input_path: PathBuf,
        output_dir: PathBuf,
        config: RustDatasetConfig,
        max_frames: Option<usize>,
        progress: ProgressSender,
    ) -> Result<WriterStats> {
        // Send started notification
        progress.started(input_path.to_string_lossy().to_string(), None);

        // Check if input file exists
        if !input_path.exists() {
            let msg = format!("Input file not found: {}", input_path.display());
            progress.error(msg.clone(), true);
            return Err(crate::RoboflowError::CodecError { message: msg });
        }

        // Convert based on format
        let stats = match config {
            RustDatasetConfig::Kps(kps_config) => {
                use crate::pipeline::dataset_converter::DatasetConverter;
                let mut converter = DatasetConverter::new_kps(&output_dir, kps_config);
                if let Some(max) = max_frames {
                    converter = converter.with_max_frames(max);
                }
                converter.convert(&input_path)?.into_writer_stats()
            }
            RustDatasetConfig::Lerobot(_) => {
                progress.error(
                    "LeRobot dataset conversion is not yet supported. Please use KPS format."
                        .to_string(),
                    false,
                );
                return Err(crate::RoboflowError::unsupported(
                    "LeRobot dataset conversion",
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

// =============================================================================
// Module exports
// =============================================================================
