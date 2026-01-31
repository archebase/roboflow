// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Python bindings for the fluent pipeline API.
//!
//! Exposes the Rust `Robocodec` fluent API to Python using PyO3.

use pyo3::exceptions::{PyIOError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyList;
use std::mem;
use std::path::{Path, PathBuf};

use robocodec::transform::TransformBuilder;
use roboflow_core::RoboflowError;
use roboflow_pipeline::PipelineReport;
use roboflow_pipeline::fluent::{BatchReport, CompressionPreset, FileResult, Robocodec, RunOutput};
use roboflow_pipeline::hyper::HyperPipelineReport;

// =============================================================================
// Error conversion
// ============================================================================

/// Convert RoboflowError to Python exception.
fn codec_error_to_py(error: RoboflowError) -> PyErr {
    let msg = error.to_string();
    if msg.contains("Failed to open") || msg.contains("No such file") || msg.contains("not found") {
        PyIOError::new_err(msg)
    } else if msg.contains("Invalid") || msg.contains("parse") || msg.contains("unknown") {
        PyValueError::new_err(msg)
    } else {
        PyRuntimeError::new_err(msg)
    }
}

// =============================================================================
// Compression Preset
// ============================================================================

/// Compression preset for the pipeline.
#[pyclass(name = "CompressionPreset")]
#[derive(Debug, Clone, Copy)]
pub struct PyCompressionPreset {
    pub(crate) preset: CompressionPreset,
}

#[pymethods]
impl PyCompressionPreset {
    /// Fast compression (ZSTD level 1).
    #[staticmethod]
    fn fast() -> Self {
        Self {
            preset: CompressionPreset::Fast,
        }
    }

    /// Balanced compression (ZSTD level 3).
    #[staticmethod]
    fn balanced() -> Self {
        Self {
            preset: CompressionPreset::Balanced,
        }
    }

    /// Slow compression (ZSTD level 9).
    #[staticmethod]
    fn slow() -> Self {
        Self {
            preset: CompressionPreset::Slow,
        }
    }

    fn __repr__(&self) -> String {
        match self.preset {
            CompressionPreset::Fast => "CompressionPreset.Fast".to_string(),
            CompressionPreset::Balanced => "CompressionPreset.Balanced".to_string(),
            CompressionPreset::Slow => "CompressionPreset.Slow".to_string(),
        }
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

// =============================================================================
// Transform Builder
// ============================================================================

/// Builder for creating transformation pipelines.
#[pyclass(name = "TransformBuilder")]
pub struct PyTransformBuilder {
    builder: TransformBuilder,
}

#[pymethods]
impl PyTransformBuilder {
    /// Create a new transform builder.
    #[new]
    fn new() -> Self {
        Self {
            builder: TransformBuilder::new(),
        }
    }

    /// Add a topic rename mapping.
    fn with_topic_rename(
        mut slf: PyRefMut<'_, Self>,
        from: String,
        to: String,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let builder = mem::take(&mut slf.builder);
        slf.builder = builder.with_topic_rename(from, to);
        Ok(slf)
    }

    /// Add a wildcard topic rename mapping.
    ///
    /// The wildcard `*` matches any topic suffix. For example:
    /// - `"/foo/*"` → `"/roboflow/*"` will rename all topics starting with "/foo/"
    fn with_topic_rename_wildcard(
        mut slf: PyRefMut<'_, Self>,
        pattern: String,
        target: String,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let builder = mem::take(&mut slf.builder);
        slf.builder = builder.with_topic_rename_wildcard(pattern, target);
        Ok(slf)
    }

    /// Add a type rename mapping.
    fn with_type_rename(
        mut slf: PyRefMut<'_, Self>,
        from: String,
        to: String,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let builder = mem::take(&mut slf.builder);
        slf.builder = builder.with_type_rename(from, to);
        Ok(slf)
    }

    /// Add a wildcard type rename mapping.
    fn with_type_rename_wildcard(
        mut slf: PyRefMut<'_, Self>,
        pattern: String,
        target: String,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let builder = mem::take(&mut slf.builder);
        slf.builder = builder.with_type_rename_wildcard(pattern, target);
        Ok(slf)
    }

    /// Add a topic-specific type rename mapping.
    fn with_topic_type_rename(
        mut slf: PyRefMut<'_, Self>,
        topic: String,
        source_type: String,
        target_type: String,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let builder = mem::take(&mut slf.builder);
        slf.builder = builder.with_topic_type_rename(topic, source_type, target_type);
        Ok(slf)
    }

    /// Build the transform pipeline (returns internal opaque reference).
    ///
    /// This returns an integer that represents the internal transform pipeline.
    /// Pass this to the `transform()` method of Robocodec.
    #[pyo3(signature = ())]
    fn build(mut slf: PyRefMut<'_, Self>) -> PyResult<u64> {
        // Store the pipeline and return a handle
        // We'll use a global registry approach
        let builder = mem::take(&mut slf.builder);
        let pipeline = builder.build();
        TRANSFORM_REGISTRY.with(|registry| {
            let mut registry = registry.borrow_mut();
            let id = registry.next_id;
            registry.next_id = registry.next_id.wrapping_add(1);
            registry.pipelines.insert(id, pipeline);
            Ok(id)
        })
    }
}

// =============================================================================
// Pipeline Reports
// ============================================================================

/// Report from a standard pipeline run.
#[pyclass(name = "PipelineReport")]
#[derive(Clone)]
pub struct PyPipelineReport {
    pub(crate) report: PipelineReport,
}

#[pymethods]
impl PyPipelineReport {
    /// Input file path.
    #[getter]
    fn input_file(&self) -> String {
        self.report.input_file.clone()
    }

    /// Output file path.
    #[getter]
    fn output_file(&self) -> String {
        self.report.output_file.clone()
    }

    /// Input file size in bytes.
    #[getter]
    fn input_size_bytes(&self) -> u64 {
        self.report.input_size_bytes
    }

    /// Output file size in bytes.
    #[getter]
    fn output_size_bytes(&self) -> u64 {
        self.report.output_size_bytes
    }

    /// Duration in seconds.
    #[getter]
    fn duration_seconds(&self) -> f64 {
        self.report.duration.as_secs_f64()
    }

    /// Average throughput in MB/s.
    #[getter]
    fn average_throughput_mb_s(&self) -> f64 {
        self.report.average_throughput_mb_s
    }

    /// Compression ratio (output / input).
    #[getter]
    fn compression_ratio(&self) -> f64 {
        self.report.compression_ratio
    }

    /// Number of compression threads used.
    #[getter]
    fn threads_used(&self) -> usize {
        self.report.threads_used
    }

    /// Number of messages processed.
    #[getter]
    fn message_count(&self) -> u64 {
        self.report.message_count
    }

    /// Number of data bytes processed.
    #[getter]
    fn data_bytes(&self) -> u64 {
        self.report.data_bytes
    }

    /// Number of chunks written.
    #[getter]
    fn chunks_written(&self) -> u64 {
        self.report.chunks_written
    }

    fn __repr__(&self) -> String {
        format!(
            "PipelineReport(input_file={}, output_file={}, throughput_mb_s={:.2})",
            self.report.input_file, self.report.output_file, self.report.average_throughput_mb_s
        )
    }
}

/// Report from a hyper pipeline run.
#[pyclass(name = "HyperPipelineReport")]
#[derive(Clone)]
pub struct PyHyperPipelineReport {
    pub(crate) report: HyperPipelineReport,
}

#[pymethods]
impl PyHyperPipelineReport {
    /// Input file path.
    #[getter]
    fn input_file(&self) -> String {
        self.report.input_file.clone()
    }

    /// Output file path.
    #[getter]
    fn output_file(&self) -> String {
        self.report.output_file.clone()
    }

    /// Input file size in bytes.
    #[getter]
    fn input_size_bytes(&self) -> u64 {
        self.report.input_size_bytes
    }

    /// Output file size in bytes.
    #[getter]
    fn output_size_bytes(&self) -> u64 {
        self.report.output_size_bytes
    }

    /// Duration in seconds.
    #[getter]
    fn duration_seconds(&self) -> f64 {
        self.report.duration.as_secs_f64()
    }

    /// Throughput in MB/s.
    #[getter]
    fn throughput_mb_s(&self) -> f64 {
        self.report.throughput_mb_s
    }

    /// Compression ratio (output / input).
    #[getter]
    fn compression_ratio(&self) -> f64 {
        self.report.compression_ratio
    }

    /// Number of messages processed.
    #[getter]
    fn message_count(&self) -> u64 {
        self.report.message_count
    }

    /// Number of chunks written.
    #[getter]
    fn chunks_written(&self) -> u64 {
        self.report.chunks_written
    }

    /// Whether CRC was enabled.
    #[getter]
    fn crc_enabled(&self) -> bool {
        self.report.crc_enabled
    }

    /// Average throughput in MB/s (alias for throughput_mb_s).
    #[getter]
    fn average_throughput_mb_s(&self) -> f64 {
        self.report.throughput_mb_s
    }

    fn __repr__(&self) -> String {
        format!(
            "HyperPipelineReport(input_file={}, output_file={}, throughput_mb_s={:.2})",
            self.report.input_file, self.report.output_file, self.report.throughput_mb_s
        )
    }
}

/// Result for a single file conversion.
#[pyclass(name = "FileResult")]
#[derive(Clone)]
pub struct PyFileResult {
    pub(crate) result: FileResult,
}

#[pymethods]
impl PyFileResult {
    /// Input file path.
    #[getter]
    fn input_path(&self) -> String {
        self.result.input_path().to_string()
    }

    /// Output file path.
    #[getter]
    fn output_path(&self) -> String {
        self.result.output_path().to_string()
    }

    /// Whether the conversion succeeded.
    #[getter]
    fn success(&self) -> bool {
        self.result.success()
    }

    /// Error message if conversion failed.
    #[getter]
    fn error(&self) -> Option<String> {
        self.result
            .error()
            .map(|e: &roboflow_core::RoboflowError| e.to_string())
    }

    /// Standard pipeline report (if available).
    #[getter]
    fn standard_report(&self, py: Python<'_>) -> Option<PyObject> {
        self.result
            .standard_report()
            .map(|r: &roboflow_pipeline::HyperPipelineReport| {
                PyHyperPipelineReport { report: r.clone() }
                    .into_pyobject(py)
                    .ok()
                    .unwrap()
                    .into()
            })
    }

    /// Hyper pipeline report (if available).
    #[getter]
    fn hyper_report(&self, py: Python<'_>) -> Option<PyObject> {
        self.result
            .hyper_report()
            .map(|r: &roboflow_pipeline::hyper::HyperPipelineReport| {
                PyHyperPipelineReport { report: r.clone() }
                    .into_pyobject(py)
                    .ok()
                    .unwrap()
                    .into()
            })
    }

    fn __repr__(&self) -> String {
        format!(
            "FileResult(input_path={}, output_path={}, success={})",
            self.result.input_path(),
            self.result.output_path(),
            self.result.success()
        )
    }
}

/// Batch processing report for multiple files.
#[pyclass(name = "BatchReport")]
pub struct PyBatchReport {
    pub(crate) report: BatchReport,
}

#[pymethods]
impl PyBatchReport {
    /// Results for each file.
    #[getter]
    fn file_reports(&self, py: Python<'_>) -> PyResult<PyObject> {
        let list = PyList::empty(py);
        for result in &self.report.file_reports {
            let result_ref: &FileResult = result;
            list.append(PyFileResult {
                result: result_ref.clone(),
            })?;
        }
        Ok(list.into())
    }

    /// Total processing time in seconds.
    #[getter]
    fn total_duration_seconds(&self) -> f64 {
        self.report.total_duration.as_secs_f64()
    }

    /// Number of successful conversions.
    #[getter]
    fn success_count(&self) -> usize {
        self.report.success_count()
    }

    /// Number of failed conversions.
    #[getter]
    fn failure_count(&self) -> usize {
        self.report.failure_count()
    }

    fn __repr__(&self) -> String {
        format!(
            "BatchReport(files={}, success_count={}, failure_count={})",
            self.report.file_reports.len(),
            self.report.success_count(),
            self.report.failure_count()
        )
    }
}

// =============================================================================
// Robocodec Fluent API
// ============================================================================

/// Roboflow fluent API for converting robotics data files.
///
/// Example:
///     >>> import roboflow
///     >>> result = roboflow.Roboflow.open(["input.bag"])
///     ...     .write_to("output.mcap")
///     ...     .run()
///     >>> print(result)
#[pyclass(name = "Roboflow")]
pub struct PyRobocodec {
    // Store the path parts and config, build the actual Robocodec when needed
    input_files: Option<Vec<PathBuf>>,
    output_path: Option<PathBuf>,
    compression_preset: CompressionPreset,
    chunk_size: Option<usize>,
    threads: Option<usize>,
    hyper_mode: bool,
    transform_id: Option<u64>,
    state: BuilderState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BuilderState {
    WithInput,
    WithOutput,
}

impl PyRobocodec {
    /// Get the transform pipeline from the registry.
    fn get_transform(id: u64) -> Option<robocodec::transform::MultiTransform> {
        TRANSFORM_REGISTRY.with(|registry| {
            let mut registry = registry.borrow_mut();
            registry.pipelines.remove(&id)
        })
    }

    /// Create a new Robocodec builder with input files (Rust-callable).
    pub fn create(paths: Vec<String>) -> PyResult<Self> {
        if paths.is_empty() {
            return Err(PyValueError::new_err("No input files provided"));
        }

        let input_files: Vec<PathBuf> = paths.into_iter().map(PathBuf::from).collect();

        // Validate all files exist
        for path in &input_files {
            if !path.exists() {
                return Err(PyIOError::new_err(format!(
                    "Input file not found: {}",
                    path.display()
                )));
            }
        }

        Ok(Self {
            input_files: Some(input_files),
            output_path: None,
            compression_preset: CompressionPreset::default(),
            chunk_size: None,
            threads: None,
            hyper_mode: false,
            transform_id: None,
            state: BuilderState::WithInput,
        })
    }
}

#[pymethods]
impl PyRobocodec {
    /// Create a new Robocodec builder with input files.
    ///
    /// Args:
    ///     paths: List of input file paths (bag or mcap files)
    ///
    /// Returns:
    ///     Robocodec builder instance
    ///
    /// Raises:
    ///     ValueError: If no input files provided or files don't exist
    #[staticmethod]
    fn open(paths: Vec<String>) -> PyResult<Self> {
        if paths.is_empty() {
            return Err(PyValueError::new_err("No input files provided"));
        }

        let input_files: Vec<PathBuf> = paths.into_iter().map(PathBuf::from).collect();

        // Validate all files exist
        for path in &input_files {
            if !path.exists() {
                return Err(PyIOError::new_err(format!(
                    "Input file not found: {}",
                    path.display()
                )));
            }
        }

        Ok(Self {
            input_files: Some(input_files),
            output_path: None,
            compression_preset: CompressionPreset::default(),
            chunk_size: None,
            threads: None,
            hyper_mode: false,
            transform_id: None,
            state: BuilderState::WithInput,
        })
    }

    /// Set the output path (directory or file).
    ///
    /// Args:
    ///     path: Output directory or file path
    ///
    /// Returns:
    ///     Self for method chaining
    fn write_to(mut slf: PyRefMut<'_, Self>, path: String) -> PyResult<PyRefMut<'_, Self>> {
        if slf.state != BuilderState::WithInput {
            return Err(PyRuntimeError::new_err(
                "write_to() must be called after open()",
            ));
        }
        slf.output_path = Some(PathBuf::from(path));
        slf.state = BuilderState::WithOutput;
        Ok(slf)
    }

    /// Set the transform pipeline.
    ///
    /// Args:
    ///     transform_id: Transform pipeline ID from TransformBuilder.build()
    ///
    /// Returns:
    ///     Self for method chaining
    fn transform(mut slf: PyRefMut<'_, Self>, transform_id: u64) -> PyResult<PyRefMut<'_, Self>> {
        if slf.state != BuilderState::WithInput {
            return Err(PyRuntimeError::new_err(
                "transform() must be called after open() and before write_to()",
            ));
        }
        slf.transform_id = Some(transform_id);
        Ok(slf)
    }

    /// Use the hyper pipeline for maximum throughput (~1800 MB/s).
    ///
    /// The hyper pipeline is a 7-stage pipeline optimized for high performance.
    /// Best for large files where throughput matters.
    ///
    /// Returns:
    ///     Self for method chaining
    fn hyper_mode(mut slf: PyRefMut<'_, Self>) -> PyResult<PyRefMut<'_, Self>> {
        if slf.state != BuilderState::WithOutput {
            return Err(PyRuntimeError::new_err(
                "hyper_mode() must be called after write_to()",
            ));
        }
        slf.hyper_mode = true;
        Ok(slf)
    }

    /// Set the compression preset.
    ///
    /// Args:
    ///     preset: CompressionPreset enum value
    ///
    /// Returns:
    ///     Self for method chaining
    fn with_compression<'a>(
        mut slf: PyRefMut<'a, Self>,
        preset: &PyCompressionPreset,
    ) -> PyResult<PyRefMut<'a, Self>> {
        if slf.state != BuilderState::WithOutput {
            return Err(PyRuntimeError::new_err(
                "with_compression() must be called after write_to()",
            ));
        }
        slf.compression_preset = preset.preset;
        Ok(slf)
    }

    /// Set the chunk size.
    ///
    /// Args:
    ///     size: Chunk size in bytes
    ///
    /// Returns:
    ///     Self for method chaining
    fn with_chunk_size(mut slf: PyRefMut<'_, Self>, size: usize) -> PyResult<PyRefMut<'_, Self>> {
        if slf.state != BuilderState::WithOutput {
            return Err(PyRuntimeError::new_err(
                "with_chunk_size() must be called after write_to()",
            ));
        }
        slf.chunk_size = Some(size);
        Ok(slf)
    }

    /// Set the number of compression threads.
    ///
    /// Args:
    ///     threads: Number of threads
    ///
    /// Returns:
    ///     Self for method chaining
    fn with_threads(mut slf: PyRefMut<'_, Self>, threads: usize) -> PyResult<PyRefMut<'_, Self>> {
        if slf.state != BuilderState::WithOutput {
            return Err(PyRuntimeError::new_err(
                "with_threads() must be called after write_to()",
            ));
        }
        slf.threads = Some(threads);
        Ok(slf)
    }

    /// Execute the pipeline.
    ///
    /// Returns:
    ///     PipelineReport, HyperPipelineReport, or BatchReport depending on mode
    ///
    /// Raises:
    ///     RuntimeError: If pipeline execution fails
    fn run(mut slf: PyRefMut<'_, Self>, py: Python<'_>) -> PyResult<PyObject> {
        if slf.state != BuilderState::WithOutput {
            return Err(PyRuntimeError::new_err(
                "run() must be called after write_to()",
            ));
        }

        let input_files = slf
            .input_files
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("Input files not set"))?;
        let output_path = slf
            .output_path
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("Output path not set"))?;

        let paths: Vec<&Path> = input_files.iter().map(|p| p.as_path()).collect();

        // Get transform pipeline if set
        let transform_pipeline = if let Some(id) = slf.transform_id {
            Some(Self::get_transform(id).ok_or_else(|| {
                PyRuntimeError::new_err(format!("Transform pipeline ID {} not found", id))
            })?)
        } else {
            None
        };

        // Build the Rust builder with proper type-state handling
        let output = if let Some(pipeline) = transform_pipeline {
            // With transform path: open() -> transform() -> write_to() -> configure -> run()
            let builder = Robocodec::open(paths).map_err(codec_error_to_py)?;
            let builder = builder.transform(pipeline);
            let builder = builder.write_to(&output_path);

            // Apply configuration
            let builder = if slf.hyper_mode {
                // Note: hyper mode falls back to standard mode with transforms
                builder.hyper_mode()
            } else {
                builder
            };
            let builder = builder.with_compression(slf.compression_preset);
            let builder = if let Some(size) = slf.chunk_size {
                builder.with_chunk_size(size)
            } else {
                builder
            };
            let builder = if let Some(threads) = slf.threads {
                builder.with_threads(threads)
            } else {
                builder
            };

            py.allow_threads(|| builder.run())
                .map_err(codec_error_to_py)?
        } else {
            // Without transform path: open() -> write_to() -> configure -> run()
            let builder = Robocodec::open(paths).map_err(codec_error_to_py)?;
            let builder = builder.write_to(&output_path);

            // Apply configuration
            let builder = if slf.hyper_mode {
                builder.hyper_mode()
            } else {
                builder
            };
            let builder = builder.with_compression(slf.compression_preset);
            let builder = if let Some(size) = slf.chunk_size {
                builder.with_chunk_size(size)
            } else {
                builder
            };
            let builder = if let Some(threads) = slf.threads {
                builder.with_threads(threads)
            } else {
                builder
            };

            py.allow_threads(|| builder.run())
                .map_err(codec_error_to_py)?
        };

        // Convert output to Python object
        match output {
            RunOutput::Hyper(report) => {
                Ok(PyHyperPipelineReport { report }.into_pyobject(py)?.into())
            }
            RunOutput::Batch(report) => Ok(PyBatchReport { report }.into_pyobject(py)?.into()),
        }
    }

    fn __repr__(&self) -> String {
        match self.state {
            BuilderState::WithInput => {
                format!(
                    "Robocodec(inputs={:?})",
                    self.input_files.as_ref().map(|v| v
                        .iter()
                        .map(|p| p.display().to_string())
                        .collect::<Vec<_>>())
                )
            }
            BuilderState::WithOutput => {
                format!(
                    "Robocodec(inputs={:?}, output={:?})",
                    self.input_files.as_ref().map(|v| v
                        .iter()
                        .map(|p| p.display().to_string())
                        .collect::<Vec<_>>()),
                    self.output_path.as_ref().map(|p| p.display().to_string())
                )
            }
        }
    }
}

// =============================================================================
// Transform Registry (thread-local storage for transform pipelines)
// ============================================================================

use std::cell::RefCell;
use std::collections::HashMap;

struct TransformRegistryInner {
    next_id: u64,
    pipelines: HashMap<u64, robocodec::transform::MultiTransform>,
}

impl Default for TransformRegistryInner {
    fn default() -> Self {
        Self {
            next_id: 1,
            pipelines: HashMap::new(),
        }
    }
}

thread_local! {
    static TRANSFORM_REGISTRY: RefCell<TransformRegistryInner> = RefCell::default();
}
