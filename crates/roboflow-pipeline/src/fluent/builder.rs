// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Type-state builder for the fluent pipeline API.
//!
//! Provides compile-time safety for the fluent API using type-state pattern.

use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::time::Instant;

use tracing::{error, warn};

use crate::hyper::{HyperPipeline, HyperPipelineConfig, HyperPipelineReport};
use roboflow_core::{Result, RoboflowError};
// TODO: Standard pipeline not yet implemented - use hyper pipeline for now
// use crate::orchestrator::{AsyncPipeline, PipelineConfig, PipelineReport};
use robocodec::transform::MultiTransform;

use super::compression::CompressionPreset;
use super::read_options::ReadOptions;

// =============================================================================
// Pipeline Mode
// =============================================================================

/// Pipeline execution mode.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PipelineMode {
    /// Standard 4-stage pipeline (Reader → Transform → Compression → Writer)
    #[default]
    Standard,
    /// Hyper 7-stage pipeline for maximum throughput
    Hyper,
}

// =============================================================================
// Type-state markers
// =============================================================================

/// Initial state - no configuration yet.
pub struct Initial;

/// State after input files have been specified.
pub struct WithInput;

/// State after transform pipeline has been specified (optional).
pub struct WithTransform;

/// State after output path has been specified (ready to run).
pub struct WithOutput;

// =============================================================================
// Robocodec Builder
// =============================================================================

/// Fluent pipeline API with type-state pattern.
///
/// The type-state pattern ensures valid API usage at compile time:
/// - Must call `open()` first
/// - Must call `write_to()` before `run()`
/// - `transform()` is optional
///
/// # Single File Mode
///
/// When a single input file is provided:
/// - If output is a directory → uses original filename + "roboflow" suffix
/// - If output is a file path → creates the file, errors if it exists
///
/// # Batch Mode
///
/// When multiple input files are provided:
/// - Output must be a directory
/// - Each input file is converted to an MCAP file in the output directory
///
/// # Examples
///
/// ```no_run
/// use roboflow::Robocodec;
/// use roboflow::pipeline::fluent::CompressionPreset;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// // Single file to directory (auto-generates output filename)
/// Robocodec::open(vec!["input.bag"])?
///     .write_to("/output/dir")
///     .run()?;
///
/// // Single file to specific file
/// Robocodec::open(vec!["input.bag"])?
///     .write_to("output.mcap")
///     .run()?;
///
/// // Batch processing
/// Robocodec::open(vec!["a.bag", "b.bag"])?
///     .write_to("/output/dir")
///     .with_compression(CompressionPreset::Fast)
///     .run()?;
/// # Ok(())
/// # }
/// ```
pub struct Robocodec<State = Initial> {
    input_files: Vec<PathBuf>,
    read_options: Option<ReadOptions>,
    transform: Option<MultiTransform>,
    output_path: Option<PathBuf>,
    compression_preset: CompressionPreset,
    chunk_size: Option<usize>,
    threads: Option<usize>,
    pipeline_mode: PipelineMode,
    _state: PhantomData<State>,
}

// =============================================================================
// Initial State
// =============================================================================

impl Robocodec<Initial> {
    /// Create a new Robocodec builder with input files.
    ///
    /// # Arguments
    ///
    /// * `paths` - Input file paths (bag or mcap files)
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - No input files provided
    /// - Any input file does not exist
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use roboflow::Robocodec;
    ///
    /// // Single file
    /// let builder = Robocodec::open(vec!["input.bag"])?;
    ///
    /// // Multiple files (batch mode)
    /// let builder = Robocodec::open(vec!["a.bag", "b.bag"])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn open<P>(paths: impl IntoIterator<Item = P>) -> Result<Robocodec<WithInput>>
    where
        P: AsRef<Path>,
    {
        let paths: Vec<PathBuf> = paths
            .into_iter()
            .map(|p| p.as_ref().to_path_buf())
            .collect();

        if paths.is_empty() {
            return Err(RoboflowError::parse(
                "Robocodec::open",
                "No input files provided",
            ));
        }

        // Validate all files exist
        for path in &paths {
            if !path.exists() {
                return Err(RoboflowError::parse(
                    "Robocodec::open",
                    format!("Input file not found: {}", path.display()),
                ));
            }
        }

        Ok(Robocodec {
            input_files: paths,
            read_options: None,
            transform: None,
            output_path: None,
            compression_preset: CompressionPreset::default(),
            chunk_size: None,
            threads: None,
            pipeline_mode: PipelineMode::default(),
            _state: PhantomData,
        })
    }
}

// =============================================================================
// WithInput State
// =============================================================================

impl Robocodec<WithInput> {
    /// Set read options for input processing.
    ///
    /// Configure topic filtering, time ranges, and message limits.
    ///
    /// # Note
    ///
    /// **Currently not implemented.** This method accepts read options but they
    /// are not yet applied to the pipeline. This is a placeholder for future
    /// functionality. A warning will be logged at runtime if options are set.
    #[doc(hidden)]
    pub fn with_read_options(mut self, options: ReadOptions) -> Self {
        warn!(
            "Read options were provided via with_read_options() but are not yet implemented. \
             The options will be ignored. This feature is planned for a future release."
        );
        self.read_options = Some(options);
        self
    }

    /// Set the transform pipeline.
    ///
    /// Transforms are applied to topic names, type names, and schemas.
    pub fn transform(self, pipeline: MultiTransform) -> Robocodec<WithTransform> {
        Robocodec {
            input_files: self.input_files,
            read_options: self.read_options,
            transform: Some(pipeline),
            output_path: self.output_path,
            compression_preset: self.compression_preset,
            chunk_size: self.chunk_size,
            threads: self.threads,
            pipeline_mode: self.pipeline_mode,
            _state: PhantomData,
        }
    }

    /// Set the output path (directory or file).
    ///
    /// # Single File Mode (1 input)
    /// - If path is a directory → uses original filename + "roboflow" suffix
    /// - If path is a file → creates that file (errors if exists)
    ///
    /// # Batch Mode (multiple inputs)
    /// - Path must be a directory
    ///
    /// # Arguments
    ///
    /// * `path` - Output directory or file path
    pub fn write_to<P: AsRef<Path>>(self, path: P) -> Robocodec<WithOutput> {
        Robocodec {
            input_files: self.input_files,
            read_options: self.read_options,
            transform: self.transform,
            output_path: Some(path.as_ref().to_path_buf()),
            compression_preset: self.compression_preset,
            chunk_size: self.chunk_size,
            threads: self.threads,
            pipeline_mode: self.pipeline_mode,
            _state: PhantomData,
        }
    }
}

// =============================================================================
// WithTransform State
// =============================================================================

impl Robocodec<WithTransform> {
    /// Set the output path (directory or file).
    ///
    /// See `WithInput::write_to` for behavior details.
    pub fn write_to<P: AsRef<Path>>(self, path: P) -> Robocodec<WithOutput> {
        Robocodec {
            input_files: self.input_files,
            read_options: self.read_options,
            transform: self.transform,
            output_path: Some(path.as_ref().to_path_buf()),
            compression_preset: self.compression_preset,
            chunk_size: self.chunk_size,
            threads: self.threads,
            pipeline_mode: self.pipeline_mode,
            _state: PhantomData,
        }
    }
}

// =============================================================================
// WithOutput State (Ready to run)
// =============================================================================

impl Robocodec<WithOutput> {
    /// Use the hyper pipeline for maximum throughput.
    ///
    /// The hyper pipeline is a 7-stage pipeline optimized for high performance:
    /// - Prefetcher with platform-specific I/O optimization
    /// - Parser/Slicer for message boundary detection
    /// - Batcher for efficient message batching
    /// - Transform stage (pass-through for now)
    /// - Parallel ZSTD compression
    /// - CRC/Packetizer for data integrity
    /// - Ordered writer with buffering
    ///
    /// # Note
    ///
    /// Transforms are currently not supported in hyper mode. If you have
    /// configured transforms, the pipeline will fall back to standard mode.
    pub fn hyper_mode(mut self) -> Self {
        self.pipeline_mode = PipelineMode::Hyper;
        self
    }

    /// Set the compression preset.
    pub fn with_compression(mut self, preset: CompressionPreset) -> Self {
        self.compression_preset = preset;
        self
    }

    /// Set the chunk size.
    ///
    /// Larger chunks = better compression, smaller chunks = better seek performance.
    pub fn with_chunk_size(mut self, size: usize) -> Self {
        self.chunk_size = Some(size);
        self
    }

    /// Set the number of compression threads.
    ///
    /// Default is auto-detected from CPU count.
    pub fn with_threads(mut self, threads: usize) -> Self {
        self.threads = Some(threads);
        self
    }

    /// Execute the pipeline.
    ///
    /// # Single File Mode
    /// Returns a `PipelineReport` or `HyperPipelineReport` for the single file.
    ///
    /// # Batch Mode
    /// Returns a `BatchReport` containing statistics for all processed files.
    ///
    /// # Hyper Mode
    ///
    /// When `.hyper_mode()` is called, the pipeline will use the 7-stage hyper
    /// pipeline for maximum throughput. Note that transforms are not currently
    /// supported in hyper mode - the pipeline will fall back to standard mode
    /// if transforms are configured.
    pub fn run(self) -> Result<RunOutput> {
        let output_path = self
            .output_path
            .ok_or_else(|| RoboflowError::parse("Robocodec::run", "Output path not set"))?;

        let compression_level = self.compression_preset.compression_level();
        let chunk_size = self
            .chunk_size
            .unwrap_or_else(|| self.compression_preset.default_chunk_size());

        // Check if we should use hyper mode
        // Hyper mode is not compatible with transforms (yet)
        let use_hyper = if self.pipeline_mode == PipelineMode::Hyper {
            if self.transform.is_some() {
                warn!(
                    "Hyper mode was requested but transforms are configured. \
                     Falling back to standard mode as transforms are not yet supported in hyper mode."
                );
                false
            } else {
                true
            }
        } else {
            false
        };

        // Single file mode
        if self.input_files.len() == 1 {
            let input_path = &self.input_files[0];
            let resolved_output = resolve_single_output(input_path, &output_path)?;

            // Create parent directory if needed
            if let Some(parent) = resolved_output.parent()
                && !parent.as_os_str().is_empty()
                && !parent.exists()
            {
                std::fs::create_dir_all(parent).map_err(|e| {
                    RoboflowError::encode(
                        "Robocodec::run",
                        format!("Failed to create output directory: {e}"),
                    )
                })?;
            }

            if use_hyper {
                // Use hyper pipeline for single file
                let mut config = HyperPipelineConfig::new(input_path, &resolved_output);
                config.compression.compression_level = compression_level;
                config.batcher.target_size = chunk_size;

                if let Some(threads) = self.threads {
                    config.compression.num_threads = threads;
                }

                let pipeline = HyperPipeline::new(config)?;
                let report = pipeline.run()?;

                return Ok(RunOutput::Hyper(report));
            }

            // TODO: Standard pipeline not yet implemented - use hyper mode for now
            // Fall back to hyper pipeline
            let mut config = HyperPipelineConfig::new(input_path, &resolved_output);
            config.compression.compression_level = compression_level;
            config.batcher.target_size = chunk_size;

            if let Some(threads) = self.threads {
                config.compression.num_threads = threads;
            }

            let pipeline = HyperPipeline::new(config)?;
            let report = pipeline.run()?;

            return Ok(RunOutput::Hyper(report));
        }

        // Batch mode
        let output_dir = if output_path.exists() && output_path.is_dir() {
            output_path.clone()
        } else {
            // For batch mode, output must be a directory
            return Err(RoboflowError::parse(
                "Robocodec::run",
                format!(
                    "Output must be a directory for batch mode, got: {}",
                    output_path.display()
                ),
            ));
        };

        // Create output directory if it doesn't exist
        std::fs::create_dir_all(&output_dir).map_err(|e| {
            RoboflowError::encode(
                "Robocodec::run",
                format!("Failed to create output directory: {e}"),
            )
        })?;

        let start = Instant::now();
        let mut file_reports = Vec::with_capacity(self.input_files.len());
        let mut used_paths: std::collections::HashSet<PathBuf> = std::collections::HashSet::new();

        for input_path in self.input_files.iter() {
            // Generate output path - continue to next file on error
            let output_file = match generate_output_path(&output_dir, input_path, &mut used_paths) {
                Ok(path) => path,
                Err(e) => {
                    error!(
                        error = %e,
                        input = %input_path.display(),
                        "Failed to generate output path for batch processing"
                    );
                    file_reports.push(FileResult::from_failure(
                        input_path.display().to_string(),
                        "N/A".to_string(),
                        e,
                    ));
                    continue;
                }
            };

            if use_hyper {
                // Use hyper pipeline
                let mut config = HyperPipelineConfig::new(input_path, &output_file);
                config.compression.compression_level = compression_level;
                config.batcher.target_size = chunk_size;

                if let Some(threads) = self.threads {
                    config.compression.num_threads = threads;
                }

                let result = HyperPipeline::new(config)
                    .and_then(|pipeline| pipeline.run())
                    .map(|report| {
                        FileResult::from_hyper_success(
                            input_path.display().to_string(),
                            output_file.display().to_string(),
                            report,
                        )
                    })
                    .unwrap_or_else(|e| {
                        error!(
                            input = %input_path.display(),
                            output = %output_file.display(),
                            error = %e,
                            "Failed to process file with hyper pipeline"
                        );
                        FileResult::from_failure(
                            input_path.display().to_string(),
                            output_file.display().to_string(),
                            e,
                        )
                    });

                file_reports.push(result);
            } else {
                // TODO: Standard pipeline not yet implemented - use hyper mode for now
                // Fall back to hyper pipeline
                let mut config = HyperPipelineConfig::new(input_path, &output_file);
                config.compression.compression_level = compression_level;
                config.batcher.target_size = chunk_size;

                if let Some(threads) = self.threads {
                    config.compression.num_threads = threads;
                }

                let result = HyperPipeline::new(config)
                    .and_then(|pipeline| pipeline.run())
                    .map(|report| {
                        FileResult::from_hyper_success(
                            input_path.display().to_string(),
                            output_file.display().to_string(),
                            report,
                        )
                    })
                    .unwrap_or_else(|e| {
                        error!(
                            input = %input_path.display(),
                            output = %output_file.display(),
                            error = %e,
                            "Failed to process file with hyper pipeline"
                        );
                        FileResult::from_failure(
                            input_path.display().to_string(),
                            output_file.display().to_string(),
                            e,
                        )
                    });

                file_reports.push(result);
            }
        }

        Ok(RunOutput::Batch(BatchReport::from_results(
            file_reports,
            start.elapsed(),
        )))
    }
}

// =============================================================================
// Output Types
// =============================================================================

/// Output from running the pipeline.
pub enum RunOutput {
    // TODO: Add Single(PipelineReport) when standard pipeline is implemented
    // /// Single file result (standard pipeline)
    // Single(PipelineReport),
    /// Single file result (hyper pipeline)
    Hyper(HyperPipelineReport),
    /// Batch processing result
    Batch(BatchReport),
}

/// Batch processing report for multiple files.
#[derive(Debug, Clone)]
pub struct BatchReport {
    /// Results for each file
    pub file_reports: Vec<FileResult>,
    /// Total processing time
    pub total_duration: std::time::Duration,
}

impl BatchReport {
    fn from_results(results: Vec<FileResult>, duration: std::time::Duration) -> Self {
        Self {
            file_reports: results,
            total_duration: duration,
        }
    }

    /// Get number of successful conversions
    pub fn success_count(&self) -> usize {
        self.file_reports.iter().filter(|r| r.success()).count()
    }

    /// Get number of failed conversions
    pub fn failure_count(&self) -> usize {
        self.file_reports.iter().filter(|r| !r.success()).count()
    }
}

/// Result for a single file conversion.
#[derive(Debug)]
pub struct FileResult {
    /// Input file path
    input_path: String,
    /// Output file path
    output_path: String,
    /// Conversion result
    result: FileResultData,
}

/// The result data for a file conversion.
/// This enum makes illegal states unrepresentable - you cannot have both
/// a success and failure result at the same time.
#[derive(Debug)]
pub enum FileResultData {
    // TODO: Add StandardSuccess(PipelineReport) when standard pipeline is implemented
    // /// Standard pipeline succeeded
    // StandardSuccess(PipelineReport),
    /// Hyper pipeline succeeded
    HyperSuccess(HyperPipelineReport),
    /// Conversion failed
    Failure { error: RoboflowError },
}

// Implement Clone manually for FileResultData since RoboflowError may not be Clone
impl Clone for FileResultData {
    fn clone(&self) -> Self {
        match self {
            // TODO: Add StandardSuccess case when standard pipeline is implemented
            // FileResultData::StandardSuccess(report) => {
            //     FileResultData::StandardSuccess(report.clone())
            // }
            FileResultData::HyperSuccess(report) => FileResultData::HyperSuccess(report.clone()),
            FileResultData::Failure { error } => {
                // For Clone, we preserve the error category and message
                // since RoboflowError may contain non-cloneable resources
                let category = error.category().as_str();
                let message = format!("{}", error);
                FileResultData::Failure {
                    error: RoboflowError::parse(category, message),
                }
            }
        }
    }
}

impl Clone for FileResult {
    fn clone(&self) -> Self {
        Self {
            input_path: self.input_path.clone(),
            output_path: self.output_path.clone(),
            result: self.result.clone(),
        }
    }
}

impl FileResult {
    /// Get the input file path.
    pub fn input_path(&self) -> &str {
        &self.input_path
    }

    /// Get the output file path.
    pub fn output_path(&self) -> &str {
        &self.output_path
    }

    /// Get the conversion result.
    pub fn result(&self) -> &FileResultData {
        &self.result
    }

    /// Whether the conversion succeeded.
    pub fn success(&self) -> bool {
        matches!(
            self.result,
            // TODO: Add StandardSuccess when standard pipeline is implemented
            // FileResultData::StandardSuccess(_) |
            FileResultData::HyperSuccess(_)
        )
    }

    /// Get the error if conversion failed.
    pub fn error(&self) -> Option<&RoboflowError> {
        match &self.result {
            FileResultData::Failure { error } => Some(error),
            _ => None,
        }
    }

    /// Get the standard report if available.
    // TODO: Implement when standard pipeline is available
    #[allow(dead_code)]
    pub fn standard_report(&self) -> Option<&HyperPipelineReport> {
        match &self.result {
            // FileResultData::StandardSuccess(report) => Some(report),
            FileResultData::HyperSuccess(report) => Some(report),
            FileResultData::Failure { .. } => None,
        }
    }

    /// Get the hyper report if available.
    pub fn hyper_report(&self) -> Option<&HyperPipelineReport> {
        match &self.result {
            FileResultData::HyperSuccess(report) => Some(report),
            _ => None,
        }
    }

    // TODO: Implement when standard pipeline is available
    #[allow(dead_code)]
    fn from_standard_success(
        input_path: String,
        output_path: String,
        report: HyperPipelineReport,
    ) -> Self {
        Self {
            input_path,
            output_path,
            result: FileResultData::HyperSuccess(report),
        }
    }

    fn from_hyper_success(
        input_path: String,
        output_path: String,
        report: HyperPipelineReport,
    ) -> Self {
        Self {
            input_path,
            output_path,
            result: FileResultData::HyperSuccess(report),
        }
    }

    fn from_failure(input_path: String, output_path: String, error: RoboflowError) -> Self {
        Self {
            input_path,
            output_path,
            result: FileResultData::Failure { error },
        }
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Resolve output path for single file mode.
///
/// Rules:
/// - If output_path exists and is a directory → use filename + "roboflow" suffix
/// - If output_path is a file → return as-is (will check existence later)
/// - If output_path doesn't exist → treat as file path
fn resolve_single_output(input_path: &Path, output_path: &Path) -> Result<PathBuf> {
    if output_path.exists() {
        if output_path.is_dir() {
            // Use original filename + "roboflow" suffix
            let stem = input_path
                .file_stem()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_else(|| "output".to_string());

            let filename = format!("{}_roboflow.mcap", stem);
            return Ok(output_path.join(filename));
        }
        // Output is a file - check if it exists
        return Err(RoboflowError::parse(
            "Robocodec::run",
            format!(
                "Output file already exists: {}. \
                 Delete the existing file or specify a different output path.",
                output_path.display()
            ),
        ));
    }

    // Output doesn't exist - check if it looks like a directory or file
    // If it ends with a separator or has no extension, treat as directory
    let path_str = output_path.to_string_lossy();
    if path_str.ends_with('/') || path_str.ends_with('\\') {
        // It's a directory path
        let stem = input_path
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| "output".to_string());
        return Ok(output_path.join(format!("{}_roboflow.mcap", stem)));
    }

    // It's a file path - return as-is
    Ok(output_path.to_path_buf())
}

/// Generate output path from input filename for batch mode.
/// Returns error if the output file already exists.
fn generate_output_path(
    output_dir: &Path,
    input_path: &Path,
    used_paths: &mut std::collections::HashSet<PathBuf>,
) -> Result<PathBuf> {
    let stem = input_path
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "output".to_string());

    let output_path = output_dir.join(format!("{}.mcap", stem));

    // Check if this path was already generated for another input in this batch
    if used_paths.contains(&output_path) {
        return Err(RoboflowError::parse(
            "Robocodec::run",
            format!(
                "Duplicate output path in batch: {} (from input: {}). \
                 Input files have the same name - rename one of the input files.",
                output_path.display(),
                input_path.display()
            ),
        ));
    }

    // Check if the file already exists on disk
    if output_path.exists() {
        return Err(RoboflowError::parse(
            "Robocodec::run",
            format!(
                "Output file already exists: {}. \
                 Delete the existing file or specify a different output directory.",
                output_path.display()
            ),
        ));
    }

    used_paths.insert(output_path.clone());
    Ok(output_path)
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open_empty_paths() {
        let result = Robocodec::open(Vec::<PathBuf>::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_open_nonexistent_file() {
        let result = Robocodec::open(vec!["/nonexistent/file.bag"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_generate_output_path() {
        let output_dir = Path::new("/output");
        let input_path = Path::new("/data/run1.bag");
        let mut used = std::collections::HashSet::new();

        let result = generate_output_path(output_dir, input_path, &mut used).unwrap();
        assert_eq!(result, PathBuf::from("/output/run1.mcap"));
        assert!(used.contains(&result));
    }

    #[test]
    fn test_generate_output_path_collision() {
        let output_dir = Path::new("/output");
        let input1 = Path::new("/data1/run1.bag");
        let input2 = Path::new("/data2/run1.bag");
        let mut used = std::collections::HashSet::new();

        let result1 = generate_output_path(output_dir, input1, &mut used).unwrap();
        assert_eq!(result1, PathBuf::from("/output/run1.mcap"));

        // Second call with same stem should error (duplicate output)
        let result2 = generate_output_path(output_dir, input2, &mut used);
        assert!(result2.is_err());
        assert!(
            result2
                .unwrap_err()
                .to_string()
                .contains("Duplicate output path")
        );
    }
}
