// Copyright (c) 2026 ArcheBase
// Roboflow is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//     http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.

//! Fluent pipeline API for file processing.
//!
//! This module provides a user-friendly, type-safe API for converting
//! robotics data files (bag, mcap) using a fluent builder pattern.
//!
//! # Overview
//!
//! The fluent API uses a type-state pattern to ensure valid API usage
//! at compile time. You must:
//!
//! 1. Call `Robocodec::open()` with input files
//! 2. Optionally configure read options and transforms
//! 3. Call `write_to()` with output path (directory or file)
//! 4. Optionally configure compression settings
//! 5. Call `run()` to execute
//!
//! # Single File Mode
//!
//! When a single input file is provided:
//! - If output is a directory → uses original filename + "_roboflow" suffix
//! - If output is a file path → creates that file (errors if exists)
//!
//! # Batch Mode
//!
//! When multiple input files are provided, output must be a directory.
//!
//! # Examples
//!
//! ## Single File to Directory
//!
//! ```no_run
//! use roboflow::Robocodec;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! Robocodec::open(vec!["input.bag"])?
//!     .write_to("/output/dir")
//!     .run()?;
//! # Ok(())
//! # }
//! // Output: /output/dir/input_roboflow.mcap
//! ```
//!
//! ## Single File to Specific Output
//!
//! ```no_run
//! use roboflow::Robocodec;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! Robocodec::open(vec!["input.bag"])?
//!     .write_to("output.mcap")
//!     .run()?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Batch Processing
//!
//! ```no_run
//! use roboflow::Robocodec;
//! use roboflow::pipeline::fluent::CompressionPreset;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! Robocodec::open(vec!["a.bag", "b.bag"])?
//!     .write_to("/output")
//!     .with_compression(CompressionPreset::Fast)
//!     .run()?;
//! # Ok(())
//! # }
//! ```
//!
//! ## With Transforms
//!
//! ```no_run
//! use roboflow::Robocodec;
//! use robocodec::TransformBuilder;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let transform = TransformBuilder::new()
//!     .with_topic_rename("/old_topic", "/new_topic")
//!     .build();
//!
//! // transform() must be called before write_to()
//! Robocodec::open(vec!["input.bag"])?
//!     .transform(transform)
//!     .write_to("output.mcap")
//!     .run()?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Hyper Mode (Maximum Throughput)
//!
//! ```no_run
//! use roboflow::Robocodec;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! Robocodec::open(vec!["input.bag"])?
//!     .write_to("output.mcap")
//!     .hyper_mode()
//!     .run()?;
//! # Ok(())
//! # }
//! ```
//!
//! Note: Hyper mode is not compatible with transforms. If transforms are configured,
//! the pipeline will fall back to standard mode with a warning.

mod builder;
mod compression;
mod read_options;

// Public API
pub use builder::{BatchReport, FileResult, FileResultData, PipelineMode, Robocodec, RunOutput};
pub use compression::CompressionPreset;
pub use read_options::ReadOptions;

// Type-state markers (public for advanced usage)
pub use builder::{Initial, WithInput, WithOutput, WithTransform};
