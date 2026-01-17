//! LeRobot dataset format support.
//!
//! This module provides conversion from MCAP/BAG files to LeRobot dataset format.
//! Supports both:
//! - HDF5 format (legacy)
//! - Parquet + MP4 format (v3.0)
//!
//! # Configuration
//!
//! Conversion is controlled via a TOML config file:
//!
//! ```toml
//! [dataset]
//! name = "my_dataset"
//! fps = 30
//!
//! [[mappings]]
//! topic = "/camera/high"
//! feature = "observation.camera_0"
//! type = "image"
//!
//! [[mappings]]
//! topic = "/joint_states"
//! feature = "observation.state"
//! type = "state"
//! ```
//!
//! # Usage
//!
//! ```bash
//! # Convert MCAP to LeRobot format
//! cargo run --bin convert -- to-lerobot data.mcap ./output/ config.toml
//! ```

pub mod config;
pub mod hdf5_writer;
pub mod info;
pub mod parquet_writer;

pub use config::{LeRobotConfig, Mapping, MappingType, OutputFormat};
pub use hdf5_writer::Hdf5LeRobotWriter;
pub use parquet_writer::ParquetLeRobotWriter;
