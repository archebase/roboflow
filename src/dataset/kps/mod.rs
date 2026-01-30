// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Kps dataset format support.
//!
//! This module provides conversion from MCAP/BAG files to Kps dataset format.
//! Supports both:
//! - HDF5 format (legacy)
//! - Parquet + MP4 format (v3.0)
//! - v1.2 specification (latest)
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
//! # Convert MCAP to Kps format
//! cargo run --bin convert -- to-kps data.mcap ./output/ config.toml
//! ```

pub mod camera_params;
pub mod config;
pub mod delivery;
pub mod delivery_v12;
pub mod hdf5_schema;
pub mod hdf5_writer;
pub mod info;
pub mod parquet_writer;
pub mod robot_calibration;
pub mod schema_extractor;
pub mod task_info;
pub mod video_encoder;

// New streaming writers
pub mod writers;

pub use camera_params::CameraParamCollector;
pub use config::{DatasetConfig, KpsConfig, Mapping, MappingType, OutputConfig, OutputFormat};
pub use delivery::{DeliveryBuilder, DeliveryConfig};
pub use delivery_v12::{
    SeriesDeliveryConfig, SeriesDeliveryConfigBuilder, StatisticsCollector, TaskStatistics,
    V12DeliveryBuilder,
};
pub use hdf5_schema::{DataType, DatasetSpec, JointGroupConfig, KpsHdf5Schema};
pub use hdf5_writer::Hdf5KpsWriter;
pub use info::KpsInfo;
pub use parquet_writer::ParquetKpsWriter;
pub use robot_calibration::{JointCalibration, RobotCalibration, RobotCalibrationGenerator};
pub use task_info::{ActionSegment, KeyFrame, LabelInfo, TaskInfo, TaskInfoBuilder};

// Re-export streaming writer types
pub use writers::{
    AlignedFrame, AudioData, DatasetWriter, ImageData, KpsWriterError, MessageExtractor,
    WriterStats, create_kps_writer,
};

#[cfg(feature = "dataset-hdf5")]
pub use writers::StreamingHdf5Writer;

#[cfg(feature = "dataset-parquet")]
pub use writers::StreamingParquetWriter;
