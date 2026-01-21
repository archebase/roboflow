//! Low-level format implementations.
//!
//! This module contains format-specific readers that implement the
//! [`FormatReader`] trait from `crate::io::traits`.
//!
//! MCAP and BAG formats have been moved to `crate::formats`.
//! This module now primarily contains the KPS dataset format.
//!
//! ## Available Formats
//!
//! - **KPS**: `kps` (dataset format for robotics learning)
//!
//! For high-level APIs with automatic decoding, use `crate::format`.

// KPS dataset format (still here)
pub mod kps;

// Re-export MCAP and BAG from crate::formats for backwards compatibility
pub use crate::formats::{BagFormat, McapFormat, ParallelBagReader, ParallelMcapReader};

// KPS re-exports
pub use kps::{
    config::{KpsConfig, Mapping, MappingType, OutputFormat},
    Hdf5KpsWriter, ParquetKpsWriter,
};
