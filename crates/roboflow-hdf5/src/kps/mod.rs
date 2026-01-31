// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! KPS HDF5 format support.
//!
//! This module provides legacy HDF5 dataset format support.

pub mod hdf5_schema;

pub use hdf5_schema::{DataType, KpsHdf5Schema, default_arm_joint_names, default_leg_joint_names};
