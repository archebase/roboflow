// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! # roboflow-hdf5
//!
//! HDF5 dataset writer for roboflow - **OPTIONAL CRATE**.
//!
//! This crate provides legacy KPS HDF5 format support.
//! It requires the system library `libhdf5-dev` to build.
//!
//! **Note:** This is a separate crate - users must explicitly add it as a dependency.
//! For new projects, use the parquet format from `roboflow-dataset` instead.

pub mod kps;

pub use kps::{DataType, KpsHdf5Schema, default_arm_joint_names, default_leg_joint_names};
