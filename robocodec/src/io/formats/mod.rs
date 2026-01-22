//! File format implementations for robotics data.
//!
//! This module contains readers and writers for different robotics file formats:
//! - [`mcap`]: MCAP (ROS2-native) format support
//! - [`bag`]: ROS1 bag format support

pub mod bag;
pub mod mcap;
