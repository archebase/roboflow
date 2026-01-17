//! File format handlers for robotics data.
//!
//! This module provides support for various robotics data file formats:
//! - [`mcap`] - MCAP file format (read, write, rewrite)
//! - [`bag`] - ROS1 bag file format (read, write)
//! - [`lerobot`] - LeRobot dataset format (write only, for MCAP→LeRobot conversion)

pub mod bag;
pub mod lerobot;
pub mod mcap;

pub use bag::{BagMessage, BagWriter};
pub use mcap::{
    ChannelInfo, DecodedMessageIter, DecodedMessageStream, DecodedMessageWithTimestampIter,
    DecodedMessageWithTimestampStream, McapReader, McapRewriter, RawMessage, RawMessageIter, RawMessageStream,
    RewriteOptions, RewriteStats, TimestampedDecodedMessage,
};

pub use lerobot::{LeRobotConfig, Mapping, MappingType};
