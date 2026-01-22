// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! CDR (Common Data Representation) module.
//!
//! Provides CDR encoding, decoding, and size calculation for ROS1/ROS2 messages.
//!
//! Based on the TypeScript implementation at:
//! https://github.com/emulated-devices/rtps-cdr

pub mod calculator;
pub mod codec;
pub mod cursor;
pub mod decoder;
pub mod encoder;
pub mod plan;

pub use calculator::CdrCalculator;
pub use codec::CdrCodec;
pub use cursor::{CdrCursor, CDR_HEADER_SIZE};
pub use decoder::CdrDecoder;
pub use encoder::{CdrEncoder, EncapsulationKind};
pub use plan::{DecodeOp, DecodePlan};
