// Copyright (c) 2026 ArcheBase
// Roboflow is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//     http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.

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
