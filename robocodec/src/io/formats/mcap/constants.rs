// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! MCAP format constants.
//!
//! This module contains all MCAP opcodes and magic bytes as defined by the
//! [MCAP specification](https://mcap.dev/spec).
//!
//! Using a single source of truth for these constants prevents bugs from
//! opcode mismatches between reader and writer implementations.

/// MCAP file magic bytes (at start and end of file).
pub const MCAP_MAGIC: [u8; 8] = [0x89, 0x4D, 0x43, 0x41, 0x50, 0x30, 0x0D, 0x0A];

// MCAP Record Opcodes per specification
// https://mcap.dev/spec#opcodes

/// Header record - must be first record after magic.
pub const OP_HEADER: u8 = 0x01;
/// Footer record - contains summary section offsets.
pub const OP_FOOTER: u8 = 0x02;
/// Schema record - defines message schemas.
pub const OP_SCHEMA: u8 = 0x03;
/// Channel record - defines channels/topics.
pub const OP_CHANNEL: u8 = 0x04;
/// Message record - contains message data.
pub const OP_MESSAGE: u8 = 0x05;
/// Chunk record - contains compressed messages.
pub const OP_CHUNK: u8 = 0x06;
/// Message index record - indexes messages within a chunk.
pub const OP_MESSAGE_INDEX: u8 = 0x07;
/// Chunk index record - indexes chunks in summary section.
pub const OP_CHUNK_INDEX: u8 = 0x08;
/// Attachment record - contains file attachments.
pub const OP_ATTACHMENT: u8 = 0x09;
/// Attachment index record - indexes attachments in summary.
pub const OP_ATTACHMENT_INDEX: u8 = 0x0A;
/// Statistics record - contains file-level statistics.
pub const OP_STATISTICS: u8 = 0x0B;
/// Metadata record - contains key-value metadata.
pub const OP_METADATA: u8 = 0x0C;
/// Metadata index record - indexes metadata in summary.
pub const OP_METADATA_INDEX: u8 = 0x0D;
/// Summary offset record - indexes summary section records.
pub const OP_SUMMARY_OFFSET: u8 = 0x0E;
/// Data end record - marks end of data section.
pub const OP_DATA_END: u8 = 0x0F;
