// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Chunk data structures for zero-copy pipeline processing.
//!
//! This module re-exports chunk types from robocodec to avoid duplication.

pub use robocodec::types::chunk::{
    ArenaMessage, ChunkConfig, CompressedChunk, MessageChunk, MessageIndexEntry,
};

// Re-export arena types too
pub use robocodec::types::arena::{ArenaSlice, MessageArena};
pub use robocodec::types::arena_pool::PooledArena;
