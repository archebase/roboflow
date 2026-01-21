// Copyright (c) 2026 ArcheBase
// Roboflow is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//     http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.

//! Chunk data structures for zero-copy pipeline processing.
//!
//! This module re-exports chunk types from robocodec to avoid duplication.

pub use robocodec::types::chunk::{
    ArenaMessage, ChunkConfig, CompressedChunk, MessageChunk, MessageIndexEntry,
};

// Re-export arena types too
pub use robocodec::types::arena::{ArenaSlice, MessageArena};
pub use robocodec::types::arena_pool::PooledArena;
