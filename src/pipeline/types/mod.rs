// Copyright (c) 2026 ArcheBase
// Roboflow is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//     http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.

//! Core pipeline data structures.
//!
//! This module contains the fundamental data structures used throughout
//! the pipeline: MessageChunk, CompressedChunk, MessageArena, and BufferPool.

pub mod buffer_pool;
pub mod chunk;

// Re-export arena types from robocodec
pub use robocodec::types::arena::{ArenaSlice, MessageArena};
pub use robocodec::types::arena_pool::{global_pool, ArenaPool, PooledArena};

pub use buffer_pool::BufferPool;
pub use chunk::{ArenaMessage, CompressedChunk, MessageChunk};
