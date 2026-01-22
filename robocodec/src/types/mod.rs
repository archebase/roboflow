// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Core pipeline data structures.
//!
//! This module contains the fundamental data structures used throughout
//! the pipeline: MessageChunk, CompressedChunk, MessageArena, and BufferPool.

pub mod arena;
pub mod arena_pool;
pub mod buffer_pool;
pub mod chunk;

pub use arena::MessageArena;
pub use arena_pool::{global_pool, ArenaPool, PooledArena};
pub use buffer_pool::BufferPool;
pub use chunk::{ArenaMessage, CompressedChunk, MessageChunk};
