// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Memory-mapped file arena for safe lifetime management.
//!
//! The `MmapArena` owns the memory-mapped file data and provides safe references
//! to its contents. This eliminates the need for unsafe lifetime transmutes by
//! establishing clear ownership: the arena owns the data, readers borrow from
//! the arena, and iterators borrow from readers.
//!
//! # Ownership Model
//!
//! ```text
//! MmapArena (owns mmap)
//!   ↓
//! Reader (borrows from arena for 'arena lifetime)
//!   ↓
//! Iterator (borrows from reader)
//! ```
//!
//! # Example
//!
//! ```rust,no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use robocodec::io::arena::MmapArena;
//!
//! // Open a file and create an arena
//! let arena = MmapArena::open("data.mcap")?;
//!
//! // The arena owns the data
//! let data: &[u8] = arena.data();
//!
//! // All references are tied to the arena's lifetime
//! // No unsafe transmute needed!
//! # Ok(())
//! # }
//! ```

use std::fs::File;
use std::ops::Deref;
use std::path::Path;

use crate::CodecError;

/// A memory-mapped file arena that owns all file data.
///
/// The arena provides safe access to memory-mapped file contents without
/// requiring unsafe lifetime extensions. All references to the data are
/// tied to the arena's lifetime, ensuring the data outlives all borrows.
///
/// # Safety
///
/// This type is a thin wrapper around `memmap2::Mmap` that enforces
/// lifetime safety at the type level. The mmap is owned by the arena,
/// and any slices borrowed from the arena are tied to its lifetime.
pub struct MmapArena {
    /// The memory-mapped file (owned)
    mmap: memmap2::Mmap,
    /// File path for diagnostics
    path: String,
}

impl MmapArena {
    /// Open a file and create a memory-mapped arena.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened or memory-mapped.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, CodecError> {
        let path_ref = path.as_ref();
        let path_str = path_ref.to_string_lossy().to_string();

        let file = File::open(path_ref).map_err(|e| {
            CodecError::encode(
                "MmapArena",
                format!("Failed to open file '{path_str}': {e}"),
            )
        })?;

        // Note: We use unsafe mmap here, but the wrapper ensures safety
        // by owning the mmap and only providing references tied to its lifetime.
        let mmap = unsafe { memmap2::Mmap::map(&file) }.map_err(|e| {
            CodecError::encode(
                "MmapArena",
                format!("Failed to mmap file '{path_str}': {e}"),
            )
        })?;

        Ok(Self {
            mmap,
            path: path_str,
        })
    }

    /// Create an arena from existing mmap data.
    pub fn from_mmap(mmap: memmap2::Mmap, path: String) -> Self {
        Self { mmap, path }
    }

    /// Get the file path.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Get a reference to the memory-mapped data.
    pub fn data(&self) -> &[u8] {
        &self.mmap
    }

    /// Get the length of the data.
    pub fn len(&self) -> usize {
        self.mmap.len()
    }

    /// Check if the arena is empty.
    pub fn is_empty(&self) -> bool {
        self.mmap.is_empty()
    }

    /// Create a reference to a slice of the data with bounds checking.
    ///
    /// # Errors
    ///
    /// Returns an error if the range is out of bounds.
    pub fn slice(&self, offset: usize, len: usize) -> Result<&[u8], CodecError> {
        let end = offset
            .checked_add(len)
            .ok_or_else(|| CodecError::buffer_too_short(len, 0, offset as u64))?;

        if end > self.mmap.len() {
            let available = if offset < self.mmap.len() {
                self.mmap.len() - offset
            } else {
                0
            };
            return Err(CodecError::buffer_too_short(len, available, offset as u64));
        }

        Ok(&self.mmap[offset..end])
    }

    /// Create a reference to a slice of the data without bounds checking.
    ///
    /// # Safety
    ///
    /// Caller must ensure the range is within bounds.
    pub unsafe fn slice_unchecked(&self, offset: usize, len: usize) -> &[u8] {
        // SAFETY: Caller ensures the range is valid
        self.mmap.get_unchecked(offset..offset + len)
    }
}

impl Deref for MmapArena {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.mmap
    }
}

impl std::fmt::Debug for MmapArena {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MmapArena")
            .field("path", &self.path)
            .field("len", &self.mmap.len())
            .finish()
    }
}

/// A reference to a memory-mapped arena with a specific lifetime.
///
/// This type is used when you need to store a reference to an arena
/// with an explicit lifetime parameter, such as in a reader struct.
///
/// # Example
///
/// ```no_run
/// use robocodec::io::arena::{MmapArena, MmapArenaRef};
/// use std::collections::HashMap;
///
/// # fn main() {
/// struct ChannelInfo { name: String }
/// struct McapReader<'arena> {
///     arena: MmapArenaRef<'arena>,
///     channels: HashMap<u16, ChannelInfo>,
/// }
///
/// impl<'arena> McapReader<'arena> {
///     fn new(arena: &'arena MmapArena) -> Self {
///         Self {
///             arena: MmapArenaRef::new(arena),
///             channels: HashMap::new(),
///         }
///     }
/// }
/// # }
/// ```
#[derive(Debug, Clone, Copy)]
pub struct MmapArenaRef<'arena> {
    /// Reference to the arena
    arena: &'arena MmapArena,
}

impl<'arena> MmapArenaRef<'arena> {
    /// Create a new arena reference.
    pub fn new(arena: &'arena MmapArena) -> Self {
        Self { arena }
    }

    /// Get the underlying arena reference.
    pub fn get(&self) -> &'arena MmapArena {
        self.arena
    }

    /// Get the file path.
    pub fn path(&self) -> &str {
        self.arena.path()
    }

    /// Get a reference to the data.
    pub fn data(&self) -> &'arena [u8] {
        self.arena.data()
    }

    /// Get the length of the data.
    pub fn len(&self) -> usize {
        self.arena.len()
    }

    /// Check if the arena is empty.
    pub fn is_empty(&self) -> bool {
        self.arena.is_empty()
    }

    /// Create a slice with bounds checking.
    pub fn slice(&self, offset: usize, len: usize) -> Result<&'arena [u8], CodecError> {
        self.arena.slice(offset, len)
    }

    /// Create a slice without bounds checking.
    ///
    /// # Safety
    ///
    /// Caller must ensure the range is valid.
    pub unsafe fn slice_unchecked(&self, offset: usize, len: usize) -> &'arena [u8] {
        // SAFETY: Caller ensures the range is valid, and the borrow is tied
        // to 'arena which is the arena's lifetime
        self.arena.slice_unchecked(offset, len)
    }
}

impl<'arena> Deref for MmapArenaRef<'arena> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.data()
    }
}

impl<'arena> AsRef<[u8]> for MmapArenaRef<'arena> {
    fn as_ref(&self) -> &[u8] {
        self.data()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;

    fn create_temp_file(name: &str, data: &[u8]) -> String {
        let path = format!(
            "/tmp/robocodec_test_arena_{}_{}.tmp",
            std::process::id(),
            name
        );
        {
            let mut temp_file = File::create(&path).unwrap();
            temp_file.write_all(data).unwrap();
            temp_file.flush().unwrap();
        }
        path
    }

    #[test]
    fn test_arena_open() {
        let path = create_temp_file("open", b"hello world");

        let arena = MmapArena::open(&path).unwrap();
        assert_eq!(arena.data(), b"hello world");
        assert_eq!(arena.len(), 11);
        assert!(!arena.is_empty());

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_arena_slice() {
        let path = create_temp_file("slice", b"hello world");

        let arena = MmapArena::open(&path).unwrap();
        let slice = arena.slice(0, 5).unwrap();
        assert_eq!(slice, b"hello");

        let slice = arena.slice(6, 5).unwrap();
        assert_eq!(slice, b"world");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_arena_slice_out_of_bounds() {
        let path = create_temp_file("oob", b"hello");

        let arena = MmapArena::open(&path).unwrap();
        let result = arena.slice(0, 100);
        assert!(result.is_err());

        let result = arena.slice(10, 1);
        assert!(result.is_err());

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_arena_ref() {
        let path = create_temp_file("ref", b"hello world");

        let arena = MmapArena::open(&path).unwrap();
        let arena_ref = MmapArenaRef::new(&arena);

        assert_eq!(arena_ref.data(), b"hello world");
        assert_eq!(arena_ref.len(), 11);

        let slice = arena_ref.slice(0, 5).unwrap();
        assert_eq!(slice, b"hello");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_arena_deref() {
        let path = create_temp_file("deref", b"hello");

        let arena = MmapArena::open(&path).unwrap();
        // Test Deref implementation
        let first = arena.first().unwrap();
        assert_eq!(*first, b'h');

        let _ = std::fs::remove_file(&path);
    }
}
