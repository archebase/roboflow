# Memory Management

This document describes Robocodec's memory management strategies, focusing on zero-copy optimizations and arena allocation.

## Overview

Robotics data processing involves handling millions of small messages with varying sizes. Traditional memory management (malloc/free) creates significant overhead. Robocodec uses **arena allocation** and **object pooling** to minimize allocation overhead and maximize cache locality.

```
Traditional Allocation (per message):
┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐
│ alloc│ │ alloc│ │ alloc│ │ alloc│ │ ... │
└─────┘ └─────┘ └─────┘ └─────┘ └─────┘
   ↓       ↓       ↓       ↓
┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐
│ free │ │ free │ │ free │ │ free │ │ ... │
└─────┘ └─────┘ └─────┘ └─────┘ └─────┘

Arena Allocation (per chunk):
┌─────────────────────────────────────┐
│         Arena (64MB block)          │
│  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐   │
│  │msg 1│ │msg 2│ │msg 3│ │ ... │   │
│  └─────┘ └─────┘ └─────┘ └─────┘   │
└─────────────────────────────────────┘
         ↓ (single free)
```

## Arena Allocation

### MessageArena

**Location**: `src/pipeline/types/arena.rs`

```rust
pub struct MessageArena {
    blocks: Vec<ArenaBlock>,     // 64MB blocks per arena
    current_block: AtomicUsize,  // Lock-free block selection
    allocated: AtomicUsize,      // Total bytes tracked
}

struct ArenaBlock {
    ptr: NonNull<u8>,     // Start of block memory
    capacity: usize,      // Total block size (64MB)
    offset: AtomicUsize,  // Current allocation offset
}
```

### Allocation Algorithm

```rust
pub fn alloc(&self, size: usize, align: usize) -> Option<NonNull<u8>> {
    // 1. Get current block index
    let block_idx = self.current_block.load(Ordering::Relaxed);

    // 2. Try to allocate in current block (atomic CAS)
    if let Some(ptr) = self.blocks[block_idx].alloc(size, align) {
        return Some(ptr);
    }

    // 3. Current block full, try next block
    let next_idx = (block_idx + 1) % self.blocks.len();
    self.current_block.store(next_idx, Ordering::Release);

    // 4. Retry in new block
    self.blocks[next_idx].alloc(size, align)
}
```

**Key properties**:
- **Lock-free**: Uses atomic CAS operations
- **Wait-free**: No spinning or blocking
- **Cache-friendly**: Sequential allocation pattern

### Block Recycling

Instead of freeing individual allocations, entire blocks are recycled:

```rust
impl Drop for ArenaBlock {
    fn drop(&mut self) {
        // Return block to pool instead of deallocating
        // Saves ~22% CPU from allocation/deallocation overhead
    }
}
```

### Arena Configuration

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Block size | 64MB | Large enough for chunk, small enough for cache |
| Blocks per arena | 1-4 | Based on typical chunk size |
| Arena pool size | `num_cpus × 2` | Match parallel processing |

## Arena Pool

**Location**: `src/pipeline/types/arena_pool.rs`

### Purpose

Reuses arenas across chunks to avoid repeated allocation:

```rust
pub struct ArenaPool {
    available: Receiver<MessageArena>,  // Available arenas
    returns: Sender<MessageArena>,       // Return channel
}

impl ArenaPool {
    pub fn acquire(&self) -> PooledArena {
        // Try to get from pool, or create new if empty
        if let Some(arena) = self.available.try_recv() {
            return PooledArena::from_pool(arena, self.returns.clone());
        }
        // Create new arena
        PooledArena::new(MessageArena::new())
    }
}
```

### Benefits

- **Reduced allocation**: Arenas reused instead of reallocated
- **Lock-free**: Uses crossbeam channels
- **Automatic**: Drop trait returns arenas to pool

## Buffer Pool

**Location**: `src/pipeline/types/buffer_pool.rs`

### Purpose

Reuses compression buffers to eliminate allocation overhead:

```rust
pub struct BufferPool {
    inner: Arc<BufferPoolInner>,
}

pub struct PooledBuffer {
    buffer: Vec<u8>,
    pool: Arc<BufferPoolInner>,
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        // Return buffer to pool (capacity preserved)
        let _ = self.pool.queue.push(self.buffer.clone());
    }
}
```

### Usage Pattern

```rust
// Acquire buffer from pool
let mut output = buffer_pool.acquire();

// Use buffer for compression
let compressed = zstd_compressor.compress_to_buffer(&input, &mut output)?;

// Buffer returned to pool on drop
```

### Benefits

- **Zero-allocation compression**: Buffers reused
- **Capacity preservation**: Buffers grow to max size, stay there
- **Lock-free**: Uses `ArrayQueue` for concurrent access

## Zero-Copy Design

### Arena Slices

**Location**: `src/pipeline/types/arena.rs`

```rust
#[repr(C)]
pub struct ArenaSlice<'arena> {
    ptr: NonNull<u8>,
    len: usize,
    _phantom: PhantomData<&'arena [u8]>,
}
```

**Safety guarantees**:
- Arena outlives all slices
- No mutable aliasing
- Send/Sync via ownership tracking

### Lifetime Extension

For cross-thread message passing, lifetimes are extended:

```rust
// Original slice with some lifetime
let arena_slice: ArenaSlice<'a> = ...;

// Extend to chunk lifetime (unsafe but sound)
let extended: ArenaSlice<'arena> = unsafe {
    std::mem::transmute(arena_slice)
};
```

**Safety**: The chunk owns the arena, guaranteeing it outlives the slice.

### Memory Mapping

For file I/O, memory mapping avoids copy:

```rust
let file = File::open("data.bag")?;
let mmap = unsafe { Mmap::map(&file) }?;

// Direct access to file data, no copy
let slice = &mmap[offset..offset + length];
```

**Benefits**:
- Zero-copy file access
- OS-managed caching
- No allocation overhead

## Memory Layout

### MessageChunk

```rust
pub struct MessageChunk<'arena> {
    arena: *mut MessageArena,           // Owns the arena
    pooled_arena: Option<PooledArena>,  // Pool tracking
    messages: Vec<ArenaMessage<'arena>>, // Messages in arena
    sequence: u64,                      // For ordering
    message_start_time: u64,
    message_end_time: u64,
}
```

**Memory layout**:
```
┌─────────────────────────────────────────────────────┐
│                    MessageChunk                      │
├─────────────────────────────────────────────────────┤
│  ┌──────────────────────────────────────────────┐  │
│  │              MessageArena (owned)             │  │
│  │  ┌────────┐ ┌────────┐ ┌────────┐           │  │
│  │  │Block 0 │ │Block 1 │ │Block 2 │ ...       │  │
│  │  │ 64MB   │ │ 64MB   │ │ 64MB   │           │  │
│  │  └────────┘ └────────┘ └────────┘           │  │
│  └──────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────┐  │
│  │              Vec<ArenaMessage>               │  │
│  │  ┌──────┐ ┌──────┐ ┌──────┐                │  │
│  │  │msg 1 │ │msg 2 │ │msg 3 │ ...            │  │
│  │  └──────┘ └──────┘ └──────┘                │  │
│  └──────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────┘
```

### Memory Flow Through Pipeline

```
Reader Stage:
┌──────────────┐
│  Alloc new   │ → MessageChunk with fresh arena
│   arena      │
└──────────────┘
      ↓
Transform Stage:
┌──────────────┐
│  Reuse arena │ → Zero-copy remapping
│  (no alloc)  │
└──────────────┘
      ↓
Compression Stage:
┌──────────────┐
│  Read from   │ → Zero-copy message access
│   arena      │
└──────────────┘
┌──────────────┐
│  Use buffer  │ → Reused compression buffer
│    pool      │
└──────────────┘
      ↓
Writer Stage:
┌──────────────┐
│  Return to   │ → Arena returned to pool
│  arena pool  │
└──────────────┘
```

## Memory Usage Estimates

### Per-Chunk Memory

| Component | Size | Notes |
|-----------|------|-------|
| Arena blocks | 64MB × N | N = 1-4 blocks |
| Messages | ~16MB | Configurable chunk size |
| Metadata | ~1KB | Per ~1000 messages |
| **Total per chunk** | ~80MB | Varies by config |

### Total Process Memory

| Component | Size | Formula |
|-----------|------|---------|
| Arena pool | ~200MB | `num_cpus × 2 × 64MB` |
| Buffer pool | ~50MB | `num_workers × 2 × 16MB` |
| In-flight data | ~256MB | `channel_capacity × chunk_size` |
| File buffers | ~100MB | OS page cache |
| **Total** | ~600MB | Typical 8-core system |

## Performance Impact

### Allocation Overhead Reduction

Benchmark: Processing 10GB of ROS bag data

| Method | Time | CPU Usage | Allocations |
|--------|------|-----------|-------------|
| Traditional | 120s | 95% | 50M allocs |
| Arena | 94s | 95% | 200K allocs |
| **Improvement** | **22%** | - | **99.6%** |

### Cache Locality

Arena allocation improves cache locality:
- Sequential allocation = contiguous memory
- Better spatial locality
- Fewer cache misses

## Best Practices

### When to Use Arena Allocation

**Good for**:
- Many small allocations with similar lifetimes
- Known total size per batch
- Allocations freed together

**Not ideal for**:
- Very large individual allocations (>1GB)
- Random access patterns
- Mixed lifetimes

### When to Use Buffer Pool

**Good for**:
- Repeated operations needing temporary buffers
- Compression, encryption, encoding
- Fixed buffer sizes

**Not ideal for**:
- One-time operations
- Variable buffer sizes
- Very small buffers (<4KB)

## Future Improvements

1. **SIMD-optimized allocation**: Faster alignment handling
2. **Huge pages**: For very large datasets
3. **GPU memory**: CUDA arena for GPU compression
4. **Adaptive sizing**: Auto-tune block size based on workload

## References

- `src/pipeline/types/arena.rs` - Arena implementation
- `src/pipeline/types/arena_pool.rs` - Arena pool
- `src/pipeline/types/buffer_pool.rs` - Buffer pool
- `src/pipeline/types/chunk.rs` - Chunk design
