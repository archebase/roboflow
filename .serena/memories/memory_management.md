# Memory Management in Robocodec

This document explains the memory management strategies used in Robocodec for optimal performance.

## Overview

Robocodec uses **arena allocation** extensively to achieve zero-copy operations and high throughput. Traditional allocation with `Box` or `Vec` would introduce significant CPU overhead (~22%) for individual allocations.

## Key Components

### 1. Arena Allocation (`robocodec/src/types/arena.rs`)

**What is Arena Allocation?**
- All allocations are done from a contiguous memory region (the "arena")
- Memory is freed all at once when the arena is dropped
- No individual `free()` calls needed

**Benefits:**
- Extremely fast allocation (just bumping a pointer)
- Excellent cache locality
- Zero-copy operations possible
- No memory fragmentation

**Usage Pattern:**
```rust
use bumpalo::Bump;

// Create an arena
let arena = Bump::new();

// Allocate from the arena
let data = arena.alloc_str("message data");
let vec: Vec<u8> = bumpalo::vec![in &arena; 1, 2, 3];

// All allocations dropped at once when arena goes out of scope
```

### 2. Arena Pool (`robocodec/src/types/arena_pool.rs`)

**Purpose:** Reuse arenas across messages to reduce allocation overhead.

**How it works:**
- Pool of pre-allocated arenas
- Check out an arena when needed
- Return it to the pool when done
- Automatic reset when returned

**Usage:**
```rust
use robocodec::types::ArenaPool;

// Create a pool
let pool = ArenaPool::new();

// Check out an arena
let arena = pool.check_out();

// Use the arena for allocations
let message_data = arena.alloc_str("...");

// Return to pool (resets arena)
pool.check_in(arena);
```

### 3. Buffer Pool (`robocodec/src/types/buffer_pool.rs`)

**Purpose:** Reuse compression buffers to avoid repeated allocations.

**How it works:**
- Pool of pre-allocated buffers
- Sized for typical compression operations
- Thread-safe with `Arc` and atomic operations

**Usage:**
```rust
use robocodec::types::BufferPool;

// Create a pool
let pool = BufferPool::new();

// Get a buffer
let buffer = pool.acquire();

// Use buffer for compression
compress_into_buffer(&mut buffer);

// Return buffer to pool
pool.release(buffer);
```

### 4. Chunk Management (`robocodec/src/types/chunk.rs`)

**Purpose:** Group messages into chunks for efficient processing.

**Benefits:**
- Better cache locality
- Reduced context switching
- Easier batch processing

**Structure:**
```rust
pub struct Chunk {
    messages: Vec<Message>,
    arena: Arena,
    size_bytes: usize,
}
```

## Memory Hierarchy

```
┌─────────────────────────────────────────┐
│  Application Level                      │
│  ┌───────────────────────────────────┐  │
│  │ Pipeline Stages                   │  │
│  └───────────┬───────────────────────┘  │
└──────────────┼──────────────────────────┘
               │
               ↓
┌─────────────────────────────────────────┐
│  Arena Management                       │
│  ┌───────────────────────────────────┐  │
│  │ Arena Pool (reuse arenas)         │  │
│  │  ┌─────────────────────────────┐  │  │
│  │  │ Individual Arena             │  │  │
│  │  │  • Message data              │  │  │
│  │  │  • Schema metadata           │  │  │
│  │  │  • Temporary allocations     │  │  │
│  │  └─────────────────────────────┘  │  │
│  └───────────────────────────────────┘  │
└─────────────────────────────────────────┘
               │
               ↓
┌─────────────────────────────────────────┐
│  Buffer Management                      │
│  ┌───────────────────────────────────┐  │
│  │ Buffer Pool (compression buffers) │  │
│  └───────────────────────────────────┘  │
└─────────────────────────────────────────┘
               │
               ↓
┌─────────────────────────────────────────┐
│  System Allocator                       │
│  (jemalloc on Linux, system on macOS)   │
└─────────────────────────────────────────┘
```

## Performance Impact

### Zero-Copy Operations

**Traditional approach (with copying):**
```
File → Vec<u8> → Decode → New Vec<u8> → Transform → Copy → Compress → Copy → Write
```
- Multiple allocations per message
- Multiple copies
- ~22% CPU overhead from allocations

**Arena approach (zero-copy):**
```
File → Arena (borrow) → Decode (in-place) → Transform (in-place) → Compress → Write
```
- One allocation per chunk (not per message)
- No copies between stages
- Minimal allocation overhead

### Benchmarks

With arena allocation:
- **Throughput**: ~1800 MB/s (HyperPipeline)
- **Allocation overhead**: <5% of CPU time
- **Cache misses**: Reduced by ~40%

Without arena allocation (traditional):
- **Throughput**: ~400 MB/s (estimated)
- **Allocation overhead**: ~22% of CPU time
- **Cache misses**: Higher

## Best Practices

### When to Use Arenas

**Use arena allocation for:**
- Message data (raw bytes from files)
- Schema metadata (parsed schema structures)
- Temporary allocations during decoding/encoding
- Transformations that create intermediate data

**Don't use arena allocation for:**
- Long-lived configuration data
- Data that needs independent lifetime management
- Very small allocations (use stack or inline instead)

### Arena Sizing

**Default arena sizes:**
- Small: 1 MB (for low-memory scenarios)
- Medium: 16 MB (default)
- Large: 64 MB (for high-throughput scenarios)

**Choosing arena size:**
```rust
// For typical robotics data (100-1000 messages per chunk)
let arena = Arena::with_capacity(16 * 1024 * 1024); // 16 MB

// For high-throughput scenarios (thousands of messages)
let arena = Arena::with_capacity(64 * 1024 * 1024); // 64 MB

// For low-memory scenarios
let arena = Arena::with_capacity(1 * 1024 * 1024); // 1 MB
```

### Pool Configuration

**Arena pool size:**
```rust
// Number of arenas in the pool should match parallelism
let num_arenas = num_cpus::get();
let pool = ArenaPool::with_capacity(num_arenas);
```

**Buffer pool size:**
```rust
// Match buffer pool size to compression threads
let pool = BufferPool::with_capacity(num_cpus::get());
```

## Common Patterns

### Pattern 1: Decode in Arena

```rust
use robocodec::types::Arena;

fn decode_message(data: &[u8], arena: &Arena) -> Message {
    // Allocate decoded message in arena
    let decoded = arena.alloc_slice_copy(data);
    
    // Parse message structure
    Message { data: decoded }
}
```

### Pattern 2: Transform with Arena

```rust
fn transform_message(msg: &Message, arena: &Arena) -> Message {
    // Create transformed version in arena
    let new_data = arena.alloc_str(&msg.data.to_uppercase());
    Message { data: new_data }
}
```

### Pattern 3: Chunk Processing

```rust
use robocodec::types::{Arena, Chunk};

fn process_chunk(chunk: &mut Chunk) {
    // All messages in chunk share the same arena
    for msg in &mut chunk.messages {
        // Transform in-place (no allocation)
        msg.process();
    }
}
```

## Memory Safety

### Lifetime Management

Arenas use Rust's lifetime system to ensure safety:

```rust
fn process<'a>(arena: &'a Arena, data: &[u8]) -> &'a str {
    // Returned string is tied to arena lifetime
    arena.alloc_str(std::str::from_utf8(data).unwrap())
}

// Arena must outlive any references to its data
```

### Thread Safety

- **Arenas**: NOT thread-safe (use per-thread arenas)
- **Arena Pools**: Thread-safe (using `Arc` and `Mutex`)
- **Buffer Pools**: Thread-safe (using `Arc` and atomics)

## Platform-Specific Considerations

### Linux
- Can use `jemalloc` for better multi-threaded allocation
- Enable with `--features jemalloc`
- io_uring for async I/O (optional)

### macOS (Darwin)
- Default system allocator is already excellent
- jemalloc is NOT used (even if feature is enabled)
- Use standard `mio` for async I/O

### Windows
- System allocator performs well
- No special optimizations needed

## Troubleshooting

### High Memory Usage

**Symptom**: Memory usage keeps growing

**Possible causes:**
1. Arenas not being returned to pool
2. Buffer pool not releasing buffers
3. Individual messages too large for arena

**Solutions:**
```rust
// Ensure arenas are returned to pool
let arena = pool.check_out();
// ... use arena ...
pool.check_in(arena); // Don't forget this!

// Or use RAII guard
let _guard = pool.check_out_guard();
// Automatically returned when guard goes out of scope
```

### Poor Cache Performance

**Symptom**: Low throughput despite fast CPU

**Possible causes:**
1. Chunks too large (doesn't fit in cache)
2. Random memory access patterns
3. Arena fragmentation

**Solutions:**
```rust
// Use smaller chunks
const CHUNK_SIZE: usize = 256 * 1024; // 256 KB

// Process sequentially when possible
for msg in chunk.messages.iter() {
    // Sequential access = better cache utilization
}
```

## Related Documentation

- `MEMORY.md` in docs/ for detailed memory architecture
- `arena_pool.rs` for arena pooling implementation
- `buffer_pool.rs` for buffer pooling implementation
- `chunk.rs` for chunk management
