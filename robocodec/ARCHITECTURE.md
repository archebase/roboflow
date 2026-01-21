# Robofmt Architecture

This document describes the architectural organization of the robocodec crate.

## Overview

Robofmt is organized as a **format-centric** library, where each robotics data format has its own module containing all related functionality (readers, writers, high-level APIs).

## Module Organization

```
robocodec/src/
├── core/              # Core types, errors, and common utilities
├── schema/            # Schema parsing (ROS .msg, IDL formats)
├── encoding/          # Codec implementations (CDR, Protobuf, JSON)
├── transform/         # Channel/topic/type transformations
├── types/             # Pipeline types (arena, chunk, buffer pool)
├── io/                # Unified I/O layer (metadata, traits, strategies)
│
├── bag/               # ROS1 bag format implementation
│   ├── mod.rs         # Module exports
│   ├── parallel.rs    # Parallel chunk-based reader
│   ├── sequential.rs  # Sequential reader
│   ├── parser.rs      # Bag file parser
│   ├── reader.rs      # Low-level reader
│   └── writer.rs      # Low-level writer
│
├── mcap/              # MCAP format implementation
│   ├── mod.rs         # Module exports
│   ├── parallel.rs    # Parallel chunk-based reader
│   ├── sequential.rs  # Sequential reader
│   ├── reader_raw.rs  # Raw reader implementation
│   ├── reader.rs      # Low-level reader
│   ├── reader_api.rs  # High-level auto-decoding reader
│   ├── writer.rs      # Low-level writer
│   └── writer_api.rs  # High-level custom writer
│
├── rewriter/          # Unified rewriter facade
│   ├── mod.rs         # Module exports
│   ├── facade.rs      # Unified facade with auto-detection
│   ├── engine.rs      # Shared rewrite engine logic
│   ├── mcap.rs        # MCAP format rewriter
│   └── bag.rs         # ROS1 bag format rewriter
│
└── surface/           # Deprecated: Backward compatibility layer
    └── mod.rs         # Deprecated re-exports
```

## Key Principles

### 1. Format-Centric Organization

Each format (MCAP, ROS1 bag) has its own module containing:
- Low-level I/O operations
- Format-specific readers and writers
- High-level convenience APIs

**Benefits**:
- Easy to locate format-specific code
- Simple to add new formats (create a new directory)
- Clear ownership boundaries

### 2. Layered Architecture

```
┌─────────────────────────────────────────────┐
│  User Layer (lib.rs re-exports)              │
│  - McapReader, BagWriter, etc.              │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│  High-Level API Layer                       │
│  - mcap/reader_api.rs (auto-decoding)       │
│  - mcap/writer_api.rs (custom writer)        │
│  - bag/writer.rs (high-level writer)         │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│  Low-Level I/O Layer                        │
│  - mcap/parallel.rs, mcap/reader.rs         │
│  - bag/parallel.rs, bag/reader.rs           │
│  - io/ (unified traits)                      │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│  Foundation Layer                           │
│  - core/ (errors, types)                    │
│  - encoding/ (codecs)                        │
│  - schema/ (parsing)                         │
└─────────────────────────────────────────────┘
```

### 3. Rewriter Architecture

The rewriter module provides a unified facade that:
- Auto-detects format from file extension
- Delegates to format-specific rewriters
- Shares common transformation logic via `engine.rs`

```
User code
  │
  ├─ RoboRewriter::open("data.mcap")
  │       │
  │       ├─ detect_format() → "mcap"
  │       │
  │       └─ creates McapRewriter
  │
  └─ RoboRewriter::open("data.bag")
          │
          ├─ detect_format() → "bag"
          │
          └─ creates BagRewriter
```

## Design Decisions

### Why Format-Centric?

**Problem**: Users think in terms of formats ("I'm working with MCAP"), not functionality layers ("I need the reader module").

**Solution**: Organize by format:
```rust
// Clear: Everything MCAP-related is in one place
use robocodec::mcap::{reader_api::McapReader, writer_api::ParallelMcapWriter};
```

### Why Separate `reader_api` and `writer_api`?

The `_api` suffix indicates high-level convenience APIs:
- `reader_api.rs` - Auto-decoding (messages automatically decoded)
- `writer_api.rs` - Custom writer with manual chunk control

These are distinct from low-level `reader.rs` and `writer.rs`.

### Why Keep `surface/` as Deprecated?

For backward compatibility during the transition period. Users get:
```rust
#[deprecated(note = "Use robocodec::mcap::reader_api::McapReader")]
pub use crate::mcap::reader_api::McapReader;
```

This allows gradual migration without breaking existing code.

## Usage Examples

### Reading MCAP with Auto-Decoding

```rust
use robocodec::mcap::reader_api::McapReader;

let reader = McapReader::open("file.mcap")?;
for result in reader.decode_messages()? {
    let (decoded, channel) = result?;
    println!("Topic: {}, Fields: {:?}", channel.topic, decoded);
}
```

### Writing MCAP with Custom Writer

```rust
use robocodec::mcap::writer_api::ParallelMcapWriter;

let writer = ParallelMcapWriter::create("output.mcap")?;
writer.add_channel(...)?;
writer.write_chunk(...)?;
writer.finish()?;
```

### Rewriting with Auto-Detection

```rust
use robocodec::RoboRewriter;

// Format auto-detected from extension
let mut rewriter = RoboRewriter::open("input.mcap")?;
rewriter.rewrite("output.mcap")?;
```

## Adding a New Format

To add a new format (e.g., ROS2 bag):

1. Create directory: `robocodec/src/ros2bag/`
2. Implement low-level I/O: `reader.rs`, `writer.rs`
3. Add high-level APIs if needed: `reader_api.rs`, `writer_api.rs`
4. Create rewriter: `rewriter/ros2bag.rs`
5. Update `rewriter/facade.rs` to detect new format
6. Add module declaration in `lib.rs`

## Migration Guide

### From `surface` Module

**Old code:**
```rust
use robocodec::surface::{McapReader, ParallelMcapWriter};
```

**New code:**
```rust
use robocodec::mcap::{reader_api::McapReader, writer_api::ParallelMcapWriter};
// Or use type aliases:
use robocodec::{McapReader, McapWriter};
```

### From `RoboRewriter`

No changes needed - `RoboRewriter` is still available from `robocodec::` root.

## Related Documentation

- [CLAUDE.md](CLAUDE.md) - Project overview and build commands
- [../../docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) - Workspace-level architecture
- [../../docs/PIPELINE.md](docs/PIPELINE.md) - Pipeline architecture
