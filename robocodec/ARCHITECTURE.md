# Robofmt Architecture

This document describes the architectural organization of the robocodec crate.

## Overview

Robofmt is organized as a **format-centric** library, where each robotics data format has its own module containing all related functionality (readers, writers, high-level APIs).

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
│  - io/formats/mcap/reader.rs (auto-decode)  │
│  - io/formats/mcap/writer.rs (custom)       │
│  - io/formats/bag/writer.rs (high-level)    │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│  Low-Level I/O Layer                        │
│  - io/formats/mcap/parallel.rs, reader.rs   │
│  - io/formats/bag/parallel.rs, reader.rs    │
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

**Solution**: Organize by format under `io/formats/`:
```rust
// Clear: Everything MCAP-related is in one place
use robocodec::io::formats::mcap::{reader::McapReader, writer::ParallelMcapWriter};
// Backward compatible (deprecated):
use robocodec::mcap::{McapReader, ParallelMcapWriter};
```

### Why `io/formats/` Directory Structure?

The I/O layer is organized as:
- `io/` - Core I/O traits, metadata, unified reader/writer
- `io/formats/` - Format-specific implementations (mcap, bag)

This structure:
- Groups related formats together
- Provides clear separation from I/O infrastructure
- Makes it easy to add new formats

### High-Level vs Low-Level APIs

Within each format module:
- `reader.rs` - High-level API with auto-decoding
- `writer.rs` - Custom writer with manual chunk control
- `parallel.rs` - Low-level parallel reader
- `sequential.rs` - Low-level sequential reader

### Backward Compatibility

The crate root maintains deprecated re-exports:
```rust
#[deprecated(note = "Use io::formats::mcap instead")]
pub use io::formats::mcap;
```

This allows gradual migration without breaking existing code.

## Usage Examples

### Reading MCAP with Auto-Decoding

```rust
use robocodec::io::formats::mcap::reader::McapReader;

let reader = McapReader::open("file.mcap")?;
for result in reader.decode_messages()? {
    let (decoded, channel) = result?;
    println!("Topic: {}, Fields: {:?}", channel.topic, decoded);
}
```

### Writing MCAP with Custom Writer

```rust
use robocodec::io::formats::mcap::writer::ParallelMcapWriter;

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

1. Create directory: `robocodec/src/io/formats/ros2bag/`
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
