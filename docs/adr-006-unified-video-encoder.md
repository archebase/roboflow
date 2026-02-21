# ADR-006: Unified Video Encoder Architecture

**Author**: Sisyphus (AI Agent)  
**Date**: 2026-02-21  
**Status**: Proposed  
**Related**: [ADR-002](./adr-002-crate-architecture-refactoring.md)

## Context

The current video encoding subsystem has grown organically and suffers from significant architectural debt:

### Current Problem: Encoder Proliferation

The codebase currently contains **9+ distinct encoder types** with overlapping functionality:

| Encoder | Location | Purpose | Issues |
|---------|----------|---------|--------|
| `FragmentEncoder` | `media/video/fragment.rs` | Batch frames → temp MP4 fragments | Duplicates `RsmpegMp4Encoder` |
| `EncoderPool` | `media/video/encoder_pool.rs` | Parallel encoding workers | Tightly coupled to fragment encoding |
| `ConcurrentVideoEncoder` | `formats/common/concurrent_video_encoder.rs` | Multi-camera orchestration | Couples encoding + threading + upload |
| `StreamingMp4Encoder` | `media/video/streaming.rs` | fMP4 output via AVIO | Different API from file-based encoders |
| `RsmpegEncoder` | `media/video/rsmpeg.rs` | Frame-by-frame native encoding | Similar to `StreamingMp4Encoder` |
| `RsmpegMp4Encoder` | `media/video/rsmpeg.rs` | File-based batch encoding | Duplicates `FragmentEncoder` |
| `PersistentEncoder` | `media/video/rsmpeg.rs` | Reuses codec context | Internal detail leaked as pub |
| `Mp4Encoder` | `media/video/hardware.rs` | FFmpeg CLI wrapper | Deprecated by native encoders |
| `NvencEncoder` | `media/video/hardware.rs` | NVENC via CLI | Duplicates rsmpeg hardware support |
| `VideoToolboxEncoder` | `media/video/hardware.rs` | macOS via CLI | Duplicates rsmpeg hardware support |

### Specific Problems

#### 1. Inconsistent APIs

```rust
// Different encoders have completely different APIs:

// FragmentEncoder - uses VideoFrame, returns FragmentInfo
fragment_encoder.encode(vec![frame])?;

// StreamingMp4Encoder - uses raw bytes, channel output
streaming_encoder.add_frame(&rgb_data)?;

// RsmpegMp4Encoder - uses VideoFrameBuffer, file output
rsmpeg_encoder.encode_buffer(&buffer, &path)?;

// Hardware encoders - use VideoFrameBuffer, CLI subprocess
hardware_encoder.encode_buffer(&buffer, &path)?;
```

#### 2. Duplicate Hardware Encoding

Both `hardware.rs` (FFmpeg CLI) and `rsmpeg.rs` (native bindings) implement:
- NVENC (NVIDIA GPU)
- VideoToolbox (macOS)

The CLI approach is slower (process spawn overhead) and less reliable than native rsmpeg.

#### 3. Mixed Concerns

`ConcurrentVideoEncoder` combines:
- Video encoding
- Thread pool management
- S3 multipart upload
- Per-camera coordination

This violates single responsibility principle.

#### 4. Unclear Abstraction Levels

- Low-level: `FragmentEncoder`, `PersistentEncoder`
- Mid-level: `EncoderPool`, `StreamingMp4Encoder`
- High-level: `ConcurrentVideoEncoder`

No clear layering - users must understand all levels to choose correctly.

## Decision

Unify all video encoding behind a single `VideoEncoder` type with pluggable strategies.

### Core Principle: Composition Over Inheritance

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         UNIFIED ENCODER STACK                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  VideoEncoder (Single Public Type)                                   │   │
│  │                                                                       │   │
│  │   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐           │   │
│  │   │ Input Source │───▶│   Engine     │───▶│ Output Sink  │           │   │
│  │   │              │    │              │    │              │           │   │
│  │   │ - Iterator   │    │ - Software   │    │ - Channel    │           │   │
│  │   │ - Channel    │    │ - Hardware   │    │ - File       │           │   │
│  │   │ - Callback   │    │ - Auto       │    │ - Fragment   │           │   │
│  │   └──────────────┘    └──────────────┘    └──────────────┘           │   │
│  │                                                                       │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  Configuration:                                                              │
│  - resolution, fps, bitrate (video params)                                   │
│  - engine: Software | HardwareAuto | Nvenc | VideoToolbox                   │
│  - pipeline: SingleThread | Parallel { workers }                            │
│  - output: Channel | File | Fragment { concat_strategy }                    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Architecture Components

#### 1. Unified Configuration

```rust
/// Single configuration for all encoding scenarios
pub struct VideoEncoderConfig {
    // Video parameters
    pub resolution: (u32, u32),
    pub fps: u32,
    pub bitrate: u64,
    pub codec: CodecType,           // H264 | HEVC | Auto
    
    // Encoding engine selection
    pub engine: EngineConfig,
    
    // Processing pipeline
    pub pipeline: PipelineConfig,
    
    // Output destination
    pub output: OutputConfig,
}

pub enum EngineConfig {
    Software(SoftwareConfig),       // libx264 settings
    HardwareAuto,                   // Detect best available
    Nvenc(NvencConfig),             // NVIDIA specific
    VideoToolbox,                   // macOS specific
}

pub enum PipelineConfig {
    SingleThread,                   // Simple, low memory
    Parallel(PipelineWorkers),      // High throughput
}

pub struct PipelineWorkers {
    pub decode: usize,              // JPEG/PNG decoding workers
    pub convert: usize,             // RGB→YUV conversion workers
    pub encode: usize,              // Video encoding workers
}

pub enum OutputConfig {
    /// Stream chunks via callback
    Stream { 
        chunk_handler: Box<dyn Fn(EncodedChunk) + Send>,
        chunk_size: usize,
    },
    /// Fragmented output with final concatenation
    Fragment {
        frames_per_fragment: usize,
        concat_method: ConcatMethod,    // FFmpeg | Mp4Box
        temp_dir: PathBuf,
    },
    /// Direct file output
    File(PathBuf),
    /// S3 multipart upload
    S3Multipart {
        bucket: String,
        key: String,
        config: S3Config,
    },
}
```

#### 2. Engine Trait (Pluggable Backends)

```rust
/// Video encoding backend (software/hardware)
pub trait EncodingEngine: Send + Sync {
    /// Initialize encoder with configuration
    fn initialize(&mut self, config: &VideoEncoderConfig) -> Result<(), Error>;
    
    /// Encode a batch of frames
    fn encode_batch(&mut self, frames: &[VideoFrame]) -> Result<Vec<EncodedChunk>, Error>;
    
    /// Flush remaining frames and finalize
    fn finalize(self) -> Result<Option<Vec<EncodedChunk>>, Error>;
    
    /// Check if engine supports frame type
    fn supports_frame_format(&self, format: PixelFormat) -> bool;
}

/// Software encoding via rsmpeg/libx264
pub struct SoftwareEngine;

/// Hardware encoding with auto-detection
pub struct HardwareEngine {
    backend: HardwareBackend,
}

enum HardwareBackend {
    Nvenc(NvencContext),
    VideoToolbox(VideoToolboxContext),
}
```

#### 3. Output Sink Trait (Pluggable Destinations)

```rust
/// Where encoded video data goes
pub trait OutputSink: Send {
    /// Write an encoded chunk
    fn write(&mut self, chunk: EncodedChunk) -> Result<(), Error>;
    
    /// Finalize output and return result
    fn finalize(self: Box<Self>) -> Result<OutputResult, Error>;
}

/// Stream to callback
pub struct StreamSink;

/// Accumulate fragments and concatenate
pub struct FragmentSink;

/// Write to local file
pub struct FileSink;

/// Upload to S3 via multipart
pub struct S3MultipartSink;
```

#### 4. The One Encoder Type

```rust
/// Unified video encoder - the only public encoder type
pub struct VideoEncoder {
    config: VideoEncoderConfig,
    pipeline: PipelineType,
    engine: Box<dyn EncodingEngine>,
    sink: Box<dyn OutputSink>,
    state: EncoderState,
}

impl VideoEncoder {
    /// Create encoder from configuration
    pub fn new(config: VideoEncoderConfig) -> Result<Self, Error> {
        let engine = create_engine(&config.engine)?;
        let sink = create_sink(&config.output)?;
        let pipeline = create_pipeline(&config.pipeline)?;
        
        Ok(Self {
            config,
            pipeline,
            engine,
            sink,
            state: EncoderState::Ready,
        })
    }
    
    /// Encode from any frame source
    pub fn encode<S: FrameSource>(&mut self, source: S) -> Result<(), Error> {
        match &self.pipeline {
            PipelineType::SingleThread => self.encode_single_threaded(source),
            PipelineType::Parallel(config) => self.encode_parallel(source, config),
        }
    }
    
    /// Add a single frame (for streaming scenarios)
    pub fn add_frame(&mut self, frame: VideoFrame) -> Result<(), Error> {
        // Buffer and encode when batch is full
        self.frame_buffer.push(frame);
        if self.frame_buffer.len() >= self.batch_size() {
            self.flush_buffer()?;
        }
        Ok(())
    }
    
    /// Finalize encoding
    pub fn finalize(mut self) -> Result<OutputResult, Error> {
        self.flush_buffer()?;
        let final_chunks = self.engine.finalize()?;
        if let Some(chunks) = final_chunks {
            for chunk in chunks {
                self.sink.write(chunk)?;
            }
        }
        self.sink.finalize()
    }
}
```

### Module Restructuring

**Current Structure (Messy):**
```
video/
  ├─ mod.rs                   # Re-exports 50+ items from all modules
  ├─ hardware.rs              # FFmpeg CLI encoders (1066 lines, legacy)
  ├─ rsmpeg.rs                # Native encoders (1532 lines, too large)
  ├─ streaming.rs             # Streaming encoder (603 lines)
  ├─ fragment.rs              # Fragment encoder (356 lines)
  ├─ encoder_pool.rs          # Parallel encoding (453 lines)
  ├─ pipeline/
  │   ├─ mod.rs               # Pipeline abstractions
  │   └─ parallel.rs          # 3-stage parallel pipeline
  ├─ concurrent_video_encoder.rs  # High-level orchestrator (748 lines)
  ├─ convert.rs               # Color conversion pool
  ├─ decode.rs                # Decode pool
  ├─ composer.rs              # Video concatenation
  ├─ config.rs                # Encoder configs
  ├─ frame.rs                 # Frame types
  ├─ codec.rs                 # Codec utilities
  ├─ simd/                    # SIMD color conversion
  ├─ arena.rs                 # Memory arena
  └─ ...
```

**Proposed Structure (Clean):**
```
video/
  ├─ lib.rs                   # Public API: VideoEncoder, VideoEncoderConfig only
  ├─ encoder.rs               # Core VideoEncoder implementation
  ├─ config.rs                # Unified configuration types
  ├─ error.rs                 # Encoding error types
  │
  ├─ engine/                  # Encoding backends
  │   ├─ mod.rs               # EncodingEngine trait
  │   ├─ software.rs          # libx264 via rsmpeg
  │   ├─ hardware.rs          # Auto-detect + hardware encoders
  │   └─ context.rs           # Reusable codec context (PersistentEncoder logic)
  │
  ├─ pipeline/                # Frame processing pipeline
  │   ├─ mod.rs               # Pipeline trait
  │   ├─ single.rs            # Single-threaded pipeline
  │   └─ parallel.rs          # 3-stage parallel pipeline
  │       ├─ decode_pool.rs   # Move from parent
  │       ├─ convert_pool.rs  # Move from parent
  │       └─ encode_pool.rs   # Move from parent
  │
  ├─ sink/                    # Output destinations
  │   ├─ mod.rs               # OutputSink trait
  │   ├─ stream.rs            # Channel/callback output
  │   ├─ fragment.rs          # Fragment + concatenation
  │   ├─ file.rs              # Direct file output
  │   └─ s3.rs                # S3 multipart upload
  │
  ├─ composer.rs              # Video concatenation (final step)
  │
  ├─ frame/                   # Frame types (extract from frame.rs)
  │   ├─ mod.rs
  │   ├─ buffer.rs
  │   ├─ format.rs
  │   └─ source.rs            # FrameSource trait
  │
  └─ util/                    # Utilities
      ├─ codec.rs             # Codec detection/helpers
      ├─ simd.rs              # SIMD exports
      └─ arena.rs             # Memory arena
```

## Migration Path

### Phase 1: Create New Unified API (Additive)

Create new modules without modifying existing code:

```rust
// video/encoder.rs - New unified encoder
pub struct VideoEncoder { ... }

// video/config.rs - Add new config types alongside existing
pub struct VideoEncoderConfig { ... }  // New unified config
// Keep existing configs for backward compatibility

// video/engine/mod.rs - New engine trait
pub trait EncodingEngine { ... }
```

### Phase 2: Implement Engines (Additive)

Implement `EncodingEngine` for existing functionality:

```rust
// video/engine/software.rs
impl EncodingEngine for SoftwareEngine {
    // Uses existing rsmpeg code internally
}

// video/engine/hardware.rs  
impl EncodingEngine for HardwareEngine {
    // Uses existing hardware detection + rsmpeg
}
```

### Phase 3: Deprecate Old Types

Mark old encoders as deprecated:

```rust
// hardware.rs
#[deprecated(since = "0.5.0", note = "Use VideoEncoder with EngineConfig::HardwareAuto")]
pub struct Mp4Encoder { ... }

// rsmpeg.rs
#[deprecated(since = "0.5.0", note = "Use VideoEncoder with OutputConfig::Stream")]
pub struct StreamingMp4Encoder { ... }
```

### Phase 4: Migrate Internal Usage

Update internal code to use new API:

```rust
// Before
let encoder = ConcurrentVideoEncoder::new(config)?;

// After
let config = VideoEncoderConfig {
    engine: EngineConfig::HardwareAuto,
    pipeline: PipelineConfig::Parallel(PipelineWorkers::auto()),
    output: OutputConfig::S3Multipart { ... },
    ..Default::default()
};
let encoder = VideoEncoder::new(config)?;
```

### Phase 5: Remove Deprecated Types (v0.6.0)

Delete deprecated modules:
- `hardware.rs` (CLI encoders)
- `streaming.rs` (merged into encoder)
- `fragment.rs` (moved to `sink/fragment.rs`)
- `encoder_pool.rs` (internal to parallel pipeline)

## API Comparison

### Before (Multiple Types, Inconsistent APIs)

```rust
use roboflow::media::video::{
    FragmentEncoder, FragmentEncoderConfig,
    StreamingMp4Encoder, StreamingEncoderConfig,
    RsmpegMp4Encoder, VideoEncoderConfig as RsmpegConfig,
    ConcurrentVideoEncoder, ConcurrentEncoderConfig,
    select_best_encoder, EncoderChoice,
};

// User must choose between 9 different encoders
let encoder = if cfg!(target_os = "macos") {
    EncoderChoice::VideoToolbox
} else if check_nvenc_available() {
    EncoderChoice::Nvenc
} else {
    EncoderChoice::Software
};

// Different APIs for each encoder type
match encoder {
    EncoderChoice::Software => {
        let mut enc = RsmpegMp4Encoder::with_config(config);
        enc.encode_buffer(&buffer, &path)?;
    }
    EncoderChoice::Hardware => {
        let mut enc = StreamingMp4Encoder::new(config, tx)?;
        for frame in frames {
            enc.add_frame(&frame.data)?;
        }
        enc.finalize()?;
    }
}
```

### After (Single Type, Declarative Config)

```rust
use roboflow::media::video::{VideoEncoder, VideoEncoderConfig};

// One type, declarative configuration
let config = VideoEncoderConfig {
    resolution: (1920, 1080),
    fps: 30,
    bitrate: 5_000_000,
    engine: EngineConfig::HardwareAuto,  // Auto-detects best
    pipeline: PipelineConfig::Parallel(PipelineWorkers::auto()),
    output: OutputConfig::Fragment {
        frames_per_fragment: 30,
        concat_method: ConcatMethod::Ffmpeg,
        temp_dir: std::env::temp_dir(),
    },
};

// Unified API regardless of backend
let mut encoder = VideoEncoder::new(config)?;
encoder.encode(frame_source)?;
let result = encoder.finalize()?;
```

## Consequences

### Positive

| Aspect | Before | After |
|--------|--------|-------|
| **Public Types** | 9+ encoder types | 1 `VideoEncoder` |
| **API Surface** | 50+ public items | ~10 core items |
| **Documentation** | "Which encoder should I use?" | "Configure `VideoEncoder` for your needs" |
| **Adding Backend** | New struct + duplicate API | Implement `EncodingEngine` trait |
| **Testing** | Test each encoder separately | Test trait implementations |
| **Hardware Support** | Duplicated (CLI + native) | Single native implementation |
| **Maintainability** | Changes needed in 5+ files | Changes isolated to engine module |

### Trade-offs

| Aspect | Consideration |
|--------|--------------|
| **Refactoring Effort** | Large-scale reorganization of video module |
| **API Migration** | Existing users must update code |
| **Compile Time** | Additional trait dispatch overhead (negligible) |
| **Binary Size** | May increase slightly due to trait objects |

## Testing Strategy

### Engine Tests

```rust
#[test]
fn test_software_engine_produces_valid_mp4() {
    let config = VideoEncoderConfig {
        engine: EngineConfig::Software(Default::default()),
        output: OutputConfig::File(temp_path()),
        ..test_config()
    };
    
    let mut encoder = VideoEncoder::new(config).unwrap();
    encoder.encode(test_frames()).unwrap();
    let result = encoder.finalize().unwrap();
    
    assert_valid_mp4(&result.path);
}

#[test]
fn test_hardware_engine_detects_available_backend() {
    let config = VideoEncoderConfig {
        engine: EngineConfig::HardwareAuto,
        ..test_config()
    };
    
    // Should work on any platform
    let encoder = VideoEncoder::new(config);
    assert!(encoder.is_ok());
}
```

### Pipeline Tests

```rust
#[test]
fn test_parallel_pipeline_matches_single_threaded() {
    let parallel_config = VideoEncoderConfig {
        pipeline: PipelineConfig::Parallel(PipelineWorkers::auto()),
        ..test_config()
    };
    
    let single_config = VideoEncoderConfig {
        pipeline: PipelineConfig::SingleThread,
        ..test_config()
    };
    
    // Both should produce identical output
    let parallel_output = encode_with(parallel_config);
    let single_output = encode_with(single_config);
    
    assert_eq!(parallel_output.frames, single_output.frames);
}
```

### Sink Tests

```rust
#[test]
fn test_fragment_sink_concatenates_correctly() {
    let config = VideoEncoderConfig {
        output: OutputConfig::Fragment {
            frames_per_fragment: 10,
            concat_method: ConcatMethod::Ffmpeg,
            temp_dir: temp_dir(),
        },
        ..test_config()
    };
    
    let mut encoder = VideoEncoder::new(config).unwrap();
    encoder.encode(100_frames()).unwrap();  // 10 fragments
    let result = encoder.finalize().unwrap();
    
    assert_eq!(result.fragments_created, 10);
    assert!(result.final_path.exists());
}
```

## Implementation Checklist

### Phase 1: Foundation
- [ ] Create `video/encoder.rs` with `VideoEncoder` type
- [ ] Create `video/config.rs` with unified configuration
- [ ] Create `video/engine/mod.rs` with `EncodingEngine` trait
- [ ] Create `video/sink/mod.rs` with `OutputSink` trait

### Phase 2: Engine Implementations
- [ ] Implement `SoftwareEngine` (wraps existing rsmpeg)
- [ ] Implement `HardwareEngine` with auto-detection
- [ ] Move hardware detection from `hardware_config.rs` to engine module

### Phase 3: Sink Implementations
- [ ] Implement `StreamSink` (replaces `StreamingMp4Encoder`)
- [ ] Implement `FragmentSink` (replaces `FragmentEncoder` + composer)
- [ ] Implement `FileSink` (replaces `RsmpegMp4Encoder` file mode)
- [ ] Implement `S3MultipartSink` (moves upload logic out of encoder)

### Phase 4: Pipeline Refactoring
- [ ] Refactor `PipelineType::Parallel` to use `DecodePool`/`ConvertPool`/`EncodePool`
- [ ] Move pool implementations to `video/pipeline/`
- [ ] Simplify `ConcurrentVideoEncoder` to use new `VideoEncoder`

### Phase 5: Deprecation
- [ ] Mark `hardware.rs` types as deprecated
- [ ] Mark `streaming.rs` types as deprecated
- [ ] Mark `fragment.rs` public types as deprecated
- [ ] Update all internal usage to new API

### Phase 6: Cleanup (v0.6.0)
- [ ] Remove deprecated modules
- [ ] Update public exports
- [ ] Update documentation

## References

- [Current video module](../crates/roboflow-dataset/src/media/video/) - Existing implementation
- [rsmpeg.rs](../crates/roboflow-dataset/src/media/video/rsmpeg.rs) - Native encoding
- [hardware.rs](../crates/roboflow-dataset/src/media/video/hardware.rs) - CLI encoders
- [ADR-002](./adr-002-crate-architecture-refactoring.md) - Related crate refactoring

## Open Questions

1. **Should SIMD conversion be an engine or pipeline concern?**
   - Option A: Engine handles all conversion internally
   - Option B: Pipeline pre-converts, engine receives NV12 directly

2. **Should we keep FFmpeg CLI fallback?**
   - Option A: Remove entirely (rsmpeg only)
   - Option B: Keep as `EngineConfig::ExternalFfmpeg` for edge cases

3. **How to handle multiple camera encoding?**
   - Option A: `VideoEncoder` handles single camera, separate orchestrator for multi
   - Option B: `VideoEncoder` accepts camera_id parameter internally

4. **Should fragment concatenation be automatic or explicit?**
   - Option A: `FragmentSink` auto-concatenates on finalize
   - Option B: Separate `Composer` step, user calls explicitly
