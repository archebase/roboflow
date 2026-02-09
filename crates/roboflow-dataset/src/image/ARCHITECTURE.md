# Image Decoding Architecture

## Overview

This document describes the **Clean Architecture** for JPEG/PNG image decoding in the roboflow distributed system. The design follows established patterns from `roboflow-pipeline/gpu/` and integrates with the distributed worker infrastructure.

## Design Principles

1. **Trait-based abstraction** - `ImageDecoderBackend` trait for pluggable implementations
2. **Factory pattern** - `ImageDecoderFactory` for auto-detection and fallback
3. **Platform-specific compilation** - CPU/GPU/Apple backends with stubs
4. **GPU-friendly memory** - Pinned allocation for efficient CPU→GPU transfers
5. **Distributed integration** - Worker-level decoder pooling for horizontal scaling

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Distributed Workers                                │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │   Worker 1  │  │   Worker 2  │  │   Worker 3  │  │   Worker N  │         │
│  │  (decode)   │  │  (decode)   │  │  (decode)   │  │  (decode)   │         │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘         │
│         │                │                │                │                  │
│         └────────────────┴────────────────┴────────────────┘                  │
│                              │                                             │
└──────────────────────────────┼─────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      Image Decoder Factory Layer                           │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │              ImageDecoderFactory::create(config)                     │   │
│  │                                                                       │   │
│  │   1. Check GPU availability (CUDA/nvJPEG)                            │   │
│  │   2. Check Apple Silicon availability (libjpeg-turbo hardware)       │   │
│  │   3. Fall back to CPU decoder (image crate)                          │   │
│  │   4. Return Box<dyn ImageDecoderBackend>                              │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
└──────────────────────────────┬─────────────────────────────────────────────┘
                               │
         ┌─────────────────────┼─────────────────────┐
         │                     │                     │
         ▼                     ▼                     ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│  GPU Decoder    │  │  Apple Decoder  │  │   CPU Decoder   │
│  (nvJPEG/cuVID) │  │  (hardware acc) │  │   (image crate)  │
│                 │  │                 │  │                 │
│ • Linux only    │  │ • macOS only    │  │ • All platforms  │
│ • CUDA required │  │ • Apple Silicon │  │ • Always avail   │
│ • Max throughput│  │ • Fast decode   │  │ • Baseline       │
└────────┬────────┘  └────────┬────────┘  └────────┬────────┘
         │                    │                    │
         └────────────────────┴────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ImageDecoderBackend Trait                               │
│                                                                              │
│  trait ImageDecoderBackend:                                               │
│      fn decode(&self, data: &[u8], format: ImageFormat)                  │
│          -> Result<DecodedImage, ImageError>                              │
│      fn decode_batch(&self, images: &[(&[u8], ImageFormat)])             │
│          -> Result<Vec<DecodedImage>, ImageError>                         │
│      fn decoder_type(&self) -> DecoderType                               │
│      fn is_available(&self) -> bool                                      │
│      fn get_memory_strategy(&self) -> MemoryStrategy                     │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Module Structure

```
crates/roboflow-dataset/src/image/
├── mod.rs                    # Public exports, module documentation
├── backend.rs                # ImageDecoderBackend trait + CPU implementation
├── config.rs                 # ImageDecoderConfig with builder pattern
├── factory.rs                # ImageDecoderFactory for auto-detection
├── gpu.rs                    # GPU decoder (nvJPEG/cuVID) - Linux only
├── apple.rs                  # Apple hardware decoder - macOS only
├── memory.rs                 # GPU-friendly memory allocation
└── format.rs                 # Format detection utilities
```

## Integration Points

### 1. Streaming Converter Integration

**File:** `crates/roboflow-dataset/src/streaming/alignment.rs`

```rust
use crate::image::{ImageDecoderFactory, ImageDecoderConfig};

// In FrameAlignmentBuffer
pub struct FrameAlignmentBuffer {
    // ... existing fields

    /// Image decoder factory (created once, reused for all frames)
    decoder_factory: ImageDecoderFactory,
}

impl FrameAlignmentBuffer {
    pub fn with_decoder(config: StreamingConfig, decoder_config: ImageDecoderConfig) -> Self {
        Self {
            // ... existing initialization
            decoder_factory: ImageDecoderFactory::new(&decoder_config),
            ..Self::new(config)
        }
    }
}

// In extract_message_to_frame_static()
if is_encoded {
    let decoder = self.decoder_factory.get_decoder();
    match decoder.decode(&image_data, format) {
        Ok(decoded) => {
            partial.frame.images.insert(
                feature_name.to_string(),
                ImageData {
                    width: decoded.width,
                    height: decoded.height,
                    data: decoded.data,      // RGB, GPU-friendly allocated
                    original_timestamp: timestamped_msg.log_time,
                    is_encoded: false,
                },
            );
        }
        Err(e) => {
            tracing::warn!(error = %e, "Failed to decode image, using encoded data");
            // Store encoded data (will fail at video encoding)
        }
    }
}
```

### 2. Distributed Worker Integration

**File:** `crates/roboflow-distributed/src/worker.rs`

```rust
use roboflow_dataset::image::{ImageDecoderFactory, ImageDecoderConfig};

pub struct Worker {
    // ... existing fields

    /// Image decoder factory for processing compressed images
    decoder_factory: ImageDecoderFactory,
}

impl Worker {
    pub fn new(
        worker_id: String,
        tikv: Arc<TikvClient>,
        storage: Arc<dyn Storage>,
        worker_config: WorkerConfig,
        decoder_config: ImageDecoderConfig,
    ) -> Result<Self> {
        Ok(Self {
            // ... existing initialization
            decoder_factory: ImageDecoderFactory::new(&decoder_config),
        })
    }

    // Workers can share GPU decoder pool via Arc<ImageDecoderFactory>
}
```

## Memory Strategy

### GPU-Friendly Allocation

**File:** `crates/roboflow-dataset/src/image/memory.rs`

```rust
/// Memory allocation strategy for decoded images.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryStrategy {
    /// Standard heap allocation (default)
    Heap,

    /// Page-aligned allocation (4096 bytes) for efficient DMA transfers
    PageAligned,

    /// CUDA pinned memory (for zero-copy GPU transfers)
    #[cfg(feature = "cuda-pinned")]
    CudaPinned,
}

/// GPU-aligned image buffer for efficient CPU→GPU transfers.
pub struct AlignedImageBuffer {
    /// RGB data with proper alignment
    pub data: Vec<u8>,
    /// Alignment used
    pub alignment: usize,
}

impl AlignedImageBuffer {
    /// Allocate buffer with page alignment for DMA transfers.
    pub fn page_aligned(size: usize) -> Self {
        const PAGE_SIZE: usize = 4096;
        let aligned_size = (size + PAGE_SIZE - 1) / PAGE_SIZE * PAGE_SIZE;
        let mut vec = Vec::with_capacity(aligned_size);
        unsafe {
            vec.set_len(aligned_size);
        }
        vec.truncate(size);
        Self { data: vec, alignment: PAGE_SIZE }
    }

    /// Get pointer suitable for GPU transfer.
    pub fn as_gpu_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }
}
```

## Configuration

**File:** `crates/roboflow-dataset/src/image/config.rs`

```rust
/// Configuration for image decoding.
#[derive(Debug, Clone)]
pub struct ImageDecoderConfig {
    /// Which decoder backend to use
    pub backend: DecoderBackendType,

    /// Memory allocation strategy
    pub memory_strategy: MemoryStrategy,

    /// GPU device ID for CUDA operations
    pub gpu_device: Option<u32>,

    /// Enable automatic CPU fallback
    pub auto_fallback: bool,

    /// Maximum image dimensions (security limit)
    pub max_width: u32,
    pub max_height: u32,

    /// Number of decode threads (for CPU decoder)
    pub cpu_threads: usize,
}

impl Default for ImageDecoderConfig {
    fn default() -> Self {
        Self {
            backend: DecoderBackendType::Auto,
            memory_strategy: MemoryStrategy::PageAligned,
            gpu_device: None,
            auto_fallback: true,
            max_width: 7680,   // 8K resolution
            max_height: 4320,
            cpu_threads: rayon::current_num_threads().max(1),
        }
    }
}
```

## Backend Trait

**File:** `crates/roboflow-dataset/src/image/backend.rs`

```rust
use super::{ImageError, ImageFormat, Result};

/// Decoder type enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecoderType {
    /// CPU-based decoding (image crate)
    Cpu,
    /// GPU-based decoding (nvJPEG/cuVID)
    Gpu,
    /// Apple hardware-accelerated decoding
    Apple,
}

/// Trait for image decoder backends.
///
/// Provides a unified interface for both CPU and GPU
/// decoding implementations, enabling seamless fallback and
/// platform-agnostic code.
pub trait ImageDecoderBackend: Send + Sync {
    /// Decode a single image to RGB.
    fn decode(&self, data: &[u8], format: ImageFormat) -> Result<DecodedImage>;

    /// Decode multiple images in parallel (GPU-accelerated).
    fn decode_batch(&self, images: &[(&[u8], ImageFormat)]) -> Result<Vec<DecodedImage>> {
        // Default: sequential decoding
        images
            .iter()
            .map(|(data, format)| self.decode(data, *format))
            .collect()
    }

    /// Get the decoder type.
    fn decoder_type(&self) -> DecoderType;

    /// Check if the decoder is available.
    fn is_available(&self) -> bool {
        true
    }

    /// Get memory allocation strategy.
    fn memory_strategy(&self) -> MemoryStrategy;
}

/// Decoded image with GPU-friendly RGB data.
#[derive(Debug, Clone)]
pub struct DecodedImage {
    pub width: u32,
    pub height: u32,
    pub data: Vec<u8>,  // RGB, GPU-aligned allocation
}

/// CPU decoder using the `image` crate.
pub struct CpuImageDecoder {
    memory_strategy: MemoryStrategy,
    threads: usize,
}

impl ImageDecoderBackend for CpuImageDecoder {
    fn decode(&self, data: &[u8], format: ImageFormat) -> Result<DecodedImage> {
        match format {
            ImageFormat::Jpeg => self.decode_jpeg(data),
            ImageFormat::Png => self.decode_png(data),
            _ => Err(ImageError::UnsupportedFormat(format!("{:?}", format))),
        }
    }

    fn decoder_type(&self) -> DecoderType {
        DecoderType::Cpu
    }

    fn memory_strategy(&self) -> MemoryStrategy {
        self.memory_strategy
    }
}
```

## GPU Decoder (nvJPEG)

**File:** `crates/roboflow-dataset/src/image/gpu.rs`

```rust
//! GPU-accelerated image decoding using NVIDIA nvJPEG.
//!
//! # Platform Support
//!
//! - Linux x86_64/aarch64 with CUDA toolkit
//! - Requires NVIDIA GPU with compute capability 6.0+
//! - Falls back to CPU decoder on error

#[cfg(all(
    feature = "gpu-decode",
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
pub use nvjpeg::{NvjpegDecoder, NvjpegDecoderConfig};

#[cfg(all(
    feature = "gpu-decode",
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
mod nvjpeg {
    use super::{DecoderType, ImageDecoderBackend, ImageError, ImageFormat, Result};
    use super::memory::{AlignedImageBuffer, MemoryStrategy};

    /// GPU decoder using NVIDIA nvJPEG library.
    pub struct NvjpegDecoder {
        cuda_ctx: cudarc::driver::CudaDevice,
        nvjpeg_handle: cudarc::nvjpeg::NvJpeg,
        device_id: u32,
    }

    impl NvjpegDecoder {
        /// Try to create a new nvJPEG decoder.
        pub fn try_new(device_id: u32) -> Result<Self> {
            let cuda_ctx = cudarc::driver::CudaDevice::new(device_id)
                .map_err(|e| ImageError::GpuUnavailable(format!("{}", e)))?;

            let nvjpeg_handle = cudarc::nvjpeg::NvJpegBuilder::new()
                .build(&cuda_ctx)
                .map_err(|e| ImageError::GpuUnavailable(format!("{}", e)))?;

            Ok(Self {
                cuda_ctx,
                nvjpeg_handle,
                device_id,
            })
        }

        /// Check if nvJPEG is available.
        pub fn is_available() -> bool {
            // Try to initialize CUDA and nvJPEG
            Self::try_new(0).is_ok()
        }
    }

    impl ImageDecoderBackend for NvjpegDecoder {
        fn decode(&self, data: &[u8], format: ImageFormat) -> Result<DecodedImage> {
            match format {
                ImageFormat::Jpeg => self.decode_jpeg(data),
                ImageFormat::Png => {
                    // nvJPEG doesn't support PNG, fallback to CPU
                    tracing::debug!("nvJPEG doesn't support PNG, using CPU decoder");
                    self.decode_png_cpu(data)
                }
                _ => Err(ImageError::UnsupportedFormat(format!("{:?}", format))),
            }
        }

        fn decode_batch(&self, images: &[(&[u8], ImageFormat)]) -> Result<Vec<DecodedImage>> {
            // GPU batch decoding - process all JPEGs in parallel
            let jpeg_images: Vec<_> = images
                .iter()
                .filter(|(_, fmt)| *fmt == ImageFormat::Jpeg)
                .collect();

            if jpeg_images.is_empty() {
                return images
                    .iter()
                    .map(|(data, fmt)| self.decode(data, *fmt))
                    .collect();
            }

            // TODO: Implement nvJPEG batch decoding
            // For now, use sequential decoding
            images
                .iter()
                .map(|(data, fmt)| self.decode(data, *fmt))
                .collect()
        }

        fn decoder_type(&self) -> DecoderType {
            DecoderType::Gpu
        }

        fn memory_strategy(&self) -> MemoryStrategy {
            MemoryStrategy::CudaPinned
        }
    }
}
```

## Factory Pattern

**File:** `crates/roboflow-dataset/src/image/factory.rs`

```rust
use super::{
    backend::{CpuImageDecoder, ImageDecoderBackend},
    config::ImageDecoderConfig,
    gpu::nvjpeg::NvjpegDecoder,
    DecoderBackendType, MemoryStrategy, Result,
};

/// Factory for creating image decoder backends with automatic fallback.
pub struct ImageDecoderFactory {
    config: ImageDecoderConfig,
    cached_decoder: Option<Box<dyn ImageDecoderBackend>>,
}

impl ImageDecoderFactory {
    /// Create a new factory with the given configuration.
    pub fn new(config: &ImageDecoderConfig) -> Self {
        Self {
            config: config.clone(),
            cached_decoder: None,
        }
    }

    /// Create a decoder backend based on the configuration.
    pub fn create_decoder(&self) -> Result<Box<dyn ImageDecoderBackend>> {
        match self.config.backend {
            DecoderBackendType::Cpu => Ok(Box::new(CpuImageDecoder::new(
                self.config.memory_strategy,
                self.config.cpu_threads,
            ))),

            DecoderBackendType::Gpu => {
                #[cfg(all(feature = "gpu-decode", target_os = "linux"))]
                {
                    match NvjpegDecoder::try_new(self.config.gpu_device.unwrap_or(0)) {
                        Ok(decoder) => {
                            tracing::info!("Using GPU decoder (nvJPEG)");
                            Ok(Box::new(decoder))
                        }
                        Err(e) if self.config.auto_fallback => {
                            tracing::warn!("GPU decoder unavailable: {}. Falling back to CPU.", e);
                            Ok(Box::new(CpuImageDecoder::new(
                                self.config.memory_strategy,
                                self.config.cpu_threads,
                            )))
                        }
                        Err(e) => Err(e),
                    }
                }
                #[cfg(not(all(feature = "gpu-decode", target_os = "linux")))]
                {
                    if self.config.auto_fallback {
                        tracing::warn!("GPU decoding not supported on this platform. Using CPU.");
                        Ok(Box::new(CpuImageDecoder::new(
                            self.config.memory_strategy,
                            self.config.cpu_threads,
                        )))
                    } else {
                        Err(ImageError::GpuUnavailable(
                            "GPU decoding requires 'gpu-decode' feature on Linux".to_string()
                        ))
                    }
                }
            }

            DecoderBackendType::Auto => {
                // Try GPU first, then CPU
                #[cfg(all(feature = "gpu-decode", target_os = "linux"))]
                {
                    if let Ok(decoder) = NvjpegDecoder::try_new(self.config.gpu_device.unwrap_or(0)) {
                        tracing::info!("Auto-detected GPU decoder (nvJPEG)");
                        return Ok(Box::new(decoder));
                    }
                }

                // Fallback to CPU
                tracing::info!("Using CPU decoder");
                Ok(Box::new(CpuImageDecoder::new(
                    self.config.memory_strategy,
                    self.config.cpu_threads,
                )))
            }
        }
    }

    /// Get a decoder (cached or newly created).
    pub fn get_decoder(&self) -> &Box<dyn ImageDecoderBackend> {
        // Return cached decoder or create new one
        // For GPU decoders, this maintains CUDA context
        &self.cached_decoder
    }
}
```

## Feature Flags

**File:** `crates/roboflow-dataset/Cargo.toml`

```toml
[features]
# Existing features...
video = ["dep:ffmpeg-next"]

# CUDA pinned memory (optional, for zero-copy transfers)
cuda-pinned = []
```

## Data Flow

```
ROS Bag (CompressedImage JPEG)
    │
    ▼
┌─────────────────────────────────────────────────────────────────┐
│  Distributed Worker (roboflow-distributed)                      │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │ 1. Claim job from TiKV                                     ││
│  │ 2. Download MCAP from S3                                   ││
│  │ 3. Create ImageDecoderFactory with GPU config               ││
│  └─────────────────────────────────────────────────────────────┘│
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  Streaming Converter (roboflow-dataset/streaming)               │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │ 1. Iterate messages from MCAP                               ││
│  │ 2. For CompressedImage:                                    ││
│  │    a. Detect format (JPEG/PNG magic bytes)                 ││
│  │    b. Get decoder from factory                              ││
│  │    c. Decode to RGB (GPU or CPU based on availability)      ││
│  │    d. Allocate with page-aligned or pinned memory           ││
│  │ 3. Store ImageData { is_encoded: false, data: RGB }        ││
│  └─────────────────────────────────────────────────────────────┘│
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  LeRobotWriter (roboflow-dataset/lerobot)                       │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │ 1. Buffer RGB frames (already decoded)                       ││
│  │ 2. Pass to Mp4Encoder (ffmpeg-next)                         ││
│  │ 3. FFmpeg uploads to GPU memory                              ││
│  │ 4. NVENC encodes to H.264                                   ││
│  │ 5. Write MP4 file                                          ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

## TODOs for Future Optimization

### Phase 1: CPU Decoding (Current)
- [x] Basic JPEG/PNG CPU decoding
- [x] Page-aligned memory allocation
- [x] Factory pattern with auto-fallback

### Phase 2: GPU Decoding (Next)
- [ ] NVIDIA nvJPEG integration for JPEG
- [ ] CUDA pinned memory allocation
- [ ] Batch decoding optimization
- [ ] GPU memory pooling

### Phase 3: Advanced Features (Future)
- [ ] cuVID for hardware video decode (H.264/H.265 compressed images)
- [ ] Direct GPU-to-GPU pipeline (decode on GPU, encode on GPU)
- [ ] Distributed decoder pool (shared GPU across workers)
- [ ] Zero-copy integration with NVENC

## Testing

### Unit Tests
- Format detection from magic bytes
- Dimension extraction from headers
- CPU decoder (JPEG/PNG)
- Memory allocation strategies
- Error handling and fallback

### Integration Tests
- End-to-end: MCAP → Decoded RGB → MP4
- GPU availability detection
- Auto-fallback behavior
- Distributed worker integration

### Benchmarks
- CPU vs GPU decode throughput
- Memory allocation strategies
- Batch vs sequential decoding
- NVENC encoding with decoded RGB (vs compressed)

## References

- `roboflow-pipeline/src/gpu/mod.rs` - Similar backend abstraction
- `roboflow-distributed/src/worker.rs` - Worker integration patterns
- FFmpeg nvdecode/nvenc documentation - GPU video processing
