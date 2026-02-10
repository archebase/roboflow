# Upload Architecture Consolidation Plan

## Executive Summary

The codebase currently has **three separate upload implementations** with overlapping responsibilities:

| Component | Location | Lines | Purpose | Status |
|-----------|----------|-------|---------|--------|
| `MultipartUploader` | `roboflow-storage/src/multipart.rs` | ~250 | Traditional "upload known file" | **Production** |
| `StreamingUploader` | `roboflow-dataset/src/common/streaming_uploader.rs` | ~400 | Fragment buffering + progressive upload | **Experimental** |
| `S3StreamingEncoder` | `roboflow-dataset/src/common/s3_encoder.rs` | ~600 | FFmpeg pipe → cloud upload | **Experimental** |

**Recommendation:** Consolidate to 2 components by integrating `StreamingUploader` into `roboflow-storage` as a first-class streaming API.

---

## Analysis: Current State

### 1. `MultipartUploader` (roboflow-storage)

**Design Pattern:** Known-size file upload

```rust
pub fn upload_from_reader<R: Read + Seek>(
    &mut self,
    reader: &mut R,
    config: &MultipartConfig,
    progress: Option<&ProgressCallback>,
) -> Result<MultipartStats>
```

**Key Characteristics:**
- Requires `Seek` - needs known file size upfront
- Synchronous `upload_part()` calls with retry logic
- Progress callbacks via closure
- Used by: `LerobotSink` (production path)

**Pros:**
- Battle-tested, production-ready
- Proper retry with exponential backoff
- Good for batch uploads

**Cons:**
- Cannot handle streaming data (no `Seek` on pipes)
- Manual part management

---

### 2. `StreamingUploader` (roboflow-dataset)

**Design Pattern:** Fragment accumulation → upload when full

```rust
pub fn add_fragment(
    &mut self,
    fragment: Vec<u8>,
    runtime: &tokio::runtime::Handle,
) -> Result<()>
```

**Key Characteristics:**
- Buffers fragments until `part_size` threshold
- Uses `WriteMultipart` internally
- Lazy initialization on first fragment
- Designed for **fMP4 fragments** from rsmpeg encoder

**Pros:**
- Handles unknown total size
- Clean API for fragment-based encoding
- Good memory efficiency

**Cons:**
- **Duplicate code** with `MultipartUploader` (both create `WriteMultipart`)
- Lives in wrong crate (dataset, not storage)
- Manual `runtime` handle passing

---

### 3. `S3StreamingEncoder` (roboflow-dataset)

**Design Pattern:** FFmpeg stdout → channel → `WriteMultipart`

```rust
// Thread reads FFmpeg stdout, sends chunks via channel
chunk_sender.send(chunk)?;

// Main thread receives and writes
upload.write(&chunk);
```

**Key Characteristics:**
- FFmpeg CLI integration (PPM frames in → fMP4 out)
- Cross-thread channel architecture
- Direct `WriteMultipart` usage
- No `StreamingUploader` dependency!

**Pros:**
- Unique FFmpeg integration requirement
- Works correctly after bug fix

**Cons:**
- Also duplicates `WriteMultipart` creation logic
- No shared upload infrastructure

---

## The Core Problem: WriteMultipart Duplication

All three components do **the same thing** to start an upload:

```rust
// MultipartUploader (line 221-226)
let multipart_upload = runtime.block_on(async {
    self.store.put_multipart(&self.key).await
        .map_err(|e| StorageError::Cloud(...))
})?;

// StreamingUploader (line 221-226) - IDENTICAL
let multipart_upload = runtime.block_on(async {
    self.store.put_multipart(&self.key).await
        .map_err(|e| RoboflowError::encode(...))
})?;

// S3StreamingEncoder (line 320-323) - IDENTICAL
let multipart_upload = runtime.block_on(async {
    self.store.put_multipart(&self.key).await
        .map_err(|e| RoboflowError::encode(...))
})?;
```

All three then wrap it in `WriteMultipart::new_with_chunk_size()`.

---

## Consolidation Strategy

### Phase 1: Unify WriteMultipart Creation (Low Risk)

**Add to `roboflow-storage/src/multipart.rs`:**

```rust
/// Create a WriteMultipart wrapper with standard configuration.
///
/// This is the common initialization pattern shared by all uploaders.
pub fn create_write_multipart(
    store: &dyn ObjectStore,
    key: &str,
    runtime: &tokio::runtime::Handle,
    chunk_size: usize,
) -> Result<object_store::WriteMultipart, StorageError> {
    let multipart_upload = runtime.block_on(async {
        store.put_multipart(key).await
            .map_err(|e| StorageError::Cloud(format!("put_multipart failed: {}", e)))
    })?;

    Ok(object_store::WriteMultipart::new_with_chunk_size(
        multipart_upload,
        chunk_size,
    ))
}
```

**Impact:**
- `StreamingUploader` and `S3StreamingEncoder` can use this helper
- Reduces duplication from 3 places → 1
- No API changes to existing code

---

### Phase 2: Move StreamingUploader to roboflow-storage (Medium Risk)

**Target:** `roboflow-storage/src/streaming_multipart.rs`

**Rationale:**
- Streaming upload is a **storage concern**, not dataset-specific
- Allows `LerobotSink` to use it for large video uploads
- Consolidates all upload logic in one place

**New API:**

```rust
use roboflow_storage::streaming_multipart::{StreamingUploader, UploadConfig};

// Create uploader
let uploader = StreamingUploader::new(
    store.clone(),
    "s3://bucket/videos/episode_001.mp4",
    UploadConfig::default()
        .with_part_size(5 * 1024 * 1024)
        .with_timeout(Duration::from_secs(30))
);

// Add fragments (lazy initialization on first call)
uploader.add_fragment(fmp4_fragment_data, &runtime)?;

// Finalize and get stats
let stats = uploader.finalize(&runtime)?;
```

**Migration Path:**
1. Add `roboflow-storage` dependency on `roboflow-dataset` (already exists)
2. Update imports: `use roboflow_storage::StreamingUploader`
3. Delete `crates/roboflow-dataset/src/common/streaming_uploader.rs`
4. Update tests in `roboflow-storage`

---

### Phase 3: Extract FFmpeg-specific logic (Keep Separate)

**`S3StreamingEncoder` should remain separate** because:

1. It's **video encoding + upload**, not pure upload
2. FFmpeg CLI integration is domain-specific
3. Cross-thread channel architecture is unique to pipe handling

**However**, it should use the Phase 1 helper:

```rust
// Before
let multipart_upload = runtime.block_on(async { /* ... */ })?;
let upload = WriteMultipart::new_with_chunk_size(multipart_upload, part_size);

// After
let upload = roboflow_storage::create_write_multipart(
    &self.store,
    &self.key,
    &self.runtime,
    self.config.upload_part_size,
)?;
```

---

## Final Architecture

```
roboflow-storage/
├── src/
│   ├── multipart.rs          # MultipartUploader (known files)
│   ├── streaming_multipart.rs # StreamingUploader (fragments) [MOVED]
│   └── lib.rs                # Re-export both
│
roboflow-dataset/
├── src/common/
│   ├── s3_encoder.rs         # FFmpeg encoder + upload (unique)
│   └── streaming_uploader.rs # DELETED
│
└── tests/
    └── streaming_integration_tests.rs (uses StreamingUploader from storage)
```

---

## Migration Checklist

### Phase 1: Helper Function
- [ ] Add `create_write_multipart()` to `roboflow-storage/src/multipart.rs`
- [ ] Add unit tests
- [ ] Update `StreamingUploader` to use helper
- [ ] Update `S3StreamingEncoder` to use helper
- [ ] Run `cargo test`

### Phase 2: Move StreamingUploader
- [ ] Create `roboflow-storage/src/streaming_multipart.rs`
- [ ] Move `StreamingUploader` + tests
- [ ] Update `roboflow-storage/src/lib.rs` re-exports
- [ ] Update `roboflow-dataset` imports
- [ ] Delete `crates/roboflow-dataset/src/common/streaming_uploader.rs`
- [ ] Run `cargo test --workspace`

### Phase 3: Verify S3StreamingEncoder
- [ ] Update `s3_encoder.rs` to use Phase 1 helper
- [ ] Run streaming integration tests
- [ ] Verify no regressions

---

## Risk Assessment

| Phase | Risk | Effort | Breaking Changes |
|-------|------|--------|------------------|
| Phase 1 | Low | ~1 hour | None (internal refactor) |
| Phase 2 | Medium | ~3 hours | Import path changes |
| Phase 3 | Low | ~1 hour | None (internal refactor) |

**Total Effort:** ~5 hours

**Rollback:** Each phase is independently revertable via git.

---

## Critical Question: Do We Need roboflow-storage at All?

### Usage Analysis

Looking at actual usage across the codebase:

| Component | Used By | How Used |
|-----------|---------|----------|
| `Storage` trait | `lerobot/writer`, `distributed` | Generic storage abstraction |
| `LocalStorage` | `lerobot/writer`, tests | Direct instantiation |
| `OssStorage` | `lerobot/writer` | `downcast_ref()` for cloud-specific APIs |
| `StorageFactory` | `lerobot/sinks`, `distributed` | `from_env()` for env-based config |
| `object_store` | `s3_encoder`, `streaming_*` | **Direct usage of `WriteMultipart`** |
| `MultipartUploader` | **NOT USED** | Dead code? |

### What roboflow-storage Actually Provides

1. **`object_store` re-export** - This is the **primary value**
2. **`Storage` trait** - Abstraction used by `LerobotWriter`
3. **`LocalStorage`/`OssStorage`** - Concrete implementations
4. **`StorageFactory`** - Environment-based storage creation

### What We Actually Use

```rust
// In s3_encoder.rs - DIRECT object_store usage
use roboflow_storage::object_store;
let multipart_upload = store.put_multipart(&key).await?;
let upload = WriteMultipart::new_with_chunk_size(...);

// In streaming_coordinator.rs - DIRECT object_store usage
use roboflow_storage::object_store;
```

### The Alternative: Use object_store Directly

**`object_store` is a mature, well-maintained crate** with:
- S3, OSS, GCS, Azure support
- `WriteMultipart` for streaming uploads
- Active development and community

**roboflow-storage is a thin wrapper** that adds:
- Custom `Storage` trait (not used by upload code)
- `LocalStorage` (could use `object_store::local::LocalFileSystem`)
- `OssStorage` (object_store already handles this)

### Recommendation: Phase Out roboflow-storage

**Option A: Keep roboflow-storage (Status Quo)**
- Pro: Existing investment, custom `Storage` trait
- Con: Maintenance burden, abstraction leak (direct `object_store` usage)

**Option B: Migrate to object_store directly (Recommended)**
- Pro: Less code to maintain, direct access to features
- Con: Migration effort for `LerobotWriter`

### Migration Path if Option B

1. **Phase 1:** Add `object_store` as direct dependency to `roboflow-dataset`
2. **Phase 2:** Replace `roboflow_storage::Storage` with `object_store::ObjectStore` in `LerobotWriter`
3. **Phase 3:** Remove `roboflow-storage` crate
4. **Phase 4:** Move any unique functionality (if any) to `roboflow-dataset`

**Estimated Effort:** ~1 day

---

## Updated Recommendation

### TL;DR: Keep roboflow-storage for LerobotSink/Sink abstraction, but streaming code should use object_store directly

**roboflow-storage serves TWO different purposes:**

#### Purpose 1: Pipeline/Sink Abstraction (KEEP - Working Well)
`roboflow-sinks` provides the **high-level pipeline API**:
```rust
// Used by roboflow-pipeline for distributed processing
Sink trait → LerobotSink → LerobotWriter → roboflow_storage::StorageFactory
```

This is **clean separation of concerns**:
- `roboflow-sinks`: Pipeline-level abstraction (`Sink` trait)
- `roboflow-storage`: Storage backend abstraction (`Storage` trait)
- `roboflow-dataset`: Dataset format logic

#### Purpose 2: Low-level Streaming Upload (DON'T USE roboflow-storage)
Streaming encoder code bypasses `roboflow-storage` entirely:
```rust
// s3_encoder.rs, streaming_coordinator.rs, streaming_uploader.rs
use roboflow_storage::object_store;  // Just using it as a re-export!
```

This is **correct** - streaming needs direct `object_store` access for:
- `WriteMultipart` (not exposed by `Storage` trait)
- Low-level control over part sizes and buffering
- Channel-based async patterns

### Decision Matrix

| Code | Should use | Why |
|------|------------|-----|
| `LerobotSink` / `LerobotWriter` | `roboflow_storage::StorageFactory` | Clean abstraction, needs local+cloud unification |
| `S3StreamingEncoder` | `object_store` directly | Needs `WriteMultipart`, pipe-specific patterns |
| `StreamingUploader` | `object_store` directly | Fragment buffering + direct upload control |
| `roboflow-distributed` | `roboflow_storage::Storage` | Generic storage operations |

### Final Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  roboflow-pipeline (distributed orchestration)              │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  roboflow-sinks (Sink trait, DatasetFrame)                  │
│  └─ LerobotSink ─────────────────────────────────┐         │
└──────────────────────────────────────────────────│─────────┘
                                                   │
                           ┌───────────────────────┴──────────────────┐
                           ▼                                          ▼
┌─────────────────────────────────────────┐    ┌────────────────────────────────┐
│  LerobotWriter (roboflow-dataset)       │    │  Streaming Upload Code         │
│  └─ Uses roboflow_storage::Storage      │    │  └─ Uses object_store directly │
│     (local + cloud unified)             │    │     (WriteMultipart control)   │
└─────────────────────────────────────────┘    └────────────────────────────────┘
```

### Recommendation

**DO NOT consolidate streaming upload into roboflow-storage.**

**Instead:**
1. **Keep `StreamingUploader` in `roboflow-dataset`** - it's dataset-specific fragment handling
2. **Keep `S3StreamingEncoder` using `object_store` directly** - FFmpeg integration is unique
3. **Keep `roboflow-storage` for `LerobotSink/LerobotWriter`** - the abstraction is valuable there
4. **Consider adding a re-export note** in lib.rs:
   ```rust
   //! Note: For streaming upload with WriteMultipart, use object_store directly.
   //! The Storage trait is for high-level operations, not low-level upload control.
   ```

**The key insight:** `roboflow-storage`'s `Storage` trait is for **file-like operations** (read, write, delete, list). Streaming video upload with `WriteMultipart` is a **different abstraction level** that shouldn't be forced through the `Storage` trait.

---

## Open Questions

1. **Error type conversion:** `StreamingUploader` uses `RoboflowError`, should it convert to `StorageError` when moved?
   - **Recommendation:** Keep `RoboflowError` via `From<StorageError>` impl to minimize churn

2. **Progress callbacks:** `MultipartUploader` has progress via closure, `StreamingUploader` doesn't. Should it?
   - **Recommendation:** Add progress callback to `StreamingUploader` API

3. **Backpressure:** `WriteMultipart.write()` is non-blocking. Should we add explicit backpressure?
   - **Recommendation:** Add optional buffer size limit to `UploadConfig`

---

## Decision Matrix

| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| Status quo | Works, no risk | Code duplication, confusion | ❌ Reject |
| Full merge (1 component) | Maximal reuse | Loses domain-specific APIs | ❌ Reject |
| **Consolidation plan** | Clean separation, reduced duplication | Requires migration | ✅ **Accept** |
