# Distributed Roboflow Implementation Plan

This document outlines the detailed implementation order for transitioning roboflow to a distributed system with Alibaba Cloud (OSS + ACK) support.

## Current State Assessment

- All I/O operations are synchronous using `std::fs::File`
- No async runtime (tokio/async-std)
- No cloud storage dependencies
- Parallelism via threads and crossbeam-channel
- File paths passed directly to `File::open()`/`File::create()`
- Tight coupling between business logic and file I/O

## Implementation Strategy

**Approach**: Incremental migration with sync-first storage abstraction to minimize disruption to existing codebase.

**Key Principle**: Each step must leave the codebase in a working state with all tests passing.

---

## Phase 1: Storage Abstraction Foundation

### 1.1 Add Core Dependencies

**Order**: First

**Tasks**:
1. Add `object_store` crate with AWS feature to Cargo.toml
2. Add `tokio` runtime dependency (for internal blocking in S3 operations)
3. Add `url` crate for path/URL parsing
4. Add `bytes` crate for efficient byte handling
5. Create new feature flag `cloud-storage` to gate cloud dependencies
6. Verify build succeeds with new dependencies
7. Run existing tests to confirm no regressions

**Outputs**: Updated Cargo.toml with new dependencies

---

### 1.2 Define Storage Trait

**Order**: After 1.1

**Tasks**:
1. Create `src/storage/mod.rs` module file
2. Define `StorageError` enum covering all failure modes:
   - NotFound
   - PermissionDenied
   - AlreadyExists
   - InvalidPath
   - NetworkError
   - Timeout
   - IoError (wrapping std::io::Error)
3. Define `ObjectMetadata` struct with size, last_modified, content_type
4. Define synchronous `Storage` trait with methods:
   - `reader()` - returns boxed Read trait object
   - `writer()` - returns boxed Write trait object
   - `exists()` - check existence
   - `size()` - get file size
   - `metadata()` - get full metadata
   - `list()` - list objects with prefix
   - `delete()` - remove object
   - `copy()` - copy between paths
5. Define `SeekableStorage` extension trait for backends supporting seek
6. Add module to `src/lib.rs` exports
7. Write documentation for all public types

**Outputs**: `src/storage/mod.rs` with trait definitions

---

### 1.3 Implement Local Filesystem Backend

**Order**: After 1.2

**Tasks**:
1. Create `src/storage/local.rs`
2. Define `LocalStorage` struct with root path field
3. Implement `Storage` trait for `LocalStorage`:
   - `reader()`: Use `File::open()` wrapped in `BufReader`
   - `writer()`: Create parent directories, use `File::create()` wrapped in `BufWriter`
   - `exists()`: Use `Path::exists()`
   - `size()`: Use `fs::metadata().len()`
   - `metadata()`: Use `fs::metadata()` and convert
   - `list()`: Use `fs::read_dir()` with recursive traversal
   - `delete()`: Use `fs::remove_file()`
   - `copy()`: Use `fs::copy()`
4. Implement `SeekableStorage` for `LocalStorage`
5. Add atomic write support (write to temp file, then rename)
6. Handle symlinks appropriately
7. Write unit tests for all methods
8. Write integration tests with temp directories
9. Add benchmarks comparing to direct `std::fs` usage

**Outputs**: `src/storage/local.rs` with complete implementation and tests

---

### 1.4 Add URL/Path Parsing

**Order**: After 1.2 (can parallel with 1.3)

**Tasks**:
1. Create `src/storage/url.rs`
2. Define `StorageUrl` enum with variants:
   - `Local(PathBuf)`
   - `S3 { bucket, key, endpoint }`
   - `Oss { bucket, key, endpoint, internal }`
3. Implement `FromStr` for `StorageUrl`
4. Support URL schemes:
   - `file://` and plain paths for local
   - `s3://bucket/key` for S3
   - `oss://bucket/key` for Alibaba OSS
5. Add query parameter parsing for endpoint override
6. Add helper to detect if URL is local or remote
7. Write unit tests for all URL formats
8. Document supported URL formats

**Outputs**: `src/storage/url.rs` with URL parsing

---

### 1.5 Create Storage Factory

**Order**: After 1.3 and 1.4

**Tasks**:
1. Create `src/storage/factory.rs`
2. Define `StorageConfig` struct for credentials and settings
3. Define `StorageFactory` with method to create `Storage` from URL
4. Implement automatic backend selection based on URL scheme
5. Add environment variable support for credentials:
   - `OSS_ACCESS_KEY_ID`
   - `OSS_ACCESS_KEY_SECRET`
   - `OSS_ENDPOINT`
   - `AWS_ACCESS_KEY_ID` (fallback)
   - `AWS_SECRET_ACCESS_KEY` (fallback)
6. Add credential chain: env vars → config file → instance metadata
7. Write integration tests with local storage
8. Add feature-gated cloud backend instantiation

**Outputs**: `src/storage/factory.rs` with factory pattern

---

## Phase 2: Cloud Storage Backend

### 2.1 Implement OSS/S3 Backend

**Order**: After Phase 1 complete

**Tasks**:
1. Create `src/storage/oss.rs`
2. Define `OssStorage` struct with:
   - `object_store` client instance
   - Internal tokio runtime for blocking operations
   - Configuration (endpoint, bucket, region)
3. Implement `Storage` trait for `OssStorage`:
   - `reader()`: Use `object_store::ObjectStore::get()` with blocking
   - `writer()`: Buffer to memory/temp file, upload on close
   - `exists()`: Use `head()` operation
   - `size()`: Use `head()` operation
   - `metadata()`: Use `head()` operation
   - `list()`: Use `list()` with pagination
   - `delete()`: Use `delete()` operation
   - `copy()`: Use `copy()` operation
4. Handle S3 eventual consistency considerations
5. Add connection pooling configuration
6. Write integration tests with localstack or MinIO
7. Document OSS-specific configuration options

**Outputs**: `src/storage/oss.rs` with S3-compatible implementation

---

### 2.2 Implement Retry Logic

**Order**: After 2.1

**Tasks**:
1. Create `src/storage/retry.rs`
2. Define `RetryConfig` struct with:
   - max_retries
   - initial_backoff
   - max_backoff
   - backoff_multiplier
   - jitter_enabled
3. Define `RetryableError` trait to classify errors
4. Implement error classification for `StorageError`:
   - Retryable: timeout, rate limit, 5xx errors, connection reset
   - Non-retryable: not found, permission denied, 4xx errors
5. Implement retry wrapper function with exponential backoff
6. Add jitter to prevent thundering herd
7. Add logging for retry attempts
8. Create `RetryingStorage` wrapper that adds retry to any `Storage`
9. Write unit tests for retry behavior
10. Write tests for backoff timing

**Outputs**: `src/storage/retry.rs` with retry logic

---

### 2.3 Implement Multipart Upload

**Order**: After 2.1

**Tasks**:
1. Create `src/storage/multipart.rs`
2. Define `MultipartConfig` struct with:
   - part_size (default 64MB)
   - max_concurrent_parts
   - threshold for multipart (default 100MB)
3. Define `MultipartUploader` struct managing upload state
4. Implement multipart upload lifecycle:
   - `start()`: Initiate multipart upload, get upload ID
   - `upload_part()`: Upload single part with part number
   - `complete()`: Finalize upload with part list
   - `abort()`: Cancel upload and cleanup
5. Implement streaming upload from `Read` trait
6. Add parallel part upload with configurable concurrency
7. Implement part retry on failure
8. Add progress callback support
9. Handle upload cleanup on failure/panic
10. Write integration tests with large files
11. Add benchmarks for different part sizes

**Outputs**: `src/storage/multipart.rs` with multipart support

---

### 2.4 Implement Cached Storage Backend

**Order**: After 2.1, 2.2, 2.3

**Tasks**:
1. Create `src/storage/cached.rs`
2. Define `CacheConfig` struct with:
   - cache_directory
   - max_cache_size
   - upload_concurrency
   - upload_buffer_size
3. Define `CachedStorage` struct with:
   - Remote storage backend
   - Local cache directory
   - Upload queue (channel-based)
   - Cache size tracker
4. Implement read-through caching:
   - Check local cache first
   - Download to cache on miss
   - Return reader from cache
5. Implement write-behind caching:
   - Write to local cache immediately
   - Queue upload to remote asynchronously
   - Track pending uploads
6. Implement cache eviction when size limit reached:
   - LRU eviction policy
   - Don't evict files with pending uploads
7. Implement graceful shutdown:
   - Wait for pending uploads to complete
   - Configurable timeout
8. Add metrics for cache hit/miss rates
9. Write unit tests for caching behavior
10. Write integration tests for async upload
11. Test failure scenarios (upload fails, disk full)

**Outputs**: `src/storage/cached.rs` with caching layer

---

## Phase 3: LeRobot Writer Migration

### 3.1 Create Writer Storage Interface

**Order**: After Phase 2 complete

**Tasks**:
1. Analyze current `LeRobotWriter` file I/O patterns
2. Identify all file creation points in writer.rs
3. Identify all directory creation points
4. Create internal `WriterStorage` interface specific to LeRobot needs:
   - `create_parquet_writer(episode_index)` 
   - `create_video_writer(camera_name, episode_index)`
   - `write_metadata(filename, content)`
5. Document required capabilities for each method
6. Define return types that work with existing Parquet/video code

**Outputs**: Interface design document and trait definition

---

### 3.2 Refactor LeRobotWriter Constructor

**Order**: After 3.1

**Tasks**:
1. Add `Storage` parameter to `LeRobotWriter::new()`
2. Add `local_buffer` path parameter for temp files
3. Store storage backend as `Arc<dyn Storage>`
4. Create `new_local()` convenience constructor that creates `LocalStorage`
5. Update all existing call sites to use `new_local()`
6. Verify all existing tests pass
7. Add new tests for constructor variations

**Outputs**: Updated `LeRobotWriter` constructor

---

### 3.3 Migrate Parquet Writing

**Order**: After 3.2

**Tasks**:
1. Identify Parquet file creation in `write_episode_parquet()`
2. Modify to write to local temp file first (Parquet needs seekable writer)
3. After Parquet write complete, upload temp file via storage backend
4. Delete temp file after successful upload
5. Update path generation to use storage-relative paths
6. Handle upload failure with proper cleanup
7. Update tests to verify both local and mocked remote scenarios
8. Verify Parquet files are identical to before

**Outputs**: Migrated Parquet writing code

---

### 3.4 Migrate Video Writing

**Order**: After 3.3

**Tasks**:
1. Identify video file creation in `encode_videos()`
2. Video encoding requires local filesystem (ffmpeg/mp4 encoder)
3. Modify to encode to local temp directory
4. After encoding complete, upload via storage backend
5. Implement parallel video upload for multiple cameras
6. Clean up temp files after successful upload
7. Handle partial upload failure (some cameras succeed, some fail)
8. Update tests for video writing

**Outputs**: Migrated video writing code

---

### 3.5 Migrate Metadata Writing

**Order**: After 3.4

**Tasks**:
1. Identify metadata file creation (info.json, meta.json, tasks.json)
2. Modify `finalize()` to use storage backend
3. Metadata files are small - can write directly without temp file
4. Implement atomic metadata update (write new, then delete old)
5. Handle metadata update failure
6. Update tests for metadata writing

**Outputs**: Migrated metadata writing code

---

### 3.6 Add Episode Upload Coordinator

**Order**: After 3.5

**Tasks**:
1. Create `src/dataset/lerobot/upload.rs`
2. Define `EpisodeUploadCoordinator` struct
3. Implement coordinated upload of episode files:
   - Parquet file
   - All video files
   - Progress tracking
4. Implement parallel upload with configurable concurrency
5. Add progress reporting callback
6. Implement upload statistics collection
7. Handle partial failure with retry or rollback
8. Integrate with `LeRobotWriter.finish_episode()`
9. Write tests for coordinator

**Outputs**: `src/dataset/lerobot/upload.rs` with coordinator

---

## Phase 4: Checkpoint and Resume

### 4.1 Design Checkpoint Format

**Order**: After Phase 3 complete

**Tasks**:
1. Define checkpoint requirements:
   - Must survive process restart
   - Must detect incompatible configuration changes
   - Must be stored in cloud storage
2. Define `ConversionCheckpoint` struct fields:
   - job_id (unique identifier)
   - dataset_id
   - input_files list
   - output_prefix
   - configuration hash
   - last_completed_episode
   - completed_episodes list with details
   - timestamp
3. Define `EpisodeCheckpoint` struct fields:
   - episode_index
   - parquet_uploaded flag
   - videos_uploaded list
   - frame_count
4. Choose serialization format (JSON for readability)
5. Define checkpoint file naming convention
6. Document checkpoint format

**Outputs**: Checkpoint format specification

---

### 4.2 Implement Checkpoint Manager

**Order**: After 4.1

**Tasks**:
1. Create `src/dataset/checkpoint.rs`
2. Define `CheckpointManager` struct with:
   - Storage backend reference
   - Checkpoint prefix path
   - Checkpoint interval (episodes between saves)
3. Implement `load()` method:
   - Attempt to read checkpoint file
   - Deserialize from JSON
   - Return None if not found
4. Implement `save()` method:
   - Serialize to JSON
   - Write to storage
   - Log checkpoint saved
5. Implement `delete()` method for cleanup
6. Implement `list()` method to find all checkpoints
7. Add checkpoint validation:
   - Compare configuration hash
   - Verify input files match
   - Detect version incompatibility
8. Write unit tests with mocked storage
9. Write integration tests with local storage

**Outputs**: `src/dataset/checkpoint.rs` with manager

---

### 4.3 Integrate Checkpoint with Converter

**Order**: After 4.2

**Tasks**:
1. Add `CheckpointManager` to `StreamingDatasetConverter`
2. Add `job_id` parameter to converter
3. Modify conversion loop:
   - Load checkpoint at start
   - Skip already-completed episodes
   - Save checkpoint periodically
   - Delete checkpoint on success
4. Handle checkpoint load failure gracefully
5. Add configuration for checkpoint interval
6. Update CLI with checkpoint options:
   - `--checkpoint` to enable
   - `--job-id` to specify ID
   - `--checkpoint-interval` for frequency
7. Add `checkpoint` subcommand to CLI:
   - `list` - show all checkpoints
   - `delete` - remove checkpoint
   - `show` - display checkpoint details
8. Write integration tests for checkpoint/resume
9. Test resume after simulated failure

**Outputs**: Integrated checkpoint support

---

## Phase 5: Streaming Converter Migration

### 5.1 Add Storage to Streaming Converter

**Order**: After Phase 4 complete

**Tasks**:
1. Analyze `StreamingDatasetConverter` input handling
2. Add `Storage` parameter for input source
3. Add `Storage` parameter for output destination
4. Modify `RoboReader` usage to work with storage:
   - If local storage, use existing path-based open
   - If cloud storage, download to temp or use streaming
5. Update converter to pass storage to writer
6. Update all call sites
7. Verify existing tests pass

**Outputs**: Storage-aware streaming converter

---

### 5.2 Implement Cloud Input Support

**Order**: After 5.1

**Tasks**:
1. Evaluate input file handling options:
   - Option A: Download entire file to local temp
   - Option B: Use FUSE mount (ossfs)
   - Option C: Streaming read (if robocodec supports)
2. Implement chosen approach for input files
3. Handle large input files (multi-GB MCAP files)
4. Add progress reporting for download
5. Implement cleanup of temp files
6. Test with various input file sizes
7. Benchmark different approaches

**Outputs**: Cloud input file support

---

### 5.3 Update CLI for Cloud URLs

**Order**: After 5.2

**Tasks**:
1. Modify `convert` binary to accept URL inputs
2. Modify `convert` binary to accept URL outputs
3. Add credential configuration options:
   - `--oss-endpoint`
   - `--oss-access-key-id`
   - `--oss-access-key-secret`
   - Or use environment variables
4. Add storage configuration file support
5. Update help text with URL examples
6. Test CLI with local paths (backward compatibility)
7. Test CLI with cloud URLs
8. Update documentation

**Outputs**: Cloud-capable CLI

---

## Phase 6: Kubernetes Deployment

### 6.1 Create Worker Container

**Order**: After Phase 5 complete

**Tasks**:
1. Create `Dockerfile.worker` in repository root
2. Define multi-stage build:
   - Builder stage with Rust toolchain
   - Runtime stage with minimal dependencies
3. Include required runtime dependencies:
   - FFmpeg for video encoding
   - HDF5 libraries (if KPS features enabled)
   - CA certificates for HTTPS
4. Configure non-root user for security
5. Set up entrypoint and default command
6. Optimize image size (strip binaries, minimal base)
7. Test container build locally
8. Test container runs conversion correctly
9. Document build process

**Outputs**: `Dockerfile.worker` and build instructions

---

### 6.2 Create Controller Application

**Order**: After 6.1 (can parallel)

**Tasks**:
1. Create `controller/` directory as separate Rust project
2. Set up Cargo.toml with required dependencies:
   - kube-rs for Kubernetes API
   - object_store for OSS access
   - tokio for async runtime
   - redis for state management
3. Implement OSS manifest watcher:
   - Poll for new manifest files in configured prefix
   - Parse manifest JSON format
   - Move processed manifests to different prefix
4. Implement job planner:
   - Split large datasets into episode ranges
   - Calculate resource requirements
   - Generate job specifications
5. Implement Kubernetes job scheduler:
   - Create Job resources via kube-rs
   - Set resource limits and requests
   - Configure environment variables and secrets
6. Implement job monitor:
   - Watch job status changes
   - Update Redis with progress
   - Handle job completion/failure
7. Implement cleanup:
   - Remove completed jobs after TTL
   - Clean up failed job resources
8. Add health check endpoint
9. Add metrics endpoint
10. Write unit tests
11. Write integration tests with kind cluster

**Outputs**: `controller/` project with job orchestration

---

### 6.3 Create Kubernetes Manifests

**Order**: After 6.1 and 6.2

**Tasks**:
1. Create `deploy/kubernetes/` directory
2. Create namespace manifest
3. Create ServiceAccount for controller
4. Create RBAC resources:
   - Role with Job create/delete/watch permissions
   - RoleBinding to ServiceAccount
5. Create Secret template for OSS credentials
6. Create ConfigMap for configuration
7. Create controller Deployment manifest
8. Create controller Service for metrics/health
9. Create worker Job template (used by controller)
10. Create Redis StatefulSet (optional, for internal Redis)
11. Create PersistentVolumeClaim for Redis
12. Add resource limits to all manifests
13. Add pod disruption budgets
14. Add network policies (optional)
15. Test deployment on local kind cluster
16. Document manifest customization

**Outputs**: `deploy/kubernetes/` with all manifests

---

### 6.4 Create Helm Chart

**Order**: After 6.3

**Tasks**:
1. Create `helm/roboflow/` directory structure
2. Create Chart.yaml with metadata
3. Create values.yaml with configurable options:
   - Controller settings
   - Worker settings
   - OSS configuration
   - Redis configuration
   - Resource limits
   - Monitoring settings
4. Create `_helpers.tpl` with template helpers
5. Template controller deployment
6. Template worker job template
7. Template RBAC resources
8. Template secrets with credential injection
9. Template ConfigMaps
10. Template ServiceMonitor (optional)
11. Template Redis resources (optional)
12. Create NOTES.txt with post-install instructions
13. Add values schema for validation
14. Test chart installation
15. Test chart upgrade
16. Document all values options

**Outputs**: `helm/roboflow/` with complete Helm chart

---

### 6.5 Set Up CI/CD for Images

**Order**: After 6.1 and 6.2

**Tasks**:
1. Create `.github/workflows/build-images.yml`
2. Configure build triggers:
   - On tag push for releases
   - On main branch for latest
3. Set up container registry authentication
4. Configure multi-architecture build (amd64, arm64)
5. Build and push worker image
6. Build and push controller image
7. Tag images with version and git SHA
8. Add vulnerability scanning step
9. Add image signing (optional)
10. Document release process

**Outputs**: CI/CD workflow for container images

---

## Phase 7: Observability

### 7.1 Add Prometheus Metrics

**Order**: After Phase 6 complete (can start earlier)

**Tasks**:
1. Add `prometheus` crate to dependencies
2. Create `src/metrics/mod.rs`
3. Define metric registry
4. Define conversion metrics:
   - episodes_processed_total (counter with labels)
   - episode_processing_duration_seconds (histogram)
   - frames_processed_total (counter)
5. Define I/O metrics:
   - bytes_read_total (counter with source label)
   - bytes_written_total (counter with destination label)
   - upload_duration_seconds (histogram)
   - upload_retries_total (counter)
6. Define resource metrics:
   - buffer_usage_bytes (gauge)
   - pending_uploads (gauge)
   - cache_hit_total (counter)
   - cache_miss_total (counter)
7. Define video encoding metrics:
   - video_encoding_duration_seconds (histogram)
   - video_frames_encoded_total (counter)
8. Instrument conversion code with metrics
9. Instrument storage code with metrics
10. Instrument upload code with metrics
11. Create metrics HTTP endpoint
12. Write tests verifying metrics recorded
13. Document available metrics

**Outputs**: `src/metrics/` with Prometheus instrumentation

---

### 7.2 Add Controller Metrics

**Order**: After 7.1

**Tasks**:
1. Add metrics to controller application
2. Define controller-specific metrics:
   - jobs_created_total (counter)
   - jobs_running (gauge)
   - jobs_completed_total (counter with status label)
   - jobs_failed_total (counter with reason label)
   - manifests_processed_total (counter)
   - manifest_processing_duration_seconds (histogram)
3. Instrument job creation code
4. Instrument job monitoring code
5. Create metrics endpoint in controller
6. Write tests for controller metrics

**Outputs**: Metrics in controller application

---

### 7.3 Create Grafana Dashboard

**Order**: After 7.1 and 7.2

**Tasks**:
1. Create `deploy/grafana/` directory
2. Design dashboard layout:
   - Overview panel with key stats
   - Conversion throughput panel
   - Job status panel
   - Resource utilization panel
   - Error rate panel
3. Create dashboard JSON file
4. Add variables for namespace and time range
5. Create panels for each metric category
6. Add alerting thresholds to panels
7. Test dashboard with sample data
8. Document dashboard usage

**Outputs**: `deploy/grafana/dashboard.json`

---

### 7.4 Add Structured Logging

**Order**: After Phase 6 (can parallel with 7.1-7.3)

**Tasks**:
1. Audit current logging usage (tracing crate already present)
2. Configure JSON log format for production
3. Add structured fields to key log points:
   - job_id
   - dataset_id
   - episode_index
   - duration_ms
   - error details
4. Add tracing spans to major operations:
   - Episode processing
   - File upload
   - Video encoding
5. Configure log levels per module
6. Add request ID propagation
7. Create logging configuration for development vs production
8. Document log format and fields
9. Create example log queries for SLS

**Outputs**: Enhanced structured logging

---

### 7.5 Configure Alibaba SLS Integration

**Order**: After 7.4

**Tasks**:
1. Document SLS project and logstore setup
2. Create Logtail configuration for pod logs
3. Configure JSON log parsing
4. Set up log indexes for common fields
5. Create saved queries for:
   - Failed episodes
   - Slow operations
   - Error aggregation
6. Set up log-based alerts
7. Document SLS integration
8. Test log collection in ACK cluster

**Outputs**: SLS integration configuration and documentation

---

## Phase 8: Testing and Validation

### 8.1 Unit Test Coverage

**Order**: Throughout all phases

**Tasks**:
1. Maintain minimum 80% coverage for new code
2. Write unit tests for all storage implementations
3. Write unit tests for retry logic
4. Write unit tests for checkpoint manager
5. Write unit tests for URL parsing
6. Mock external dependencies (S3, Redis)
7. Add test utilities for common patterns
8. Run coverage reports in CI

**Outputs**: Comprehensive unit test suite

---

### 8.2 Integration Tests

**Order**: After each phase

**Tasks**:
1. Set up integration test infrastructure
2. Create docker-compose for local testing:
   - MinIO for S3-compatible storage
   - Redis for state management
3. Write integration tests for storage backends
4. Write integration tests for full conversion flow
5. Write integration tests for checkpoint/resume
6. Add integration test CI job
7. Document how to run integration tests locally

**Outputs**: Integration test suite

---

### 8.3 End-to-End Tests

**Order**: After Phase 6 complete

**Tasks**:
1. Create test dataset with known characteristics
2. Create end-to-end test script:
   - Upload test data to OSS
   - Create conversion manifest
   - Wait for job completion
   - Verify output in OSS
3. Test with small dataset (fast feedback)
4. Test with medium dataset (realistic scenario)
5. Test failure and recovery scenarios
6. Document E2E test procedure

**Outputs**: E2E test suite and documentation

---

### 8.4 Performance Benchmarks

**Order**: After Phase 5 complete

**Tasks**:
1. Create benchmark dataset of various sizes
2. Benchmark local-to-local conversion (baseline)
3. Benchmark local-to-OSS conversion
4. Benchmark OSS-to-OSS conversion
5. Measure memory usage during conversion
6. Measure network bandwidth utilization
7. Compare with pre-migration performance
8. Document performance characteristics
9. Identify optimization opportunities

**Outputs**: Performance benchmark results and analysis

---

## Phase 9: Documentation and Release

### 9.1 Update User Documentation

**Order**: After Phase 6 complete

**Tasks**:
1. Update README with cloud conversion examples
2. Create cloud deployment guide
3. Document OSS configuration options
4. Document Kubernetes deployment steps
5. Create troubleshooting guide
6. Update API documentation
7. Add architecture diagrams
8. Create video/gif demos

**Outputs**: Updated documentation

---

### 9.2 Migration Guide

**Order**: After Phase 5 complete

**Tasks**:
1. Document breaking changes
2. Provide migration examples for common patterns
3. Document backward compatibility guarantees
4. Create changelog entries
5. Document deprecation timeline (if any)

**Outputs**: MIGRATION.md guide

---

### 9.3 Release Planning

**Order**: After all phases

**Tasks**:
1. Define version number for release
2. Create release branch
3. Final testing on release branch
4. Update version in Cargo.toml
5. Update CHANGELOG.md
6. Create GitHub release with notes
7. Publish container images with release tag
8. Publish Helm chart
9. Announce release

**Outputs**: Released version

---

## Dependency Graph

```
Phase 1: Storage Foundation
├── 1.1 Add Dependencies
├── 1.2 Define Storage Trait (depends on 1.1)
├── 1.3 LocalStorage Backend (depends on 1.2)
├── 1.4 URL Parsing (depends on 1.2, parallel with 1.3)
└── 1.5 Storage Factory (depends on 1.3, 1.4)

Phase 2: Cloud Storage
├── 2.1 OSS Backend (depends on Phase 1)
├── 2.2 Retry Logic (depends on 2.1)
├── 2.3 Multipart Upload (depends on 2.1, parallel with 2.2)
└── 2.4 Cached Storage (depends on 2.1, 2.2, 2.3)

Phase 3: LeRobot Migration
├── 3.1 Writer Storage Interface (depends on Phase 2)
├── 3.2 Refactor Constructor (depends on 3.1)
├── 3.3 Migrate Parquet Writing (depends on 3.2)
├── 3.4 Migrate Video Writing (depends on 3.3)
├── 3.5 Migrate Metadata Writing (depends on 3.4)
└── 3.6 Upload Coordinator (depends on 3.5)

Phase 4: Checkpoint
├── 4.1 Checkpoint Format (depends on Phase 3)
├── 4.2 Checkpoint Manager (depends on 4.1)
└── 4.3 Converter Integration (depends on 4.2)

Phase 5: Streaming Converter
├── 5.1 Add Storage to Converter (depends on Phase 4)
├── 5.2 Cloud Input Support (depends on 5.1)
└── 5.3 Update CLI (depends on 5.2)

Phase 6: Kubernetes
├── 6.1 Worker Container (depends on Phase 5)
├── 6.2 Controller Application (parallel with 6.1)
├── 6.3 Kubernetes Manifests (depends on 6.1, 6.2)
├── 6.4 Helm Chart (depends on 6.3)
└── 6.5 CI/CD for Images (depends on 6.1, 6.2)

Phase 7: Observability
├── 7.1 Prometheus Metrics (can start after Phase 3)
├── 7.2 Controller Metrics (depends on 7.1, 6.2)
├── 7.3 Grafana Dashboard (depends on 7.1, 7.2)
├── 7.4 Structured Logging (parallel with 7.1)
└── 7.5 SLS Integration (depends on 7.4)

Phase 8: Testing (continuous throughout)
├── 8.1 Unit Tests
├── 8.2 Integration Tests
├── 8.3 E2E Tests (depends on Phase 6)
└── 8.4 Performance Benchmarks (depends on Phase 5)

Phase 9: Documentation (after features complete)
├── 9.1 User Documentation
├── 9.2 Migration Guide
└── 9.3 Release
```

---

## Risk Mitigation

### Technical Risks

| Risk | Mitigation |
|------|------------|
| Breaking existing functionality | Feature flags, comprehensive tests, backward-compatible constructors |
| Performance regression | Benchmarks at each phase, optimization pass before release |
| Cloud API rate limiting | Retry logic, backoff, connection pooling |
| Large file handling | Streaming uploads, multipart, temp file management |
| Memory exhaustion | Bounded buffers, cache eviction, resource limits |

### Schedule Risks

| Risk | Mitigation |
|------|------------|
| Scope creep | Clear phase boundaries, MVP focus |
| Dependency delays | Parallel work where possible, early integration |
| Testing gaps | Continuous testing, not just at end |

---

## Success Criteria

### Phase 1-2 Complete
- [ ] Can create LocalStorage and read/write files
- [ ] Can create OssStorage and read/write objects
- [ ] All existing tests pass

### Phase 3-4 Complete
- [ ] LeRobotWriter works with cloud storage
- [ ] Checkpoint/resume works correctly
- [ ] Can convert local input to cloud output

### Phase 5 Complete
- [ ] CLI accepts cloud URLs
- [ ] Full conversion works end-to-end with cloud

### Phase 6 Complete
- [ ] Can deploy to Kubernetes with Helm
- [ ] Jobs process datasets automatically
- [ ] Horizontal scaling works

### Phase 7 Complete
- [ ] Metrics visible in Prometheus/Grafana
- [ ] Logs aggregated in SLS
- [ ] Alerts configured

### Final Release
- [ ] Documentation complete
- [ ] Performance meets targets
- [ ] No critical bugs
