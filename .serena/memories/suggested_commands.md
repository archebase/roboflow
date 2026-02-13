# Suggested Commands for Roboflow Development

Essential commands for developing, testing, and building the Roboflow project.

## Build Commands

### Rust Library
```bash
# Build debug version
cargo build

# Build release version
cargo build --release
```

### Python Package
```bash
# Build Python wheel (development mode - required before running Python tests)
maturin develop --features python

# Build Python wheel for distribution
maturin build --features python
```

## Testing Commands

### Rust Tests
```bash
# Run Rust tests only
cargo test

# Run specific Rust test
cargo test test_name

# Run MinIO integration tests (requires docker-compose)
docker compose up -d minio minio-init
cargo test --test minio_integration_tests
```

### Python Tests
**IMPORTANT:** Always build the extension first before running Python tests.
```bash
# Build extension first
maturin develop --features python

# Run Python tests
pytest python/

# Run with verbose output
pytest python/ -v
```

## Code Quality Commands

### Format Code
```bash
# Format Rust code
cargo fmt
```

### Lint/Check Code
```bash
# Lint Rust code
cargo clippy --all-targets -- -D warnings
```

## Running CLI Tools

The CLI is unified under a single binary with subcommands:

```bash
# Submit jobs to distributed queue
cargo run --bin roboflow -- submit <args>

# Manage jobs (list, get, retry, cancel, delete, stats)
cargo run --bin roboflow -- jobs <subcommand>

# Manage batch jobs
cargo run --bin roboflow -- batch <subcommand>

# Run unified service (worker + finalizer + reaper)
cargo run --bin roboflow -- run
```

### Environment Variables

**TiKV Configuration:**
- `TIKV_PD_ENDPOINTS` - PD endpoints (default: 127.0.0.1:2379)

**Storage Configuration:**
- `OSS_ACCESS_KEY_ID` - Alibaba OSS access key
- `OSS_ACCESS_KEY_SECRET` - Alibaba OSS secret key
- `OSS_ENDPOINT` - Alibaba OSS endpoint
- `AWS_ACCESS_KEY_ID` - AWS access key
- `AWS_SECRET_ACCESS_KEY` - AWS secret key

**Worker Configuration:**
- `ROLE` - Role to run: `worker`, `finalizer`, or `unified` (default)
- `WORKER_POLL_INTERVAL_SECS` - Job poll interval (default: 5)
- `WORKER_MAX_CONCURRENT_JOBS` - Max concurrent jobs (default: 1)

## Infrastructure (Docker Compose)

```bash
# Start all services (MinIO, TiKV, PD)
docker compose up -d

# Start only MinIO
docker compose up -d minio minio-init

# Stop all services
docker compose down
```

## Clean Build Artifacts

```bash
cargo clean
```

## Platform-Specific Notes

- On macOS, `jemalloc` is not used (system allocator is already excellent)
- Linux-specific features (like `io_uring`) are not available on macOS
- Python tests require `maturin develop` before running
