# CLAUDE.md

Guidance for Claude Code working on the roboflow repository.

## Project

Roboflow: Distributed data transformation pipeline converting robotics bag/MCAP files to trainingable datasets (LeRobot format).

**Key characteristics:**
- Horizontal scaling for large dataset processing
- Schema-driven message translation (CDR, Protobuf, JSON)
- Zero-copy arena allocation for memory efficiency
- Cloud storage support (OSS, S3) for distributed workloads

## Workspace Structure

The project uses a Cargo workspace with 5 crates:

| Crate | Purpose |
|-------|---------|
| `roboflow-core` | Error types, registry, values |
| `roboflow-storage` | S3, OSS, Local storage (always available) |
| `roboflow-dataset` | KPS, LeRobot, streaming converters |
| `roboflow-distributed` | TiKV client, catalog, circuit breaker |
| `roboflow-hdf5` | Optional HDF5 format support |

**Import patterns:**
- Use facade re-exports from `roboflow`: `use roboflow::{Robocodec, DatasetWriter, ...}`
- Or direct crate imports: `use roboflow_core::Result;`

## Build & Test

```bash
cargo build                              # Standard build
cargo test                               # All tests
cargo test --test minio_integration_tests # MinIO integration tests
```

**Note:** MinIO integration tests require running docker-compose infrastructure:
```bash
docker compose up -d minio minio-init
```

## Code Quality

```bash
cargo fmt
cargo clippy --all-targets -- -D warnings
```

## Commit Message Convention

Follow [Conventional Commits](https://www.conventionalcommits.org/) format:

```
<type>: <description>

[optional body]

[optional footer]
```

**Types:**
| Type | Usage |
|------|-------|
| `feat:` | New feature |
| `fix:` | Bug fix |
| `refactor:` | Code restructuring (no functional change) |
| `style:` | Code style changes (formatting, etc.) |
| `docs:` | Documentation only |
| `test:` | Adding or updating tests |
| `chore:` | Maintenance tasks (deps, config, etc.) |
| `perf:` | Performance improvements |

**Examples:**
```
feat: add distributed catalog for TiKV backend
fix: correct frame alignment in streaming converter
refactor: extract storage layer into separate crate
style: apply code formatting fixes
```

## Pull Request Titles

PR titles should follow the same Conventional Commits format as commit messages:

**Do:**
- `feat: add graceful shutdown handling for distributed workers`
- `fix: correct frame alignment in streaming converter`
- `docs: update storage configuration examples`

**Don't:**
- `[Phase 7.2] Add graceful shutdown` ← No internal project tags
- `Adding graceful shutdown` ← Use imperative mood
- `Added graceful shutdown` ← Use imperative mood

PR titles are public-facing and should be clean, descriptive, and free of internal project management artifacts (phase numbers, sprint tags, etc.).

## Branch Naming

Use descriptive branch names with prefixes:

| Prefix | Usage |
|--------|-------|
| `feat/` | New features |
| `fix/` | Bug fixes |
| `docs/` | Documentation changes |
| `refactor/` | Code restructuring |
| `test/` | Test changes |

**Examples:**
- `feat/graceful-shutdown`
- `fix/frame-alignment`
- `docs/pr-conventions`
- `refactor/storage-layer`

## Git Workflow

1. Create feature branch from `main`
2. Make commits following the convention above
3. Push to remote
4. Create PR with clear description and test checklist
5. Ensure CI passes (`make lint && cargo test`)

## Rebasing

If your branch falls behind `main`:

```bash
git fetch origin main
git rebase origin/main
```

If conflicts occur:
1. Resolve conflicts in the affected files
2. `git add <resolved-files>`
3. `git rebase --continue`

After successful rebase, force push:
```bash
git push --force-with-lease
```

**Never use `git push --force`** - always use `--force-with-lease` to prevent overwriting others' work.

## Recovering Unmerged Changes

If your PR is merged but you have local commits that weren't included (e.g., documentation updates made after the PR was created):

1. Switch to main and pull latest:
   ```bash
   git checkout main && git pull
   ```

2. Create a new branch for the orphaned changes:
   ```bash
   git checkout -b new-branch-name
   ```

3. Cherry-pick the specific commit:
   ```bash
   git cherry-pick <commit-hash>
   ```

4. Push and create a new PR.

## Code Review

Automated review tools (e.g., Greptile) may provide feedback on PRs. When addressing review comments:

- Read the comment carefully to understand the specific issue
- Make targeted fixes that address the exact concern
- Verify tests pass after changes
- Commit fixes with descriptive messages (e.g., `fix: address Greptile review comments`)
- Push updates; the PR will automatically re-run checks

## Feature Flags

| Flag | Purpose |
|------|---------|
| `distributed` | TiKV distributed coordination (always enabled) |
| `dataset-hdf5` | HDF5 dataset format support |
| `dataset-parquet` | Parquet dataset format support |
| `dataset-depth` | Depth image support |
| `dataset-all` | All dataset formats |
| `cloud-storage` | S3/OSS cloud storage support |
| `gpu` | GPU compression (Linux only) |
| `jemalloc` | jemalloc allocator (Linux only) |
| `cli` | CLI support for binaries |
| `profiling` | Profiling support |
| `cpuid` | CPU-aware detection (x86_64 only) |
| `io-uring-io` | io_uring support (Linux 5.6+) |

**Note:** Storage (S3/OSS) and dataset formats (Parquet, LeRobot) are always available.

## Development Infrastructure

The project uses docker-compose for local development infrastructure:

```bash
docker compose up -d       # Start all services (MinIO, TiKV, PD)
docker compose up -d minio minio-init  # Start only MinIO
docker compose down        # Stop all services
```

**Services:**
| Service | Purpose | Ports |
|---------|---------|-------|
| MinIO | S3-compatible object storage | 9000 (API), 9001 (Console) |
| TiKV | Distributed KV storage | 20160 |
| PD | TiKV placement driver | 2379, 2380 |

**Pre-created buckets:** `roboflow-datasets`, `roboflow-raw`, `roboflow-temp`

## LeRobot v2.1 Format

Video files follow the LeRobot v2.1 directory structure:

```
{prefix}/videos/chunk-{chunk:03d}/{camera}/episode_{episode:06d}.mp4
```

**Example:**
```
dataset/episode_001/videos/chunk-000/observation.images.cam_left/episode_000000.mp4
```

**Key configuration:**
- `ConcurrentEncoderConfig.key_prefix`: Relative path within bucket (e.g., `"dataset/episode_001"`), NOT a full S3 URL
- `chunk_index`: Typically 0 for single-episode datasets
- `episode_index`: Zero-padded episode number

## Key Conventions

### Storage Layer
- `Storage` trait uses `&Path` (not `impl AsRef<Path>`) for dyn-compatibility
- `LocalStorage` implements `SeekableStorage` for seekable reads
- `StorageFactory` creates backends from URL schemes (file://, s3://, oss://)
- Environment variables for OSS: `OSS_ACCESS_KEY_ID`, `OSS_ACCESS_KEY_SECRET`, `OSS_ENDPOINT`

### KPS Dataset
- TOML config at `crates/roboflow-dataset/src/kps/config.rs` for topic mappings
- v1.2 spec tests in `tests/kps_v12_tests.rs` are authoritative
- Writers use streaming patterns

### Video Encoding
- `FragmentEncoder` requires unique `camera_id` for each camera to prevent temp file collisions
- `ConcurrentEncoderConfig.key_prefix` must be a relative path (not `s3://bucket/...`)
- Temp filenames include camera ID: `fragment_{pid}_{camera_id}_{counter}_{nonce}.mp4`

### Memory
- **Always use arena allocation** for message data (~22% overhead if skipped)
- Arena types are in `robocodec`, imported via `use robocodec::arena::Arena`

### Dead Code
- **Remove unused code** rather than marking it as `#[allow(dead_code)]`
- Compiler warnings about unused functions/imports indicate code that should be removed
- Keep the codebase lean - only add `#[allow(dead_code)]` when explicitly requested

### Unused Variables
- When encountering unused variable warnings, **think critically** about whether:
  1. The variable should be **removed** entirely (if truly not needed)
  2. The variable should be **used** (if it serves a purpose that was overlooked)
- Prefixing with `_` (e.g., `_fragment_index`) suppresses the warning but may hide bugs
- Example: `_fragment_index` in an upload function likely indicates the URL should include the fragment index to prevent overwrites

## External Dependencies

- `robocodec`: https://github.com/archebase/robocodec (I/O, codecs, arena)
