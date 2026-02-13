# Task Completion Checklist for Roboflow

This checklist should be followed when completing any development task in the Roboflow project.

## Pre-Task Checklist

Before starting work:
- [ ] Ensure you understand the requirements and scope
- [ ] Create a new branch if working on a feature or bug fix
- [ ] Check existing issues/PRs for related work
- [ ] Identify which crate(s) are affected

## Implementation Phase

While implementing:
- [ ] Follow the code style in CLAUDE.md
- [ ] Use arena allocation for message data (via robocodec)
- [ ] Add appropriate error handling with context
- [ ] Include documentation comments for public APIs
- [ ] Keep changes focused and minimal (avoid over-engineering)

## Post-Task Checklist

### 1. Build Verification

```bash
# Build Rust library (debug)
cargo build

# Build Rust library (release)
cargo build --release

# If Python bindings were modified:
maturin develop --features python
```

[ ] Debug build succeeds
[ ] Release build succeeds
[ ] Python build succeeds (if applicable)

### 2. Testing

**CRITICAL:** Run Rust and Python tests separately.

```bash
# Rust tests only
cargo test

# MinIO integration tests (requires docker-compose)
docker compose up -d minio minio-init
cargo test --test minio_integration_tests

# Python tests (ALWAYS build extension first)
maturin develop --features python
pytest python/
```

[ ] All Rust tests pass
[ ] All Python tests pass (if Python code modified)
[ ] Added new tests for new functionality

### 3. Code Quality

```bash
# Format code
cargo fmt

# Run lint checks
cargo clippy --all-targets -- -D warnings
```

[ ] Code is formatted
[ ] No clippy warnings

### 4. Documentation

[ ] Updated relevant documentation
[ ] Added/updated doc comments for public APIs
[ ] Updated CLAUDE.md if workflow/convention changes

### 5. Review

[ ] Self-review your changes
[ ] Check for unnecessary additions
[ ] Verify no debug/TODO comments left in code
[ ] Ensure imports are clean (remove unused imports)

## Specific Scenarios

### Adding to roboflow-dataset
- [ ] Implement in `crates/roboflow-dataset/src/`
- [ ] Add tests in `tests/` directory
- [ ] Update crate's lib.rs exports if needed

### Adding to roboflow-storage
- [ ] Implement in `crates/roboflow-storage/src/`
- [ ] Update StorageFactory if adding new backend
- [ ] Test with MinIO integration tests

### Adding to roboflow-distributed
- [ ] Implement in `crates/roboflow-distributed/src/`
- [ ] Consider TiKV integration requirements
- [ ] Test with docker-compose infrastructure

## Git Workflow

### Committing Changes
[ ] Staged only relevant files
[ ] Write clear commit messages following Conventional Commits
[ ] No WIP commits in final PR

### Example Commit Message
```
feat: add support for XYZ format

- Implement XYZ reader in roboflow-sources
- Add integration tests
- Update documentation

Fixes #123
```

## Common Pitfalls to Avoid

### Testing Pitfalls
- ❌ DON'T run `cargo test --features python` (PyO3 linking issues)
- ✅ DO run Rust and Python tests separately
- ❌ DON'T forget to build Python extension before testing
- ✅ DO use `maturin develop --features python` first

### Build Pitfalls
- ❌ DON'T use `--features python` for Rust binaries
- ✅ DO use `cargo build` without `--features python`

## Final Checklist Before Push/PR

[ ] All tests pass
[ ] Code is formatted and linted
[ ] Documentation is updated
[ ] Commit messages are clear
[ ] No sensitive data in commits

## Quick Reference Commands

```bash
# Full validation workflow
cargo build --release
cargo test
cargo fmt
cargo clippy --all-targets -- -D warnings
```
