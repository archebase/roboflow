# Task Completion Checklist for Robocodec

This checklist should be followed when completing any development task in the Robocodec project.

## Pre-Task Checklist

Before starting work:
- [ ] Ensure you understand the requirements and scope
- [ ] Create a new branch if working on a feature or bug fix
- [ ] Check existing issues/PRs for related work
- [ ] Identify which crate(s) are affected (`roboflow` or `robocodec`)

## Implementation Phase

While implementing:
- [ ] Follow the code style and conventions in `style_and_conventions.md`
- [ ] Use arena allocation for message data (not individual allocations)
- [ ] Add appropriate error handling with context
- [ ] Include documentation comments for public APIs
- [ ] Keep changes focused and minimal (avoid over-engineering)

## Post-Task Checklist

### 1. Build Verification

```bash
# Build Rust library (debug)
make build
cargo build

# Build Rust library (release)
make build-release
cargo build --release

# If Python bindings were modified:
make build-python-dev
maturin develop --features python
```

[ ] Debug build succeeds
[ ] Release build succeeds
[ ] Python build succeeds (if applicable)

### 2. Testing

**CRITICAL:** Run Rust and Python tests separately.

```bash
# Rust tests only
make test-rust
cargo test

# If KPS features were modified:
make test-all
cargo test --features kps-all

# Python tests (ALWAYS build extension first)
make test-python
pytest python/
```

[ ] All Rust tests pass
[ ] All Python tests pass (if Python code modified)
[ ] Added new tests for new functionality
[ ] Tests cover edge cases

### 3. Code Quality

```bash
# Format all code
make fmt
cargo fmt
ruff format python/  # if Python modified

# Run lint checks
make lint
cargo clippy --all-targets -- -D warnings

# Python type checking (if applicable)
make lint-python
mypy python/roboflow
```

[ ] Code is formatted (Rust + Python)
[ ] No clippy warnings
[ ] No ruff warnings (if Python modified)
[ ] Type checking passes (if Python modified)

### 4. Documentation

[ ] Updated relevant documentation
[ ] Added/updated doc comments for public APIs
[ ] Updated CHANGELOG.md if user-facing change
[ ] Updated README.md if new feature added
[ ] Documented any breaking changes

### 5. Review

[ ] Self-review your changes
[ ] Check for unnecessary additions
[ ] Verify no debug/TODO comments left in code
[ ] Ensure imports are clean (remove unused imports)
[ ] Check for proper error messages

## Specific Scenarios

### Adding a New Codec
- [ ] Implemented in `robocodec/src/encoding/`
- [ ] Registered in `robocodec/src/core/registry.rs`
- [ ] Added schema parser if needed
- [ ] Added round-trip tests
- [ ] Tested with both Rust and Python APIs

### Adding a New File Format
- [ ] Implemented reader in `robocodec/src/io/`
- [ ] Implemented writer in `robocodec/src/io/writer/`
- [ ] Added format detection logic
- [ ] Added integration tests
- [ ] Updated documentation

### Adding Python Bindings
- [ ] Added `#[pyfunction]` or `#[pymethods]` in `roboflow/src/python/`
- [ ] Exported from `python/roboflow/__init__.py`
- [ ] Built with `maturin develop --features python`
- [ ] Added Python tests
- [ ] Updated type hints and docstrings

### Performance-Critical Changes
- [ ] Ran benchmarks before and after
- [ ] Used `cargo flamegraph` or similar profiler
- [ ] Verified no regressions in throughput
- [ ] Considered HyperPipeline impact

### Memory-Related Changes
- [ ] Used arena allocation appropriately
- [ ] Verified no memory leaks
- [ ] Checked buffer pool usage
- [ ] Considered zero-copy opportunities

## Git Workflow

### Committing Changes
[ ] Staged only relevant files
[ ] Write clear, descriptive commit messages
[ ] Include Co-Authored-By: Claude Sonnet if AI-assisted
[ ] No WIP commits in final PR

### Example Commit Message
```
Add support for XYZ codec

- Implement XYZ codec in robocodec/src/encoding/xyz.rs
- Register codec in core registry
- Add round-trip tests
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
- ❌ DON'T forget to check both debug and release builds
- ✅ DO test `cargo build --release` for performance verification

### Code Quality Pitfalls
- ❌ DON'T leave unused imports
- ❌ DON'T leave `dbg!()` or `println!()` in production code
- ❌ DON'T commit with clippy warnings
- ✅ DO run `cargo clippy` and fix all warnings

## Final Checklist Before Push/PR

[ ] All tests pass (Rust + Python)
[ ] Code is formatted and linted
[ ] Documentation is updated
[ ] Commit messages are clear
[ ] No sensitive data in commits
[ ] Ready for review

## Quick Reference Commands

```bash
# Full validation workflow
make build-release
make test-rust
make test-python
make fmt
make lint

# Verify everything passes
cargo test --all-features
cargo clippy --all-targets --all-features -- -D warnings
```

## Getting Help

If stuck during the task:
1. Check existing code for similar patterns
2. Review architecture docs in `docs/`
3. Look at test files for usage examples
4. Consult CLAUDE.md for project-specific guidance
5. Ask for clarification if requirements are unclear
