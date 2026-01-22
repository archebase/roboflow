# Quick Reference - Robocodec Development

A quick cheat sheet for common development tasks in Robocodec.

## Table of Contents
- [Common Commands](#common-commands)
- [File Locations](#file-locations)
- [Code Patterns](#code-patterns)
- [Testing Patterns](#testing-patterns)
- [Debugging Tips](#debugging-tips)
- [Git Workflow](#git-workflow)

## Common Commands

### Build & Test
```bash
# Quick iteration loop
make build && make test-rust

# Full validation (before committing)
make build-release && make test-rust && make test-python && make check

# Python development
make build-python-dev && pytest python/ -v
```

### Code Quality
```bash
# Fix all formatting
make fmt

# Check for issues
make lint
cargo clippy --all-targets -- -D warnings
```

### Running Tools
```bash
# Convert files
cargo run --bin convert -- input.bag output.mcap

# Inspect file
cargo run --bin inspect -- data.mcap

# Extract topics
cargo run --bin extract -- data.bag --topics /camera --output out/
```

## File Locations

### Core Implementation
```
robocodec/src/
├── encoding/          # Codecs (CDR, Protobuf, JSON)
├── schema/            # Schema parsers (ROS msg, IDL)
├── io/                # Format readers/writers
├── transform/         # Data transformations
├── types/             # Arena allocation, buffers
└── core/              # Core types, errors

src/ (roboflow)
├── pipeline/          # Pipeline implementations
│   ├── stages/        # Standard pipeline
│   ├── hyper/         # HyperPipeline
│   └── fluent/        # Builder API
├── python/            # PyO3 bindings
└── bin/               # CLI tools
```

### Configuration
```
Cargo.toml             # Workspace dependencies
Makefile               # Build commands
pyproject.toml         # Python config
```

### Tests
```
tests/                 # Integration tests
├── fixtures/          # Test data
python/tests/          # Python tests
```

## Code Patterns

### Adding a New Transform

1. **Create transform in `robocodec/src/transform/`**:
```rust
use robocodec::core::Transform;

pub struct MyTransform {
    config: MyConfig,
}

impl Transform for MyTransform {
    fn transform(&self, message: &mut Message) -> Result<()> {
        // Transform logic here
        Ok(())
    }
}
```

2. **Add to TransformBuilder**:
```rust
// In robocodec/src/transform/mod.rs
impl TransformBuilder {
    pub fn with_my_transform(mut self, config: MyConfig) -> Self {
        self.transforms.push(Box::new(MyTransform::new(config)));
        self
    }
}
```

3. **Add tests**:
```rust
#[test]
fn test_my_transform() {
    let transform = MyTransform::new(config);
    let mut msg = create_test_message();
    transform.transform(&mut msg).unwrap();
    assert_eq!(msg.data, expected_data);
}
```

### Adding Python Bindings

1. **Define function in `src/python/`**:
```rust
use pyo3::prelude::*;

#[pyfunction]
fn my_function(arg: &str) -> PyResult<String> {
    Ok(format!("Hello, {}!", arg))
}

#[pymodule]
fn _roboflow(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(my_function, m)?)?;
    Ok(())
}
```

2. **Export from `python/roboflow/__init__.py`**:
```python
from roboflow._roboflow import my_function

__all__ = ["my_function"]
```

3. **Rebuild**:
```bash
maturin develop --features python
```

### Error Handling Pattern

```rust
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MyError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Parse error: {0}")]
    Parse(String),
}

pub fn my_function() -> Result<(), MyError> {
    let data = std::fs::read_to_string("file.txt")?;
    // Use ? for automatic error conversion
    Ok(())
}
```

### Arena Allocation Pattern

```rust
use bumpalo::Bump;

fn process_messages(messages: &[Message]) {
    let arena = Bump::new();
    
    for msg in messages {
        // Allocate in arena
        let processed = arena.alloc_str(&msg.data);
        // Process...
    }
    // All allocations freed at once
}
```

## Testing Patterns

### Round-Trip Test
```rust
#[test]
fn test_round_trip() {
    // Encode
    let original = create_test_message();
    let encoded = encode(&original).unwrap();
    
    // Decode
    let decoded = decode(&encoded).unwrap();
    
    // Verify
    assert_eq!(original, decoded);
}
```

### Property-Based Test
```rust
#[test]
fn test_compression_preserves_data() {
    let data = generate_random_data(1024);
    
    // Compress
    let compressed = compress(&data);
    
    // Decompress
    let decompressed = decompress(&compressed);
    
    // Should match original
    assert_eq!(data, decompressed);
}
```

### Integration Test
```rust
#[test]
fn test_bag_to_mcap_conversion() {
    let input = "tests/fixtures/test.bag";
    let output = "/tmp/test_output.mcap";
    
    // Convert
    Robocodec::open(vec![input])
        .unwrap()
        .write_to(output)
        .run()
        .unwrap();
    
    // Verify output exists and is valid
    assert!(std::path::Path::new(output).exists());
}
```

## Debugging Tips

### Enable Logging
```bash
RUST_LOG=debug cargo run --bin convert -- input.bag output.mcap
```

### Use Debug Build
```bash
cargo build
# Better error messages, backtraces
```

### Flamegraph Profiling
```bash
# Build with profiling feature
cargo build --release --features profiling

# Run profiler
cargo flamegraph --bin convert -- input.bag output.mcap

# View flamegraph.svg
```

### GDB/LLDB
```bash
# Rust symbols are preserved in debug builds
rust-lldb -- target/debug/convert input.bag output.mcap

# In LLDB:
(lldb) b my_function
(lldb) run
(lldb) bt  # backtrace
```

### Memory Profiling
```bash
# Use valgrind on Linux
valgrind --leak-check=full target/debug/convert input.bag output.mcap

# Use Instruments on macOS
instruments -t Leaks target/debug/convert input.bag output.mcap
```

## Git Workflow

### Feature Branch Workflow
```bash
# Create feature branch
git checkout -b feature/my-feature

# Make changes and commit
git add .
git commit -m "Add my feature"

# Push to remote
git push -u origin feature/my-feature

# Create PR on GitHub
```

### Commit Message Format
```
Add feature XYZ

- Implement ABC in module.rs
- Add tests for new functionality
- Update documentation

Fixes #123

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>
```

### Squashing Commits
```bash
# Before final PR, squash WIP commits
git rebase -i HEAD~5  # Interactive rebase

# Mark commits as "squash" or "fixup"
# Save and exit
```

## Common Issues

### Issue: Python Tests Fail
```bash
# Solution: Always rebuild extension first
maturin develop --features python
pytest python/
```

### Issue: Import Error in Python
```bash
# Solution: Check that you're using the right Python
which python
maturin develop --features python  # Rebuild with correct python
```

### Issue: Linking Errors
```bash
# Solution: Don't use --features python for Rust tests
cargo test  # NOT: cargo test --features python
```

### Issue: Slow Compilation
```bash
# Solution: Use cargo check for faster iteration
cargo check

# Or build only specific crate
cargo build -p robocodec
```

## Performance Tips

### Profile Before Optimizing
```bash
# Measure first
cargo flamegraph --bin convert -- input.bag output.mcap

# Identify hot functions
# Then optimize
```

### Use Arena Allocation
```rust
// BAD: Individual allocations
let data = vec.iter().map(|x| x.clone()).collect::<Vec<_>>();

// GOOD: Arena allocation
let arena = Bump::new();
let data: Vec<_> = vec.iter().map(|x| arena.alloc_str(x)).collect();
```

### Parallelize with Rayon
```rust
use rayon::prelude::*;

// Parallel iteration
messages.par_iter_mut().for_each(|msg| {
    process_message(msg);
});
```

### Use Release Build for Benchmarks
```bash
cargo build --release
cargo test --release --features profiling
```

## Documentation

### Adding Documentation
```rust
/// Does something useful.
///
/// # Examples
/// ```
/// use my_crate::function;
/// let result = function();
/// assert!(result.is_ok());
/// ```
///
/// # Errors
///
/// This function will return an error if ...
///
/// # Panics
///
/// This function will panic if ...
pub fn function() -> Result<()> {
    // ...
}
```

### Generating Documentation
```bash
# Generate and open docs
cargo doc --open

# Document all features
cargo doc --all-features --open
```

## Keyboard Shortcuts (VS Code)

- `Cmd+Shift+B` - Build
- `Cmd+Shift+M` - Show Problems
- `Cmd+Shift+F` - Search in files
- `Ctrl+` (backtick) - Show terminal
- `Cmd+P` - Quick file open

## Resources

- [CLAUDE.md](../CLAUDE.md) - Project overview
- [README.md](../README.md) - User documentation
- [docs/](../docs/) - Detailed architecture docs
- [Serena memories] - Use `list_memories` to see all

## Quick Help Commands

```bash
# Show all make targets
make help

# Show cargo help
cargo help

# Show pytest help
pytest --help

# List all Serena memories
# (Use Serena tool: list_memories)
```

---

**Need more help?**
- Check `suggested_commands.md` for all commands
- Check `style_and_conventions.md` for coding patterns
- Check `architecture_overview.md` for system design
