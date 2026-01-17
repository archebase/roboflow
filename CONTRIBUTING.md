# Contributing to Robocodec

Thank you for your interest in contributing to Robocodec! This document provides guidelines and instructions for contributing to the project.

## Code of Conduct

Please be respectful and constructive in all interactions. See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) for details.

## How to Contribute

### Reporting Bugs

Before creating bug reports, please check existing issues to avoid duplicates. When creating a bug report, include:

- **Clear title and description**: Summarize the issue
- **Steps to reproduce**: Detailed steps to reproduce the bug
- **Expected behavior**: What you expected to happen
- **Actual behavior**: What actually happened
- **Environment**: OS, Rust version, Python version (if applicable)
- **Logs/error messages**: Any relevant error messages or stack traces
- **Test files**: If applicable, provide sample data files that reproduce the issue

### Suggesting Enhancements

Enhancement suggestions are welcome! Provide:

- **Clear description**: Describe the proposed feature
- **Use case**: Explain the use case and why it would be useful
- **Alternatives considered**: Any alternative solutions you've considered

### Pull Requests

#### Setup

1. Fork the repository
2. Clone your fork and add the upstream remote:
   ```bash
   git clone https://github.com/YOUR_USERNAME/robocodec.git
   cd robocodec
   git remote add upstream https://github.com/archebase/robocodec.git
   ```

3. Create a branch for your changes:
   ```bash
   git checkout -b feature/your-feature-name
   # or
   git checkout -b fix/your-bug-fix
   ```

#### Making Changes

1. **Follow the existing code style**: The project uses standard Rust formatting
2. **Write tests**: Add tests for new functionality or bug fixes
3. **Update documentation**: Update relevant documentation, comments, and README
4. **Commit messages**: Use clear, descriptive commit messages:
   ```
   feat: add support for XYZ format
   fix: handle edge case in CDR decoder
   docs: update installation instructions
   ```

#### Testing

Run the test suite before submitting:

```bash
# Run Rust tests
cargo test --all-features

# Run Python tests (if applicable)
maturin develop && pytest

# Run clippy
cargo clippy --all-features -- -D warnings

# Check formatting
cargo fmt -- --check
```

#### Submitting

1. Push your branch to your fork
2. Create a pull request to the `main` branch
3. Fill out the pull request template
4. Wait for review and address any feedback

## Development Workflow

### Project Structure

```
robocodec/
├── src/
│   ├── bin/          # Command-line tools
│   ├── codec/        # Codec implementations
│   ├── core/         # Core types and errors
│   ├── encoding/     # Encoding/decoding implementations
│   ├── format/       # File format handlers
│   ├── schema/       # Schema parsers
│   └── python/       # Python bindings
├── python/           # Python package
├── tests/            # Integration tests
└── examples/         # Example code
```

### Adding Features

1. **New codec support**: Add to `src/codec/` and update `src/core/registry.rs`
2. **New file format**: Add to `src/format/` with Reader/Writer implementations
3. **New schema format**: Add parser to `src/schema/`
4. **CLI tool**: Add binary to `src/bin/` and update Cargo.toml

### Python Bindings

Python bindings are managed via PyO3. When adding Rust APIs that should be exposed to Python:

1. Add `#[pyfunction]` or `#[pymethods]` attributes
2. Register in `src/python/mod.rs`
3. Add type stubs to `python/robocodec/` if needed
4. Update Python documentation

### Testing Guidelines

- **Unit tests**: Test individual functions and modules
- **Integration tests**: Test end-to-end functionality
- **Round-trip tests**: Verify encode/decode consistency
- **Cross-language tests**: Verify Rust and Python API parity

## Release Process

Maintainers follow this process for releases:

1. Update version in `Cargo.toml`
2. Update CHANGELOG.md
3. Create git tag
4. Publish to crates.io
5. Build and publish Python package to PyPI

## Questions?

Feel free to open an issue for questions or discussion about contributions.
