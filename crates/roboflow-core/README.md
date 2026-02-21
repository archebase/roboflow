# roboflow-core

[![License: MulanPSL-2.0](https://img.shields.io/badge/License-MulanPSL--2.0-blue.svg)](http://license.coscl.org.cn/MulanPSL2)

Core types and abstractions for the roboflow ecosystem.

## Features

- **Error Types**: Unified `RoboflowError` with categorized error handling
- **Type Registry**: Thread-safe `TypeRegistry` for schema storage
- **Value Types**: `CodecValue` enum for decoded message values
- **Validation Traits**: Shared validation utilities with `Validate` trait

## Usage

```rust
use roboflow_core::{Result, RoboflowError, TypeRegistry, Validate};

// Create a type registry
let registry = TypeRegistry::new();
registry.register("my_type", schema)?;

// Use validation
struct Config { fps: u32 }
impl Validate for Config {
    fn validate(&self) -> Result<()> {
        validators::positive(self.fps, "fps")?;
        Ok(())
    }
}
```

## Error Categories

| Category | Description |
|----------|-------------|
| `Parse` | Configuration or schema parsing errors |
| `Encode` | Video/image encoding errors |
| `Decode` | Message decoding errors |
| `Io` | File or network I/O errors |
| `Storage` | S3/OSS/Local storage errors |
| `Other` | Miscellaneous errors |

## Re-exports

This crate re-exports commonly used types for convenience:

- `Result<T>` - Alias for `std::result::Result<T, RoboflowError>`
- `validators` - Validation helper functions

## License

MulanPSL-2.0
