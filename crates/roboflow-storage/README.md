# roboflow-storage

[![License: MulanPSL-2.0](https://img.shields.io/badge/License-MulanPSL--2.0-blue.svg)](http://license.coscl.org.cn/MulanPSL2)

Unified storage abstraction for local, S3, and Alibaba OSS backends.

## Features

- **Multi-Backend**: S3, Alibaba OSS, and Local filesystem support
- **Async-First**: Native async/await with Tokio runtime
- **Caching**: Built-in LRU cache with configurable eviction
- **Multipart Upload**: Parallel multipart uploads for large files
- **Retry Logic**: Automatic retry with exponential backoff

## Usage

```rust
use roboflow_storage::{Storage, StorageFactory, StorageUrl};

// Create storage from URL
let storage = StorageFactory::from_url("s3://bucket/path")?;

// Or use specific implementations
let local = LocalStorage::new("/data");
let s3 = S3Storage::new(S3Config {
    bucket: "my-bucket".to_string(),
    region: "us-east-1".to_string(),
    ..Default::default()
})?;

// Common operations
let data = storage.read(&path).await?;
storage.write(&path, &data).await?;
let metadata = storage.metadata(&path).await?;
```

## Storage URLs

| Scheme | Example |
|--------|---------|
| `file://` | `file:///local/path` |
| `s3://` | `s3://bucket/key` |
| `oss://` | `oss://bucket/key` |

## Caching

```rust
use roboflow_storage::{CachedStorage, CacheConfig, EvictionPolicy};

let cached = CachedStorage::new(
    inner_storage,
    CacheConfig {
        max_size: 1024 * 1024 * 1024, // 1GB
        eviction: EvictionPolicy::Lru,
        ..Default::default()
    }
);
```

## Environment Variables

### S3 Configuration
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION` (default: `us-east-1`)

### OSS Configuration
- `OSS_ACCESS_KEY_ID`
- `OSS_ACCESS_KEY_SECRET`
- `OSS_ENDPOINT`

## License

MulanPSL-2.0
