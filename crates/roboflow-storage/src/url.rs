// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! URL parsing for storage backends.
//!
//! Provides unified URL parsing for multiple storage schemes:
//! - `file://` or plain paths for local filesystem
//! - `s3://` for Amazon S3 and S3-compatible storage (Alibaba OSS, MinIO, etc.)
//!
//! # Example
//!
//! ```
//! # use roboflow_storage::url::StorageUrl;
//! # use std::str::FromStr;
//! #
//! let url = StorageUrl::from_str("s3://my-bucket/path/to/file.txt").unwrap();
//! ```

use std::path::PathBuf;
use std::str::FromStr;

use super::StorageError;

/// Parsed storage URL with scheme-specific information.
///
/// This enum represents the result of parsing a storage URL, with
/// variants for each supported storage backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageUrl {
    /// Local filesystem path.
    ///
    /// Can be specified as:
    /// - Plain path: `/tmp/file.txt` or `./file.txt` or `C:\file.txt`
    /// - file:// URL: `file:///tmp/file.txt`
    Local {
        /// The filesystem path.
        path: PathBuf,
    },

    /// S3-compatible storage URL (Amazon S3, Alibaba OSS, MinIO, etc.).
    ///
    /// Uses the S3 protocol which is compatible with most cloud storage providers.
    S3 {
        /// Bucket name.
        bucket: String,
        /// Object key (path within bucket).
        key: String,
        /// Optional endpoint override (for S3-compatible services).
        endpoint: Option<String>,
        /// Optional region.
        region: Option<String>,
    },
}

impl StorageUrl {
    /// Check if this URL refers to local storage.
    pub fn is_local(&self) -> bool {
        matches!(self, Self::Local { .. })
    }

    /// Check if this URL refers to remote/cloud storage.
    pub fn is_remote(&self) -> bool {
        !self.is_local()
    }

    /// Get the path/key component of this URL.
    ///
    /// For local URLs, this is the filesystem path.
    /// For S3 URLs, this is the object key.
    pub fn path(&self) -> &str {
        match self {
            Self::Local { path } => path.to_str().unwrap_or(""),
            Self::S3 { key, .. } => key,
        }
    }

    /// Get the bucket name for cloud storage URLs.
    ///
    /// Returns None for local URLs.
    pub fn bucket(&self) -> Option<&str> {
        match self {
            Self::Local { .. } => None,
            Self::S3 { bucket, .. } => Some(bucket),
        }
    }

    /// Create a local storage URL from a path.
    pub fn local(path: impl Into<PathBuf>) -> Self {
        Self::Local { path: path.into() }
    }

    /// Create an S3 storage URL.
    pub fn s3(bucket: impl Into<String>, key: impl Into<String>) -> Self {
        Self::S3 {
            bucket: bucket.into(),
            key: key.into(),
            endpoint: None,
            region: None,
        }
    }
}

impl FromStr for StorageUrl {
    type Err = StorageError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();

        // Check for URL scheme
        if let Some(scheme_end) = s.find("://") {
            let scheme = &s[..scheme_end];
            let rest = &s[scheme_end + 3..];

            match scheme {
                "file" => {
                    // file:// URLs - parse the path component
                    // After finding "://", the rest depends on the format:
                    // - file:///tmp/file.txt → rest = /tmp/file.txt (absolute path)
                    // - file://localhost/tmp/file.txt → rest = localhost/tmp/file.txt (no leading /)
                    let path = if rest.starts_with("localhost/") {
                        // file://localhost/absolute/path - strip localhost/ and add leading /
                        format!("/{}", &rest[9..]) // Skip "localhost/"
                    } else if rest.starts_with('/') {
                        // file:///path - rest already has leading /
                        rest.to_string()
                    } else {
                        // file:path (relative, non-standard)
                        format!("/{}", rest)
                    };
                    Ok(Self::Local {
                        path: PathBuf::from(path),
                    })
                }
                "s3" => {
                    // s3://bucket/key/path (compatible with S3, Alibaba OSS, MinIO, etc.)
                    parse_s3_url(rest)
                }
                _ => Err(StorageError::invalid_path(format!(
                    "unsupported URL scheme: {scheme}"
                ))),
            }
        } else {
            // No scheme - treat as local path, but reject malformed S3 URLs
            // (e.g. "s3:" or "s3:/bucket" missing "//") to avoid silently writing
            // to a local directory named "s3:" instead of S3
            if s.starts_with("s3:") {
                return Err(StorageError::invalid_path(format!(
                    "malformed cloud URL '{}': use s3://bucket/path (double slash required)",
                    s
                )));
            }
            Ok(Self::Local {
                path: PathBuf::from(s),
            })
        }
    }
}

/// Parse an S3 URL (after the `s3://` prefix).
fn parse_s3_url(rest: &str) -> Result<StorageUrl, StorageError> {
    // Format: s3://bucket/key/path
    // Optional query params: ?endpoint=...&region=...

    let (path, query) = match rest.find('?') {
        Some(q) => (&rest[..q], Some(&rest[q + 1..])),
        None => (rest, None),
    };

    // Split bucket from key
    let (bucket, key) = match path.find('/') {
        Some(slash) => (&path[..slash], &path[slash + 1..]),
        None => (path, ""),
    };

    if bucket.is_empty() {
        return Err(StorageError::invalid_path("S3 URL missing bucket name"));
    }

    // Parse query parameters
    let mut endpoint = None;
    let mut region = None;

    if let Some(query) = query {
        for param in query.split('&') {
            let mut parts = param.splitn(2, '=');
            if let (Some(key), Some(value)) = (parts.next(), parts.next()) {
                match key {
                    "endpoint" => endpoint = Some(value.to_string()),
                    "region" => region = Some(value.to_string()),
                    _ => {}
                }
            }
        }
    }

    Ok(StorageUrl::S3 {
        bucket: bucket.to_string(),
        key: key.to_string(),
        endpoint,
        region,
    })
}

impl std::fmt::Display for StorageUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Local { path } => {
                write!(f, "file://{}", path.display())
            }
            Self::S3 {
                bucket,
                key,
                endpoint,
                region,
            } => {
                write!(f, "s3://{}/{}", bucket, key)?;
                if endpoint.is_some() || region.is_some() {
                    write!(f, "?")?;
                    let mut first = true;
                    if let Some(ep) = endpoint {
                        write!(f, "endpoint={}", ep)?;
                        first = false;
                    }
                    if let Some(r) = region {
                        if !first {
                            write!(f, "&")?;
                        }
                        write!(f, "region={}", r)?;
                    }
                }
                Ok(())
            }
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_local_path() {
        let url = StorageUrl::from_str("/tmp/file.txt").unwrap();
        assert_eq!(
            url,
            StorageUrl::Local {
                path: PathBuf::from("/tmp/file.txt")
            }
        );
        assert!(url.is_local());
    }

    #[test]
    fn test_parse_relative_path() {
        let url = StorageUrl::from_str("./file.txt").unwrap();
        assert_eq!(
            url,
            StorageUrl::Local {
                path: PathBuf::from("./file.txt")
            }
        );
    }

    #[test]
    fn test_parse_file_url() {
        let url = StorageUrl::from_str("file:///tmp/file.txt").unwrap();
        assert_eq!(
            url,
            StorageUrl::Local {
                path: PathBuf::from("/tmp/file.txt")
            }
        );
    }

    #[test]
    fn test_parse_file_url_localhost() {
        let url = StorageUrl::from_str("file://localhost/tmp/file.txt").unwrap();
        assert_eq!(
            url,
            StorageUrl::Local {
                path: PathBuf::from("/tmp/file.txt")
            }
        );
    }

    #[test]
    fn test_parse_s3_url() {
        let url = StorageUrl::from_str("s3://my-bucket/path/to/file.txt").unwrap();
        assert!(matches!(url, StorageUrl::S3 { ref bucket, .. } if bucket == "my-bucket"));
        assert_eq!(url.path(), "path/to/file.txt");
        assert_eq!(url.bucket(), Some("my-bucket"));
    }

    #[test]
    fn test_parse_s3_url_with_query() {
        let url = StorageUrl::from_str("s3://bucket/file.txt?region=us-west-2").unwrap();
        assert!(matches!(url, StorageUrl::S3 { region: Some(ref r), .. } if r == "us-west-2"));
    }
    #[test]
    fn test_parse_invalid_scheme() {
        let result = StorageUrl::from_str("ftp://server/file.txt");
        assert!(matches!(result, Err(StorageError::InvalidPath(_))));
    }

    #[test]
    fn test_parse_empty_bucket() {
        let result = StorageUrl::from_str("s3:///file.txt");
        assert!(matches!(result, Err(StorageError::InvalidPath(_))));
    }

    #[test]
    fn test_parse_malformed_s3_url_single_slash() {
        // s3:/bucket (missing one /) was incorrectly treated as local path, creating dir "s3:"
        let result = StorageUrl::from_str("s3:/roboflow-datasets");
        assert!(matches!(result, Err(StorageError::InvalidPath(_))));
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("malformed"));
        assert!(err_msg.contains("s3://"));
    }

    #[test]
    fn test_parse_malformed_s3_url_scheme_only() {
        let result = StorageUrl::from_str("s3:");
        assert!(matches!(result, Err(StorageError::InvalidPath(_))));
    }

    #[test]
    fn test_storage_url_local() {
        let url = StorageUrl::local("/tmp/test");
        assert!(url.is_local());
        assert!(!url.is_remote());
        assert_eq!(url.path(), "/tmp/test");
        assert_eq!(url.bucket(), None);
    }

    #[test]
    fn test_storage_url_s3() {
        let url = StorageUrl::s3("bucket", "key/file.txt");
        assert!(!url.is_local());
        assert!(url.is_remote());
        assert_eq!(url.path(), "key/file.txt");
        assert_eq!(url.bucket(), Some("bucket"));
    }

    #[test]
    fn test_display_local() {
        let url = StorageUrl::local("/tmp/file.txt");
        assert_eq!(url.to_string(), "file:///tmp/file.txt");
    }

    #[test]
    fn test_display_s3() {
        let url = StorageUrl::s3("bucket", "file.txt");
        assert_eq!(url.to_string(), "s3://bucket/file.txt");
    }
}
