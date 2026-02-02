// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Shared utilities for CLI commands.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Validate a storage path component to prevent path traversal and injection attacks.
///
/// Returns an error if the path contains:
/// - Path traversal sequences (../..)
/// - Null bytes
/// - Control characters
fn validate_path_component(component: &str, name: &str) -> Result<(), String> {
    // Check for path traversal attempts
    if component.contains("..") {
        return Err(format!(
            "Invalid {}: path traversal sequences (..) are not allowed",
            name
        ));
    }

    // Check for null bytes
    if component.contains('\0') {
        return Err(format!("Invalid {}: null bytes are not allowed", name));
    }

    // Check for suspicious characters that might indicate injection
    if component.contains('\r') || component.contains('\n') {
        return Err(format!("Invalid {}: line breaks are not allowed", name));
    }

    // Check for empty component
    if component.is_empty() {
        return Err(format!("Invalid {}: cannot be empty", name));
    }

    Ok(())
}

/// Parse a storage URL into (bucket, key) components with security validation.
///
/// This function validates the input to prevent:
/// - Path traversal attacks (../.. sequences)
/// - Null byte injection
/// - Control character injection
pub fn parse_storage_url(url: &str) -> Result<(String, String), String> {
    // Validate URL length (prevent DoS via extremely long URLs)
    if url.len() > 4096 {
        return Err("Storage URL too long (max 4096 characters)".to_string());
    }

    if let Some(rest) = url
        .strip_prefix("oss://")
        .or_else(|| url.strip_prefix("s3://"))
    {
        let mut parts = rest.splitn(2, '/');
        let bucket = parts
            .next()
            .ok_or_else(|| "Invalid storage URL: missing bucket name".to_string())?;

        // Validate bucket name
        validate_path_component(bucket, "bucket name")?;

        // Additional bucket name validation per cloud provider rules
        if bucket.len() > 255 {
            return Err("Bucket name too long (max 255 characters)".to_string());
        }

        let key = parts.next().unwrap_or("").to_string();

        // Validate key for path traversal
        if !key.is_empty() {
            validate_path_component(&key, "key")?;
        }

        Ok((bucket.to_string(), key))
    } else if let Some(path) = url.strip_prefix("file://") {
        // Validate file path
        validate_path_component(path, "file path")?;
        Ok(("local".to_string(), path.to_string()))
    } else {
        // Validate local path
        validate_path_component(url, "path")?;
        Ok(("local".to_string(), url.to_string()))
    }
}

/// Compute a hash-based job ID from file key and size.
pub fn compute_file_hash(key: &str, size: u64) -> String {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    size.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// Check if a file path matches a glob pattern.
pub fn glob_match(pattern: &str, text: &str) -> bool {
    use regex::Regex;

    // Convert glob pattern to regex
    let regex_pattern = pattern
        .replace('.', "\\.") // Escape dots
        .replace('?', ".") // ? -> any single char
        .replace('*', ".*"); // * -> any chars

    // Anchor the pattern to match the entire string
    let full_pattern = format!("^{}$", regex_pattern);

    match Regex::new(&full_pattern) {
        Ok(re) => re.is_match(text),
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_storage_url_oss() {
        let (bucket, key) = parse_storage_url("oss://my-bucket/path/to/file.mcap").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "path/to/file.mcap");
    }

    #[test]
    fn test_parse_storage_url_s3() {
        let (bucket, key) = parse_storage_url("s3://my-bucket/path/to/file.mcap").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "path/to/file.mcap");
    }

    #[test]
    fn test_parse_storage_url_file() {
        let (bucket, key) = parse_storage_url("file://./data/file.mcap").unwrap();
        assert_eq!(bucket, "local");
        assert_eq!(key, "./data/file.mcap");
    }

    #[test]
    fn test_compute_file_hash() {
        let hash1 = compute_file_hash("test-key", 1024);
        let hash2 = compute_file_hash("test-key", 1024);
        let hash3 = compute_file_hash("test-key", 2048);
        assert_eq!(hash1, hash2);
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_glob_match() {
        assert!(glob_match("*.mcap", "file.mcap"));
        assert!(!glob_match("*.mcap", "file.txt"));
        assert!(glob_match("test*", "test123"));
        assert!(glob_match("test?file", "test1file"));
        assert!(!glob_match("test?file", "test12file"));
        assert!(glob_match("*file*", "mydata/file.csv"));
        assert!(glob_match("data/*.mcap", "data/file.mcap"));
    }
}
