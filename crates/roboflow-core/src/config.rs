// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Configuration validation utilities.
//!
//! This module provides a standard trait and helper functions for validating
//! configuration types throughout the roboflow workspace.
//!
//! # Example
//!
//! ```rust
//! use roboflow_core::{Validate, Result, validators};
//!
//! struct ServerConfig {
//!     port: u16,
//!     max_connections: usize,
//! }
//!
//! impl Validate for ServerConfig {
//!     fn validate(&self) -> Result<()> {
//!         validators::port(self.port, "port")?;
//!         validators::positive(self.max_connections, "max_connections")?;
//!         Ok(())
//!     }
//! }
//! ```

use crate::{Result, RoboflowError};

/// Trait for configuration validation.
///
/// Configuration types should implement this trait to provide
/// semantic validation beyond what deserialization provides.
///
/// The `validate` method should be called after loading configuration
/// to ensure all values are within acceptable bounds and constraints
/// are satisfied.
pub trait Validate {
    /// Validate the configuration.
    ///
    /// Returns `Ok(())` if valid, or an error describing what's invalid.
    fn validate(&self) -> Result<()>;
}

/// Reusable validation helper functions.
///
/// These functions provide common validation patterns that can be
/// composed in `Validate::validate` implementations.
pub mod validators {
    use super::*;

    /// Validate that a value is positive (> 0).
    ///
    /// # Arguments
    ///
    /// * `value` - The value to validate
    /// * `field` - Field name for error messages
    ///
    /// # Example
    ///
    /// ```rust
    /// use roboflow_core::validators;
    ///
    /// validators::positive(5usize, "count").unwrap(); // Ok
    /// validators::positive(0usize, "count").unwrap_err(); // Err
    /// ```
    pub fn positive<T>(value: T, field: &str) -> Result<()>
    where
        T: PartialOrd + std::fmt::Display + Default,
    {
        if value > T::default() {
            Ok(())
        } else {
            Err(RoboflowError::parse(
                field,
                format!("must be greater than 0 (got {})", value),
            ))
        }
    }

    /// Validate that a value is non-negative (>= 0).
    ///
    /// # Arguments
    ///
    /// * `value` - The value to validate
    /// * `field` - Field name for error messages
    pub fn non_negative<T>(value: T, field: &str) -> Result<()>
    where
        T: PartialOrd + std::fmt::Display + Default,
    {
        if value >= T::default() {
            Ok(())
        } else {
            Err(RoboflowError::parse(
                field,
                format!("must be non-negative (got {})", value),
            ))
        }
    }

    /// Validate that a value is within a range [min, max] (inclusive).
    ///
    /// # Arguments
    ///
    /// * `value` - The value to validate
    /// * `min` - Minimum allowed value (inclusive)
    /// * `max` - Maximum allowed value (inclusive)
    /// * `field` - Field name for error messages
    ///
    /// # Example
    ///
    /// ```rust
    /// use roboflow_core::validators;
    ///
    /// validators::range(18u32, 0, 51, "crf").unwrap(); // Ok
    /// validators::range(100u32, 0, 51, "crf").unwrap_err(); // Err
    /// ```
    pub fn range<T>(value: T, min: T, max: T, field: &str) -> Result<()>
    where
        T: PartialOrd + std::fmt::Display + Copy,
    {
        if value >= min && value <= max {
            Ok(())
        } else {
            Err(RoboflowError::parse(
                field,
                format!("must be in range [{}, {}] (got {})", min, max, value),
            ))
        }
    }

    /// Validate that a value is within a range [min, max) (half-open).
    ///
    /// The minimum is inclusive, the maximum is exclusive.
    pub fn range_exclusive<T>(value: T, min: T, max: T, field: &str) -> Result<()>
    where
        T: PartialOrd + std::fmt::Display + Copy,
    {
        if value >= min && value < max {
            Ok(())
        } else {
            Err(RoboflowError::parse(
                field,
                format!("must be in range [{}, {}) (got {})", min, max, value),
            ))
        }
    }

    /// Validate that a slice is not empty.
    ///
    /// # Arguments
    ///
    /// * `value` - The slice to validate
    /// * `field` - Field name for error messages
    ///
    /// # Example
    ///
    /// ```rust
    /// use roboflow_core::validators;
    ///
    /// validators::not_empty(&[1, 2, 3] as &[i32], "items").unwrap(); // Ok
    /// validators::not_empty(&[] as &[i32], "items").unwrap_err(); // Err
    /// ```
    pub fn not_empty<T>(value: &[T], field: &str) -> Result<()> {
        if !value.is_empty() {
            Ok(())
        } else {
            Err(RoboflowError::parse(field, "must not be empty"))
        }
    }

    /// Validate that a string is not empty.
    pub fn not_empty_str(value: &str, field: &str) -> Result<()> {
        if !value.is_empty() {
            Ok(())
        } else {
            Err(RoboflowError::parse(field, "must not be empty"))
        }
    }

    /// Validate that a string starts with a prefix.
    ///
    /// # Arguments
    ///
    /// * `value` - The string to validate
    /// * `prefix` - Required prefix
    /// * `field` - Field name for error messages
    ///
    /// # Example
    ///
    /// ```rust
    /// use roboflow_core::validators;
    ///
    /// validators::starts_with("/path/to/file", "/", "path").unwrap(); // Ok
    /// validators::starts_with("path/to/file", "/", "path").unwrap_err(); // Err
    /// ```
    pub fn starts_with(value: &str, prefix: &str, field: &str) -> Result<()> {
        if value.starts_with(prefix) {
            Ok(())
        } else {
            Err(RoboflowError::parse(
                field,
                format!("must start with '{}' (got '{}')", prefix, value),
            ))
        }
    }

    /// Validate a port number (1-65535).
    ///
    /// Port 0 is typically reserved and should not be used in production.
    pub fn port(value: u16, field: &str) -> Result<()> {
        if value > 0 {
            Ok(())
        } else {
            Err(RoboflowError::parse(
                field,
                "must be a valid port (1-65535)",
            ))
        }
    }

    /// Validate that a value satisfies a custom predicate.
    ///
    /// # Arguments
    ///
    /// * `value` - The value to validate
    /// * `predicate` - Function that returns true if valid
    /// * `field` - Field name for error messages
    /// * `message` - Error message when predicate fails
    ///
    /// # Example
    ///
    /// ```rust
    /// use roboflow_core::validators;
    ///
    /// validators::satisfies(&5, |v| *v % 2 == 1, "number", "must be odd").unwrap(); // Ok
    /// validators::satisfies(&4, |v| *v % 2 == 1, "number", "must be odd").unwrap_err(); // Err
    /// ```
    pub fn satisfies<T, F>(value: &T, predicate: F, field: &str, message: &str) -> Result<()>
    where
        F: Fn(&T) -> bool,
    {
        if predicate(value) {
            Ok(())
        } else {
            Err(RoboflowError::parse(field, message.to_string()))
        }
    }

    /// Validate that two optional values are either both set or both unset.
    ///
    /// This is useful for validating paired configuration like TLS cert/key.
    ///
    /// # Arguments
    ///
    /// * `a` - First optional value
    /// * `b` - Second optional value
    /// * `field_a` - Name of first field
    /// * `field_b` - Name of second field
    ///
    /// # Example
    ///
    /// ```rust
    /// use roboflow_core::validators;
    ///
    /// // Both set - Ok
    /// validators::paired(Some("cert"), Some("key"), "cert", "key").unwrap();
    /// // Both unset - Ok
    /// validators::paired(None::<&str>, None, "cert", "key").unwrap();
    /// // Only one set - Err
    /// validators::paired(Some("cert"), None, "cert", "key").unwrap_err();
    /// ```
    pub fn paired<T>(
        a: Option<T>,
        b: Option<T>,
        field_a: &str,
        field_b: &str,
    ) -> Result<()> {
        let a_set = a.is_some();
        let b_set = b.is_some();
        if a_set == b_set {
            Ok(())
        } else {
            Err(RoboflowError::parse(
                field_a,
                format!("must be set together with '{}' (one is set, other is not)", field_b),
            ))
        }
    }

    /// Validate that a usize value is at least a minimum.
    pub fn at_least<T>(value: T, min: T, field: &str) -> Result<()>
    where
        T: PartialOrd + std::fmt::Display + Copy,
    {
        if value >= min {
            Ok(())
        } else {
            Err(RoboflowError::parse(
                field,
                format!("must be at least {} (got {})", min, value),
            ))
        }
    }

    /// Validate that a usize value is at most a maximum.
    pub fn at_most<T>(value: T, max: T, field: &str) -> Result<()>
    where
        T: PartialOrd + std::fmt::Display + Copy,
    {
        if value <= max {
            Ok(())
        } else {
            Err(RoboflowError::parse(
                field,
                format!("must be at most {} (got {})", max, value),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_positive() {
        assert!(validators::positive(5usize, "count").is_ok());
        assert!(validators::positive(1i32, "count").is_ok());
        assert!(validators::positive(0usize, "count").is_err());
        assert!(validators::positive(0i32, "count").is_err());

        let err = validators::positive(0usize, "count").unwrap_err();
        assert!(format!("{}", err).contains("must be greater than 0"));
    }

    #[test]
    fn test_non_negative() {
        assert!(validators::non_negative(5i32, "value").is_ok());
        assert!(validators::non_negative(0i32, "value").is_ok());
        assert!(validators::non_negative(-1i32, "value").is_err());
    }

    #[test]
    fn test_range() {
        assert!(validators::range(18u32, 0, 51, "crf").is_ok());
        assert!(validators::range(0u32, 0, 51, "crf").is_ok());
        assert!(validators::range(51u32, 0, 51, "crf").is_ok());
        assert!(validators::range(52u32, 0, 51, "crf").is_err());

        let err = validators::range(100u32, 0, 51, "crf").unwrap_err();
        assert!(format!("{}", err).contains("must be in range [0, 51]"));
    }

    #[test]
    fn test_range_exclusive() {
        assert!(validators::range_exclusive(5u32, 0, 10, "value").is_ok());
        assert!(validators::range_exclusive(0u32, 0, 10, "value").is_ok());
        assert!(validators::range_exclusive(10u32, 0, 10, "value").is_err());
        assert!(validators::range_exclusive(11u32, 0, 10, "value").is_err());
    }

    #[test]
    fn test_not_empty() {
        let items = vec![1, 2, 3];
        assert!(validators::not_empty(&items, "items").is_ok());
        let empty: Vec<i32> = vec![];
        assert!(validators::not_empty(&empty, "items").is_err());

        let err = validators::not_empty(&empty, "items").unwrap_err();
        assert!(format!("{}", err).contains("must not be empty"));
    }

    #[test]
    fn test_not_empty_str() {
        assert!(validators::not_empty_str("hello", "name").is_ok());
        assert!(validators::not_empty_str("", "name").is_err());
    }

    #[test]
    fn test_starts_with() {
        assert!(validators::starts_with("/path", "/", "path").is_ok());
        assert!(validators::starts_with("path", "/", "path").is_err());

        let err = validators::starts_with("path", "/", "path").unwrap_err();
        assert!(format!("{}", err).contains("must start with '/'"));
    }

    #[test]
    fn test_port() {
        assert!(validators::port(8080, "port").is_ok());
        assert!(validators::port(1, "port").is_ok());
        assert!(validators::port(0, "port").is_err());
    }

    #[test]
    fn test_satisfies() {
        assert!(validators::satisfies(&5, |v| *v % 2 == 1, "number", "must be odd").is_ok());
        assert!(validators::satisfies(&4, |v| *v % 2 == 1, "number", "must be odd").is_err());

        let err = validators::satisfies(&4, |v| *v % 2 == 1, "number", "must be odd").unwrap_err();
        assert!(format!("{}", err).contains("must be odd"));
    }

    #[test]
    fn test_paired() {
        // Both set
        assert!(validators::paired(Some("cert"), Some("key"), "cert", "key").is_ok());
        // Both unset
        assert!(validators::paired(None::<&str>, None, "cert", "key").is_ok());
        // Only first set
        assert!(validators::paired(Some("cert"), None, "cert", "key").is_err());
        // Only second set
        assert!(validators::paired(None::<&str>, Some("key"), "cert", "key").is_err());

        let err = validators::paired(Some("cert"), None, "cert", "key").unwrap_err();
        assert!(format!("{}", err).contains("must be set together"));
    }

    #[test]
    fn test_at_least() {
        assert!(validators::at_least(10u32, 5u32, "value").is_ok());
        assert!(validators::at_least(5u32, 5u32, "value").is_ok());
        assert!(validators::at_least(4u32, 5u32, "value").is_err());

        let err = validators::at_least(4u32, 5u32, "value").unwrap_err();
        assert!(format!("{}", err).contains("must be at least 5"));
    }

    #[test]
    fn test_at_most() {
        assert!(validators::at_most(5u32, 10u32, "value").is_ok());
        assert!(validators::at_most(10u32, 10u32, "value").is_ok());
        assert!(validators::at_most(11u32, 10u32, "value").is_err());

        let err = validators::at_most(11u32, 10u32, "value").unwrap_err();
        assert!(format!("{}", err).contains("must be at most 10"));
    }
}
