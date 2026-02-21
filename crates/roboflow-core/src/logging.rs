// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! # Structured logging with tracing
//!
//! Provides unified logging initialization for all roboflow binaries and libraries.
//!
//! ## Features
//! - JSON format for production (controlled by `LOG_FORMAT=json`)
//! - Pretty format for development (default)
//! - Dynamic log levels via `RUST_LOG` environment variable
//! - Request/Job ID propagation via tracing span
//!
//! ## Environment Variables
//!
//! | Variable | Purpose | Default | Values |
//! |----------|---------|---------|--------|
//! | `LOG_FORMAT` | Output format | `pretty` | `pretty`, `json` |
//! | `LOG_LEVEL` | Default log level | (uses RUST_LOG) | `trace`, `debug`, `info`, `warn`, `error` |
//! | `RUST_LOG` | Per-module log levels | `info` | `crate=level` syntax |
//! | `LOG_SPAN_EVENTS` | Enable span tracing | `0` | `0`, `1` |
//!
//! ## Examples
//!
//! ```ignore
//! use roboflow_core::init_logging;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Initialize with default settings from environment
//!     init_logging()?;
//!
//!     // Or with custom configuration
//!     let config = LoggingConfig {
//!         format: LogFormat::Json,
//!         default_level: Some("debug".to_string()),
//!         ..Default::default()
//!     };
//!     init_logging_with(config)?;
//!
//!     tracing::info!("Application started");
//!     Ok(())
//! }
//! ```

use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

/// Log format options
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    /// Pretty-printed format for development
    Pretty,
    /// JSON format for production (SLS-compatible)
    Json,
}

impl LogFormat {
    /// Parse from environment variable string
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "json" => Some(LogFormat::Json),
            "pretty" => Some(LogFormat::Pretty),
            _ => None,
        }
    }
}

/// Logging configuration
#[derive(Debug, Clone)]
pub struct LoggingConfig {
    /// Log format (pretty or json)
    pub format: LogFormat,
    /// Default log level (overrides RUST_LOG if set)
    pub default_level: Option<String>,
    /// Whether to include span events (for tracing)
    pub span_events: bool,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            format: LogFormat::Pretty,
            default_level: None,
            span_events: false,
        }
    }
}

impl LoggingConfig {
    /// Create configuration from environment variables
    ///
    /// Reads the following environment variables:
    /// - `LOG_FORMAT`: Output format (`pretty` or `json`)
    /// - `LOG_LEVEL`: Default log level
    /// - `LOG_SPAN_EVENTS`: Enable span events (`1` to enable)
    pub fn from_env() -> Self {
        let format = std::env::var("LOG_FORMAT")
            .ok()
            .as_deref()
            .and_then(LogFormat::parse)
            .unwrap_or(LogFormat::Pretty);

        let default_level = std::env::var("LOG_LEVEL").ok();
        let span_events = std::env::var("LOG_SPAN_EVENTS").as_deref() == Ok("1");

        Self {
            format,
            default_level,
            span_events,
        }
    }

    /// Initialize the global tracing subscriber
    ///
    /// # Panics
    ///
    /// Panics if a global subscriber has already been set.
    ///
    /// # Errors
    ///
    /// Returns an error if log level parsing fails.
    pub fn init(self) -> Result<(), anyhow::Error> {
        // Build env filter - RUST_LOG or default
        let env_filter = if let Some(level) = self.default_level {
            EnvFilter::new(level)
        } else {
            EnvFilter::from_default_env()
                .add_directive("roboflow=info".parse()?)
                .add_directive("robocodec=info".parse()?)
        };

        let span_events = if self.span_events {
            FmtSpan::NEW | FmtSpan::CLOSE
        } else {
            FmtSpan::NONE
        };

        // Build and set the subscriber based on format
        match self.format {
            LogFormat::Json => {
                // JSON format for production - optimized for SLS ingestion
                let subscriber = tracing_subscriber::registry().with(env_filter).with(
                    fmt::layer()
                        .json()
                        .with_current_span(true)
                        .with_span_list(self.span_events)
                        .with_span_events(span_events),
                );
                tracing::subscriber::set_global_default(subscriber)
                    .map_err(|e| anyhow::anyhow!("Failed to set tracing subscriber: {}", e))?;
            }
            LogFormat::Pretty => {
                // Pretty format for development
                let subscriber = tracing_subscriber::registry().with(env_filter).with(
                    fmt::layer()
                        .pretty()
                        .with_target(true)
                        .with_thread_ids(false)
                        .with_file(true)
                        .with_line_number(true)
                        .with_span_events(span_events),
                );
                tracing::subscriber::set_global_default(subscriber)
                    .map_err(|e| anyhow::anyhow!("Failed to set tracing subscriber: {}", e))?;
            }
        }

        Ok(())
    }
}

/// Initialize logging with default configuration from environment
///
/// This is the simplest way to initialize logging. It reads configuration
/// from environment variables and sets up the global tracing subscriber.
///
/// # Example
///
/// ```ignore
/// roboflow_core::init_logging().unwrap();
/// ```
pub fn init_logging() -> Result<(), anyhow::Error> {
    LoggingConfig::from_env().init()
}

/// Initialize logging with custom configuration
///
/// # Example
///
/// ```ignore
/// use roboflow_core::{init_logging_with, LoggingConfig, LogFormat};
///
/// let config = LoggingConfig {
///     format: LogFormat::Json,
///     default_level: Some("debug".to_string()),
///     ..Default::default()
/// };
/// init_logging_with(config)?;
/// ```
pub fn init_logging_with(config: LoggingConfig) -> Result<(), anyhow::Error> {
    config.init()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_format_parse() {
        assert_eq!(LogFormat::parse("json"), Some(LogFormat::Json));
        assert_eq!(LogFormat::parse("JSON"), Some(LogFormat::Json));
        assert_eq!(LogFormat::parse("pretty"), Some(LogFormat::Pretty));
        assert_eq!(LogFormat::parse("PRETTY"), Some(LogFormat::Pretty));
        assert_eq!(LogFormat::parse("invalid"), None);
        // Test whitespace handling
        assert_eq!(LogFormat::parse(" json"), None); // no trim, should be None
        assert_eq!(LogFormat::parse(""), None);
    }

    #[test]
    fn test_logging_config_default() {
        let config = LoggingConfig::default();
        assert_eq!(config.format, LogFormat::Pretty);
        assert_eq!(config.default_level, None);
        assert!(!config.span_events);
    }

    #[test]
    fn test_logging_config_from_env() {
        // Test with no env vars set
        let config = LoggingConfig::from_env();
        assert_eq!(config.format, LogFormat::Pretty); // default
    }
}
