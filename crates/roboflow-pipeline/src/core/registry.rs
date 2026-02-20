// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Format registry for dynamic format discovery and creation.
//!
//! This module provides a registry system that allows formats to register
//! themselves and be created dynamically based on configuration.

use super::error::{PipelineError, Result};
use super::traits::{FormatContext, FormatWriter};
use std::collections::HashMap;
use std::sync::{LazyLock, RwLock};

/// Global format registry.
static REGISTRY: LazyLock<RwLock<FormatRegistry>> =
    LazyLock::new(|| RwLock::new(FormatRegistry::new()));

/// Description of a format that can be registered.
#[derive(Clone)]
pub struct FormatDescriptor {
    /// Format name (e.g., "lerobot", "hdf5").
    pub name: &'static str,

    /// Human-readable description.
    pub description: &'static str,

    /// File extension for the format (e.g., "parquet", "h5").
    pub file_extension: &'static str,

    /// Feature flag required (if any).
    pub feature_flag: Option<&'static str>,

    /// Factory function to create the writer.
    pub factory: fn(&serde_json::Value, &FormatContext) -> Result<Box<dyn FormatWriter>>,
}

/// Registry of available dataset formats.
pub struct FormatRegistry {
    formats: HashMap<&'static str, FormatDescriptor>,
}

impl FormatRegistry {
    /// Create a new empty registry.
    fn new() -> Self {
        Self {
            formats: HashMap::new(),
        }
    }

    /// Get the global registry instance.
    pub fn global() -> &'static RwLock<FormatRegistry> {
        &REGISTRY
    }

    /// Register a format descriptor.
    pub fn register(&mut self, descriptor: FormatDescriptor) {
        self.formats.insert(descriptor.name, descriptor);
    }

    /// Get a format descriptor by name.
    pub fn get(&self, name: &str) -> Option<&FormatDescriptor> {
        self.formats.get(name)
    }

    /// List all registered formats.
    pub fn list(&self) -> Vec<&FormatDescriptor> {
        self.formats.values().collect()
    }

    /// Check if a format is available.
    pub fn is_available(&self, name: &str) -> bool {
        self.formats.contains_key(name)
    }

    /// Create a writer for the specified format.
    ///
    /// # Arguments
    ///
    /// * `format` - Format name (e.g., "lerobot")
    /// * `config` - Format-specific configuration as JSON
    /// * `context` - Creation context
    pub fn create_writer(
        &self,
        format: &str,
        config: &serde_json::Value,
        context: &FormatContext,
    ) -> Result<Box<dyn FormatWriter>> {
        let descriptor = self
            .formats
            .get(format)
            .ok_or_else(|| PipelineError::FormatNotSupported(format.to_string()))?;

        // Note: Feature flag checking is done at registration time.
        // If a format is registered, it's available.

        (descriptor.factory)(config, context)
    }
}

impl Default for FormatRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Register a format in the global registry.
///
/// This is typically called from a format module's `init` function.
pub fn register_format(descriptor: FormatDescriptor) {
    let mut registry = REGISTRY.write().unwrap();
    registry.register(descriptor);
}

/// Get the global registry (convenience function).
pub fn registry() -> &'static RwLock<FormatRegistry> {
    FormatRegistry::global()
}

/// Macro to declare a format registration.
///
/// This macro creates a static initializer that registers the format
/// when the module is loaded.
///
/// # Example
///
/// ```rust,ignore
/// register_format! {
///     name: "lerobot",
///     description: "LeRobot v2.1 dataset format",
///     file_extension: "parquet",
///     factory: |config, context| {
///         // Create writer
///     }
/// }
/// ```
#[macro_export]
macro_rules! register_format {
    (
        name: $name:literal,
        description: $desc:literal,
        file_extension: $ext:literal,
        feature_flag: $feature:expr,
        factory: $factory:expr
    ) => {
        $crate::core::registry::register_format($crate::core::registry::FormatDescriptor {
            name: $name,
            description: $desc,
            file_extension: $ext,
            feature_flag: $feature,
            factory: $factory,
        });
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_new() {
        let registry = FormatRegistry::new();
        assert!(!registry.is_available("nonexistent"));
    }

    #[test]
    fn test_registry_register() {
        let mut registry = FormatRegistry::new();

        registry.register(FormatDescriptor {
            name: "test",
            description: "Test format",
            file_extension: "test",
            feature_flag: None,
            factory: |_, _| Err(PipelineError::NotSupported("test".to_string())),
        });

        assert!(registry.is_available("test"));
        assert!(registry.get("test").is_some());
    }

    #[test]
    fn test_registry_list() {
        let mut registry = FormatRegistry::new();

        registry.register(FormatDescriptor {
            name: "test1",
            description: "Test format 1",
            file_extension: "t1",
            feature_flag: None,
            factory: |_, _| Err(PipelineError::NotSupported("test".to_string())),
        });

        registry.register(FormatDescriptor {
            name: "test2",
            description: "Test format 2",
            file_extension: "t2",
            feature_flag: None,
            factory: |_, _| Err(PipelineError::NotSupported("test".to_string())),
        });

        let list = registry.list();
        assert_eq!(list.len(), 2);
    }
}
