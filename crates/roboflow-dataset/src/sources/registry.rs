// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Source registry for creating sources from configuration.

use crate::sources::{Source, SourceConfig, SourceError, SourceFactory, error::SourceResult};
use roboflow_core::GlobalFactoryRegistry;

/// Global registry of source factories.
///
/// Sources register themselves at startup, and the registry creates
/// instances on demand from configuration.
static GLOBAL_SOURCE_REGISTRY: GlobalFactoryRegistry<SourceFactory> = GlobalFactoryRegistry::new();

/// Register a source type with the global registry.
///
/// # Arguments
///
/// * `name` - Name of the source type (e.g., "mcap", "bag")
/// * `factory` - Function that creates new source instances
pub fn register_source(name: impl Into<String>, factory: SourceFactory) {
    GLOBAL_SOURCE_REGISTRY.get_or_init().register(name, factory);
}

/// Create a source from configuration using the global registry.
///
/// This is a convenience function that uses the global registry.
///
/// # Arguments
///
/// * `config` - Source configuration
///
/// # Returns
///
/// A boxed source instance
pub fn create_source(config: &SourceConfig) -> SourceResult<Box<dyn Source>> {
    let source_type = config.source_type.name();

    let registry = GLOBAL_SOURCE_REGISTRY.get_or_init();
    let factory = registry
        .get_ref(source_type)
        .ok_or_else(|| SourceError::UnsupportedFormat(source_type.to_string()))?;

    Ok(factory())
}

/// Check if a source type is registered.
pub fn has_source(name: &str) -> bool {
    GLOBAL_SOURCE_REGISTRY.get_or_init().contains(name)
}

/// Get all registered source names.
pub fn registered_sources() -> Vec<String> {
    GLOBAL_SOURCE_REGISTRY.get_or_init().names()
}

/// Get a reference to the global source registry.
///
/// This is provided for advanced use cases where direct registry access is needed.
pub fn global_registry() -> &'static roboflow_core::FactoryRegistry<SourceFactory> {
    GLOBAL_SOURCE_REGISTRY.get_or_init()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::{SourceMetadata, TimestampedMessage};
    use async_trait::async_trait;

    // Mock source for testing
    struct MockSource;

    #[async_trait]
    impl Source for MockSource {
        async fn initialize(&mut self, _config: &SourceConfig) -> SourceResult<SourceMetadata> {
            Ok(SourceMetadata::new("mock".to_string(), "test".to_string()))
        }

        async fn read_batch(
            &mut self,
            _size: usize,
        ) -> SourceResult<Option<Vec<TimestampedMessage>>> {
            Ok(None)
        }

        async fn metadata(&self) -> SourceResult<SourceMetadata> {
            Ok(SourceMetadata::new("mock".to_string(), "test".to_string()))
        }

        fn supports_seeking(&self) -> bool {
            false
        }
    }

    #[test]
    fn test_has_source() {
        // Create a new registry scope for testing
        let registry = roboflow_core::FactoryRegistry::new();
        registry.register(
            "mock",
            Box::new(|| Box::new(MockSource) as Box<dyn Source>) as SourceFactory,
        );

        assert!(registry.contains("mock"));
        assert!(!registry.contains("other"));
    }

    #[test]
    fn test_registered_sources() {
        let registry = roboflow_core::FactoryRegistry::new();
        registry.register(
            "mock",
            Box::new(|| Box::new(MockSource) as Box<dyn Source>) as SourceFactory,
        );

        let sources = registry.names();
        assert_eq!(sources, vec!["mock".to_string()]);
    }

    #[test]
    fn test_create_source_error() {
        let registry = roboflow_core::FactoryRegistry::new();
        registry.register(
            "mock",
            Box::new(|| Box::new(MockSource) as Box<dyn Source>) as SourceFactory,
        );

        let config = SourceConfig::mcap("test.mcap");
        // Try to get a non-registered source
        assert!(registry.get_ref(config.source_type.name()).is_none());
    }

    #[test]
    fn test_builtin_source_registration() {
        // After calling register_builtin_sources, all builtin types should be registered
        crate::sources::register_builtin_sources();

        // Verify all builtin sources are registered
        assert!(has_source("bag"), "bag source should be registered");
        assert!(has_source("mcap"), "mcap source should be registered");
        assert!(has_source("rrd"), "rrd source should be registered");

        // Verify we can create configs for each type
        let bag_config = SourceConfig::bag("test.bag");
        let mcap_config = SourceConfig::mcap("test.mcap");
        let rrd_config = SourceConfig::rrd("test.rrd");

        // Creating sources should work (they will fail during initialize since paths are fake)
        let bag_source = create_source(&bag_config);
        assert!(bag_source.is_ok(), "bag source creation should succeed");

        let mcap_source = create_source(&mcap_config);
        assert!(mcap_source.is_ok(), "mcap source creation should succeed");

        let rrd_source = create_source(&rrd_config);
        assert!(rrd_source.is_ok(), "rrd source creation should succeed");
    }
}
