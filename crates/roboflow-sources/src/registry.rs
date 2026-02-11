// Source registry for creating sources from configuration

use crate::{Source, SourceConfig, SourceError, SourceFactory, error::SourceResult};
use std::sync::RwLock;

/// Global registry of source factories.
///
/// Sources register themselves at startup, and the registry creates
/// instances on demand from configuration.
pub struct SourceRegistry {
    factories: RwLock<std::collections::HashMap<String, SourceFactory>>,
}

impl SourceRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            factories: RwLock::new(std::collections::HashMap::new()),
        }
    }

    /// Register a source factory.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the source type (e.g., "mcap", "bag")
    /// * `factory` - Function that creates new source instances
    pub fn register(&self, name: impl Into<String>, factory: SourceFactory) {
        let mut factories = self.factories.write().unwrap();
        factories.insert(name.into(), factory);
    }

    /// Create a source from configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - Source configuration
    ///
    /// # Returns
    ///
    /// A boxed source instance
    pub fn create(&self, config: &SourceConfig) -> SourceResult<Box<dyn Source>> {
        let factories = self.factories.read().unwrap();
        let source_type = config.source_type.name();

        let factory = factories
            .get(source_type)
            .ok_or_else(|| SourceError::UnsupportedFormat(source_type.to_string()))?;

        Ok(factory())
    }

    /// Check if a source type is registered.
    pub fn has_source(&self, name: &str) -> bool {
        let factories = self.factories.read().unwrap();
        factories.contains_key(name)
    }

    /// Get all registered source names.
    pub fn registered_sources(&self) -> Vec<String> {
        let factories = self.factories.read().unwrap();
        factories.keys().cloned().collect()
    }
}

impl Default for SourceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Global source registry instance.
static GLOBAL_REGISTRY: std::sync::OnceLock<SourceRegistry> = std::sync::OnceLock::new();

/// Get the global source registry.
pub fn global_registry() -> &'static SourceRegistry {
    GLOBAL_REGISTRY.get_or_init(SourceRegistry::new)
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
    global_registry().create(config)
}

/// Register a source type with the global registry.
///
/// # Arguments
///
/// * `name` - Name of the source type
/// * `factory` - Function that creates new source instances
pub fn register_source(name: impl Into<String>, factory: SourceFactory) {
    global_registry().register(name, factory);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SourceMetadata, TimestampedMessage};
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
    fn test_registry() {
        let registry = SourceRegistry::new();

        // Register a mock source
        registry.register("mock", Box::new(|| Box::new(MockSource) as Box<dyn Source>));

        assert!(registry.has_source("mock"));
        assert!(!registry.has_source("other"));

        let sources = registry.registered_sources();
        assert_eq!(sources, vec!["mock".to_string()]);
    }

    #[test]
    fn test_create_source() {
        let registry = SourceRegistry::new();

        registry.register("mock", Box::new(|| Box::new(MockSource) as Box<dyn Source>));

        let config = SourceConfig::mcap("test.mcap");
        // Try to create a non-registered source
        assert!(registry.create(&config).is_err());
    }

    #[test]
    fn test_builtin_source_registration() {
        // After calling register_builtin_sources, all builtin types should be registered
        crate::register_builtin_sources();

        let registry = global_registry();

        // Verify all builtin sources are registered
        assert!(
            registry.has_source("bag"),
            "bag source should be registered"
        );
        assert!(
            registry.has_source("mcap"),
            "mcap source should be registered"
        );
        assert!(
            registry.has_source("rrd"),
            "rrd source should be registered"
        );

        // Verify we can create configs for each type
        let bag_config = SourceConfig::bag("test.bag");
        let mcap_config = SourceConfig::mcap("test.mcap");
        let rrd_config = SourceConfig::rrd("test.rrd");

        // Creating sources should work (they will fail during initialize since paths are fake)
        let bag_source = registry.create(&bag_config);
        assert!(bag_source.is_ok(), "bag source creation should succeed");

        let mcap_source = registry.create(&mcap_config);
        assert!(mcap_source.is_ok(), "mcap source creation should succeed");

        let rrd_source = registry.create(&rrd_config);
        assert!(rrd_source.is_ok(), "rrd source creation should succeed");
    }
}
