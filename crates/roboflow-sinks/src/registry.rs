// Sink registry for creating sinks from configuration

use crate::{Sink, SinkConfig, SinkError, SinkFactory, error::SinkResult};
use std::sync::RwLock;

/// Global registry of sink factories.
///
/// Sinks register themselves at startup, and the registry creates
/// instances on demand from configuration.
pub struct SinkRegistry {
    factories: RwLock<std::collections::HashMap<String, SinkFactory>>,
}

impl SinkRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            factories: RwLock::new(std::collections::HashMap::new()),
        }
    }

    /// Register a sink factory.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the sink type (e.g., "lerobot", "kps")
    /// * `factory` - Function that creates new sink instances
    pub fn register(&self, name: impl Into<String>, factory: SinkFactory) {
        let mut factories = self.factories.write().unwrap();
        factories.insert(name.into(), factory);
    }

    /// Create a sink from configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - Sink configuration
    ///
    /// # Returns
    ///
    /// A boxed sink instance
    pub fn create(&self, config: &SinkConfig) -> SinkResult<Box<dyn Sink>> {
        let factories = self.factories.read().unwrap();
        let sink_type = config.sink_type.name();

        let factory = factories
            .get(sink_type)
            .ok_or_else(|| SinkError::UnsupportedFormat(sink_type.to_string()))?;

        Ok(factory())
    }

    /// Check if a sink type is registered.
    pub fn has_sink(&self, name: &str) -> bool {
        let factories = self.factories.read().unwrap();
        factories.contains_key(name)
    }

    /// Get all registered sink names.
    pub fn registered_sinks(&self) -> Vec<String> {
        let factories = self.factories.read().unwrap();
        factories.keys().cloned().collect()
    }
}

impl Default for SinkRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Global sink registry instance.
static GLOBAL_REGISTRY: std::sync::OnceLock<SinkRegistry> = std::sync::OnceLock::new();

/// Get the global sink registry.
pub fn global_registry() -> &'static SinkRegistry {
    GLOBAL_REGISTRY.get_or_init(SinkRegistry::new)
}

/// Create a sink from configuration using the global registry.
///
/// This is a convenience function that uses the global registry.
///
/// # Arguments
///
/// * `config` - Sink configuration
///
/// # Returns
///
/// A boxed sink instance
pub fn create_sink(config: &SinkConfig) -> SinkResult<Box<dyn Sink>> {
    global_registry().create(config)
}

/// Register a sink type with the global registry.
///
/// # Arguments
///
/// * `name` - Name of the sink type
/// * `factory` - Function that creates new sink instances
pub fn register_sink(name: impl Into<String>, factory: SinkFactory) {
    global_registry().register(name, factory);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DatasetFrame, SinkCheckpoint, SinkStats};
    use async_trait::async_trait;

    // Mock sink for testing
    struct MockSink;

    #[async_trait]
    impl Sink for MockSink {
        async fn initialize(&mut self, _config: &SinkConfig) -> SinkResult<()> {
            Ok(())
        }

        async fn write_frame(&mut self, _frame: DatasetFrame) -> SinkResult<()> {
            Ok(())
        }

        async fn flush(&mut self) -> SinkResult<()> {
            Ok(())
        }

        async fn finalize(&mut self) -> SinkResult<SinkStats> {
            Ok(SinkStats::new())
        }

        async fn checkpoint(&self) -> SinkResult<SinkCheckpoint> {
            Ok(SinkCheckpoint::new(0, 0))
        }

        fn supports_checkpointing(&self) -> bool {
            false
        }
    }

    #[test]
    fn test_registry() {
        let registry = SinkRegistry::new();

        // Register a mock sink
        registry.register("mock", Box::new(|| Box::new(MockSink) as Box<dyn Sink>));

        assert!(registry.has_sink("mock"));
        assert!(!registry.has_sink("other"));

        let sinks = registry.registered_sinks();
        assert_eq!(sinks, vec!["mock".to_string()]);
    }

    #[test]
    fn test_create_sink() {
        let registry = SinkRegistry::new();

        registry.register("mock", Box::new(|| Box::new(MockSink) as Box<dyn Sink>));

        let config = SinkConfig::lerobot("/output");
        // Try to create a non-registered sink
        assert!(registry.create(&config).is_err());
    }
}
