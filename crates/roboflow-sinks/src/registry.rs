// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Sink registry for creating sinks from configuration.

use crate::{Sink, SinkConfig, SinkError, SinkFactory, error::SinkResult};
use roboflow_core::GlobalFactoryRegistry;

/// Global registry of sink factories.
///
/// Sinks register themselves at startup, and the registry creates
/// instances on demand from configuration.
static GLOBAL_SINK_REGISTRY: GlobalFactoryRegistry<SinkFactory> = GlobalFactoryRegistry::new();

/// Register a sink type with the global registry.
///
/// # Arguments
///
/// * `name` - Name of the sink type (e.g., "lerobot", "kps")
/// * `factory` - Function that creates new sink instances
pub fn register_sink(name: impl Into<String>, factory: SinkFactory) {
    GLOBAL_SINK_REGISTRY.get_or_init().register(name, factory);
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
    let sink_type = config.sink_type.name();

    let registry = GLOBAL_SINK_REGISTRY.get_or_init();
    let factory = registry
        .get_ref(sink_type)
        .ok_or_else(|| SinkError::UnsupportedFormat(sink_type.to_string()))?;

    Ok(factory())
}

/// Check if a sink type is registered.
pub fn has_sink(name: &str) -> bool {
    GLOBAL_SINK_REGISTRY.get_or_init().contains(name)
}

/// Get all registered sink names.
pub fn registered_sinks() -> Vec<String> {
    GLOBAL_SINK_REGISTRY.get_or_init().names()
}

/// Get a reference to the global sink registry.
///
/// This is provided for advanced use cases where direct registry access is needed.
pub fn global_registry() -> &'static roboflow_core::FactoryRegistry<SinkFactory> {
    GLOBAL_SINK_REGISTRY.get_or_init()
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
    fn test_has_sink() {
        // Create a new registry scope for testing
        let registry = roboflow_core::FactoryRegistry::new();
        registry.register(
            "mock",
            Box::new(|| Box::new(MockSink) as Box<dyn Sink>) as SinkFactory,
        );

        assert!(registry.contains("mock"));
        assert!(!registry.contains("other"));
    }

    #[test]
    fn test_registered_sinks() {
        let registry = roboflow_core::FactoryRegistry::new();
        registry.register(
            "mock",
            Box::new(|| Box::new(MockSink) as Box<dyn Sink>) as SinkFactory,
        );

        let sinks = registry.names();
        assert_eq!(sinks, vec!["mock".to_string()]);
    }

    #[test]
    fn test_create_sink_error() {
        let registry = roboflow_core::FactoryRegistry::new();
        registry.register(
            "mock",
            Box::new(|| Box::new(MockSink) as Box<dyn Sink>) as SinkFactory,
        );

        let config = SinkConfig::lerobot("/output");
        // Try to get a non-registered sink
        assert!(registry.get_ref(config.sink_type.name()).is_none());
    }
}
