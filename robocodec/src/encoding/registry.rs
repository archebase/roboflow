// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Codec registry for plugin-based codec selection.
//!
//! This module provides a registry pattern for codecs, allowing:
//! - Dynamic codec registration
//! - Plugin-based extensibility
//! - Centralized codec management
//!
//! # Example
//!
//! ```no_run
//! use robocodec::encoding::{CodecRegistry, CdrCodecFactory};
//!
//! let mut registry = CodecRegistry::default();
//! registry.register("cdr", Box::new(CdrCodecFactory));
//! let codec = registry.get_codec("cdr").unwrap();
//! ```

use std::collections::HashMap;
use std::sync::RwLock;

use crate::core::{CodecError, Result};

/// Factory for creating codec instances.
pub trait CodecProviderFactory: Send + Sync {
    /// Create a new codec instance.
    fn create(&self) -> Box<dyn Codec>;
}

/// Codec trait for encoding/decoding operations.
pub trait Codec: Send + Sync {
    /// Get the encoding name (e.g., "cdr", "protobuf", "json").
    fn encoding(&self) -> &str;
}

/// Registry for codec factories.
///
/// This registry allows dynamic registration of codecs and provides
/// a centralized way to create codec instances by encoding name.
#[derive(Default)]
pub struct CodecRegistry {
    // Use RwLock for thread-safe access
    factories: RwLock<HashMap<String, Box<dyn CodecProviderFactory>>>,
}

impl CodecRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a codec factory for an encoding.
    ///
    /// # Arguments
    ///
    /// * `encoding` - Encoding name (e.g., "cdr", "protobuf", "json")
    /// * `factory` - Factory for creating codec instances
    ///
    /// # Example
    ///
    /// ```
    /// # use robocodec::encoding::{CodecRegistry, CodecProviderFactory};
    ///
    /// let mut registry = CodecRegistry::new();
    /// # struct MockFactory;
    /// # impl CodecProviderFactory for MockFactory {
    /// #     fn create(&self) -> Box<dyn super::Codec> { unimplemented!() }
    /// # }
    /// registry.register("cdr", Box::new(MockFactory));
    /// ```
    pub fn register(&self, encoding: impl Into<String>, factory: Box<dyn CodecProviderFactory>) {
        let mut factories = self.factories.write().unwrap();
        factories.insert(encoding.into(), factory);
    }

    /// Unregister a codec factory.
    ///
    /// # Arguments
    ///
    /// * `encoding` - Encoding name to unregister
    ///
    /// # Returns
    ///
    /// `true` if a factory was unregistered, `false` if not found
    pub fn unregister(&self, encoding: &str) -> bool {
        let mut factories = self.factories.write().unwrap();
        factories.remove(encoding).is_some()
    }

    /// Check if an encoding is registered.
    ///
    /// # Arguments
    ///
    /// * `encoding` - Encoding name to check
    ///
    /// # Returns
    ///
    /// `true` if registered, `false` otherwise
    pub fn has_encoding(&self, encoding: &str) -> bool {
        let factories = self.factories.read().unwrap();
        factories.contains_key(encoding)
    }

    /// Get a codec by encoding name.
    ///
    /// # Arguments
    ///
    /// * `encoding` - Encoding name (e.g., "cdr", "protobuf", "json")
    ///
    /// # Returns
    ///
    /// A codec instance, or error if encoding not found
    ///
    /// # Errors
    ///
    /// Returns `CodecError::UnknownCodec` if the encoding is not registered
    pub fn get_codec(&self, encoding: &str) -> Result<Box<dyn Codec>> {
        let factories = self.factories.read().unwrap();
        factories
            .get(encoding)
            .map(|factory| factory.create())
            .ok_or_else(|| CodecError::unknown_codec(encoding.to_string()))
    }

    /// Get all registered encoding names.
    ///
    /// # Returns
    ///
    /// A vector of encoding names
    pub fn registered_encodings(&self) -> Vec<String> {
        let factories = self.factories.read().unwrap();
        factories.keys().cloned().collect()
    }

    /// Get the number of registered codecs.
    pub fn count(&self) -> usize {
        let factories = self.factories.read().unwrap();
        factories.len()
    }
}

/// Global codec registry.
///
/// This is a convenience singleton for accessing the global registry.
/// For custom registries, create a `CodecRegistry` instance directly.
static GLOBAL_REGISTRY: std::sync::OnceLock<CodecRegistry> = std::sync::OnceLock::new();

fn init_global_registry() -> CodecRegistry {
    // Register built-in codecs
    // These would be registered in the module init
    // For now, this is left for future implementation

    CodecRegistry::new()
}

/// Get the global codec registry.
///
/// # Example
///
/// ```
/// # use robocodec::encoding::global_registry;
/// let codec = global_registry().get_codec("cdr")?;
/// ```
pub fn global_registry() -> &'static CodecRegistry {
    GLOBAL_REGISTRY.get_or_init(init_global_registry)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Mock codec factory for testing
    struct MockCodecFactory;

    impl CodecProviderFactory for MockCodecFactory {
        fn create(&self) -> Box<dyn Codec> {
            Box::new(MockCodec)
        }
    }

    struct MockCodec;

    impl Codec for MockCodec {
        fn encoding(&self) -> &str {
            "mock"
        }
    }

    #[test]
    fn test_register_codec() {
        let registry = CodecRegistry::new();
        registry.register("mock", Box::new(MockCodecFactory));

        assert!(registry.has_encoding("mock"));
        assert_eq!(registry.count(), 1);

        // Test that we can get the codec back
        let codec = registry.get_codec("mock");
        assert!(codec.is_ok());
        assert_eq!(codec.unwrap().encoding(), "mock");
    }

    #[test]
    fn test_unregister_codec() {
        let registry = CodecRegistry::new();
        registry.register("mock", Box::new(MockCodecFactory));
        assert!(registry.unregister("mock"));
        assert!(!registry.has_encoding("mock"));
    }

    #[test]
    fn test_get_codec() {
        let registry = CodecRegistry::new();
        registry.register("mock", Box::new(MockCodecFactory));

        let codec = registry.get_codec("mock");
        assert!(codec.is_ok());
        assert_eq!(codec.unwrap().encoding(), "mock");
    }

    #[test]
    fn test_get_unknown_codec() {
        let registry = CodecRegistry::new();
        let result = registry.get_codec("unknown");
        assert!(result.is_err());
    }

    #[test]
    fn test_registered_encodings() {
        let registry = CodecRegistry::new();
        registry.register("mock", Box::new(MockCodecFactory));
        registry.register("test", Box::new(MockCodecFactory));

        let encodings = registry.registered_encodings();
        assert_eq!(encodings.len(), 2);
        assert!(encodings.contains(&"mock".to_string()));
        assert!(encodings.contains(&"test".to_string()));
    }

    #[test]
    fn test_concurrent_access() {
        use std::thread;

        let registry = std::sync::Arc::new(CodecRegistry::new());
        registry.register("mock", Box::new(MockCodecFactory));

        // Spawn multiple threads accessing the registry
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let registry = registry.clone();
                thread::spawn(move || {
                    for _ in 0..10 {
                        let _codec = registry.get_codec("mock");
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        // Registry should still be valid
        assert!(registry.has_encoding("mock"));
    }
}
