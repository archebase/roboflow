// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Core registry traits for type and schema management.
//!
//! Defines the abstractions that all roboflow crates use for schema registration
//! and lookup.

use super::error::Result;
use std::collections::HashMap;
use std::sync::RwLock;

/// Trait for types that can provide schema information.
///
/// Implementations can parse schemas from various formats (IDL, .proto, etc.)
/// and provide type descriptors for decoding.
pub trait SchemaProvider {
    /// Type of schema this provider produces.
    type Schema;

    /// Parse a schema from a string.
    fn parse_schema(&self, name: &str, definition: &str) -> Result<Self::Schema>;
}

/// Trait for accessing type definitions from a schema.
pub trait TypeAccessor {
    /// The type descriptor this accessor provides.
    type TypeDescriptor;

    /// Look up a type by name.
    fn get_type(&self, type_name: &str) -> Option<&Self::TypeDescriptor>;

    /// Look up a type by name with variant resolution.
    ///
    /// Tries multiple resolution strategies:
    /// - Exact match
    /// - With /msg/ suffix (e.g., "std_msgs/Header" → "std_msgs/msg/Header")
    /// - Without /msg/ suffix (e.g., "std_msgs/msg/Header" → "std_msgs/Header")
    /// - Short name match (e.g., "Pose" → "geometry_msgs/Pose")
    fn get_type_variants(&self, type_name: &str) -> Option<&Self::TypeDescriptor>;
}

/// Thread-safe registry for parsed schemas and type descriptors.
///
/// Uses RwLock for concurrent read access with exclusive write access.
/// Suitable for use across multiple decoder instances.
pub struct TypeRegistry<T> {
    inner: RwLock<TypeRegistryInner<T>>,
}

struct TypeRegistryInner<T> {
    schemas: HashMap<String, T>,
}

impl<T> TypeRegistry<T> {
    /// Create a new empty type registry.
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(TypeRegistryInner {
                schemas: HashMap::new(),
            }),
        }
    }

    /// Register a schema with this registry.
    pub fn register(&self, name: impl Into<String>, schema: T) -> Result<()> {
        let mut inner = self.inner.write().map_err(|e| {
            super::error::RoboflowError::Other(format!("Registry lock poisoned: {e}"))
        })?;
        inner.schemas.insert(name.into(), schema);
        Ok(())
    }

    /// Get a schema by name.
    pub fn get(&self, name: &str) -> Result<Option<T>>
    where
        T: Clone,
    {
        let inner = self.inner.read().map_err(|e| {
            super::error::RoboflowError::Other(format!("Registry lock poisoned: {e}"))
        })?;
        Ok(inner.schemas.get(name).cloned())
    }

    /// Check if a schema is registered.
    pub fn contains(&self, name: &str) -> Result<bool> {
        let inner = self.inner.read().map_err(|e| {
            super::error::RoboflowError::Other(format!("Registry lock poisoned: {e}"))
        })?;
        Ok(inner.schemas.contains_key(name))
    }

    /// Get all registered schema names.
    pub fn names(&self) -> Result<Vec<String>> {
        let inner = self.inner.read().map_err(|e| {
            super::error::RoboflowError::Other(format!("Registry lock poisoned: {e}"))
        })?;
        Ok(inner.schemas.keys().cloned().collect())
    }

    /// Remove a schema from the registry.
    pub fn remove(&self, name: &str) -> Result<bool> {
        let mut inner = self.inner.write().map_err(|e| {
            super::error::RoboflowError::Other(format!("Registry lock poisoned: {e}"))
        })?;
        Ok(inner.schemas.remove(name).is_some())
    }

    /// Clear all schemas from the registry.
    pub fn clear(&self) -> Result<()> {
        let mut inner = self.inner.write().map_err(|e| {
            super::error::RoboflowError::Other(format!("Registry lock poisoned: {e}"))
        })?;
        inner.schemas.clear();
        Ok(())
    }

    /// Get the number of registered schemas.
    pub fn len(&self) -> Result<usize> {
        let inner = self.inner.read().map_err(|e| {
            super::error::RoboflowError::Other(format!("Registry lock poisoned: {e}"))
        })?;
        Ok(inner.schemas.len())
    }

    /// Check if the registry is empty.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }
}

impl<T> Default for TypeRegistry<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Encoding format identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Encoding {
    /// CDR (Common Data Representation) - used by ROS1/ROS2
    Cdr,
    /// Protobuf binary format
    Protobuf,
    /// JSON text format
    Json,
}

impl std::str::FromStr for Encoding {
    type Err = ();

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "cdr" | "ros1" | "ros2" => Ok(Encoding::Cdr),
            "protobuf" | "proto" | "pb" => Ok(Encoding::Protobuf),
            "json" => Ok(Encoding::Json),
            _ => Err(()),
        }
    }
}

impl std::fmt::Display for Encoding {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Encoding::Cdr => write!(f, "cdr"),
            Encoding::Protobuf => write!(f, "protobuf"),
            Encoding::Json => write!(f, "json"),
        }
    }
}

// =============================================================================
// Factory Registry (Generic Pattern for Source/Sink/Codec Registries)
// =============================================================================

/// Generic factory registry for plugin-style component registration.
///
/// This provides a common pattern for registries that:
/// - Store factory functions keyed by name
/// - Support thread-safe registration and lookup
/// - Use RwLock for concurrent read access
///
/// # Example
///
/// ```
/// use roboflow_core::FactoryRegistry;
///
/// let registry = FactoryRegistry::<Box<dyn Fn() -> String>>::new();
/// registry.register("greet", Box::new(|| "Hello".to_string()));
///
/// if let Some(factory) = registry.get("greet") {
///     assert_eq!(factory(), "Hello");
/// }
/// ```
pub struct FactoryRegistry<V> {
    inner: RwLock<std::collections::HashMap<String, V>>,
}

impl<V> FactoryRegistry<V> {
    /// Create a new empty factory registry.
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(std::collections::HashMap::new()),
        }
    }

    /// Register a factory with this registry.
    ///
    /// If a factory with the same name already exists, it will be replaced.
    pub fn register(&self, name: impl Into<String>, factory: V) {
        let mut inner = self.inner.write().unwrap();
        inner.insert(name.into(), factory);
    }

    /// Get a factory by name.
    ///
    /// Returns a cloned reference to the factory if found.
    pub fn get(&self, name: &str) -> Option<V>
    where
        V: Clone,
    {
        let inner = self.inner.read().unwrap();
        inner.get(name).cloned()
    }

    /// Get a reference to a factory by name (without cloning).
    ///
    /// Returns a reference that is valid while the guard is held.
    pub fn get_ref(&self, name: &str) -> Option<FactoryGuard<'_, V>> {
        let guard = self.inner.read().unwrap();
        // Safety: The guard keeps the read lock alive
        let value_ptr = guard.get(name)? as *const V;
        Some(FactoryGuard {
            _guard: guard,
            value_ptr,
        })
    }

    /// Check if a factory is registered.
    pub fn contains(&self, name: &str) -> bool {
        let inner = self.inner.read().unwrap();
        inner.contains_key(name)
    }

    /// Get all registered factory names.
    pub fn names(&self) -> Vec<String> {
        let inner = self.inner.read().unwrap();
        inner.keys().cloned().collect()
    }

    /// Remove a factory from the registry.
    ///
    /// Returns true if the factory was removed.
    pub fn remove(&self, name: &str) -> bool {
        let mut inner = self.inner.write().unwrap();
        inner.remove(name).is_some()
    }

    /// Get the number of registered factories.
    pub fn len(&self) -> usize {
        let inner = self.inner.read().unwrap();
        inner.len()
    }

    /// Check if the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clear all factories from the registry.
    pub fn clear(&self) {
        let mut inner = self.inner.write().unwrap();
        inner.clear();
    }
}

impl<V> Default for FactoryRegistry<V> {
    fn default() -> Self {
        Self::new()
    }
}

/// Guard holding a reference to a factory value.
///
/// This keeps the read lock alive while the reference is in use.
pub struct FactoryGuard<'a, V> {
    _guard: std::sync::RwLockReadGuard<'a, std::collections::HashMap<String, V>>,
    value_ptr: *const V,
}

impl<V> std::ops::Deref for FactoryGuard<'_, V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        // Safety: value_ptr is valid as long as _guard is alive
        unsafe { &*self.value_ptr }
    }
}

/// Global singleton wrapper for factory registries.
///
/// Provides lazy initialization of a global registry using OnceLock.
///
/// # Example
///
/// ```
/// use roboflow_core::GlobalFactoryRegistry;
///
/// static REGISTRY: GlobalFactoryRegistry<Box<dyn Fn() -> i32>> =
///     GlobalFactoryRegistry::new();
///
/// // Lazily initializes on first access
/// REGISTRY.get_or_init().register("answer", Box::new(|| 42));
///
/// let factory = REGISTRY.get_or_init().get("answer");
/// assert_eq!(factory.map(|f| f()), Some(42));
/// ```
pub struct GlobalFactoryRegistry<V> {
    inner: std::sync::OnceLock<FactoryRegistry<V>>,
}

impl<V> GlobalFactoryRegistry<V> {
    /// Create a new global registry placeholder.
    ///
    /// The actual registry is lazily initialized on first access.
    pub const fn new() -> Self {
        Self {
            inner: std::sync::OnceLock::new(),
        }
    }

    /// Get or initialize the global registry.
    pub fn get_or_init(&self) -> &FactoryRegistry<V> {
        self.inner.get_or_init(FactoryRegistry::new)
    }

    /// Check if the registry has been initialized.
    pub fn is_initialized(&self) -> bool {
        self.inner.get().is_some()
    }
}

impl<V> Default for GlobalFactoryRegistry<V> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_registry() {
        let registry = TypeRegistry::new();

        assert!(registry.register("test", 42).is_ok());
        assert_eq!(registry.get("test").unwrap(), Some(42));
        assert!(registry.contains("test").unwrap());
        assert_eq!(registry.len().unwrap(), 1);
        assert!(!registry.is_empty().unwrap());

        assert!(registry.remove("test").unwrap());
        assert!(!registry.contains("test").unwrap());
        assert!(registry.is_empty().unwrap());
    }

    #[test]
    fn test_encoding_from_str() {
        assert_eq!("cdr".parse::<Encoding>(), Ok(Encoding::Cdr));
        assert_eq!("CDR".parse::<Encoding>(), Ok(Encoding::Cdr));
        assert_eq!("protobuf".parse::<Encoding>(), Ok(Encoding::Protobuf));
        assert_eq!("json".parse::<Encoding>(), Ok(Encoding::Json));
        assert!("unknown".parse::<Encoding>().is_err());
    }

    #[test]
    fn test_factory_registry_basic() {
        let registry = FactoryRegistry::<i32>::new();

        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);

        registry.register("one", 1);
        registry.register("two", 2);

        assert!(!registry.is_empty());
        assert_eq!(registry.len(), 2);
        assert!(registry.contains("one"));
        assert!(registry.contains("two"));
        assert!(!registry.contains("three"));

        assert_eq!(registry.get("one"), Some(1));
        assert_eq!(registry.get("two"), Some(2));
        assert_eq!(registry.get("three"), None);
    }

    #[test]
    fn test_factory_registry_replace() {
        let registry = FactoryRegistry::<String>::new();

        registry.register("key", "first".to_string());
        assert_eq!(registry.get("key"), Some("first".to_string()));

        registry.register("key", "second".to_string());
        assert_eq!(registry.get("key"), Some("second".to_string()));
    }

    #[test]
    fn test_factory_registry_remove() {
        let registry = FactoryRegistry::<i32>::new();

        registry.register("one", 1);
        registry.register("two", 2);

        assert!(registry.remove("one"));
        assert!(!registry.contains("one"));
        assert!(registry.contains("two"));

        assert!(!registry.remove("nonexistent"));
    }

    #[test]
    fn test_factory_registry_names() {
        let registry = FactoryRegistry::<i32>::new();

        assert!(registry.names().is_empty());

        registry.register("alpha", 1);
        registry.register("beta", 2);
        registry.register("gamma", 3);

        let mut names = registry.names();
        names.sort();
        assert_eq!(names, vec!["alpha", "beta", "gamma"]);
    }

    #[test]
    fn test_factory_registry_clear() {
        let registry = FactoryRegistry::<i32>::new();

        registry.register("one", 1);
        registry.register("two", 2);

        registry.clear();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn test_factory_registry_get_ref() {
        let registry = FactoryRegistry::<String>::new();
        registry.register("greeting", "Hello".to_string());

        let guard = registry.get_ref("greeting").expect("should exist");
        assert_eq!(&*guard, "Hello");
    }

    #[test]
    fn test_global_factory_registry() {
        static REGISTRY: GlobalFactoryRegistry<i32> = GlobalFactoryRegistry::new();

        assert!(!REGISTRY.is_initialized());

        REGISTRY.get_or_init().register("answer", 42);
        assert!(REGISTRY.is_initialized());

        assert_eq!(REGISTRY.get_or_init().get("answer"), Some(42));
        assert!(REGISTRY.get_or_init().contains("answer"));
    }
}
