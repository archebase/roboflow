// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Core registry traits for type and schema management.
//!
//! Defines the abstractions that all codec crates use for schema registration
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
        let mut inner = self
            .inner
            .write()
            .map_err(|e| super::error::CodecError::Other(format!("Registry lock poisoned: {e}")))?;
        inner.schemas.insert(name.into(), schema);
        Ok(())
    }

    /// Get a schema by name.
    pub fn get(&self, name: &str) -> Result<Option<T>>
    where
        T: Clone,
    {
        let inner = self
            .inner
            .read()
            .map_err(|e| super::error::CodecError::Other(format!("Registry lock poisoned: {e}")))?;
        Ok(inner.schemas.get(name).cloned())
    }

    /// Check if a schema is registered.
    pub fn contains(&self, name: &str) -> Result<bool> {
        let inner = self
            .inner
            .read()
            .map_err(|e| super::error::CodecError::Other(format!("Registry lock poisoned: {e}")))?;
        Ok(inner.schemas.contains_key(name))
    }

    /// Get all registered schema names.
    pub fn names(&self) -> Result<Vec<String>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| super::error::CodecError::Other(format!("Registry lock poisoned: {e}")))?;
        Ok(inner.schemas.keys().cloned().collect())
    }

    /// Remove a schema from the registry.
    pub fn remove(&self, name: &str) -> Result<bool> {
        let mut inner = self
            .inner
            .write()
            .map_err(|e| super::error::CodecError::Other(format!("Registry lock poisoned: {e}")))?;
        Ok(inner.schemas.remove(name).is_some())
    }

    /// Clear all schemas from the registry.
    pub fn clear(&self) -> Result<()> {
        let mut inner = self
            .inner
            .write()
            .map_err(|e| super::error::CodecError::Other(format!("Registry lock poisoned: {e}")))?;
        inner.schemas.clear();
        Ok(())
    }

    /// Get the number of registered schemas.
    pub fn len(&self) -> Result<usize> {
        let inner = self
            .inner
            .read()
            .map_err(|e| super::error::CodecError::Other(format!("Registry lock poisoned: {e}")))?;
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
}
