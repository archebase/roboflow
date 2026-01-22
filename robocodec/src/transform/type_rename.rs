// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Message type renaming transformation with schema text rewriting.

use std::collections::{HashMap, HashSet};
use std::fmt;

use super::{ChannelInfo, McapTransform, TransformError};

// =============================================================================
// Namespace Rewrite Types
// =============================================================================

/// A namespace rewrite rule with wildcard support.
///
/// Represents a pattern like "genie_msgs/msg/*" -> "roboflow_msgs/msg/*"
/// and provides methods to rewrite type references in schemas.
#[derive(Debug, Clone, PartialEq, Eq)]
struct NamespaceRule {
    /// The prefix before the wildcard (e.g., "genie_msgs/msg/")
    source_prefix: String,
    /// The target prefix (e.g., "roboflow_msgs/msg/")
    target_prefix: String,
    /// Whether this rule has a wildcard suffix
    has_wildcard: bool,
}

impl NamespaceRule {
    /// Parse a wildcard pattern string into a NamespaceRule.
    ///
    /// # Examples
    /// - "foo/msg/*" -> "bar/msg/*"
    /// - "baz.msg.*" -> "pux.*"
    fn parse(pattern: &str, target: &str) -> Result<Self, String> {
        // Validate: both must end with * or neither
        let pattern_has_wildcard = pattern.ends_with('*');
        let target_has_wildcard = target.ends_with('*');

        if pattern_has_wildcard != target_has_wildcard {
            return Err(format!(
                "Invalid wildcard: pattern '{}' has {} but target '{}' has {}",
                pattern,
                if pattern_has_wildcard {
                    "wildcard"
                } else {
                    "no wildcard"
                },
                target,
                if target_has_wildcard {
                    "wildcard"
                } else {
                    "no wildcard"
                }
            ));
        }

        // Extract prefixes (remove trailing wildcard and separator)
        let source_prefix = pattern
            .trim_end_matches('*')
            .trim_end_matches('/')
            .trim_end_matches('.');
        let target_prefix = target
            .trim_end_matches('*')
            .trim_end_matches('/')
            .trim_end_matches('.');

        if source_prefix.is_empty() {
            return Err("Wildcard pattern must have a prefix before *".to_string());
        }

        Ok(Self {
            source_prefix: source_prefix.to_string(),
            target_prefix: target_prefix.to_string(),
            has_wildcard: pattern_has_wildcard,
        })
    }

    /// Check if a type name matches this rule's pattern.
    fn matches(&self, type_name: &str) -> bool {
        if self.has_wildcard {
            // Wildcard match: type name should start with source_prefix
            // Need to handle format differences (/, ::, .)
            let type_normal = type_name.replace("::", "/").replace('.', "/");
            let prefix_normal = self.source_prefix.replace("::", "/").replace('.', "/");
            type_normal.starts_with(&prefix_normal)
        } else {
            // Exact match
            type_name == self.source_prefix
        }
    }

    /// Apply this rule to a type name, returning the rewritten name.
    fn apply(&self, type_name: &str) -> String {
        if self.matches(type_name) {
            if self.has_wildcard {
                // Replace the prefix portion
                if type_name.starts_with(&self.source_prefix) {
                    format!(
                        "{}{}",
                        self.target_prefix,
                        &type_name[self.source_prefix.len()..]
                    )
                } else {
                    // Handle format differences
                    let type_normal = type_name.replace("::", "/").replace('.', "/");
                    let prefix_normal = self.source_prefix.replace("::", "/").replace('.', "/");
                    if type_normal.starts_with(&prefix_normal) {
                        // Preserve original separator format
                        let remaining = &type_normal[prefix_normal.len()..];
                        if type_name.contains("::") {
                            format!(
                                "{}::{}",
                                self.target_prefix.replace('/', "::"),
                                remaining.replace('/', "::")
                            )
                        } else if type_name.contains('.') {
                            format!(
                                "{}.{}",
                                self.target_prefix.replace('/', "."),
                                remaining.replace('/', ".")
                            )
                        } else {
                            format!("{}/{}", self.target_prefix, remaining)
                        }
                    } else {
                        type_name.to_string()
                    }
                }
            } else {
                self.target_prefix.clone()
            }
        } else {
            type_name.to_string()
        }
    }
}

/// Compiled namespace replacement strategies for different formats.
///
/// Pre-computes all the string variants needed for schema rewriting
/// across different formats (IDL ::, ROS /, Proto .).
#[derive(Debug, Clone)]
struct NamespaceRewriteStrategy {
    /// Original namespace mapping (channel format)
    channel_mapping: (String, String),
    /// IDL format mappings (e.g., "genie_msgs::msg::" -> "roboflow_msgs::msg::")
    idl_mappings: Vec<(String, String)>,
    /// Dot-notation for schemas (e.g., "genie_msgs.msg." -> "roboflow_msgs.msg.")
    dot_mappings: Vec<(String, String)>,
    /// Module declaration rewrite (e.g., "module genie_msgs {" -> "module roboflow_msgs {")
    module_mapping: Option<(String, String)>,
}

impl NamespaceRewriteStrategy {
    /// Build a rewrite strategy from a source and target prefix.
    fn build(source_prefix: &str, target_prefix: &str) -> Self {
        let mut idl_mappings = Vec::new();
        let mut dot_mappings = Vec::new();
        let mut module_mapping = None;

        // Extract base package/module name
        let source_module = extract_base_module(source_prefix);
        let target_module = extract_base_module(target_prefix);

        if !source_module.is_empty() && source_module != target_module {
            // Module declaration: "module old {" -> "module new {"
            module_mapping = Some((source_module.to_string(), target_module.to_string()));

            // IDL format: "old_pkg/msg/Type" -> "old_pkg::msg::Type"
            if source_prefix.contains('/') {
                idl_mappings.push((
                    source_prefix.replace('/', "::"),
                    target_prefix.replace('/', "::"),
                ));
            } else if source_prefix.contains('.') {
                idl_mappings.push((
                    source_prefix.replace('.', "::"),
                    target_prefix.replace('.', "::"),
                ));
            }

            // Dot notation for schemas: "old_pkg.msg.Type" -> "new_pkg.msg.Type"
            if source_prefix.contains('/') {
                dot_mappings.push((
                    source_prefix.replace('/', "."),
                    target_prefix.replace('/', "."),
                ));
            } else if source_prefix.contains('.') {
                // Already dot format
                dot_mappings.push((source_prefix.to_string(), target_prefix.to_string()));
            }
        }

        Self {
            channel_mapping: (source_prefix.to_string(), target_prefix.to_string()),
            idl_mappings,
            dot_mappings,
            module_mapping,
        }
    }

    /// Apply this strategy to schema text, replacing all occurrences.
    fn apply_to_schema(&self, schema_text: &str) -> String {
        let mut result = schema_text.to_string();

        // Apply IDL format replacements (::)
        for (old, new) in &self.idl_mappings {
            result = result.replace(old, new);
        }

        // Apply dot notation replacements (.)
        for (old, new) in &self.dot_mappings {
            result = replace_type_reference(&result, old, new);
        }

        // Apply module declaration rewrite
        if let Some((old_module, new_module)) = &self.module_mapping {
            result = rewrite_module_declarations(&result, old_module, new_module);
        }

        result
    }
}

/// Namespace rewrite engine with wildcard support.
///
/// Pre-compiles exact type mappings and wildcard patterns into efficient
/// rewrite strategies, then applies them to schema text.
#[derive(Clone)]
pub struct NamespaceRewriter {
    /// Exact type mappings for direct replacement (source -> target)
    exact_mappings: Vec<(String, String)>,
    /// Compiled strategies for exact type namespace mappings
    exact_strategies: Vec<NamespaceRewriteStrategy>,
    /// Compiled wildcard rules for namespace-level rewriting
    wildcard_rules: Vec<NamespaceRule>,
    /// Wildcard strategies for namespace replacement
    wildcard_strategies: Vec<NamespaceRewriteStrategy>,
}

impl NamespaceRewriter {
    /// Create a new rewriter from type mappings and wildcard patterns.
    fn from_mappings(
        exact_mappings: &HashMap<String, String>,
        wildcard_patterns: &[(String, String)],
    ) -> Self {
        let mut exact_mappings_vec = Vec::new();
        let mut exact_strategies = Vec::new();
        let mut wildcard_rules = Vec::new();
        let mut wildcard_strategies = Vec::new();

        // Build strategies for exact mappings
        for (source, target) in exact_mappings {
            let source_prefix = extract_namespace_prefix(source);
            let target_prefix = extract_namespace_prefix(target);
            if !source_prefix.is_empty() {
                exact_strategies.push(NamespaceRewriteStrategy::build(
                    &source_prefix,
                    &target_prefix,
                ));
            }
            // Also store the exact mapping for direct replacement
            exact_mappings_vec.push((source.clone(), target.clone()));
        }

        // Parse and build strategies for wildcard patterns
        for (pattern, target) in wildcard_patterns {
            match NamespaceRule::parse(pattern, target) {
                Ok(rule) => {
                    let source_prefix = extract_namespace_prefix(pattern);
                    let target_prefix = extract_namespace_prefix(target);
                    if !source_prefix.is_empty() && rule.has_wildcard {
                        wildcard_strategies.push(NamespaceRewriteStrategy::build(
                            &source_prefix,
                            &target_prefix,
                        ));
                    }
                    wildcard_rules.push(rule);
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to parse wildcard pattern '{}' -> '{}': {}. Skipping this pattern.",
                        pattern,
                        target,
                        e
                    );
                }
            }
        }

        // Sort by prefix length (descending) for correct precedence
        exact_strategies.sort_by_key(|s| std::cmp::Reverse(s.channel_mapping.0.len()));
        wildcard_strategies.sort_by_key(|s| std::cmp::Reverse(s.channel_mapping.0.len()));
        exact_mappings_vec.sort_by_key(|(k, _)| std::cmp::Reverse(k.len()));

        Self {
            exact_mappings: exact_mappings_vec,
            exact_strategies,
            wildcard_rules,
            wildcard_strategies,
        }
    }

    /// Rewrite schema text using all compiled rules.
    pub fn rewrite_schema(&self, schema_text: &str) -> String {
        let mut result = schema_text.to_string();

        // Apply wildcard strategies first (broad namespace replacement)
        for strategy in &self.wildcard_strategies {
            result = strategy.apply_to_schema(&result);
        }

        // Apply exact strategies (namespace-level for exact type mappings)
        for strategy in &self.exact_strategies {
            result = strategy.apply_to_schema(&result);
        }

        // Apply exact type name replacements (e.g., "foo/Msg" -> "bar/Msg")
        for (old_type, new_type) in &self.exact_mappings {
            // Direct replacement in channel format
            result = replace_type_reference(&result, old_type, new_type);

            // Also handle schema format conversions
            if old_type.contains('/') {
                // Convert to double-colon notation (IDL)
                let old_colon = old_type.replace('/', "::");
                let new_colon = new_type.replace('/', "::");
                result = replace_type_reference(&result, &old_colon, &new_colon);

                // Convert to dot notation (proto-like)
                let old_dot = old_type.replace("/msg/", ".").replace('/', ".");
                let new_dot = new_type.replace("/msg/", ".").replace('/', ".");
                result = replace_type_reference(&result, &old_dot, &new_dot);
            }
        }

        result
    }

    /// Rewrite a specific type name using the compiled rules.
    pub fn rewrite_type(&self, type_name: &str) -> String {
        // Try wildcard rules first
        for rule in &self.wildcard_rules {
            if rule.matches(type_name) {
                return rule.apply(type_name);
            }
        }

        // Try exact mappings
        for (source, target) in &self.exact_mappings {
            if type_name == source {
                return target.clone();
            }
        }

        type_name.to_string()
    }
}

impl fmt::Debug for NamespaceRewriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NamespaceRewriter")
            .field("exact_mappings", &self.exact_mappings.len())
            .field("exact_strategies", &self.exact_strategies.len())
            .field("wildcard_rules", &self.wildcard_rules.len())
            .finish()
    }
}

/// Extract the namespace prefix from a type name.
///
/// Examples:
/// - "genie_msgs/msg/ArmState" -> "genie_msgs/msg"
/// - "nmx.msg.LowdimData" -> "nmx.msg"
/// - "sensor_msgs" -> "sensor_msgs"
fn extract_namespace_prefix(type_name: &str) -> String {
    if let Some(last_slash) = type_name.rfind('/') {
        type_name[..last_slash].to_string()
    } else if let Some(last_dot) = type_name.rfind('.') {
        type_name[..last_dot].to_string()
    } else {
        String::new()
    }
}

/// Extract the base module/package name for module declarations.
///
/// Examples:
/// - "genie_msgs/msg" -> "genie_msgs"
/// - "nmx.msg" -> "nmx"
/// - "sensor_msgs" -> "sensor_msgs"
fn extract_base_module(prefix: &str) -> String {
    if let Some(first_slash) = prefix.find('/') {
        prefix[..first_slash].to_string()
    } else if let Some(first_dot) = prefix.find('.') {
        prefix[..first_dot].to_string()
    } else {
        prefix.to_string()
    }
}

/// Rewrite schema text by replacing package names.
///
/// This is a shared helper function used by both `TypeRenameTransform` and
/// `TopicAwareTypeRenameTransform` to avoid code duplication.
///
/// # Arguments
///
/// * `old_type` - Original type name (e.g., "sensor_msgs/msg/JointState")
/// * `new_type` - New type name (e.g., "my_msgs/JointState")
/// * `schema_text` - Original schema text
///
/// # Returns
///
/// Rewritten schema text with package names replaced.
fn rewrite_schema_package(old_type: &str, new_type: &str, schema_text: &str) -> String {
    // Extract package names from type names
    let old_package = extract_package(old_type);
    let new_package = extract_package(new_type);

    // No change needed if packages are the same or empty
    if old_package.is_empty() || new_package.is_empty() || old_package == new_package {
        return schema_text.to_string();
    }

    // Replace package name patterns in schema text
    let mut result = schema_text.to_string();

    // Replace "old_pkg/Type" patterns (ROS2 style)
    result = result.replace(&format!("{old_package}/"), &format!("{new_package}/"));

    // Replace "old_pkg::Type" patterns (IDL style)
    result = result.replace(&format!("{old_package}::"), &format!("{new_package}::"));

    // For proto style (dots), handle nested type conversion
    // e.g., "old_pkg.nested.Type" -> "new_pkg.nested.Type"
    result = result.replace(&format!("{old_package}."), &format!("{new_package}."));

    result
}

/// Rewrite IDL module declarations in schema text.
///
/// Replaces patterns like "module old_name {" with "module new_name {".
fn rewrite_module_declarations(text: &str, old_module: &str, new_module: &str) -> String {
    // Match "module old_module {" patterns
    let pattern = format!("module {old_module} {{");
    let replacement = format!("module {new_module} {{");

    text.replace(&pattern, &replacement)
}

/// Replace type references in schema text with word boundary handling.
///
/// This ensures we only replace whole type references, not partial matches.
/// For example, "sensor_msgs/Header" should not match inside "my_sensor_msgs/Header".
fn replace_type_reference(text: &str, old_type: &str, new_type: &str) -> String {
    // Common delimiters that surround type references in schemas
    let delimiters = [
        ' ', '\n', '\t', '\r', '<', '>', ',', '[', ']', '{', '}', '(', ')', ';',
    ];

    let mut result = String::new();
    let mut last_end = 0;

    // Find all occurrences of old_type
    let mut start = 0;
    while let Some(pos) = text[start..].find(old_type) {
        let abs_pos = start + pos;

        // Check if this is a whole type reference (word boundary before)
        let valid_before =
            abs_pos == 0 || delimiters.contains(&text.chars().nth(abs_pos - 1).unwrap_or(' '));

        // Check if this is a whole type reference (word boundary after)
        let after_pos = abs_pos + old_type.len();
        let valid_after = after_pos >= text.len()
            || delimiters.contains(&text.chars().nth(after_pos).unwrap_or(' '));

        if valid_before && valid_after {
            // Copy text before the match
            result.push_str(&text[last_end..abs_pos]);
            // Copy the replacement
            result.push_str(new_type);
            last_end = after_pos;
        }

        start = abs_pos + 1;
    }

    // Copy remaining text
    result.push_str(&text[last_end..]);
    result
}

/// Extract the package name from a type name.
///
/// Handles different formats:
/// - ROS2: "sensor_msgs/msg/JointState" -> "sensor_msgs/msg"
/// - ROS1: "sensor_msgs/JointState" -> "sensor_msgs"
/// - Proto: "nmx.msg.LowdimData" -> "nmx.msg"
fn extract_package(type_name: &str) -> String {
    if let Some(last_slash) = type_name.rfind('/') {
        // ROS style: "pkg/msg/Type" -> "pkg/msg" or "pkg/Type" -> "pkg"
        let before_slash = &type_name[..last_slash];
        if let Some(second_slash) = before_slash.rfind('/') {
            // Has /msg/ component
            type_name[..second_slash].to_string()
        } else {
            // Simple "pkg/Type" format
            before_slash.to_string()
        }
    } else if let Some(last_dot) = type_name.rfind('.') {
        // Proto style: "pkg.nested.Type" -> "pkg.nested"
        type_name[..last_dot].to_string()
    } else {
        // No package
        String::new()
    }
}

/// Detect the encoding format from a type name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeFormat {
    /// ROS2 format: "sensor_msgs/msg/JointState"
    Ros2,
    /// ROS1 format: "sensor_msgs/JointState"
    Ros1,
    /// Proto format: "nmx.msg.LowdimData"
    Proto,
    /// Unknown format
    Unknown,
}

impl TypeFormat {
    /// Detect format from a type name string.
    pub fn from_type_name(type_name: &str) -> Self {
        if type_name.contains('/') {
            if type_name.contains("/msg/") {
                TypeFormat::Ros2
            } else {
                TypeFormat::Ros1
            }
        } else if type_name.contains('.') {
            TypeFormat::Proto
        } else {
            TypeFormat::Unknown
        }
    }

    /// Get the separator for this format.
    pub fn separator(&self) -> &str {
        match self {
            TypeFormat::Ros2 => "/",
            TypeFormat::Ros1 => "/",
            TypeFormat::Proto => ".",
            TypeFormat::Unknown => ".",
        }
    }

    /// Convert a type name from this format to another format.
    pub fn convert_type_name(&self, type_name: &str, target_format: TypeFormat) -> String {
        // Extract components
        let (package, msg_part) = Self::parse_type_name(type_name);

        match target_format {
            TypeFormat::Ros2 => {
                // Convert to "package/msg/Type" format
                if package.contains('.') {
                    // Proto: "nmx.msg.Type" -> "nmx/msg/Type"
                    let pkg = package.replace('.', "/");
                    format!("{pkg}/msg/{msg_part}")
                } else if !package.is_empty() && !package.contains('/') {
                    // Simple: "sensor_msgs" -> "sensor_msgs/msg/Type"
                    format!("{package}/msg/{msg_part}")
                } else if package.contains("/msg/") {
                    // Already ROS2
                    format!("{package}/{msg_part}")
                } else {
                    format!("{package}/{msg_part}")
                }
            }
            TypeFormat::Ros1 => {
                // Convert to "package/Type" format
                if package.contains('.') {
                    // Proto: "nmx.msg.Type" -> "nmx/Type"
                    let pkg = package.replace('.', "/");
                    format!("{pkg}/{msg_part}")
                } else if !package.is_empty() {
                    format!("{package}/{msg_part}")
                } else {
                    msg_part.to_string()
                }
            }
            TypeFormat::Proto => {
                // Convert to "package.Type" format
                // For nested types, use underscore: "camid_1.intrinsic" -> "camid_1_intrinsic"
                if package.contains('/') {
                    // ROS: "sensor_msgs/msg" -> "sensor_msgs.msg"
                    let pkg = package.replace('/', ".");
                    format!("{pkg}.{msg_part}")
                } else if !package.is_empty() {
                    format!("{package}.{msg_part}")
                } else {
                    msg_part.to_string()
                }
            }
            TypeFormat::Unknown => type_name.to_string(),
        }
    }

    /// Parse a type name into (package, type_name) components.
    fn parse_type_name(type_name: &str) -> (String, String) {
        if let Some(last_slash) = type_name.rfind('/') {
            (
                type_name[..last_slash].to_string(),
                type_name[last_slash + 1..].to_string(),
            )
        } else if let Some(last_dot) = type_name.rfind('.') {
            (
                type_name[..last_dot].to_string(),
                type_name[last_dot + 1..].to_string(),
            )
        } else {
            (String::new(), type_name.to_string())
        }
    }
}

/// Message type renaming transformation with schema text rewriting.
///
/// Renames message types and updates the corresponding schema text
/// to match package name changes. Supports both exact mappings and wildcard patterns.
///
/// Schema rewriting uses wildcard namespace replacement, which recursively
/// rewrites ALL type references in the schema to ensure correctness.
///
/// # Example
///
/// ```no_run
/// use roboflow::transform::TypeRenameTransform;
///
/// # fn main() {
/// let mut rename = TypeRenameTransform::new();
/// rename.add_mapping("sensor_msgs/msg/JointState", "my_robot_msgs/JointState");
///
/// // Wildcard: rename all foo/* types to bar/*
/// rename.add_wildcard_mapping("foo/*", "bar/*");
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct TypeRenameTransform {
    /// Type mappings: source -> target
    mappings: HashMap<String, String>,
    /// Wildcard patterns: "prefix/*" -> target_prefix
    wildcard_patterns: Vec<(String, String)>,
    /// Cache for rewritten schemas (using string keys for flexibility)
    schema_cache: HashMap<String, String>,
    /// Compiled namespace rewriter for wildcard schema rewriting
    namespace_rewriter: Option<NamespaceRewriter>,
}

impl Default for TypeRenameTransform {
    fn default() -> Self {
        Self::new()
    }
}

impl TypeRenameTransform {
    /// Create a new empty type rename transform.
    pub fn new() -> Self {
        Self {
            mappings: HashMap::new(),
            wildcard_patterns: Vec::new(),
            schema_cache: HashMap::new(),
            namespace_rewriter: None,
        }
    }

    /// Create a transform from a HashMap of mappings.
    pub fn from_map(mappings: HashMap<String, String>) -> Self {
        // Pre-compile the namespace rewriter for immediate use
        let namespace_rewriter = NamespaceRewriter::from_mappings(&mappings, &[]);
        Self {
            mappings,
            wildcard_patterns: Vec::new(),
            schema_cache: HashMap::new(),
            namespace_rewriter: Some(namespace_rewriter),
        }
    }

    /// Ensure the namespace rewriter is compiled.
    ///
    /// Lazily compiles the rewriter from exact mappings and wildcard patterns.
    fn ensure_rewriter(&mut self) {
        if self.namespace_rewriter.is_none() {
            self.namespace_rewriter = Some(NamespaceRewriter::from_mappings(
                &self.mappings,
                &self.wildcard_patterns,
            ));
        }
    }

    /// Compile the namespace rewriter.
    ///
    /// Called whenever mappings change to pre-compile for immutable use.
    fn compile_rewriter(&mut self) {
        self.namespace_rewriter = Some(NamespaceRewriter::from_mappings(
            &self.mappings,
            &self.wildcard_patterns,
        ));
    }

    /// Add a type rename mapping.
    ///
    /// # Arguments
    ///
    /// * `source` - Original type name (e.g., "sensor_msgs/msg/JointState")
    /// * `target` - New type name (e.g., "custom_msgs/JointState")
    pub fn add_mapping(&mut self, source: impl Into<String>, target: impl Into<String>) {
        self.mappings.insert(source.into(), target.into());
        // Clear cache and recompile rewriter when mappings change
        self.schema_cache.clear();
        self.compile_rewriter();
    }

    /// Get the number of mappings configured.
    pub fn len(&self) -> usize {
        self.mappings.len()
    }

    /// Check if any mappings are configured.
    pub fn is_empty(&self) -> bool {
        self.mappings.is_empty()
    }

    /// Get all mappings.
    pub fn mappings(&self) -> &HashMap<String, String> {
        &self.mappings
    }

    /// Add a wildcard type rename mapping.
    ///
    /// The wildcard `*` matches any type name suffix. For example:
    /// - `"foo/*"` → `"bar/*"` will rename:
    ///   - `"foo/TypeName"` → `"bar/TypeName"`
    ///   - `"foo/OtherType"` → `"bar/OtherType"`
    ///
    /// # Arguments
    ///
    /// * `pattern` - Wildcard pattern like "foo/*"
    /// * `target` - Target pattern like "bar/*"
    pub fn add_wildcard_mapping(&mut self, pattern: impl Into<String>, target: impl Into<String>) {
        self.wildcard_patterns.push((pattern.into(), target.into()));
        // Clear cache and recompile rewriter when mappings change
        self.schema_cache.clear();
        self.compile_rewriter();
    }

    /// Try to apply a wildcard pattern to a type name.
    ///
    /// Returns `Some(new_name)` if a wildcard pattern matches, `None` otherwise.
    fn apply_wildcard_type(&self, type_name: &str) -> Option<String> {
        for (pattern, target) in &self.wildcard_patterns {
            // Both pattern and target should have the form "prefix/*"
            if let Some(stripped_pattern) = pattern.strip_suffix('*') {
                if let Some(suffix) = type_name.strip_prefix(stripped_pattern) {
                    let new_target = if let Some(stripped_target) = target.strip_suffix('*') {
                        format!("{stripped_target}{suffix}")
                    } else {
                        // Target doesn't end with *, just use it as-is
                        target.clone()
                    };
                    return Some(new_target);
                }
            }
        }
        None
    }

    /// Apply the transformation to a type name.
    ///
    /// Returns the new type name, or the original if no mapping exists.
    pub fn apply_type(&self, type_name: &str) -> String {
        // Check exact mappings first
        if let Some(target) = self.mappings.get(type_name) {
            return target.clone();
        }
        // Check wildcard patterns
        if let Some(target) = self.apply_wildcard_type(type_name) {
            return target;
        }
        type_name.to_string()
    }

    /// Rewrite schema text using the namespace rewriter.
    ///
    /// This function performs namespace replacement in the schema text,
    /// applying both wildcard patterns and exact mappings to rewrite ALL
    /// type references in the schema.
    fn rewrite_schema(&mut self, _old_type: &str, _new_type: &str, schema_text: &str) -> String {
        self.ensure_rewriter();

        // Use a hash of schema_text and all mappings as cache key
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        schema_text.hash(&mut hasher);
        for k in self.mappings.keys() {
            k.hash(&mut hasher);
        }
        for (p, _) in &self.wildcard_patterns {
            p.hash(&mut hasher);
        }
        let cache_key = format!("full:{}", hasher.finish());

        if let Some(cached) = self.schema_cache.get(&cache_key) {
            return cached.clone();
        }

        let rewriter = self.namespace_rewriter.as_ref().unwrap();
        let rewritten = rewriter.rewrite_schema(schema_text);
        self.schema_cache.insert(cache_key, rewritten.clone());
        rewritten
    }

    /// Apply transformation to both type name and schema text.
    pub fn apply(
        &mut self,
        type_name: &str,
        schema_text: Option<&str>,
    ) -> (String, Option<String>) {
        self.ensure_rewriter();

        // Check exact mappings first
        if let Some(target) = self.mappings.get(type_name).cloned() {
            let rewritten_schema = schema_text.map(|s| self.rewrite_schema(type_name, &target, s));
            (target, rewritten_schema)
        } else if let Some(target) = self.apply_wildcard_type(type_name) {
            // For wildcard, use the namespace rewriter for schema rewriting
            // Then also replace the specific type that was matched
            let rewritten_schema = schema_text.map(|s| {
                let mut result = self.rewrite_schema(type_name, &target, s);
                // Also replace the specific type that was matched
                result = replace_type_reference(&result, type_name, &target);
                // Handle schema format conversions
                if type_name.contains('/') {
                    let old_colon = type_name.replace('/', "::");
                    let new_colon = target.replace('/', "::");
                    result = replace_type_reference(&result, &old_colon, &new_colon);
                    let old_dot = type_name.replace("/msg/", ".").replace('/', ".");
                    let new_dot = target.replace("/msg/", ".").replace('/', ".");
                    result = replace_type_reference(&result, &old_dot, &new_dot);
                }
                result
            });
            (target, rewritten_schema)
        } else {
            (type_name.to_string(), schema_text.map(|s| s.to_string()))
        }
    }
}

impl McapTransform for TypeRenameTransform {
    fn transform_topic(&self, topic: &str) -> Option<String> {
        // Type transform doesn't modify topics
        Some(topic.to_string())
    }

    fn transform_type(
        &self,
        type_name: &str,
        schema_text: Option<&str>,
    ) -> (String, Option<String>) {
        // Check exact mappings first
        let mapping = self.mappings.get(type_name).cloned();

        if let Some(target) = mapping {
            // Use pre-compiled namespace rewriter for schema rewriting
            let rewritten_schema = schema_text.and_then(|s| {
                self.namespace_rewriter
                    .as_ref()
                    .map(|r| r.rewrite_schema(s))
            });
            (
                target,
                rewritten_schema.or(schema_text.map(|s| s.to_string())),
            )
        } else if let Some(target) = self.apply_wildcard_type(type_name) {
            // For wildcard patterns, use the namespace rewriter first
            let rewritten_schema = self.namespace_rewriter.as_ref().and(schema_text).map(|s| {
                let mut result = self.namespace_rewriter.as_ref().unwrap().rewrite_schema(s);
                // Also replace the specific type that was matched
                result = replace_type_reference(&result, type_name, &target);
                // Handle schema format conversions
                if type_name.contains('/') {
                    let old_colon = type_name.replace('/', "::");
                    let new_colon = target.replace('/', "::");
                    result = replace_type_reference(&result, &old_colon, &new_colon);
                    let old_dot = type_name.replace("/msg/", ".").replace('/', ".");
                    let new_dot = target.replace("/msg/", ".").replace('/', ".");
                    result = replace_type_reference(&result, &old_dot, &new_dot);
                }
                result
            });
            (
                target,
                rewritten_schema.or(schema_text.map(|s| s.to_string())),
            )
        } else {
            (type_name.to_string(), schema_text.map(|s| s.to_string()))
        }
    }

    fn validate(&self, channels: &[ChannelInfo]) -> std::result::Result<(), TransformError> {
        if self.mappings.is_empty() {
            return Ok(());
        }

        // Collect all existing types
        let existing_types: HashSet<&str> =
            channels.iter().map(|c| c.message_type.as_str()).collect();

        // Check that all source types exist
        for source in self.mappings.keys() {
            if !existing_types.contains(source.as_str()) {
                return Err(TransformError::NotFound {
                    name: source.clone(),
                    kind: "type",
                });
            }
        }

        // Build mapping from target type to source types
        let mut target_to_sources: HashMap<&str, Vec<&str>> = HashMap::new();

        for (source, target) in &self.mappings {
            target_to_sources.entry(target).or_default().push(source);
        }

        // Check for collisions where multiple sources map to the same target
        for (target, sources) in &target_to_sources {
            if sources.len() > 1 {
                // Check if schemas are identical
                use std::collections::HashSet;
                let schemas: HashSet<&String> = sources
                    .iter()
                    .filter_map(|s| {
                        channels
                            .iter()
                            .find(|c| c.message_type == *s)
                            .and_then(|c| c.schema.as_ref())
                    })
                    .collect();

                // If there's more than one unique schema, it's a collision
                if schemas.len() > 1 {
                    return Err(TransformError::TypeCollision {
                        sources: sources.iter().map(|s| s.to_string()).collect(),
                        target: target.to_string(),
                    });
                }
            }

            // Check if target conflicts with an existing type that isn't one of the sources
            if !sources.contains(target) && existing_types.contains(*target) {
                return Err(TransformError::TypeCollision {
                    sources: sources.iter().map(|s| s.to_string()).collect(),
                    target: target.to_string(),
                });
            }
        }

        Ok(())
    }

    fn modifies_types(&self) -> bool {
        !self.mappings.is_empty()
    }

    fn modifies_schemas(&self) -> bool {
        !self.mappings.is_empty()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn box_clone(&self) -> Box<dyn McapTransform> {
        Box::new(self.clone())
    }
}

/// Topic-aware type renaming transformation.
///
/// Unlike `TypeRenameTransform` which applies the same type mapping regardless of topic,
/// this transform allows different type transformations based on the channel topic.
///
/// This enables use cases like:
/// - `/lowdim/joint` with `nmx.msg.LowdimData` → `nmx.msg.JointStates`
/// - `/lowdim/tcp` with `nmx.msg.LowdimData` → `nmx.msg.eef_pose`
///
/// Schema rewriting always uses package-based replacement.
///
/// # Example
///
/// ```no_run
/// # fn main() {
/// use roboflow::transform::TopicAwareTypeRenameTransform;
///
/// let mut transform = TopicAwareTypeRenameTransform::new();
/// transform.add_mapping("/lowdim/joint", "nmx.msg.LowdimData", "nmx.msg.JointStates");
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct TopicAwareTypeRenameTransform {
    /// Topic-specific type mappings: (topic, source_type) -> target_type
    mappings: HashMap<(String, String), String>,
}

impl Default for TopicAwareTypeRenameTransform {
    fn default() -> Self {
        Self::new()
    }
}

impl TopicAwareTypeRenameTransform {
    /// Create a new empty topic-aware type rename transform.
    pub fn new() -> Self {
        Self {
            mappings: HashMap::new(),
        }
    }

    /// Add a topic-specific type rename mapping.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic pattern (exact match for now)
    /// * `source_type` - Original type name (e.g., "nmx.msg.LowdimData")
    /// * `target_type` - New type name (e.g., "nmx.msg.JointStates")
    pub fn add_mapping(
        &mut self,
        topic: impl Into<String>,
        source_type: impl Into<String>,
        target_type: impl Into<String>,
    ) {
        self.mappings
            .insert((topic.into(), source_type.into()), target_type.into());
    }

    /// Create a transform from a HashMap of mappings.
    pub fn from_map(mappings: HashMap<(String, String), String>) -> Self {
        Self { mappings }
    }

    /// Get the number of mappings configured.
    pub fn len(&self) -> usize {
        self.mappings.len()
    }

    /// Check if any mappings are configured.
    pub fn is_empty(&self) -> bool {
        self.mappings.is_empty()
    }

    /// Get all mappings.
    pub fn mappings(&self) -> &HashMap<(String, String), String> {
        &self.mappings
    }

    /// Apply the transformation for a specific topic and type.
    ///
    /// Returns the new type name, or the original if no mapping exists for this (topic, type) pair.
    pub fn apply_for_topic(&self, topic: &str, type_name: &str) -> String {
        if let Some(target) = self
            .mappings
            .get(&(topic.to_string(), type_name.to_string()))
        {
            return target.clone();
        }
        type_name.to_string()
    }

    /// Apply transformation for a specific topic, type, and schema.
    ///
    /// This method can be called from both mutable and immutable references.
    pub fn apply_for_topic_with_schema(
        &self,
        topic: &str,
        type_name: &str,
        schema_text: Option<&str>,
    ) -> (String, Option<String>) {
        if let Some(target) = self
            .mappings
            .get(&(topic.to_string(), type_name.to_string()))
        {
            let rewritten_schema =
                schema_text.map(|s| rewrite_schema_package(type_name, target, s));
            (target.clone(), rewritten_schema)
        } else {
            (type_name.to_string(), schema_text.map(|s| s.to_string()))
        }
    }

    /// Check if there's a mapping for a given source type across any topic.
    ///
    /// This is used to detect conflicts with global type mappings.
    pub fn has_mapping_for_type(&self, type_name: &str) -> bool {
        self.mappings.keys().any(|(_, source)| source == type_name)
    }

    /// Get all topics that have a mapping for the given source type.
    pub fn topics_for_type(&self, type_name: &str) -> Vec<&str> {
        self.mappings
            .keys()
            .filter_map(|(topic, source)| {
                if source == type_name {
                    Some(topic.as_str())
                } else {
                    None
                }
            })
            .collect()
    }
}

impl McapTransform for TopicAwareTypeRenameTransform {
    fn transform_topic(&self, topic: &str) -> Option<String> {
        // Topic transform doesn't modify topics
        Some(topic.to_string())
    }

    fn transform_type(
        &self,
        type_name: &str,
        schema_text: Option<&str>,
    ) -> (String, Option<String>) {
        // Without topic context, we can't apply topic-specific mappings
        // Return original - the topic-aware version should be used instead
        (type_name.to_string(), schema_text.map(|s| s.to_string()))
    }

    fn validate(&self, channels: &[ChannelInfo]) -> std::result::Result<(), TransformError> {
        if self.mappings.is_empty() {
            return Ok(());
        }

        // Collect existing (topic, type) pairs
        let existing_pairs: HashSet<(&str, &str)> = channels
            .iter()
            .map(|c| (c.topic.as_str(), c.message_type.as_str()))
            .collect();

        // Check that all (topic, source_type) pairs exist
        for (topic, source_type) in self.mappings.keys() {
            if !existing_pairs.contains(&(topic.as_str(), source_type.as_str())) {
                return Err(TransformError::NotFound {
                    name: format!("{source_type}@{topic}"),
                    kind: "topic-type pair",
                });
            }
        }

        Ok(())
    }

    fn modifies_types(&self) -> bool {
        !self.mappings.is_empty()
    }

    fn modifies_schemas(&self) -> bool {
        !self.mappings.is_empty()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn box_clone(&self) -> Box<dyn McapTransform> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_channel(id: u16, msg_type: &str, schema: Option<&str>) -> ChannelInfo {
        ChannelInfo::new(
            id,
            "/test".to_string(),
            msg_type.to_string(),
            "cdr".to_string(),
            schema.map(|s| s.to_string()),
            Some("ros2msg".to_string()),
        )
    }

    #[test]
    fn test_new() {
        let transform = TypeRenameTransform::new();
        assert!(transform.is_empty());
        assert_eq!(transform.len(), 0);
    }

    #[test]
    fn test_add_mapping() {
        let mut transform = TypeRenameTransform::new();
        transform.add_mapping("old_pkg/Msg", "new_pkg/Msg");
        assert_eq!(transform.len(), 1);
    }

    #[test]
    fn test_from_map() {
        let mut map = HashMap::new();
        map.insert("a/A".to_string(), "b/A".to_string());

        let transform = TypeRenameTransform::from_map(map);
        assert_eq!(transform.len(), 1);
    }

    #[test]
    fn test_apply_type() {
        let mut transform = TypeRenameTransform::new();
        transform.add_mapping("old/Msg", "new/Msg");

        assert_eq!(transform.apply_type("old/Msg"), "new/Msg");
        assert_eq!(transform.apply_type("other/Msg"), "other/Msg");
    }

    #[test]
    fn test_apply_no_schema() {
        let mut transform = TypeRenameTransform::new();
        transform.add_mapping("old/Msg", "new/Msg");

        let (new_type, schema) = transform.apply("old/Msg", None);
        assert_eq!(new_type, "new/Msg");
        assert_eq!(schema, None);
    }

    #[test]
    fn test_apply_with_schema_no_rewrite() {
        let mut transform = TypeRenameTransform::new();
        transform.add_mapping("old/Msg", "new/Msg");

        let (new_type, schema) = transform.apply("old/Msg", Some("old/Header field"));
        assert_eq!(new_type, "new/Msg");
        assert_eq!(schema, Some("old/Header field".to_string()));
    }

    #[test]
    fn test_apply_with_schema_rewrite_exact_type() {
        let mut transform = TypeRenameTransform::new();
        transform.add_mapping("old/Msg", "new/Msg");

        // The schema contains the exact type being renamed - it should be rewritten
        let (new_type, schema) = transform.apply("old/Msg", Some("old/Msg field\nold/Msg another"));
        assert_eq!(new_type, "new/Msg");
        // Only exact matches of "old/Msg" get rewritten, not "old/Type"
        let rewritten = schema.unwrap();
        assert!(rewritten.contains("new/Msg field"));
        assert!(rewritten.contains("new/Msg another"));
        assert!(!rewritten.contains("old/Msg"));
    }

    #[test]
    fn test_apply_with_schema_module_declaration() {
        let mut transform = TypeRenameTransform::new();
        transform.add_mapping("sensor_msgs/msg/Image", "my_msgs/msg/Image");

        // Module declarations should be rewritten
        let schema = "module sensor_msgs {\n  struct Header {\n    int32 x;\n  };\n};";
        let (_new_type, rewritten_schema) = transform.apply("sensor_msgs/msg/Image", Some(schema));

        let rewritten = rewritten_schema.unwrap();
        assert!(rewritten.contains("module my_msgs {"));
        assert!(!rewritten.contains("module sensor_msgs {"));
    }

    #[test]
    fn test_apply_with_schema_double_colon_exact_match() {
        let mut transform = TypeRenameTransform::new();
        transform.add_mapping("sensor_msgs/Image", "my_msgs/Image");

        // The schema contains the exact type in double-colon format
        let schema = "sensor_msgs::Image img\nsensor_msgs::Header header";
        let (_new_type, rewritten_schema) = transform.apply("sensor_msgs/Image", Some(schema));

        let rewritten = rewritten_schema.unwrap();
        // Exact type match gets rewritten (with :: conversion)
        assert!(rewritten.contains("my_msgs::Image img"));
        // Header is NOT in mappings, so it stays unchanged
        assert!(rewritten.contains("sensor_msgs::Header"));
    }

    #[test]
    fn test_validate_empty() {
        let transform = TypeRenameTransform::new();
        let channels = vec![make_channel(1, "std_msgs/String", None)];
        assert!(transform.validate(&channels).is_ok());
    }

    #[test]
    fn test_validate_success() {
        let mut transform = TypeRenameTransform::new();
        transform.add_mapping("std_msgs/String", "my_msgs/String");

        let channels = vec![make_channel(1, "std_msgs/String", Some("data"))];
        assert!(transform.validate(&channels).is_ok());
    }

    #[test]
    fn test_validate_not_found() {
        let mut transform = TypeRenameTransform::new();
        transform.add_mapping("nonexistent/Msg", "new/Msg");

        let channels = vec![make_channel(1, "std_msgs/String", None)];
        let result = transform.validate(&channels);
        assert!(result.is_err());
        match result.unwrap_err() {
            TransformError::NotFound { name, .. } => assert_eq!(name, "nonexistent/Msg"),
            _ => panic!("Expected NotFound error"),
        }
    }

    #[test]
    fn test_validate_collision() {
        let mut transform = TypeRenameTransform::new();
        transform.add_mapping("a/Msg", "c/Msg");
        transform.add_mapping("b/Msg", "c/Msg");

        let channels = vec![
            make_channel(1, "a/Msg", Some("schema a")),
            make_channel(2, "b/Msg", Some("schema b")),
        ];
        let result = transform.validate(&channels);
        assert!(result.is_err());
        match result.unwrap_err() {
            TransformError::TypeCollision { sources, target } => {
                assert_eq!(target, "c/Msg");
                assert!(sources.contains(&"a/Msg".to_string()));
                assert!(sources.contains(&"b/Msg".to_string()));
            }
            _ => panic!("Expected TypeCollision error"),
        }
    }

    #[test]
    fn test_validate_collision_with_same_schema_ok() {
        let mut transform = TypeRenameTransform::new();
        transform.add_mapping("a/Msg", "c/Msg");
        transform.add_mapping("b/Msg", "c/Msg");

        // Same schema is OK - can merge types
        let channels = vec![
            make_channel(1, "a/Msg", Some("same schema")),
            make_channel(2, "b/Msg", Some("same schema")),
        ];
        match transform.validate(&channels) {
            Ok(()) => {}
            Err(e) => panic!("Validation failed: {e}"),
        }
    }

    #[test]
    fn test_validate_conflicts_with_existing() {
        let mut transform = TypeRenameTransform::new();
        transform.add_mapping("a/Msg", "c/Msg");

        let channels = vec![
            make_channel(1, "a/Msg", Some("schema a")),
            make_channel(2, "c/Msg", Some("schema c")),
        ];
        let result = transform.validate(&channels);
        assert!(result.is_err());
        match result.unwrap_err() {
            TransformError::TypeCollision { .. } => {}
            _ => panic!("Expected TypeCollision error"),
        }
    }

    #[test]
    fn test_transform_topic_passthrough() {
        let transform = TypeRenameTransform::new();
        assert_eq!(
            transform.transform_topic("/test"),
            Some("/test".to_string())
        );
    }

    #[test]
    fn test_transform_type() {
        let mut transform = TypeRenameTransform::new();
        transform.add_mapping("old/Msg", "new/Msg");

        let (new_type, schema) = transform.transform_type("old/Msg", Some("schema"));
        assert_eq!(new_type, "new/Msg");
        assert_eq!(schema, Some("schema".to_string()));
    }

    #[test]
    fn test_modifies_types() {
        let mut transform = TypeRenameTransform::new();
        assert!(!transform.modifies_types());

        transform.add_mapping("a/A", "b/A");
        assert!(transform.modifies_types());
    }

    #[test]
    fn test_modifies_schemas() {
        let transform = TypeRenameTransform::new();
        assert!(!transform.modifies_schemas());

        // With new behavior, any mapping means schemas will be modified
        let mut transform = TypeRenameTransform::new();
        transform.add_mapping("a/A", "b/A");
        assert!(transform.modifies_schemas());
    }

    #[test]
    fn test_wildcard_matching() {
        let mut transform = TypeRenameTransform::new();
        transform.add_wildcard_mapping("foo/*", "bar/*");

        // Should match and rename
        assert_eq!(transform.apply_type("foo/TypeA"), "bar/TypeA");
        assert_eq!(transform.apply_type("foo/TypeB"), "bar/TypeB");

        // Should not match
        assert_eq!(transform.apply_type("baz/TypeA"), "baz/TypeA");
    }

    #[test]
    fn test_wildcard_with_schema_rewrite() {
        let mut transform = TypeRenameTransform::new();
        transform.add_wildcard_mapping("foo/*", "bar/*");

        // The type itself gets rewritten by wildcard
        let (new_type, schema) = transform.apply("foo/Msg", Some("foo/Msg field"));
        assert_eq!(new_type, "bar/Msg");
        // Schema contains exact type match, so it gets rewritten
        assert_eq!(schema, Some("bar/Msg field".to_string()));
    }

    #[test]
    fn test_wildcard_exact_takes_precedence() {
        let mut transform = TypeRenameTransform::new();
        transform.add_mapping("foo/Specific", "exact/Target");
        transform.add_wildcard_mapping("foo/*", "wildcard/*");

        // Exact mapping should take precedence
        assert_eq!(transform.apply_type("foo/Specific"), "exact/Target");
        // Other types use wildcard
        assert_eq!(transform.apply_type("foo/Other"), "wildcard/Other");
    }
}

#[cfg(test)]
mod topic_aware_tests {
    use super::*;

    fn make_channel(id: u16, topic: &str, msg_type: &str) -> ChannelInfo {
        ChannelInfo::new(
            id,
            topic.to_string(),
            msg_type.to_string(),
            "cdr".to_string(),
            Some("schema text".to_string()),
            Some("ros2msg".to_string()),
        )
    }

    #[test]
    fn test_topic_aware_new() {
        let transform = TopicAwareTypeRenameTransform::new();
        assert!(transform.is_empty());
        assert_eq!(transform.len(), 0);
    }

    #[test]
    fn test_topic_aware_add_mapping() {
        let mut transform = TopicAwareTypeRenameTransform::new();
        transform.add_mapping("/lowdim/joint", "nmx.msg.LowdimData", "nmx.msg.JointStates");
        assert_eq!(transform.len(), 1);
    }

    #[test]
    fn test_topic_aware_apply_for_topic() {
        let mut transform = TopicAwareTypeRenameTransform::new();
        transform.add_mapping("/lowdim/joint", "nmx.msg.LowdimData", "nmx.msg.JointStates");

        // Should match topic and type
        assert_eq!(
            transform.apply_for_topic("/lowdim/joint", "nmx.msg.LowdimData"),
            "nmx.msg.JointStates"
        );

        // Different topic - no match
        assert_eq!(
            transform.apply_for_topic("/lowdim/tcp", "nmx.msg.LowdimData"),
            "nmx.msg.LowdimData"
        );

        // Different type - no match
        assert_eq!(
            transform.apply_for_topic("/lowdim/joint", "nmx.msg.OtherType"),
            "nmx.msg.OtherType"
        );
    }

    #[test]
    fn test_topic_aware_apply_with_schema_no_rewrite() {
        let mut transform = TopicAwareTypeRenameTransform::new();
        transform.add_mapping("/lowdim/joint", "nmx.msg.LowdimData", "nmx.msg.JointStates");

        let (new_type, schema) = transform.apply_for_topic_with_schema(
            "/lowdim/joint",
            "nmx.msg.LowdimData",
            Some("original schema"),
        );
        assert_eq!(new_type, "nmx.msg.JointStates");
        assert_eq!(schema, Some("original schema".to_string()));
    }

    #[test]
    fn test_topic_aware_apply_with_schema_rewrite_package() {
        let mut transform = TopicAwareTypeRenameTransform::new();
        transform.add_mapping("/lowdim/joint", "nmx.msg.LowdimData", "nmx.msg.JointStates");

        let (new_type, schema) = transform.apply_for_topic_with_schema(
            "/lowdim/joint",
            "nmx.msg.LowdimData",
            Some("nmx.msg/Header field"),
        );
        assert_eq!(new_type, "nmx.msg.JointStates");
        assert_eq!(schema, Some("nmx.msg/Header field".to_string()));
    }

    #[test]
    fn test_topic_aware_has_mapping_for_type() {
        let mut transform = TopicAwareTypeRenameTransform::new();
        assert!(!transform.has_mapping_for_type("nmx.msg.LowdimData"));

        transform.add_mapping("/lowdim/joint", "nmx.msg.LowdimData", "nmx.msg.JointStates");
        assert!(transform.has_mapping_for_type("nmx.msg.LowdimData"));
        assert!(!transform.has_mapping_for_type("nmx.msg.Other"));
    }

    #[test]
    fn test_topic_aware_topics_for_type() {
        let mut transform = TopicAwareTypeRenameTransform::new();
        transform.add_mapping("/lowdim/joint", "nmx.msg.LowdimData", "nmx.msg.JointStates");
        transform.add_mapping("/lowdim/tcp", "nmx.msg.LowdimData", "nmx.msg.eef_pose");

        let topics = transform.topics_for_type("nmx.msg.LowdimData");
        assert_eq!(topics.len(), 2);
        assert!(topics.contains(&"/lowdim/joint"));
        assert!(topics.contains(&"/lowdim/tcp"));
    }

    #[test]
    fn test_topic_aware_validate_success() {
        let mut transform = TopicAwareTypeRenameTransform::new();
        transform.add_mapping("/lowdim/joint", "nmx.msg.LowdimData", "nmx.msg.JointStates");

        let channels = vec![make_channel(1, "/lowdim/joint", "nmx.msg.LowdimData")];
        assert!(transform.validate(&channels).is_ok());
    }

    #[test]
    fn test_topic_aware_validate_not_found() {
        let mut transform = TopicAwareTypeRenameTransform::new();
        transform.add_mapping(
            "/nonexistent/topic",
            "nmx.msg.LowdimData",
            "nmx.msg.JointStates",
        );

        let channels = vec![make_channel(1, "/lowdim/joint", "nmx.msg.LowdimData")];
        let result = transform.validate(&channels);
        assert!(result.is_err());
    }

    #[test]
    fn test_topic_aware_transform_type_without_topic() {
        let transform = TopicAwareTypeRenameTransform::new();
        // Without topic context, returns original
        let (new_type, schema) = transform.transform_type("nmx.msg.LowdimData", Some("schema"));
        assert_eq!(new_type, "nmx.msg.LowdimData");
        assert_eq!(schema, Some("schema".to_string()));
    }
}
