// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! OMG IDL format parser using Pest.
//!
//! This module handles parsing of OMG IDL v4.1 format files.

use crate::core::CodecError;
use crate::core::Result as CoreResult;
use crate::schema::ast::MessageSchema;
use crate::schema::ast::{Field, FieldType, MessageType, PrimitiveType};
use crate::schema::parser::msg_parser::RosVersion;
use pest::Parser;
use pest_derive::Parser;

/// Pest parser for OMG IDL schema files.
#[derive(Parser)]
#[grammar = "schema/parser/idl_parser/omg_idl.pest"] // Path relative to src/ directory
pub struct IdlParser;

/// Parse pure OMG IDL format.
pub fn parse(name: &str, definition: &str) -> CoreResult<MessageSchema> {
    // Default to ROS2 for pure IDL format (most common case)
    parse_with_version(name, definition, RosVersion::Ros2)
}

/// Parse OMG IDL format with explicit encoding info.
pub fn parse_with_encoding(
    name: &str,
    definition: &str,
    encoding: &str,
) -> CoreResult<MessageSchema> {
    let ros_version = RosVersion::from_encoding(encoding);
    parse_with_version(name, definition, ros_version)
}

/// Parse OMG IDL format with explicit ROS version.
pub fn parse_with_version(
    name: &str,
    definition: &str,
    ros_version: RosVersion,
) -> CoreResult<MessageSchema> {
    let pairs = IdlParser::parse(Rule::specification, definition)
        .map_err(|e| CodecError::parse("IDL schema", format!("{e}")))?;

    let mut schema = MessageSchema::new(name.to_string());

    for pair in pairs {
        // The top-level rule is `specification`, which contains definition* and EOI
        // We need to process the inner pairs directly
        let inner_pairs: Vec<_> = pair.into_inner().collect();

        for inner in inner_pairs {
            match inner.as_rule() {
                Rule::EOI => {}
                Rule::definition => {
                    parse_definition_from_inner(inner.into_inner().collect(), &mut schema, None)?;
                }
                Rule::module_dcl => {
                    parse_module_from_inner(inner.into_inner().collect(), &mut schema, None)?;
                }
                _ => {}
            }
        }
    }

    // Post-processing: Add seq field to all std_msgs/Header variants if missing
    // This handles schema-data version mismatch where CDR data has seq but IDL doesn't
    // Only add seq for ROS1 data - ROS2 (CDR encoding) doesn't have seq field
    if ros_version == RosVersion::Ros1 {
        add_seq_field_to_header_types(&mut schema);
    }

    Ok(schema)
}

/// Add seq field to all std_msgs::msg::Header variants if missing.
/// This handles backward compatibility with ROS1/older ROS2 data that includes seq.
fn add_seq_field_to_header_types(schema: &mut MessageSchema) {
    // Find all Header type variants in the schema (with different naming conventions)
    let header_variants: Vec<String> = schema
        .types
        .keys()
        .filter(|k| {
            // Match any of these patterns:
            // 1. "std_msgs/Header" (without /msg/)
            // 2. "std_msgs/msg/Header" (with /msg/)
            // 3. "std_msgs::msg::Header" (with ::)
            k.contains("Header") && k.contains("std_msgs")
        })
        .cloned()
        .collect();

    for variant_name in &header_variants {
        if let Some(header_type) = schema.types.get_mut(variant_name) {
            let has_seq = header_type.fields.iter().any(|f| f.name == "seq");
            if !has_seq {
                // Insert seq field after stamp field (at index 1)
                // Header should be: stamp, seq, frame_id
                let seq_field = Field {
                    name: "seq".to_string(),
                    type_name: FieldType::Primitive(PrimitiveType::UInt32),
                };
                header_type.fields.insert(1, seq_field);
                // Update max_alignment since UInt32 has alignment 4
                header_type.max_alignment = header_type.max_alignment.max(4);
            }
        }
    }
}

/// Parse from inner pairs of a definition.
fn parse_definition_from_inner(
    inner_pairs: Vec<pest::iterators::Pair<Rule>>,
    schema: &mut MessageSchema,
    parent_module_path: Option<&str>,
) -> CoreResult<()> {
    for inner in inner_pairs {
        match inner.as_rule() {
            Rule::struct_dcl => {
                // struct_dcl contains struct_def or struct_forward_dcl
                for struct_inner in inner.into_inner() {
                    if struct_inner.as_rule() == Rule::struct_def {
                        parse_struct(struct_inner, schema, None, parent_module_path)?;
                    }
                }
            }
            Rule::type_dcl => {
                // type_dcl contains constr_type_dcl, native_dcl, or typedef_dcl
                for type_inner in inner.into_inner() {
                    if type_inner.as_rule() == Rule::constr_type_dcl {
                        for constr_inner in type_inner.into_inner() {
                            if constr_inner.as_rule() == Rule::struct_dcl {
                                for struct_inner in constr_inner.into_inner() {
                                    if struct_inner.as_rule() == Rule::struct_def {
                                        parse_struct(
                                            struct_inner,
                                            schema,
                                            None,
                                            parent_module_path,
                                        )?;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Rule::module_dcl => {
                parse_module(inner, schema, parent_module_path)?;
            }
            _ => {}
        }
    }
    Ok(())
}

/// Parse from inner pairs of a module declaration.
fn parse_module_from_inner(
    inner_pairs: Vec<pest::iterators::Pair<Rule>>,
    schema: &mut MessageSchema,
    parent_module_path: Option<&str>,
) -> CoreResult<()> {
    for inner in inner_pairs {
        if inner.as_rule() == Rule::definition {
            parse_definition_from_inner(inner.into_inner().collect(), schema, parent_module_path)?;
        }
    }
    Ok(())
}

/// Parse a struct definition from IDL.
fn parse_struct(
    pair: pest::iterators::Pair<Rule>,
    schema: &mut MessageSchema,
    override_name: Option<&str>,
    parent_module_path: Option<&str>,
) -> CoreResult<()> {
    // Collect all inner items first
    let inner_items: Vec<_> = pair.into_inner().collect();

    // Get struct name
    let name = override_name
        .map(|s| s.to_string())
        .or_else(|| {
            inner_items
                .iter()
                .find(|p| p.as_rule() == Rule::identifier)
                .map(|p| p.as_str().to_string())
        })
        .unwrap_or_default();

    // Combine parent module path with struct name if provided
    let full_name = if let Some(parent_path) = parent_module_path {
        format!("{parent_path}/{name}")
    } else {
        name.clone()
    };

    let mut msg_type = MessageType::new(full_name.clone());

    // Parse members
    for item in inner_items {
        if item.as_rule() == Rule::member {
            if let Some(field) = parse_member(item) {
                msg_type.add_field(field);
            }
        }
    }

    schema.add_type(msg_type);
    Ok(())
}

/// Parse a module definition from IDL.
fn parse_module(
    pair: pest::iterators::Pair<Rule>,
    schema: &mut MessageSchema,
    parent_module_path: Option<&str>,
) -> CoreResult<()> {
    let mut inner = pair.into_inner();

    let module_name = inner
        .find(|p| p.as_rule() == Rule::identifier)
        .map(|p| p.as_str().to_string());

    // Build the full module path by combining parent path with current module name
    let full_module_path = if let Some(parent_path) = parent_module_path {
        if let Some(ref mod_name) = module_name {
            Some(format!("{parent_path}/{mod_name}"))
        } else {
            parent_module_path.map(|s| s.to_string())
        }
    } else {
        module_name.clone()
    };

    // Parse nested definitions (module_dcl contains definition*)
    for item in inner {
        if item.as_rule() == Rule::definition {
            // Each definition may contain struct_dcl, type_dcl, or module_dcl
            for def_inner in item.into_inner() {
                match def_inner.as_rule() {
                    Rule::struct_dcl => {
                        // struct_dcl contains struct_def
                        for struct_inner in def_inner.into_inner() {
                            if struct_inner.as_rule() == Rule::struct_def {
                                let struct_name = struct_inner
                                    .clone()
                                    .into_inner()
                                    .find(|p| p.as_rule() == Rule::identifier)
                                    .map(|p| p.as_str().to_string());

                                let full_name = if let Some(ref mod_path) = full_module_path {
                                    format!("{}/{}", mod_path, struct_name.unwrap_or_default())
                                } else {
                                    struct_name.unwrap_or_default()
                                };

                                parse_struct(struct_inner, schema, Some(full_name.as_str()), None)?;
                            }
                        }
                    }
                    Rule::type_dcl => {
                        // type_dcl contains constr_type_dcl, native_dcl, or typedef_dcl
                        for type_inner in def_inner.into_inner() {
                            if type_inner.as_rule() == Rule::constr_type_dcl {
                                for constr_inner in type_inner.into_inner() {
                                    if constr_inner.as_rule() == Rule::struct_dcl {
                                        for struct_inner in constr_inner.into_inner() {
                                            if struct_inner.as_rule() == Rule::struct_def {
                                                let struct_name = struct_inner
                                                    .clone()
                                                    .into_inner()
                                                    .find(|p| p.as_rule() == Rule::identifier)
                                                    .map(|p| p.as_str().to_string());

                                                let full_name =
                                                    if let Some(ref mod_path) = full_module_path {
                                                        format!(
                                                            "{}/{}",
                                                            mod_path,
                                                            struct_name.unwrap_or_default()
                                                        )
                                                    } else {
                                                        struct_name.unwrap_or_default()
                                                    };

                                                parse_struct(
                                                    struct_inner,
                                                    schema,
                                                    Some(full_name.as_str()),
                                                    None,
                                                )?;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Rule::module_dcl => {
                        parse_module(def_inner, schema, full_module_path.as_deref())?;
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(())
}

/// Parse an IDL member (field in a struct).
fn parse_member(pair: pest::iterators::Pair<Rule>) -> Option<Field> {
    let inner_items: Vec<_> = pair.into_inner().collect();

    // Get type_spec
    let type_spec = inner_items
        .iter()
        .find(|p| p.as_rule() == Rule::type_spec)?;

    // Get field name - declarators contains one or more declarator
    let declarators = inner_items
        .iter()
        .find(|p| p.as_rule() == Rule::declarators)?;

    // declarators contains: declarator ~ ("," ~ declarator)*
    // declarator contains: array_declarator | simple_declarator
    // simple_declarator contains: identifier
    let declarator = declarators.clone().into_inner().next()?;

    // If the rule is `declarator`, we need to get its inner to find the actual type
    let decl_inner: Vec<_> = declarator.clone().into_inner().collect();
    let field_name = if declarator.as_rule() == Rule::array_declarator {
        declarator
            .into_inner()
            .find(|p| p.as_rule() == Rule::identifier)
    } else if declarator.as_rule() == Rule::simple_declarator {
        declarator.into_inner().next()
    } else if declarator.as_rule() == Rule::declarator {
        // Need to go one level deeper - get the inner of declarator
        let actual_declarator = decl_inner.first()?;
        if actual_declarator.as_rule() == Rule::simple_declarator {
            actual_declarator.clone().into_inner().next()
        } else if actual_declarator.as_rule() == Rule::array_declarator {
            actual_declarator
                .clone()
                .into_inner()
                .find(|p| p.as_rule() == Rule::identifier)
        } else if actual_declarator.as_rule() == Rule::identifier {
            Some(actual_declarator.clone())
        } else {
            None
        }
    } else {
        Some(declarator)
    }
    .map(|p| p.as_str().to_string())?;

    let type_name = parse_type_spec(type_spec.clone())?;

    Some(Field {
        name: field_name,
        type_name,
    })
}

/// Parse an IDL type specification.
fn parse_type_spec(pair: pest::iterators::Pair<Rule>) -> Option<FieldType> {
    let inner = pair.into_inner().next()?;

    match inner.as_rule() {
        Rule::sequence_type => {
            // sequence<T> becomes T[] (dynamic array)
            let seq_inner_items: Vec<_> = inner.into_inner().collect();
            let type_spec_pair = seq_inner_items
                .iter()
                .find(|p| p.as_rule() == Rule::type_spec)?;
            Some(FieldType::Array {
                base_type: Box::new(parse_type_spec(type_spec_pair.clone())?),
                size: None,
            })
        }
        Rule::string_type | Rule::wide_string_type => {
            Some(FieldType::Primitive(PrimitiveType::String))
        }
        Rule::template_type_spec => {
            // template_type_spec contains: sequence_type, string_type, wide_string_type, fixed_pt_type, map_type
            let template_inner_items: Vec<_> = inner.into_inner().collect();
            let template_inner = template_inner_items.first()?;
            match template_inner.as_rule() {
                Rule::sequence_type => {
                    let seq_inner_items: Vec<_> = template_inner.clone().into_inner().collect();
                    let type_spec_pair = seq_inner_items
                        .iter()
                        .find(|p| p.as_rule() == Rule::type_spec)?;
                    Some(FieldType::Array {
                        base_type: Box::new(parse_type_spec(type_spec_pair.clone())?),
                        size: None,
                    })
                }
                Rule::string_type | Rule::wide_string_type => {
                    Some(FieldType::Primitive(PrimitiveType::String))
                }
                _ => parse_type_base(template_inner.clone()),
            }
        }
        Rule::simple_type_spec => parse_simple_type_spec(inner),
        _ => parse_simple_type_spec(inner),
    }
}

/// Parse an IDL simple type specification.
fn parse_simple_type_spec(pair: pest::iterators::Pair<Rule>) -> Option<FieldType> {
    let inner = pair.into_inner().next()?;

    match inner.as_rule() {
        Rule::scoped_name => {
            let type_name = inner.as_str().trim().to_string();
            if let Some(prim) = PrimitiveType::try_from_str(&type_name) {
                Some(FieldType::Primitive(prim))
            } else {
                Some(FieldType::Nested(type_name))
            }
        }
        Rule::floating_pt_type => {
            let type_str = inner.as_str().to_lowercase();
            if type_str.contains("double") {
                Some(FieldType::Primitive(PrimitiveType::Float64))
            } else {
                Some(FieldType::Primitive(PrimitiveType::Float32))
            }
        }
        Rule::integer_type => {
            let type_str = inner.as_str().to_lowercase();
            if type_str.contains("unsigned") {
                if type_str.contains("64") || type_str.contains("long long") {
                    Some(FieldType::Primitive(PrimitiveType::UInt64))
                } else if type_str.contains("32") || type_str.contains("long") {
                    Some(FieldType::Primitive(PrimitiveType::UInt32))
                } else if type_str.contains("16") || type_str.contains("short") {
                    Some(FieldType::Primitive(PrimitiveType::UInt16))
                } else {
                    Some(FieldType::Primitive(PrimitiveType::UInt8))
                }
            } else if type_str.contains("64") || type_str.contains("long long") {
                Some(FieldType::Primitive(PrimitiveType::Int64))
            } else if type_str.contains("32") || type_str.contains("long") {
                Some(FieldType::Primitive(PrimitiveType::Int32))
            } else if type_str.contains("16") || type_str.contains("short") {
                Some(FieldType::Primitive(PrimitiveType::Int16))
            } else {
                Some(FieldType::Primitive(PrimitiveType::Int8))
            }
        }
        Rule::boolean_type => Some(FieldType::Primitive(PrimitiveType::Bool)),
        Rule::char_type => Some(FieldType::Primitive(PrimitiveType::Int8)),
        Rule::octet_type => Some(FieldType::Primitive(PrimitiveType::UInt8)),
        Rule::base_type_spec => {
            // base_type_spec contains floating_pt_type, integer_type, etc.
            let base_inner = inner.into_inner().next()?;
            match base_inner.as_rule() {
                Rule::floating_pt_type => {
                    let type_str = base_inner.as_str().to_lowercase();
                    if type_str.contains("double") {
                        Some(FieldType::Primitive(PrimitiveType::Float64))
                    } else {
                        Some(FieldType::Primitive(PrimitiveType::Float32))
                    }
                }
                Rule::integer_type => {
                    let type_str = base_inner.as_str().to_lowercase();
                    if type_str.contains("unsigned") {
                        if type_str.contains("64") || type_str.contains("long long") {
                            Some(FieldType::Primitive(PrimitiveType::UInt64))
                        } else if type_str.contains("32") || type_str.contains("long") {
                            Some(FieldType::Primitive(PrimitiveType::UInt32))
                        } else if type_str.contains("16") || type_str.contains("short") {
                            Some(FieldType::Primitive(PrimitiveType::UInt16))
                        } else {
                            Some(FieldType::Primitive(PrimitiveType::UInt8))
                        }
                    } else if type_str.contains("64") || type_str.contains("long long") {
                        Some(FieldType::Primitive(PrimitiveType::Int64))
                    } else if type_str.contains("32") || type_str.contains("long") {
                        Some(FieldType::Primitive(PrimitiveType::Int32))
                    } else if type_str.contains("16") || type_str.contains("short") {
                        Some(FieldType::Primitive(PrimitiveType::Int16))
                    } else {
                        Some(FieldType::Primitive(PrimitiveType::Int8))
                    }
                }
                Rule::boolean_type => Some(FieldType::Primitive(PrimitiveType::Bool)),
                Rule::char_type => Some(FieldType::Primitive(PrimitiveType::Int8)),
                Rule::octet_type => Some(FieldType::Primitive(PrimitiveType::UInt8)),
                _ => None,
            }
        }
        _ => {
            let type_str = inner.as_str().trim().to_lowercase();
            PrimitiveType::try_from_str(&type_str).map(FieldType::Primitive)
        }
    }
}

/// Parse an IDL type base (primitive or scoped name).
fn parse_type_base(pair: pest::iterators::Pair<Rule>) -> Option<FieldType> {
    let rule = pair.as_rule();

    if rule == Rule::type_spec {
        parse_type_spec(pair)
    } else if rule == Rule::scoped_name {
        let type_name = pair.as_str().trim().to_string();
        if let Some(prim) = PrimitiveType::try_from_str(&type_name) {
            Some(FieldType::Primitive(prim))
        } else {
            Some(FieldType::Nested(type_name))
        }
    } else {
        let type_str = pair.as_str().trim().to_lowercase();
        PrimitiveType::try_from_str(&type_str).map(FieldType::Primitive)
    }
}
