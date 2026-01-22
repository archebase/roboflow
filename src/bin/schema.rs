// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Unified schema inspection and validation tool for robotics data.
//!
//! Usage:
//!   schema list <file>              - List all message types in the file
//!   schema show <file> <type>       - Show full schema for a message type
//!   schema validate <file>          - Validate all schemas can be parsed
//!   schema search <file> <pattern>  - Search for message types matching pattern
//!   schema common <file>            - Show standard ROS types (sensor_msgs, std_msgs, etc.)

use std::env;
use std::path::Path;

enum Command {
    List,
    Show { msg_type: String },
    Validate,
    Search { pattern: String },
    Common,
}

fn parse_args(args: &[String]) -> Result<(String, Command), String> {
    if args.len() < 3 {
        return Err(format!(
            "Usage: {} <command> <file> [options]\n\
             Commands:\n\
               list <file>              - List all message types\n\
               show <file> <type>       - Show full schema for message type\n\
               validate <file>          - Validate all schemas can be parsed\n\
               search <file> <pattern>  - Search for message types matching pattern\n\
               common <file>            - Show standard ROS types",
            args[0]
        ));
    }

    let command = &args[1];
    let file = args[2].clone();

    let cmd = match command.as_str() {
        "list" => Command::List,
        "show" => {
            if args.len() < 4 {
                return Err("show command requires a message type argument".to_string());
            }
            let msg_type = args[3].clone();
            Command::Show { msg_type }
        }
        "validate" => Command::Validate,
        "search" => {
            if args.len() < 4 {
                return Err("search command requires a pattern argument".to_string());
            }
            let pattern = args[3].clone();
            Command::Search { pattern }
        }
        "common" => Command::Common,
        _ => return Err(format!("Unknown command: {command}")),
    };

    Ok((file, cmd))
}

fn run_schema(file: &str, cmd: Command) -> Result<(), Box<dyn std::error::Error>> {
    let ext = Path::new(file)
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_lowercase();

    match cmd {
        Command::List => list_types(file, &ext)?,
        Command::Show { msg_type } => show_schema(file, &ext, &msg_type)?,
        Command::Validate => validate_schemas(file, &ext)?,
        Command::Search { pattern } => search_types(file, &ext, &pattern)?,
        Command::Common => show_common_types(file, &ext)?,
    }

    Ok(())
}

#[derive(Debug)]
struct TypeInfo {
    type_name: String,
    topics: Vec<String>,
    count: usize,
}

/// List all unique message types in the file.
fn list_types(file: &str, ext: &str) -> Result<(), Box<dyn std::error::Error>> {
    let types = get_message_types(file, ext)?;

    println!("=== Message Types in {} ===", file);
    println!();

    for msg_type in types {
        println!("{}", msg_type.type_name);
        for topic in &msg_type.topics {
            println!("  @ {}", topic);
        }
        if msg_type.count > 1 {
            println!("  ({} channel(s))", msg_type.count);
        }
        println!();
    }

    Ok(())
}

fn get_message_types(file: &str, ext: &str) -> Result<Vec<TypeInfo>, Box<dyn std::error::Error>> {
    let mut type_map: std::collections::HashMap<String, TypeInfo> =
        std::collections::HashMap::new();

    match ext {
        "mcap" => {
            let reader = robocodec::mcap::McapReader::open(file)?;
            for channel in reader.channels().values() {
                type_map
                    .entry(channel.message_type.clone())
                    .or_insert_with(|| TypeInfo {
                        type_name: channel.message_type.clone(),
                        topics: Vec::new(),
                        count: 0,
                    })
                    .topics
                    .push(channel.topic.clone());
            }
        }
        "bag" => {
            use robocodec::io::traits::FormatReader;
            let reader = robocodec::BagFormat::open(file)?;
            for channel in reader.channels().values() {
                type_map
                    .entry(channel.message_type.clone())
                    .or_insert_with(|| TypeInfo {
                        type_name: channel.message_type.clone(),
                        topics: Vec::new(),
                        count: 0,
                    })
                    .topics
                    .push(channel.topic.clone());
            }
        }
        _ => {
            // Try MCAP first
            match robocodec::mcap::McapReader::open(file) {
                Ok(reader) => {
                    for channel in reader.channels().values() {
                        type_map
                            .entry(channel.message_type.clone())
                            .or_insert_with(|| TypeInfo {
                                type_name: channel.message_type.clone(),
                                topics: Vec::new(),
                                count: 0,
                            })
                            .topics
                            .push(channel.topic.clone());
                    }
                }
                Err(_) => {
                    use robocodec::io::traits::FormatReader;
                    let reader = robocodec::BagFormat::open(file)?;
                    for channel in reader.channels().values() {
                        type_map
                            .entry(channel.message_type.clone())
                            .or_insert_with(|| TypeInfo {
                                type_name: channel.message_type.clone(),
                                topics: Vec::new(),
                                count: 0,
                            })
                            .topics
                            .push(channel.topic.clone());
                    }
                }
            }
        }
    }

    let mut types: Vec<_> = type_map.into_values().collect();
    types.sort_by(|a, b| a.type_name.cmp(&b.type_name));
    for t in &mut types {
        t.count = t.topics.len();
    }

    Ok(types)
}

/// Show full schema for a specific message type.
fn show_schema(file: &str, ext: &str, msg_type: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut found = false;

    match ext {
        "mcap" => {
            let reader = robocodec::mcap::McapReader::open(file)?;
            for ch in reader.channels().values() {
                if ch.message_type.contains(msg_type) {
                    found = true;
                    println!("=== {} @ {} ===", ch.message_type, ch.topic);
                    println!(
                        "Encoding: {:?}",
                        ch.schema_encoding.as_deref().unwrap_or("unknown")
                    );
                    println!();
                    if let Some(schema) = &ch.schema {
                        println!("{}", schema);
                    } else {
                        println!("(no schema available)");
                    }
                    println!();
                }
            }
        }
        "bag" => {
            use robocodec::io::traits::FormatReader;
            let reader = robocodec::BagFormat::open(file)?;
            for ch in reader.channels().values() {
                if ch.message_type.contains(msg_type) {
                    found = true;
                    println!("=== {} @ {} ===", ch.message_type, ch.topic);
                    println!(
                        "Encoding: {:?}",
                        ch.schema_encoding.as_deref().unwrap_or("unknown")
                    );
                    println!();
                    if let Some(schema) = &ch.schema {
                        println!("{}", schema);
                    } else {
                        println!("(no schema available)");
                    }
                    println!();
                }
            }
        }
        _ => {
            // Try MCAP first
            match robocodec::mcap::McapReader::open(file) {
                Ok(reader) => {
                    for ch in reader.channels().values() {
                        if ch.message_type.contains(msg_type) {
                            found = true;
                            println!("=== {} @ {} ===", ch.message_type, ch.topic);
                            println!();
                            if let Some(schema) = &ch.schema {
                                println!("{}", schema);
                            } else {
                                println!("(no schema available)");
                            }
                            println!();
                        }
                    }
                }
                Err(_) => {
                    use robocodec::io::traits::FormatReader;
                    let reader = robocodec::BagFormat::open(file)?;
                    for ch in reader.channels().values() {
                        if ch.message_type.contains(msg_type) {
                            found = true;
                            println!("=== {} @ {} ===", ch.message_type, ch.topic);
                            println!();
                            if let Some(schema) = &ch.schema {
                                println!("{}", schema);
                            } else {
                                println!("(no schema available)");
                            }
                            println!();
                        }
                    }
                }
            }
        }
    }

    if !found {
        eprintln!("No message type matching '{msg_type}' found");
        std::process::exit(1);
    }

    Ok(())
}

/// Validate all schemas can be parsed.
fn validate_schemas(file: &str, ext: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Validating Schemas ===");
    println!();

    let (ok_count, err_count) = match ext {
        "mcap" => validate_schemas_mcap(file)?,
        "bag" => validate_schemas_bag(file)?,
        _ => {
            // Try MCAP first
            match robocodec::mcap::McapReader::open(file) {
                Ok(reader) => validate_schemas_mcap_direct(&reader)?,
                Err(_) => validate_schemas_bag(file)?,
            }
        }
    };

    println!();
    println!("Results: {} valid, {} errors", ok_count, err_count);

    if err_count > 0 {
        std::process::exit(1);
    }

    Ok(())
}

fn validate_schemas_mcap(file: &str) -> Result<(usize, usize), Box<dyn std::error::Error>> {
    let reader = robocodec::mcap::McapReader::open(file)?;
    validate_schemas_mcap_direct(&reader)
}

fn validate_schemas_mcap_direct(
    reader: &robocodec::mcap::McapReader,
) -> Result<(usize, usize), Box<dyn std::error::Error>> {
    let mut ok = 0;
    let mut err = 0;

    for ch in reader.channels().values() {
        let Some(schema) = &ch.schema else {
            println!("  ⚠ {} @ {}: no schema", ch.message_type, ch.topic);
            err += 1;
            continue;
        };

        let encoding = ch.schema_encoding.as_deref().unwrap_or("unknown");

        match robocodec::schema::parser::parse_schema_with_encoding_str(
            &ch.message_type,
            schema,
            encoding,
        ) {
            Ok(_) => {
                println!("  ✓ {} @ {}", ch.message_type, ch.topic);
                ok += 1;
            }
            Err(e) => {
                println!("  ✗ {} @ {}: {}", ch.message_type, ch.topic, e);
                err += 1;
            }
        }
    }

    Ok((ok, err))
}

fn validate_schemas_bag(file: &str) -> Result<(usize, usize), Box<dyn std::error::Error>> {
    use robocodec::io::traits::FormatReader;
    let reader = robocodec::BagFormat::open(file)?;

    let mut ok = 0;
    let mut err = 0;

    for ch in reader.channels().values() {
        let Some(schema) = &ch.schema else {
            println!("  ⚠ {} @ {}: no schema", ch.message_type, ch.topic);
            err += 1;
            continue;
        };

        let encoding = ch.schema_encoding.as_deref().unwrap_or("unknown");

        match robocodec::schema::parser::parse_schema_with_encoding_str(
            &ch.message_type,
            schema,
            encoding,
        ) {
            Ok(_) => {
                println!("  ✓ {} @ {}", ch.message_type, ch.topic);
                ok += 1;
            }
            Err(e) => {
                println!("  ✗ {} @ {}: {}", ch.message_type, ch.topic, e);
                err += 1;
            }
        }
    }

    Ok((ok, err))
}

/// Search for message types matching a pattern.
fn search_types(file: &str, ext: &str, pattern: &str) -> Result<(), Box<dyn std::error::Error>> {
    let pattern_lower = pattern.to_lowercase();

    println!("=== Searching for '{}' ===", pattern);
    println!();

    match ext {
        "mcap" => {
            let reader = robocodec::mcap::McapReader::open(file)?;
            search_types_mcap(&reader, &pattern_lower)?;
        }
        "bag" => {
            let reader = robocodec::BagFormat::open(file)?;
            search_types_bag(&reader, &pattern_lower)?;
        }
        _ => {
            // Try MCAP first
            match robocodec::mcap::McapReader::open(file) {
                Ok(reader) => {
                    search_types_mcap(&reader, &pattern_lower)?;
                }
                Err(_) => {
                    let reader = robocodec::BagFormat::open(file)?;
                    search_types_bag(&reader, &pattern_lower)?;
                }
            }
        }
    }

    Ok(())
}

fn search_types_mcap(
    reader: &robocodec::mcap::McapReader,
    pattern_lower: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    for ch in reader.channels().values() {
        let msg_type_lower = ch.message_type.to_lowercase();
        let topic_lower = ch.topic.to_lowercase();

        if msg_type_lower.contains(pattern_lower) || topic_lower.contains(pattern_lower) {
            println!("Type: {}", ch.message_type);
            println!("Topic: {}", ch.topic);
            println!(
                "Encoding: {}",
                ch.schema_encoding.as_deref().unwrap_or("unknown")
            );

            if let Some(schema) = &ch.schema {
                let preview: String = schema.lines().take(10).collect::<Vec<_>>().join("\n");
                println!("Schema preview:");
                println!("{}", preview);
                if schema.lines().count() > 10 {
                    println!("... ({} lines total)", schema.lines().count());
                }
            }
            println!();
        }
    }
    Ok(())
}

fn search_types_bag<R>(reader: &R, pattern_lower: &str) -> Result<(), Box<dyn std::error::Error>>
where
    R: robocodec::io::traits::FormatReader,
{
    for ch in reader.channels().values() {
        let msg_type_lower = ch.message_type.to_lowercase();
        let topic_lower = ch.topic.to_lowercase();

        if msg_type_lower.contains(pattern_lower) || topic_lower.contains(pattern_lower) {
            println!("Type: {}", ch.message_type);
            println!("Topic: {}", ch.topic);
            println!(
                "Encoding: {}",
                ch.schema_encoding.as_deref().unwrap_or("unknown")
            );

            if let Some(schema) = &ch.schema {
                let preview: String = schema.lines().take(10).collect::<Vec<_>>().join("\n");
                println!("Schema preview:");
                println!("{}", preview);
                if schema.lines().count() > 10 {
                    println!("... ({} lines total)", schema.lines().count());
                }
            }
            println!();
        }
    }
    Ok(())
}

/// Show only standard/common ROS message types.
fn show_common_types(file: &str, ext: &str) -> Result<(), Box<dyn std::error::Error>> {
    const COMMON_PREFIXES: &[&str] = &[
        "sensor_msgs/",
        "std_msgs/",
        "geometry_msgs/",
        "nav_msgs/",
        "tf2_msgs/",
        "trajectory_msgs/",
        "visualization_msgs/",
        "diagnostic_msgs/",
        "actionlib_msgs/",
    ];

    println!("=== Standard ROS Message Types ===");
    println!();

    let mut found_any = false;

    match ext {
        "mcap" => {
            let reader = robocodec::mcap::McapReader::open(file)?;
            for ch in reader.channels().values() {
                let mut is_common = false;
                for prefix in COMMON_PREFIXES {
                    if ch.message_type.starts_with(prefix)
                        || ch.message_type.starts_with(&prefix.replace('/', "msg/"))
                    {
                        is_common = true;
                        break;
                    }
                }
                if is_common {
                    found_any = true;
                    println!("{} @ {}", ch.message_type, ch.topic);
                }
            }
        }
        "bag" => {
            use robocodec::io::traits::FormatReader;
            let reader = robocodec::BagFormat::open(file)?;
            for ch in reader.channels().values() {
                let mut is_common = false;
                for prefix in COMMON_PREFIXES {
                    if ch.message_type.starts_with(prefix)
                        || ch.message_type.starts_with(&prefix.replace('/', "msg/"))
                    {
                        is_common = true;
                        break;
                    }
                }
                if is_common {
                    found_any = true;
                    println!("{} @ {}", ch.message_type, ch.topic);
                }
            }
        }
        _ => {
            // Try MCAP first
            match robocodec::mcap::McapReader::open(file) {
                Ok(reader) => {
                    for ch in reader.channels().values() {
                        let mut is_common = false;
                        for prefix in COMMON_PREFIXES {
                            if ch.message_type.starts_with(prefix)
                                || ch.message_type.starts_with(&prefix.replace('/', "msg/"))
                            {
                                is_common = true;
                                break;
                            }
                        }
                        if is_common {
                            found_any = true;
                            println!("{} @ {}", ch.message_type, ch.topic);
                        }
                    }
                }
                Err(_) => {
                    use robocodec::io::traits::FormatReader;
                    let reader = robocodec::BagFormat::open(file)?;
                    for ch in reader.channels().values() {
                        let mut is_common = false;
                        for prefix in COMMON_PREFIXES {
                            if ch.message_type.starts_with(prefix)
                                || ch.message_type.starts_with(&prefix.replace('/', "msg/"))
                            {
                                is_common = true;
                                break;
                            }
                        }
                        if is_common {
                            found_any = true;
                            println!("{} @ {}", ch.message_type, ch.topic);
                        }
                    }
                }
            }
        }
    }

    if !found_any {
        println!("(no standard ROS types found)");
    }

    Ok(())
}

fn main() {
    let args: Vec<String> = env::args().collect();

    let (file, cmd) = match parse_args(&args) {
        Ok(result) => result,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    };

    if let Err(e) = run_schema(&file, cmd) {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}
