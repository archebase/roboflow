//! Unified schema inspection and validation tool for robotics data.
//!
//! Usage:
//!   schema list <file>              - List all message types in the file
//!   schema show <file> <type>       - Show full schema for a message type
//!   schema validate <file>          - Validate all schemas can be parsed
//!   schema search <file> <pattern>  - Search for message types matching pattern
//!   schema common <file>            - Show standard ROS types (sensor_msgs, std_msgs, etc.)

use std::env;

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
    let reader = robocodec::RoboReader::open(file)?;

    match cmd {
        Command::List => list_types(&reader),
        Command::Show { msg_type } => show_schema(&reader, &msg_type)?,
        Command::Validate => validate_schemas(&reader)?,
        Command::Search { pattern } => search_types(&reader, &pattern),
        Command::Common => show_common_types(&reader),
    }

    Ok(())
}

/// List all unique message types in the file.
fn list_types(reader: &robocodec::RoboReader) {
    let mut types: Vec<String> = reader
        .channels()
        .values()
        .map(|ch| ch.message_type.clone())
        .collect();

    types.sort();
    types.dedup();

    println!("=== Message Types in {} ===", reader.path());
    println!();

    for msg_type in types {
        // Count channels using this type
        let count = reader
            .channels()
            .values()
            .filter(|ch| ch.message_type == msg_type)
            .count();

        let topics: Vec<String> = reader
            .channels()
            .values()
            .filter(|ch| ch.message_type == msg_type)
            .map(|ch| ch.topic.clone())
            .collect();

        println!("{} ({} channel(s))", msg_type, count);
        for topic in topics {
            println!("  @ {}", topic);
        }
        println!();
    }
}

/// Show full schema for a specific message type.
fn show_schema(reader: &robocodec::RoboReader, msg_type: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut found = false;

    for ch in reader.channels().values() {
        if ch.message_type.contains(msg_type) {
            found = true;
            println!("=== {} @ {} ===", ch.message_type, ch.topic);
            println!("Encoding: {:?}", ch.schema_encoding.as_deref().unwrap_or("unknown"));

            if let Some(schema) = &ch.schema {
                println!();
                println!("{}", schema);
            } else {
                println!("(no schema available)");
            }
            println!();
        }
    }

    if !found {
        eprintln!("No message type matching '{msg_type}' found");
        std::process::exit(1);
    }

    Ok(())
}

/// Validate all schemas can be parsed.
fn validate_schemas(reader: &robocodec::RoboReader) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Validating Schemas ===");
    println!();

    let mut ok_count = 0;
    let mut err_count = 0;

    for ch in reader.channels().values() {
        let Some(schema) = &ch.schema else {
            println!("  ⚠ {} @ {}: no schema", ch.message_type, ch.topic);
            err_count += 1;
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
                ok_count += 1;
            }
            Err(e) => {
                println!("  ✗ {} @ {}: {}", ch.message_type, ch.topic, e);
                err_count += 1;
            }
        }
    }

    println!();
    println!("Results: {} valid, {} errors", ok_count, err_count);

    if err_count > 0 {
        std::process::exit(1);
    }

    Ok(())
}

/// Search for message types matching a pattern.
fn search_types(reader: &robocodec::RoboReader, pattern: &str) {
    let pattern_lower = pattern.to_lowercase();

    println!("=== Searching for '{}' ===", pattern);
    println!();

    for ch in reader.channels().values() {
        let msg_type_lower = ch.message_type.to_lowercase();
        let topic_lower = ch.topic.to_lowercase();

        if msg_type_lower.contains(&pattern_lower) || topic_lower.contains(&pattern_lower) {
            println!("Type: {}", ch.message_type);
            println!("Topic: {}", ch.topic);
            println!("Encoding: {}", ch.schema_encoding.as_deref().unwrap_or("unknown"));

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
}

/// Show only standard/common ROS message types.
fn show_common_types(reader: &robocodec::RoboReader) {
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

    if !found_any {
        println!("(no standard ROS types found)");
    }
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
