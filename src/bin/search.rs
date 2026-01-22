// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Unified search and analysis tool for robotics data files.
//!
//! Usage:
//!   search bytes <file> <pattern>           - Search for byte pattern in file
//!   search string <file> <text>             - Search for UTF-8 string in file
//!   search topics <file> <pattern>          - Find topics matching pattern
//!   search fields <file> <topic>            - Show field names for a topic
//!   search values <file> <topic> <field>    - Find values for a field
//!   search stats <file>                     - Show file statistics

use std::env;
use std::path::Path;

enum Command {
    Bytes {
        file: String,
        pattern: Vec<u8>,
    },
    String {
        file: String,
        text: String,
    },
    Topics {
        file: String,
        pattern: String,
    },
    Fields {
        file: String,
        topic: String,
    },
    Values {
        file: String,
        topic: String,
        field: String,
    },
    Stats {
        file: String,
    },
}

fn parse_args(args: &[String]) -> Result<Command, String> {
    if args.len() < 3 {
        return Err(format!(
            "Usage: {} <command> <file> [options]\n\
             Commands:\n\
               bytes <file> <hex_pattern>       - Search for hex byte pattern (e.g. \"1a ff 00\")\n\
               string <file> <text>             - Search for UTF-8 string in file\n\
               topics <file> <pattern>          - Find topics matching pattern\n\
               fields <file> <topic>            - Show field names for a topic\n\
               values <file> <topic> <field>    - Find values for a field across messages\n\
               stats <file>                     - Show file statistics",
            args[0]
        ));
    }

    let command = &args[1];
    let file = args[2].clone();

    let cmd = match command.as_str() {
        "bytes" => {
            if args.len() < 4 {
                return Err("bytes command requires a hex pattern argument".to_string());
            }
            let pattern_str = &args[3];
            let pattern: Result<Vec<u8>, _> = pattern_str
                .split_whitespace()
                .map(|s| u8::from_str_radix(s, 16))
                .collect();
            let pattern = pattern.map_err(|_| "invalid hex pattern".to_string())?;
            Command::Bytes { file, pattern }
        }
        "string" => {
            if args.len() < 4 {
                return Err("string command requires a text argument".to_string());
            }
            let text = args[3].clone();
            Command::String { file, text }
        }
        "topics" => {
            if args.len() < 4 {
                return Err("topics command requires a pattern argument".to_string());
            }
            let pattern = args[3].clone();
            Command::Topics { file, pattern }
        }
        "fields" => {
            if args.len() < 4 {
                return Err("fields command requires a topic argument".to_string());
            }
            let topic = args[3].clone();
            Command::Fields { file, topic }
        }
        "values" => {
            if args.len() < 5 {
                return Err("values command requires topic and field arguments".to_string());
            }
            let topic = args[3].clone();
            let field = args[4].clone();
            Command::Values { file, topic, field }
        }
        "stats" => Command::Stats { file },
        _ => return Err(format!("Unknown command: {command}")),
    };

    Ok(cmd)
}

fn run_search(cmd: Command) -> Result<(), Box<dyn std::error::Error>> {
    match cmd {
        Command::Bytes { file, pattern } => search_bytes(&file, &pattern),
        Command::String { file, text } => search_string(&file, &text),
        Command::Topics { file, pattern } => search_topics(&file, &pattern),
        Command::Fields { file, topic } => show_fields(&file, &topic),
        Command::Values { file, topic, field } => show_values(&file, &topic, &field),
        Command::Stats { file } => show_stats(&file),
    }
}

/// Search for byte pattern in file.
fn search_bytes(file: &str, pattern: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(file)?;

    println!("Searching for byte pattern: {:02x?}", pattern);
    println!("File size: {} bytes", data.len());
    println!();

    let mut found_count = 0;
    let mut search_pos = 0;

    while search_pos + pattern.len() <= data.len() {
        if let Some(pos) = data[search_pos..]
            .windows(pattern.len())
            .position(|w| w == pattern)
        {
            let actual_pos = search_pos + pos;
            found_count += 1;

            println!("Found at offset: 0x{:08x} ({})", actual_pos, actual_pos);

            // Show context (16 bytes before and after)
            let start = actual_pos.saturating_sub(16);
            let end = (actual_pos + 16 + pattern.len()).min(data.len());

            println!("  Context:");
            for (i, chunk) in data[start..end].chunks(16).enumerate() {
                let offset = start + i * 16;
                print!("    {:08x}: ", offset);
                for (j, b) in chunk.iter().enumerate() {
                    if offset + j >= actual_pos && offset + j < actual_pos + pattern.len() {
                        // Highlight matched bytes
                        print!("*{:02x}* ", b);
                    } else {
                        print!("{:02x} ", b);
                    }
                }
                println!();
            }
            println!();

            search_pos = actual_pos + pattern.len();

            if found_count >= 10 {
                println!("(... showing first 10 occurrences)");
                break;
            }
        } else {
            break;
        }
    }

    if found_count == 0 {
        println!("Pattern not found");
    } else {
        println!("Total occurrences: {}", found_count);
    }

    Ok(())
}

/// Search for UTF-8 string in file.
fn search_string(file: &str, text: &str) -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(file)?;

    println!("Searching for string: {:?}", text);
    println!("File size: {} bytes", data.len());
    println!();

    let pattern = text.as_bytes();
    let mut found_count = 0;
    let mut search_pos = 0;

    while search_pos + pattern.len() <= data.len() {
        if let Some(pos) = data[search_pos..]
            .windows(pattern.len())
            .position(|w| w == pattern)
        {
            let actual_pos = search_pos + pos;
            found_count += 1;

            println!("Found at offset: 0x{:08x} ({})", actual_pos, actual_pos);

            // Show surrounding text
            let start = actual_pos.saturating_sub(32);
            let end = (actual_pos + 32 + pattern.len()).min(data.len());

            print!("  Context: \"");
            for (i, &b) in data[start..end].iter().enumerate() {
                let abs_pos = start + i;
                if abs_pos >= actual_pos && abs_pos < actual_pos + pattern.len() {
                    print!(">>>{}<<<", b as char);
                } else if (32..=126).contains(&b) {
                    print!("{}", b as char);
                } else if b == b'\n' {
                    print!("\\n");
                } else if b == b'\r' {
                    print!("\\r");
                } else if b == b'\t' {
                    print!("\\t");
                } else {
                    print!("\\x{:02x}", b);
                }
            }
            println!("\"");
            println!();

            search_pos = actual_pos + pattern.len();

            if found_count >= 10 {
                println!("(... showing first 10 occurrences)");
                break;
            }
        } else {
            break;
        }
    }

    if found_count == 0 {
        println!("String not found");
    } else {
        println!("Total occurrences: {}", found_count);
    }

    Ok(())
}

/// Find topics matching pattern.
fn search_topics(file: &str, pattern: &str) -> Result<(), Box<dyn std::error::Error>> {
    let ext = Path::new(file)
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_lowercase();

    let pattern_lower = pattern.to_lowercase();
    let mut found = false;

    match ext.as_str() {
        "mcap" => {
            let reader = robocodec::mcap::McapReader::open(file)?;
            println!("Searching for topics matching: {:?}", pattern);
            println!();

            for channel in reader.channels().values() {
                if channel.topic.to_lowercase().contains(&pattern_lower)
                    || channel.message_type.to_lowercase().contains(&pattern_lower)
                {
                    found = true;
                    println!("Topic: {}", channel.topic);
                    println!("  Type: {}", channel.message_type);
                    println!("  Messages: {}", channel.message_count);
                    println!();
                }
            }
        }
        "bag" => {
            use robocodec::io::traits::FormatReader;
            let reader = robocodec::bag::BagFormat::open(file)?;
            println!("Searching for topics matching: {:?}", pattern);
            println!();

            for channel in reader.channels().values() {
                if channel.topic.to_lowercase().contains(&pattern_lower)
                    || channel.message_type.to_lowercase().contains(&pattern_lower)
                {
                    found = true;
                    println!("Topic: {}", channel.topic);
                    println!("  Type: {}", channel.message_type);
                    println!("  Messages: {}", channel.message_count);
                    println!();
                }
            }
        }
        _ => {
            // Try MCAP first
            match robocodec::mcap::McapReader::open(file) {
                Ok(reader) => {
                    println!("Searching for topics matching: {:?}", pattern);
                    println!();

                    for channel in reader.channels().values() {
                        if channel.topic.to_lowercase().contains(&pattern_lower)
                            || channel.message_type.to_lowercase().contains(&pattern_lower)
                        {
                            found = true;
                            println!("Topic: {}", channel.topic);
                            println!("  Type: {}", channel.message_type);
                            println!("  Messages: {}", channel.message_count);
                            println!();
                        }
                    }
                }
                Err(_) => {
                    use robocodec::io::traits::FormatReader;
                    let reader = robocodec::bag::BagFormat::open(file)?;
                    println!("Searching for topics matching: {:?}", pattern);
                    println!();

                    for channel in reader.channels().values() {
                        if channel.topic.to_lowercase().contains(&pattern_lower)
                            || channel.message_type.to_lowercase().contains(&pattern_lower)
                        {
                            found = true;
                            println!("Topic: {}", channel.topic);
                            println!("  Type: {}", channel.message_type);
                            println!("  Messages: {}", channel.message_count);
                            println!();
                        }
                    }
                }
            }
        }
    }

    if !found {
        println!("No matching topics found");
    }

    Ok(())
}

/// Show field names for a topic.
fn show_fields(file: &str, topic: &str) -> Result<(), Box<dyn std::error::Error>> {
    let ext = Path::new(file)
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_lowercase();

    let (channel, message_type, schema, schema_encoding): (String, String, String, Option<String>) =
        match ext.as_str() {
            "mcap" => {
                let reader = robocodec::mcap::McapReader::open(file)?;

                let channel = reader
                    .channels()
                    .values()
                    .find(|ch| ch.topic == topic || ch.topic.contains(topic));

                let channel = match channel {
                    Some(ch) => ch,
                    None => {
                        eprintln!("Topic '{}' not found", topic);
                        eprintln!();
                        eprintln!("Available topics:");
                        for ch in reader.channels().values() {
                            eprintln!("  {}", ch.topic);
                        }
                        std::process::exit(1);
                    }
                };

                let schema = channel.schema.clone().unwrap_or_default();
                let schema_encoding = channel.schema_encoding.clone();
                (
                    channel.topic.clone(),
                    channel.message_type.clone(),
                    schema,
                    schema_encoding,
                )
            }
            "bag" => {
                use robocodec::io::traits::FormatReader;
                let reader = robocodec::bag::BagFormat::open(file)?;

                let channel = reader
                    .channels()
                    .values()
                    .find(|ch| ch.topic == topic || ch.topic.contains(topic));

                let channel = match channel {
                    Some(ch) => ch,
                    None => {
                        eprintln!("Topic '{}' not found", topic);
                        eprintln!();
                        eprintln!("Available topics:");
                        for ch in reader.channels().values() {
                            eprintln!("  {}", ch.topic);
                        }
                        std::process::exit(1);
                    }
                };

                let schema = channel.schema.clone().unwrap_or_default();
                let schema_encoding = channel.schema_encoding.clone();
                (
                    channel.topic.clone(),
                    channel.message_type.clone(),
                    schema,
                    schema_encoding,
                )
            }
            _ => {
                // Try MCAP first
                match robocodec::mcap::McapReader::open(file) {
                    Ok(reader) => {
                        let channel = reader
                            .channels()
                            .values()
                            .find(|ch| ch.topic == topic || ch.topic.contains(topic));

                        let channel = match channel {
                            Some(ch) => ch,
                            None => {
                                eprintln!("Topic '{}' not found", topic);
                                std::process::exit(1);
                            }
                        };

                        let schema = channel.schema.clone().unwrap_or_default();
                        let schema_encoding = channel.schema_encoding.clone();
                        (
                            channel.topic.clone(),
                            channel.message_type.clone(),
                            schema,
                            schema_encoding,
                        )
                    }
                    Err(_) => {
                        use robocodec::io::traits::FormatReader;
                        let reader = robocodec::bag::BagFormat::open(file)?;

                        let channel = reader
                            .channels()
                            .values()
                            .find(|ch| ch.topic == topic || ch.topic.contains(topic));

                        let channel = match channel {
                            Some(ch) => ch,
                            None => {
                                eprintln!("Topic '{}' not found", topic);
                                std::process::exit(1);
                            }
                        };

                        let schema = channel.schema.clone().unwrap_or_default();
                        let schema_encoding = channel.schema_encoding.clone();
                        (
                            channel.topic.clone(),
                            channel.message_type.clone(),
                            schema,
                            schema_encoding,
                        )
                    }
                }
            }
        };

    println!("Fields for topic: {}", channel);
    println!("Message type: {}", message_type);
    println!();

    if schema.is_empty() {
        println!("(no schema available)");
        return Ok(());
    }

    // Parse the schema and extract field names
    let parsed = robocodec::schema::parser::parse_schema_with_encoding_str(
        &message_type,
        &schema,
        schema_encoding.as_deref().unwrap_or("ros2msg"),
    );

    let parsed = match parsed {
        Ok(p) => p,
        Err(e) => {
            // Fall back to simple schema parsing
            eprintln!("Warning: Failed to parse schema: {}", e);
            println!("Schema (parsed from text):");
            println!();
            print_schema_fields(&schema);
            return Ok(());
        }
    };

    // Display field information from parsed schema
    println!("Schema fields:");
    println!();

    // Get the first message type (main type)
    if let Some(main_type) = parsed.types.values().next() {
        for field in &main_type.fields {
            println!("  {} : {:?}", field.name, field.type_name);
        }
    } else {
        println!("(no types found in schema)");
    }

    Ok(())
}

/// Print fields from schema text (fallback).
fn print_schema_fields(schema: &str) {
    for line in schema.lines() {
        let line = line.trim();
        // Skip empty lines, comments, and header fields
        if line.is_empty()
            || line.starts_with('#')
            || line.starts_with("Header header")
            || line.contains("Header header")
        {
            continue;
        }

        // Try to extract field name and type
        // Format: "type name" or "type name=default_value" or "type name[length]"
        if let Some(space_pos) = line.find(char::is_whitespace) {
            let rest = &line[space_pos..].trim_start();
            if let Some(name_end) = rest.find(|c: char| c == '=' || c == '[' || c.is_whitespace()) {
                let field_name = &rest[..name_end];
                let field_type = &line[..space_pos].trim();
                println!("  {} : {}", field_name, field_type);
            }
        }
    }
}

/// Show values for a field across messages.
/// Note: This currently only works for MCAP files.
fn show_values(file: &str, topic: &str, field: &str) -> Result<(), Box<dyn std::error::Error>> {
    let ext = Path::new(file)
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_lowercase();

    if ext != "mcap" {
        eprintln!("Error: The 'values' command currently only supports MCAP files");
        eprintln!("For BAG files, use 'inspect messages' to see message data");
        std::process::exit(1);
    }

    let reader = robocodec::mcap::McapReader::open(file)?;

    println!("Searching for field '{}' in topic '{}'", field, topic);
    println!();

    // Find the channel
    let target_channel = reader
        .channels()
        .values()
        .find(|ch| ch.topic == topic || ch.topic.contains(topic))
        .cloned();

    let target_channel = match target_channel {
        Some(ch) => ch,
        None => {
            eprintln!("Topic '{}' not found", topic);
            std::process::exit(1);
        }
    };

    let mut found_count = 0;
    let field_lower = field.to_lowercase();

    // Decode messages and search for the field
    for result in reader.decode_messages()? {
        let (msg, channel_info) = result?;

        if channel_info.id != target_channel.id {
            continue;
        }

        // Search for the field in the decoded message
        for (key, value) in msg.iter() {
            if key.to_lowercase().contains(&field_lower) {
                found_count += 1;

                if found_count == 1 {
                    println!(
                        "Found field '{}' with {} messages:",
                        key, channel_info.topic
                    );
                    println!();
                }

                println!(
                    "  Message {}: {} = {}",
                    found_count,
                    key,
                    format_value(value)
                );
                println!();

                if found_count >= 10 {
                    println!("(... showing first 10 occurrences)");
                    break;
                }
            }
        }
    }

    if found_count == 0 {
        println!("Field '{}' not found in topic '{}'", field, topic);
    }

    Ok(())
}

/// Format a CodecValue for display.
fn format_value(value: &roboflow::CodecValue) -> String {
    match value {
        roboflow::CodecValue::Bool(b) => b.to_string(),
        roboflow::CodecValue::UInt8(n) => n.to_string(),
        roboflow::CodecValue::UInt16(n) => n.to_string(),
        roboflow::CodecValue::UInt32(n) => n.to_string(),
        roboflow::CodecValue::UInt64(n) => n.to_string(),
        roboflow::CodecValue::Int8(n) => n.to_string(),
        roboflow::CodecValue::Int16(n) => n.to_string(),
        roboflow::CodecValue::Int32(n) => n.to_string(),
        roboflow::CodecValue::Int64(n) => n.to_string(),
        roboflow::CodecValue::Float32(n) => n.to_string(),
        roboflow::CodecValue::Float64(n) => n.to_string(),
        roboflow::CodecValue::String(s) => format!("\"{}\"", s),
        roboflow::CodecValue::Bytes(b) => format!("[{} bytes]", b.len()),
        roboflow::CodecValue::Array(_) => "[array]".to_string(),
        roboflow::CodecValue::Struct(_) => "[struct]".to_string(),
        roboflow::CodecValue::Null => "[null]".to_string(),
        roboflow::CodecValue::Timestamp(_) => "[timestamp]".to_string(),
        roboflow::CodecValue::Duration(_) => "[duration]".to_string(),
    }
}

/// Show file statistics.
fn show_stats(file: &str) -> Result<(), Box<dyn std::error::Error>> {
    let ext = Path::new(file)
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_lowercase();

    println!("=== File Statistics ===");
    println!();
    println!("File: {}", file);

    match ext.as_str() {
        "mcap" => {
            let reader = robocodec::mcap::McapReader::open(file)?;
            println!("Channels: {}", reader.channels().len());
            println!("Messages: {}", reader.message_count());

            if let (Some(start), Some(end)) = (reader.start_time(), reader.end_time()) {
                let duration = (end - start) / 1_000_000_000;
                let start_sec = start / 1_000_000_000;
                let end_sec = end / 1_000_000_000;
                println!("Start time: {} s ({})", start_sec, start);
                println!("End time: {} s ({})", end_sec, end);
                println!("Duration: {} s", duration);
            }

            println!();
            println!("=== Channel Details ===");
            println!();

            let mut channel_msgs: Vec<_> = reader.channels().values().collect();
            channel_msgs.sort_by(|a, b| b.message_count.cmp(&a.message_count));

            for channel in channel_msgs {
                let percentage = if reader.message_count() > 0 {
                    (channel.message_count as f64 / reader.message_count() as f64) * 100.0
                } else {
                    0.0
                };
                println!(
                    "  {}: {} ({:.1}% of messages)",
                    channel.topic, channel.message_count, percentage
                );
                println!("    Type: {}", channel.message_type);
                println!();
            }
        }
        "bag" => {
            use robocodec::io::traits::FormatReader;
            let reader = robocodec::bag::BagFormat::open(file)?;
            println!("Channels: {}", reader.channels().len());
            println!("Messages: {}", reader.message_count());

            if let (Some(start), Some(end)) = (reader.start_time(), reader.end_time()) {
                let duration = (end - start) / 1_000_000_000;
                let start_sec = start / 1_000_000_000;
                let end_sec = end / 1_000_000_000;
                println!("Start time: {} s ({})", start_sec, start);
                println!("End time: {} s ({})", end_sec, end);
                println!("Duration: {} s", duration);
            }

            println!();
            println!("=== Channel Details ===");
            println!();

            let mut channel_msgs: Vec<_> = reader.channels().values().collect();
            channel_msgs.sort_by(|a, b| b.message_count.cmp(&a.message_count));

            for channel in channel_msgs {
                let percentage = if reader.message_count() > 0 {
                    (channel.message_count as f64 / reader.message_count() as f64) * 100.0
                } else {
                    0.0
                };
                println!(
                    "  {}: {} ({:.1}% of messages)",
                    channel.topic, channel.message_count, percentage
                );
                println!("    Type: {}", channel.message_type);
                println!();
            }
        }
        _ => {
            // Try MCAP first
            match robocodec::mcap::McapReader::open(file) {
                Ok(reader) => {
                    println!("Channels: {}", reader.channels().len());
                    println!("Messages: {}", reader.message_count());

                    if let (Some(start), Some(end)) = (reader.start_time(), reader.end_time()) {
                        let duration = (end - start) / 1_000_000_000;
                        println!("Duration: {} s", duration);
                    }

                    println!();
                    println!("=== Channel Details ===");
                    println!();

                    for channel in reader.channels().values() {
                        println!("  {}: {}", channel.topic, channel.message_count);
                        println!("    Type: {}", channel.message_type);
                        println!();
                    }
                }
                Err(_) => {
                    use robocodec::io::traits::FormatReader;
                    let reader = robocodec::bag::BagFormat::open(file)?;
                    println!("Channels: {}", reader.channels().len());
                    println!("Messages: {}", reader.message_count());

                    if let (Some(start), Some(end)) = (reader.start_time(), reader.end_time()) {
                        let duration = (end - start) / 1_000_000_000;
                        println!("Duration: {} s", duration);
                    }

                    println!();
                    println!("=== Channel Details ===");
                    println!();

                    for channel in reader.channels().values() {
                        println!("  {}: {}", channel.topic, channel.message_count);
                        println!("    Type: {}", channel.message_type);
                        println!();
                    }
                }
            }
        }
    }

    Ok(())
}

fn main() {
    let args: Vec<String> = env::args().collect();

    let cmd = match parse_args(&args) {
        Ok(cmd) => cmd,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    };

    if let Err(e) = run_search(cmd) {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}
