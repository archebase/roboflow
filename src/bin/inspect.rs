//! Unified robotics data inspector for MCAP and BAG files.
//!
//! Usage:
//!   inspect info <file>              - Show file info and channel list
//!   inspect topics <file>            - List topics with message types
//!   inspect channels <file>          - Detailed channel information
//!   inspect schema <file> [topic]    - Show schema for a topic (or all)
//!   inspect messages <file> [n]      - Show sample messages (default: 3)
//!   inspect hex <file> [n]           - Hex dump of first n messages

use std::collections::HashMap;
use std::env;
use std::path::Path;

enum Command {
    Info,
    Topics,
    Channels,
    Schema { topic: Option<String> },
    Messages { count: usize },
    Hex { count: usize },
}

fn parse_args(args: &[String]) -> Result<(String, Command), String> {
    if args.len() < 3 {
        return Err(format!(
            "Usage: {} <command> <file> [options]\n\
             Commands:\n\
               info <file>              - Show file info and channel list\n\
               topics <file>            - List topics with message types\n\
               channels <file>          - Detailed channel information\n\
               schema <file> [topic]    - Show schema for topic (or all)\n\
               messages <file> [n]      - Show sample messages (default: 3)\n\
               hex <file> [n]           - Hex dump of first n messages (default: 1)",
            args[0]
        ));
    }

    let command = &args[1];
    let file = args[2].clone();

    let cmd = match command.as_str() {
        "info" => Command::Info,
        "topics" => Command::Topics,
        "channels" => Command::Channels,
        "schema" => {
            let topic = args.get(4).cloned();
            Command::Schema { topic }
        }
        "messages" => {
            let count = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(3);
            Command::Messages { count }
        }
        "hex" => {
            let count = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(1);
            Command::Hex { count }
        }
        _ => {
            return Err(format!("Unknown command: {command}"));
        }
    };

    Ok((file, cmd))
}

fn run_inspect(file: &str, cmd: Command) -> Result<(), Box<dyn std::error::Error>> {
    let reader = robocodec::RoboReader::open(file)?;

    match cmd {
        Command::Info => show_info(&reader, file)?,
        Command::Topics => show_topics(&reader, file)?,
        Command::Channels => show_channels(&reader)?,
        Command::Schema { topic } => show_schema(&reader, topic.as_deref())?,
        Command::Messages { count } => show_messages(&reader, count)?,
        Command::Hex { count } => show_hex_dump(&reader, count)?,
    }

    Ok(())
}

fn show_info(reader: &robocodec::RoboReader, file: &str) -> Result<(), Box<dyn std::error::Error>> {
    let ext = Path::new(file)
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown");

    println!("=== Robotics Data File: {file} ===");
    println!("Format: {ext}");

    let channels = reader.channels();
    println!("Channels: {}", channels.len());
    println!("Message count: {}", reader.message_count());

    if let (Some(start), Some(end)) = (reader.start_time(), reader.end_time()) {
        println!("Duration: {}s", (end - start) / 1_000_000_000);
    }
    println!();

    println!("Channels:");
    for (&id, ch) in reader.channels() {
        println!(
            "  [{}] {} | {} | {}",
            id, ch.topic, ch.message_type, ch.encoding
        );
    }

    Ok(())
}

fn show_topics(
    reader: &robocodec::RoboReader,
    file: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Topics in {file} ===");
    println!();

    for channel in reader.channels().values() {
        println!("Topic: {}", channel.topic);
        println!("  Type: {}", channel.message_type);
        println!("  Encoding: {}", channel.encoding);
        println!("  Messages: {}", channel.message_count);

        if let Some(encoding) = &channel.schema_encoding {
            println!("  Schema encoding: {}", encoding);
        }

        // Check for ROS1 header that needs special handling
        if let Some(schema) = &channel.schema {
            if schema.trim().starts_with("Header header") {
                println!("  Note: Schema has ROS1 Header (will be handled for ROS1)");
            }
        }
        println!();
    }

    Ok(())
}

fn show_channels(reader: &robocodec::RoboReader) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Detailed Channel Information ===");
    println!();

    for (&id, ch) in reader.channels() {
        println!("Channel ID: {}", id);
        println!("  Topic: {}", ch.topic);
        println!("  Message Type: {}", ch.message_type);
        println!("  Encoding: {}", ch.encoding);
        println!("  Schema Encoding: {:?}", ch.schema_encoding);
        println!("  Message Count: {}", ch.message_count);

        if let Some(schema) = &ch.schema {
            let preview: String = schema.chars().take(300).collect();
            println!("  Schema (preview):");
            for line in preview.lines() {
                println!("    {}", line);
            }
            if schema.len() > 300 {
                println!("    ... ({} bytes total)", schema.len());
            }
        }
        println!();
    }

    Ok(())
}

fn show_schema(
    reader: &robocodec::RoboReader,
    topic_filter: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Schema Definitions ===");
    println!();

    for ch in reader.channels().values() {
        if let Some(filter) = topic_filter {
            if !ch.topic.contains(filter) && !ch.message_type.contains(filter) {
                continue;
            }
        }

        println!("=== {} ===", ch.topic);
        println!("Type: {}", ch.message_type);
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

    Ok(())
}

fn show_messages(
    reader: &robocodec::RoboReader,
    sample_count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Sample Messages (first {sample_count} per channel) ===");
    println!();

    let iter = reader.iter_raw()?;
    let mut stream = iter.into_stream()?;
    let mut counts: HashMap<u16, usize> = HashMap::new();

    while let Some(result) = stream.next() {
        let (msg, channel_info) = result?;
        let count = counts.entry(msg.channel_id).or_insert(0);
        *count += 1;

        if *count <= sample_count {
            println!("Channel {} ({})", msg.channel_id, channel_info.topic);
            println!("  Type: {}", channel_info.message_type);
            println!("  Log time: {} ns", msg.log_time);
            println!("  Publish time: {} ns", msg.publish_time);
            println!("  Data: {} bytes", msg.data.len());
            println!();
        }
    }

    Ok(())
}

fn show_hex_dump(
    reader: &robocodec::RoboReader,
    sample_count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Hex Dump (first {sample_count} messages per channel) ===");
    println!();

    let iter = reader.iter_raw()?;
    let mut stream = iter.into_stream()?;
    let mut counts: HashMap<u16, usize> = HashMap::new();

    while let Some(result) = stream.next() {
        let (msg, channel_info) = result?;
        let count = counts.entry(msg.channel_id).or_insert(0);
        *count += 1;

        if *count <= sample_count {
            println!("Channel {} ({})", msg.channel_id, channel_info.topic);
            println!("  Type: {}", channel_info.message_type);
            println!("  Data (first 128 bytes):");

            for (i, chunk) in msg.data.chunks(32).enumerate() {
                print!("    {:04x}: ", i * 32);
                for (j, byte) in chunk.iter().enumerate() {
                    print!("{:02x} ", byte);
                    if (j + 1) % 8 == 0 {
                        print!(" ");
                    }
                }
                println!();
                if i >= 3 {
                    break;
                }
            }
            println!();
        }
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

    if let Err(e) = run_inspect(&file, cmd) {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}
