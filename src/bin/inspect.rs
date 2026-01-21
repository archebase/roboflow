//! Unified robotics data inspector for MCAP and BAG files.
//!
//! Usage:
//!   inspect info <file>              - Show file info and channel list
//!   inspect topics <file>            - List topics with message types
//!   inspect channels <file>          - Detailed channel information
//!   inspect schema <file> [topic]    - Show schema for a topic (or all)
//!   inspect messages <file> [n]      - Show sample messages (default: 3)
//!   inspect hex <file> [n]           - Hex dump of first n messages
//!   inspect chunks <file>            - Show chunk size information

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
    Chunks,
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
               hex <file> [n]           - Hex dump of first n messages (default: 1)\n\
               chunks <file>            - Show chunk size information",
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
        "chunks" => Command::Chunks,
        _ => {
            return Err(format!("Unknown command: {command}"));
        }
    };

    Ok((file, cmd))
}

fn run_inspect(file: &str, cmd: Command) -> Result<(), Box<dyn std::error::Error>> {
    let ext = Path::new(file)
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown");

    match cmd {
        Command::Info => show_info(file, ext)?,
        Command::Topics => show_topics(file, ext)?,
        Command::Channels => show_channels(file, ext)?,
        Command::Schema { topic } => show_schema(file, ext, topic.as_deref())?,
        Command::Messages { count } => show_messages(file, ext, count)?,
        Command::Hex { count } => show_hex_dump(file, ext, count)?,
        Command::Chunks => show_chunks(file, ext)?,
    }

    Ok(())
}

fn show_info(file: &str, ext: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Robotics Data File: {file} ===");
    println!("Format: {ext}");

    match ext {
        "mcap" => {
            let reader = robocodec::mcap::McapReader::open(file)?;
            println!("Channels: {}", reader.channels().len());
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
        }
        "bag" => {
            use robocodec::io::traits::FormatReader;
            let reader = robocodec::BagFormat::open(file)?;
            println!("Channels: {}", reader.channels().len());
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
        }
        _ => {
            // Try MCAP first
            match robocodec::mcap::McapReader::open(file) {
                Ok(reader) => {
                    println!("Channels: {}", reader.channels().len());
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
                }
                Err(_) => {
                    use robocodec::io::traits::FormatReader;
                    let reader = robocodec::BagFormat::open(file)?;
                    println!("Channels: {}", reader.channels().len());
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
                }
            }
        }
    }

    Ok(())
}

fn show_topics(file: &str, ext: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Topics in {file} ===");
    println!();

    match ext {
        "mcap" => {
            let reader = robocodec::mcap::McapReader::open(file)?;
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
        }
        "bag" => {
            use robocodec::io::traits::FormatReader;
            let reader = robocodec::BagFormat::open(file)?;
            for channel in reader.channels().values() {
                println!("Topic: {}", channel.topic);
                println!("  Type: {}", channel.message_type);
                println!("  Encoding: {}", channel.encoding);
                println!("  Messages: {}", channel.message_count);

                if let Some(encoding) = &channel.schema_encoding {
                    println!("  Schema encoding: {}", encoding);
                }

                if let Some(schema) = &channel.schema {
                    if schema.trim().starts_with("Header header") {
                        println!("  Note: Schema has ROS1 Header (will be handled for ROS1)");
                    }
                }
                println!();
            }
        }
        _ => {
            // Try MCAP first
            match robocodec::mcap::McapReader::open(file) {
                Ok(reader) => {
                    for channel in reader.channels().values() {
                        println!("Topic: {}", channel.topic);
                        println!("  Type: {}", channel.message_type);
                        println!("  Encoding: {}", channel.encoding);
                        println!("  Messages: {}", channel.message_count);
                        println!();
                    }
                }
                Err(_) => {
                    use robocodec::io::traits::FormatReader;
                    let reader = robocodec::BagFormat::open(file)?;
                    for channel in reader.channels().values() {
                        println!("Topic: {}", channel.topic);
                        println!("  Type: {}", channel.message_type);
                        println!("  Encoding: {}", channel.encoding);
                        println!("  Messages: {}", channel.message_count);
                        println!();
                    }
                }
            }
        }
    }

    Ok(())
}

fn show_channels(file: &str, ext: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Detailed Channel Information ===");
    println!();

    match ext {
        "mcap" => {
            let reader = robocodec::mcap::McapReader::open(file)?;
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
        }
        "bag" => {
            use robocodec::io::traits::FormatReader;
            let reader = robocodec::BagFormat::open(file)?;
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
        }
        _ => {
            // Try MCAP first
            match robocodec::mcap::McapReader::open(file) {
                Ok(reader) => {
                    for (&id, ch) in reader.channels() {
                        println!("Channel ID: {}", id);
                        println!("  Topic: {}", ch.topic);
                        println!("  Message Type: {}", ch.message_type);
                        println!("  Encoding: {}", ch.encoding);
                        println!();
                    }
                }
                Err(_) => {
                    use robocodec::io::traits::FormatReader;
                    let reader = robocodec::BagFormat::open(file)?;
                    for (&id, ch) in reader.channels() {
                        println!("Channel ID: {}", id);
                        println!("  Topic: {}", ch.topic);
                        println!("  Message Type: {}", ch.message_type);
                        println!("  Encoding: {}", ch.encoding);
                        println!();
                    }
                }
            }
        }
    }

    Ok(())
}

fn show_schema(
    file: &str,
    ext: &str,
    topic_filter: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Schema Definitions ===");
    println!();

    match ext {
        "mcap" => {
            let reader = robocodec::mcap::McapReader::open(file)?;
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
        }
        "bag" => {
            use robocodec::io::traits::FormatReader;
            let reader = robocodec::BagFormat::open(file)?;
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
        }
        _ => {
            // Try MCAP first
            match robocodec::mcap::McapReader::open(file) {
                Ok(reader) => {
                    for ch in reader.channels().values() {
                        if let Some(filter) = topic_filter {
                            if !ch.topic.contains(filter) && !ch.message_type.contains(filter) {
                                continue;
                            }
                        }

                        println!("=== {} ===", ch.topic);
                        println!("Type: {}", ch.message_type);
                        println!();

                        if let Some(schema) = &ch.schema {
                            println!("{}", schema);
                        } else {
                            println!("(no schema available)");
                        }
                        println!();
                    }
                }
                Err(_) => {
                    use robocodec::io::traits::FormatReader;
                    let reader = robocodec::BagFormat::open(file)?;
                    for ch in reader.channels().values() {
                        if let Some(filter) = topic_filter {
                            if !ch.topic.contains(filter) && !ch.message_type.contains(filter) {
                                continue;
                            }
                        }

                        println!("=== {} ===", ch.topic);
                        println!("Type: {}", ch.message_type);
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

    Ok(())
}

fn show_messages(
    file: &str,
    ext: &str,
    sample_count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Sample Messages (first {sample_count} per channel) ===");
    println!();

    match ext {
        "mcap" => {
            let reader = robocodec::mcap::McapReader::open(file)?;
            let iter = reader.iter_raw()?;
            let stream = iter.stream()?;
            let mut counts: HashMap<u16, usize> = HashMap::new();

            for result in stream {
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
        }
        "bag" => {
            let reader = robocodec::BagFormat::open(file)?;
            let iter = reader.iter_raw()?;
            let mut counts: HashMap<u16, usize> = HashMap::new();

            for result in iter {
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
        }
        _ => {
            // Try MCAP first
            match robocodec::mcap::McapReader::open(file) {
                Ok(reader) => {
                    let iter = reader.iter_raw()?;
                    let stream = iter.stream()?;
                    for result in stream.take(sample_count) {
                        let (msg, channel_info) = result?;
                        println!("Channel {} ({})", msg.channel_id, channel_info.topic);
                        println!("  Type: {}", channel_info.message_type);
                        println!("  Data: {} bytes", msg.data.len());
                        println!();
                    }
                }
                Err(_) => {
                    let reader = robocodec::BagFormat::open(file)?;
                    let iter = reader.iter_raw()?;
                    for result in iter.take(sample_count) {
                        let (msg, channel_info) = result?;
                        println!("Channel {} ({})", msg.channel_id, channel_info.topic);
                        println!("  Type: {}", channel_info.message_type);
                        println!("  Data: {} bytes", msg.data.len());
                        println!();
                    }
                }
            }
        }
    }

    Ok(())
}

fn show_hex_dump(
    file: &str,
    ext: &str,
    sample_count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Hex Dump (first {sample_count} messages per channel) ===");
    println!();

    match ext {
        "mcap" => {
            let reader = robocodec::mcap::McapReader::open(file)?;
            let iter = reader.iter_raw()?;
            let stream = iter.stream()?;
            let mut counts: HashMap<u16, usize> = HashMap::new();

            for result in stream {
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
        }
        "bag" => {
            let reader = robocodec::BagFormat::open(file)?;
            let iter = reader.iter_raw()?;
            let mut counts: HashMap<u16, usize> = HashMap::new();

            for result in iter {
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
        }
        _ => {
            // Try MCAP first
            match robocodec::mcap::McapReader::open(file) {
                Ok(reader) => {
                    let iter = reader.iter_raw()?;
                    let stream = iter.stream()?;
                    for result in stream.take(sample_count) {
                        let (msg, channel_info) = result?;
                        println!("Channel {} ({})", msg.channel_id, channel_info.topic);
                        println!("  Data (first 128 bytes):");
                        for (i, chunk) in msg.data.chunks(32).enumerate() {
                            print!("    {:04x}: ", i * 32);
                            for byte in chunk.iter() {
                                print!("{:02x} ", byte);
                            }
                            println!();
                            if i >= 3 {
                                break;
                            }
                        }
                        println!();
                    }
                }
                Err(_) => {
                    let reader = robocodec::BagFormat::open(file)?;
                    let iter = reader.iter_raw()?;
                    for result in iter.take(sample_count) {
                        let (msg, channel_info) = result?;
                        println!("Channel {} ({})", msg.channel_id, channel_info.topic);
                        println!("  Data (first 128 bytes):");
                        for (i, chunk) in msg.data.chunks(32).enumerate() {
                            print!("    {:04x}: ", i * 32);
                            for byte in chunk.iter() {
                                print!("{:02x} ", byte);
                            }
                            println!();
                            if i >= 3 {
                                break;
                            }
                        }
                        println!();
                    }
                }
            }
        }
    }

    Ok(())
}

fn show_chunks(file: &str, ext: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Chunk Information ===");
    println!();

    match ext {
        "mcap" => {
            use robocodec::mcap::ParallelMcapReader;
            let reader = ParallelMcapReader::open(file)?;
            let chunks = reader.chunk_indexes();

            if chunks.is_empty() {
                println!("No chunks found in file.");
                return Ok(());
            }

            println!("Total chunks: {}", chunks.len());
            println!();

            let mut sizes: Vec<usize> = chunks
                .iter()
                .map(|c| c.uncompressed_size as usize)
                .collect();
            sizes.sort();

            let min = *sizes.first().unwrap();
            let max = *sizes.last().unwrap();
            let sum: usize = sizes.iter().sum();
            let avg = sum / sizes.len();
            let median = sizes[sizes.len() / 2];

            println!("Chunk size (uncompressed):");
            println!("  Min: {:.2} MB", min as f64 / (1024.0 * 1024.0));
            println!("  Max: {:.2} MB", max as f64 / (1024.0 * 1024.0));
            println!("  Avg: {:.2} MB", avg as f64 / (1024.0 * 1024.0));
            println!("  Median: {:.2} MB", median as f64 / (1024.0 * 1024.0));
            println!(
                "  Total uncompressed: {:.2} MB",
                sum as f64 / (1024.0 * 1024.0)
            );
            println!();

            // Show compression ratio
            let compressed_sum: u64 = chunks.iter().map(|c| c.compressed_size).sum();
            let compression_ratio = compressed_sum as f64 / sum as f64;
            println!("Compression:");
            println!(
                "  Total compressed: {:.2} MB",
                compressed_sum as f64 / (1024.0 * 1024.0)
            );
            println!("  Compression ratio: {:.2}%", compression_ratio * 100.0);
            println!();

            // Show size distribution
            println!("Size distribution:");
            let max_mb = max / (1024 * 1024) + 1;
            let bucket_count = 10usize;
            let bucket_size = (max_mb / bucket_count).max(1);
            let mut buckets = vec![0usize; bucket_count];

            for size in &sizes {
                let bucket = (*size / (1024 * 1024) / bucket_size).min(bucket_count - 1);
                buckets[bucket] += 1;
            }

            for (i, count) in buckets.iter().enumerate() {
                if *count > 0 {
                    println!(
                        "  {}-{} MB: {} chunks ({:.1}%)",
                        i * bucket_size,
                        (i + 1) * bucket_size,
                        count,
                        (*count as f64 / chunks.len() as f64) * 100.0
                    );
                }
            }

            // WindowLog recommendation for Zstd
            println!();
            println!("Zstd WindowLog recommendation:");
            let max_power_of_2 = max.next_power_of_two();
            let window_log = max_power_of_2.trailing_zeros();
            println!("  Max chunk size: {} bytes (2^{})", max, window_log);
            println!("  Recommended WindowLog: {}", window_log);
        }
        "bag" => {
            use robocodec::bag::ParallelBagReader;
            let reader = ParallelBagReader::open(file)?;
            let chunks = reader.chunks();

            if chunks.is_empty() {
                println!("No chunks found in file.");
                return Ok(());
            }

            println!("Total chunks: {}", chunks.len());
            println!();

            let mut sizes: Vec<usize> = chunks
                .iter()
                .map(|c| c.uncompressed_size as usize)
                .collect();
            sizes.sort();

            let min = *sizes.first().unwrap();
            let max = *sizes.last().unwrap();
            let sum: usize = sizes.iter().sum();
            let avg = sum / sizes.len();
            let median = sizes[sizes.len() / 2];

            println!("Chunk size (uncompressed in BAG):");
            println!("  Min: {:.2} MB", min as f64 / (1024.0 * 1024.0));
            println!("  Max: {:.2} MB", max as f64 / (1024.0 * 1024.0));
            println!("  Avg: {:.2} MB", avg as f64 / (1024.0 * 1024.0));
            println!("  Median: {:.2} MB", median as f64 / (1024.0 * 1024.0));
            println!("  Total: {:.2} MB", sum as f64 / (1024.0 * 1024.0));
            println!();

            // Show compression format distribution
            use std::collections::HashMap;
            let mut compression_counts: HashMap<&str, usize> = HashMap::new();
            for chunk in chunks {
                *compression_counts.entry(&chunk.compression).or_insert(0) += 1;
            }
            println!("Compression formats:");
            for (compression, count) in &compression_counts {
                println!(
                    "  {}: {} chunks ({:.1}%)",
                    compression,
                    count,
                    (*count as f64 / chunks.len() as f64) * 100.0
                );
            }

            // WindowLog recommendation
            println!();
            println!("Zstd WindowLog recommendation:");
            let max_power_of_2 = max.next_power_of_two();
            let window_log = max_power_of_2.trailing_zeros();
            println!("  Max chunk size: {} bytes (2^{})", max, window_log);
            println!("  Recommended WindowLog: {}", window_log);
        }
        _ => {
            // Try MCAP first
            match robocodec::mcap::ParallelMcapReader::open(file) {
                Ok(reader) => {
                    let chunks = reader.chunk_indexes();
                    if !chunks.is_empty() {
                        return show_chunks(file, "mcap");
                    }
                }
                Err(_) => {
                    if let Ok(reader) = robocodec::bag::ParallelBagReader::open(file) {
                        if !reader.chunks().is_empty() {
                            return show_chunks(file, "bag");
                        }
                    }
                }
            }
            println!("No chunk information available for this file format.");
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
