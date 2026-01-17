//! Unified data extraction tool for robotics data files.
//!
//! Usage:
//!   extract messages <input> <output> [count]   - Extract first N messages (default: all)
//!   extract topics <input> <output> <topics>    - Extract only specified topics (comma-separated)
//!   extract per-topic <input> <output> <count>  - Extract N messages per topic
//!   extract fixture <input> <name>              - Create minimal fixture with one message
//!   extract time <input> <output> <start> <end>  - Extract messages in time range (nanoseconds)

use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;

enum Command {
    Messages {
        output: String,
        count: Option<usize>,
    },
    Topics {
        output: String,
        topics: Vec<String>,
    },
    PerTopic {
        output: String,
        count: usize,
    },
    Fixture {
        name: String,
    },
    TimeRange {
        output: String,
        start: u64,
        end: u64,
    },
}

fn parse_args(args: &[String]) -> Result<(String, Command), String> {
    if args.len() < 4 {
        return Err(format!(
            "Usage: {} <command> <input> <output> [options]\n\
             Commands:\n\
               messages <input> <output> [count]   - Extract first N messages (default: all)\n\
               topics <input> <output> <topics>    - Extract only specified topics (comma-separated)\n\
               per-topic <input> <output> <count>  - Extract N messages per topic\n\
               fixture <input> <name>              - Create minimal fixture with one message\n\
               time <input> <output> <start> <end>  - Extract messages in time range (nanoseconds)",
            args[0]
        ));
    }

    let command = &args[1];
    let input = args[2].clone();

    let cmd = match command.as_str() {
        "messages" => {
            let output = args[3].clone();
            let count = args.get(4).and_then(|s| s.parse().ok());
            Command::Messages { output, count }
        }
        "topics" => {
            if args.len() < 5 {
                return Err("topics command requires a comma-separated list of topics".to_string());
            }
            let output = args[3].clone();
            let topics: Vec<String> = args[4].split(',').map(|s| s.trim().to_string()).collect();
            Command::Topics { output, topics }
        }
        "per-topic" => {
            if args.len() < 5 {
                return Err("per-topic command requires a count".to_string());
            }
            let output = args[3].clone();
            let count = args[4].parse().map_err(|_| "invalid count")?;
            if count == 0 {
                return Err("count must be greater than 0".to_string());
            }
            Command::PerTopic { output, count }
        }
        "fixture" => {
            let name = args[3].clone();
            Command::Fixture { name }
        }
        "time" => {
            if args.len() < 6 {
                return Err("time command requires start and end timestamps".to_string());
            }
            let output = args[3].clone();
            let start = args[4].parse().map_err(|_| "invalid start timestamp")?;
            let end = args[5].parse().map_err(|_| "invalid end timestamp")?;
            Command::TimeRange { output, start, end }
        }
        _ => return Err(format!("Unknown command: {command}")),
    };

    Ok((input, cmd))
}

fn run_extract(input: &str, cmd: Command) -> Result<(), Box<dyn std::error::Error>> {
    let ext = Path::new(input)
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_lowercase();

    match cmd {
        Command::Messages { output, count } => {
            if ext == "bag" {
                extract_bag_messages(input, &output, count)?
            } else {
                extract_mcap_messages(input, &output, count)?
            }
        }
        Command::Topics { output, topics } => {
            if ext == "bag" {
                extract_bag_topics(input, &output, &topics)?
            } else {
                extract_mcap_topics(input, &output, &topics)?
            }
        }
        Command::PerTopic { output, count } => extract_per_topic(input, &output, count, &ext)?,
        Command::Fixture { name } => {
            if ext == "bag" {
                create_fixture_from_bag(input, &name)?
            } else {
                create_fixture_from_mcap(input, &name)?
            }
        }
        Command::TimeRange { output, start, end } => {
            if ext == "bag" {
                extract_bag_time_range(input, &output, start, end)?
            } else {
                extract_mcap_time_range(input, &output, start, end)?
            }
        }
    }

    Ok(())
}

/// Extract first N messages from MCAP file.
fn extract_mcap_messages(
    input: &str,
    output: &str,
    count: Option<usize>,
) -> Result<(), Box<dyn std::error::Error>> {
    let reader = robocodec::RoboReader::open(input)?;

    println!("Extracting from MCAP: {}", input);
    println!("Output: {}", output);
    if let Some(n) = count {
        println!("Message limit: {}", n);
    }

    // Create output MCAP
    let output_file = File::create(output)?;
    let mut mcap_writer = mcap::Writer::new(BufWriter::new(output_file))?;

    // Add schemas and channels
    let mut schema_ids: std::collections::HashMap<String, u16> = std::collections::HashMap::new();
    let mut channel_ids: std::collections::HashMap<u16, u16> = std::collections::HashMap::new();
    let mut sequences: std::collections::HashMap<u16, u32> = std::collections::HashMap::new();

    for (&ch_id, channel) in reader.channels() {
        let schema_id = if let Some(schema) = &channel.schema {
            let encoding = channel.schema_encoding.as_deref().unwrap_or("ros2msg");
            *schema_ids
                .entry(channel.message_type.clone())
                .or_insert_with(|| {
                    mcap_writer
                        .add_schema(&channel.message_type, encoding, schema.as_bytes())
                        .unwrap_or(0)
                })
        } else {
            0
        };

        let out_ch_id = mcap_writer.add_channel(
            schema_id,
            &channel.topic,
            &channel.encoding,
            &BTreeMap::new(),
        )?;
        channel_ids.insert(ch_id, out_ch_id);
        sequences.insert(out_ch_id, 0);
    }

    // Copy messages
    let iter = reader.iter_raw()?;
    let stream = iter.into_stream()?;
    let mut written = 0;

    for result in stream {
        if let Some(limit) = count {
            if written >= limit {
                break;
            }
        }

        let (msg, _channel) = result?;
        let out_ch_id = channel_ids.get(&msg.channel_id).copied().unwrap_or(0);

        let seq = *sequences.get(&out_ch_id).unwrap_or(&0);
        mcap_writer.write_to_known_channel(
            &mcap::records::MessageHeader {
                channel_id: out_ch_id,
                sequence: seq,
                log_time: msg.log_time,
                publish_time: msg.publish_time,
            },
            &msg.data,
        )?;

        sequences.insert(out_ch_id, seq + 1);
        written += 1;

        if written % 1000 == 0 {
            println!("Written {} messages...", written);
        }
    }

    drop(mcap_writer);
    println!("Extracted {} messages to {}", written, output);

    Ok(())
}

/// Extract first N messages from BAG file.
fn extract_bag_messages(
    input: &str,
    output: &str,
    count: Option<usize>,
) -> Result<(), Box<dyn std::error::Error>> {
    let reader = robocodec::RoboReader::open(input)?;

    println!("Extracting from BAG: {}", input);
    println!("Output: {}", output);
    if let Some(n) = count {
        println!("Message limit: {}", n);
    }

    let mut writer = robocodec::BagWriter::create(output)?;

    // Copy connections, preserving callerid from the original bag
    for (ch_id, channel) in reader.channels() {
        let schema = channel.schema.as_deref().unwrap_or("");
        let callerid = channel.callerid.as_deref().unwrap_or("");
        writer.add_connection_with_callerid(
            *ch_id,
            &channel.topic,
            &channel.message_type,
            schema,
            callerid,
        )?;
    }

    // Copy messages
    let iter = reader.iter_raw()?;
    let stream = iter.into_stream()?;
    let mut written = 0;

    for result in stream {
        if let Some(limit) = count {
            if written >= limit {
                break;
            }
        }

        let (msg, _channel) = result?;
        let bag_msg = robocodec::BagMessage::from_raw(msg.channel_id, msg.publish_time, msg.data);
        writer.write_message(&bag_msg)?;
        written += 1;

        if written % 100 == 0 {
            println!("Written {} messages...", written);
        }
    }

    writer.finish()?;
    println!("Extracted {} messages to {}", written, output);

    Ok(())
}

/// Extract specific topics from MCAP file.
fn extract_mcap_topics(
    input: &str,
    output: &str,
    topics: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    let reader = robocodec::RoboReader::open(input)?;

    println!("Extracting topics from MCAP: {}", input);
    println!("Topics: {:?}", topics);
    println!("Output: {}", output);

    // Build channel ID filter
    let mut channel_filter = std::collections::HashSet::new();
    for (&ch_id, channel) in reader.channels() {
        for topic in topics {
            if channel.topic == *topic || channel.topic.contains(topic) {
                channel_filter.insert(ch_id);
            }
        }
    }

    if channel_filter.is_empty() {
        eprintln!("No matching topics found");
        std::process::exit(1);
    }

    // Create output MCAP
    let output_file = File::create(output)?;
    let mut mcap_writer = mcap::Writer::new(BufWriter::new(output_file))?;

    // Add schemas and channels for filtered topics
    let mut schema_ids: std::collections::HashMap<String, u16> = std::collections::HashMap::new();
    let mut channel_ids: std::collections::HashMap<u16, u16> = std::collections::HashMap::new();
    let mut sequences: std::collections::HashMap<u16, u32> = std::collections::HashMap::new();

    for (&ch_id, channel) in reader.channels() {
        if !channel_filter.contains(&ch_id) {
            continue;
        }

        let schema_id = if let Some(schema) = &channel.schema {
            let encoding = channel.schema_encoding.as_deref().unwrap_or("ros2msg");
            *schema_ids
                .entry(channel.message_type.clone())
                .or_insert_with(|| {
                    mcap_writer
                        .add_schema(&channel.message_type, encoding, schema.as_bytes())
                        .unwrap_or(0)
                })
        } else {
            0
        };

        let out_ch_id = mcap_writer.add_channel(
            schema_id,
            &channel.topic,
            &channel.encoding,
            &BTreeMap::new(),
        )?;
        channel_ids.insert(ch_id, out_ch_id);
        sequences.insert(out_ch_id, 0);
    }

    // Copy filtered messages
    let iter = reader.iter_raw()?;
    let stream = iter.into_stream()?;
    let mut written = 0;

    for result in stream {
        let (msg, _channel) = result?;

        if let Some(&out_ch_id) = channel_ids.get(&msg.channel_id) {
            let seq = *sequences.get(&out_ch_id).unwrap_or(&0);
            mcap_writer.write_to_known_channel(
                &mcap::records::MessageHeader {
                    channel_id: out_ch_id,
                    sequence: seq,
                    log_time: msg.log_time,
                    publish_time: msg.publish_time,
                },
                &msg.data,
            )?;

            sequences.insert(out_ch_id, seq + 1);
            written += 1;
        }
    }

    drop(mcap_writer);
    println!("Extracted {} messages to {}", written, output);

    Ok(())
}

/// Extract specific topics from BAG file.
fn extract_bag_topics(
    input: &str,
    output: &str,
    topics: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    let reader = robocodec::RoboReader::open(input)?;

    println!("Extracting topics from BAG: {}", input);
    println!("Topics: {:?}", topics);
    println!("Output: {}", output);

    // Build channel ID filter
    let mut channel_filter = std::collections::HashSet::new();
    let mut channel_map: std::collections::HashMap<u16, u16> = std::collections::HashMap::new();
    let mut new_conn_id = 0u16;

    for (&ch_id, channel) in reader.channels() {
        for topic in topics {
            if channel.topic == *topic || channel.topic.contains(topic) {
                channel_filter.insert(ch_id);
                channel_map.insert(ch_id, new_conn_id);
                new_conn_id += 1;
                break;
            }
        }
    }

    if channel_filter.is_empty() {
        eprintln!("No matching topics found");
        std::process::exit(1);
    }

    let mut writer = robocodec::BagWriter::create(output)?;

    // Add filtered connections, preserving callerid
    for (&ch_id, channel) in reader.channels() {
        if let Some(&new_id) = channel_map.get(&ch_id) {
            let schema = channel.schema.as_deref().unwrap_or("");
            let callerid = channel.callerid.as_deref().unwrap_or("");
            writer.add_connection_with_callerid(
                new_id,
                &channel.topic,
                &channel.message_type,
                schema,
                callerid,
            )?;
        }
    }

    // Copy filtered messages
    let iter = reader.iter_raw()?;
    let stream = iter.into_stream()?;
    let mut written = 0;

    for result in stream {
        let (msg, _channel) = result?;

        if let Some(&new_id) = channel_map.get(&msg.channel_id) {
            let bag_msg = robocodec::BagMessage::from_raw(new_id, msg.publish_time, msg.data);
            writer.write_message(&bag_msg)?;
            written += 1;
        }
    }

    writer.finish()?;
    println!("Extracted {} messages to {}", written, output);

    Ok(())
}

/// Extract N messages per topic from BAG or MCAP file.
fn extract_per_topic(
    input: &str,
    output: &str,
    count: usize,
    ext: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let reader = robocodec::RoboReader::open(input)?;

    println!(
        "Extracting {} messages per topic from {}: {}",
        count,
        ext.to_uppercase(),
        input
    );
    println!("Output: {}", output);

    // Track messages written per topic
    let mut messages_per_topic: HashMap<String, usize> = HashMap::new();
    let mut written = 0;

    if ext == "bag" {
        let mut writer = robocodec::BagWriter::create(output)?;

        // Copy all connections, preserving callerid from the original bag
        for (ch_id, channel) in reader.channels() {
            let schema = channel.schema.as_deref().unwrap_or("");
            let callerid = channel.callerid.as_deref().unwrap_or("");
            writer.add_connection_with_callerid(
                *ch_id,
                &channel.topic,
                &channel.message_type,
                schema,
                callerid,
            )?;
        }

        // Copy messages up to count per topic using unified iter_raw
        let iter = reader.iter_raw()?;
        let stream = iter.into_stream()?;

        for result in stream {
            let (msg, channel) = result?;

            let topic = &channel.topic;
            let written_for_topic = messages_per_topic.entry(topic.clone()).or_insert(0);

            if *written_for_topic >= count {
                continue;
            }

            let bag_msg =
                robocodec::BagMessage::from_raw(msg.channel_id, msg.publish_time, msg.data);
            writer.write_message(&bag_msg)?;

            *written_for_topic += 1;
            written += 1;
        }

        writer.finish()?;
    } else {
        // MCAP output
        let output_file = File::create(output)?;
        let mut mcap_writer = mcap::Writer::new(BufWriter::new(output_file))?;

        // Add schemas and channels
        let mut schema_ids: HashMap<String, u16> = HashMap::new();
        let mut channel_ids: HashMap<u16, u16> = HashMap::new();
        let mut sequences: HashMap<u16, u32> = HashMap::new();

        for (&ch_id, channel) in reader.channels() {
            let schema_id = if let Some(schema) = &channel.schema {
                let encoding = channel.schema_encoding.as_deref().unwrap_or("ros2msg");
                *schema_ids
                    .entry(channel.message_type.clone())
                    .or_insert_with(|| {
                        mcap_writer
                            .add_schema(&channel.message_type, encoding, schema.as_bytes())
                            .unwrap_or(0)
                    })
            } else {
                0
            };

            let out_ch_id = mcap_writer.add_channel(
                schema_id,
                &channel.topic,
                &channel.encoding,
                &BTreeMap::new(),
            )?;
            channel_ids.insert(ch_id, out_ch_id);
            sequences.insert(out_ch_id, 0);
        }

        // Copy messages up to count per topic
        let iter = reader.iter_raw()?;
        let stream = iter.into_stream()?;

        for result in stream {
            let (msg, channel) = result?;

            let topic = &channel.topic;
            let written_for_topic = messages_per_topic.entry(topic.clone()).or_insert(0);

            if *written_for_topic >= count {
                continue;
            }

            let out_ch_id = channel_ids.get(&msg.channel_id).copied().unwrap_or(0);
            let seq = *sequences.get(&out_ch_id).unwrap_or(&0);

            mcap_writer.write_to_known_channel(
                &mcap::records::MessageHeader {
                    channel_id: out_ch_id,
                    sequence: seq,
                    log_time: msg.log_time,
                    publish_time: msg.publish_time,
                },
                &msg.data,
            )?;

            sequences.insert(out_ch_id, seq + 1);
            *written_for_topic += 1;
            written += 1;
        }

        drop(mcap_writer);
    }

    println!(
        "Extracted {} messages ({} per topic) to {}",
        written, count, output
    );

    Ok(())
}

/// Create minimal fixture from BAG file.
fn create_fixture_from_bag(input: &str, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("Creating fixture from BAG: {}", input);

    let bag = rosbag::RosBag::new(Path::new(input))?;

    // Find the first message
    for record in bag.chunk_records() {
        let record = record?;
        if let rosbag::ChunkRecord::Chunk(chunk) = record {
            for msg_result in chunk.messages() {
                let msg_result = msg_result?;
                if let rosbag::MessageRecord::MessageData(msg) = msg_result {
                    // Find connection info
                    for conn_record in bag.index_records() {
                        if let Ok(rosbag::IndexRecord::Connection(conn)) = conn_record {
                            if conn.id == msg.conn_id {
                                write_fixture_mcap(
                                    name,
                                    msg.data,
                                    msg.time,
                                    conn.topic,
                                    conn.tp,
                                    conn.message_definition,
                                )?;
                                return Ok(());
                            }
                        }
                    }
                }
            }
        }
    }

    eprintln!("No messages found in bag file");
    std::process::exit(1);
}

/// Create minimal fixture from MCAP file.
fn create_fixture_from_mcap(input: &str, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("Creating fixture from MCAP: {}", input);

    let reader = robocodec::RoboReader::open(input)?;

    match reader.iter_raw()?.into_stream()?.next() {
        Some(Ok((raw_msg, channel_info))) => {
            write_fixture_mcap(
                name,
                &raw_msg.data,
                raw_msg.log_time,
                &channel_info.topic,
                &channel_info.message_type,
                channel_info.schema.as_deref().unwrap_or(""),
            )?;
        }
        _ => {
            eprintln!("No messages found in MCAP file");
            std::process::exit(1);
        }
    }

    Ok(())
}

/// Write a single-message MCAP fixture.
fn write_fixture_mcap(
    name: &str,
    data: &[u8],
    timestamp: u64,
    topic: &str,
    msg_type: &str,
    schema: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let fixture_dir = Path::new("tests/fixtures");
    let output_path = fixture_dir.join(format!("{name}.mcap"));

    println!("Creating fixture: {}", output_path.display());
    println!("  Topic: {}", topic);
    println!("  Type: {}", msg_type);

    let output_file = File::create(&output_path)?;
    let mut mcap_writer = mcap::Writer::new(BufWriter::new(output_file))?;

    // Determine encoding from schema content
    let is_ros1 = schema.trim().starts_with("Header header") || schema.contains("ros1msg");
    let encoding = if is_ros1 { "ros1msg" } else { "ros2msg" };

    let schema_id = mcap_writer.add_schema(msg_type, encoding, schema.as_bytes())?;
    let ch_id = mcap_writer.add_channel(schema_id, topic, "cdr", &BTreeMap::new())?;

    mcap_writer.write_to_known_channel(
        &mcap::records::MessageHeader {
            channel_id: ch_id,
            sequence: 0,
            log_time: timestamp,
            publish_time: timestamp,
        },
        data,
    )?;

    drop(mcap_writer);

    let output_size = std::fs::metadata(&output_path)?.len();
    println!("  Size: {} bytes", output_size);

    Ok(())
}

/// Extract messages within time range from MCAP.
fn extract_mcap_time_range(
    input: &str,
    output: &str,
    start: u64,
    end: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let reader = robocodec::RoboReader::open(input)?;

    println!("Extracting from MCAP: {}", input);
    println!("Time range: {} - {} ns", start, end);
    println!("Output: {}", output);

    // Create output MCAP
    let output_file = File::create(output)?;
    let mut mcap_writer = mcap::Writer::new(BufWriter::new(output_file))?;

    // Add schemas and channels
    let mut schema_ids: std::collections::HashMap<String, u16> = std::collections::HashMap::new();
    let mut channel_ids: std::collections::HashMap<u16, u16> = std::collections::HashMap::new();
    let mut sequences: std::collections::HashMap<u16, u32> = std::collections::HashMap::new();

    for (&ch_id, channel) in reader.channels() {
        let schema_id = if let Some(schema) = &channel.schema {
            let encoding = channel.schema_encoding.as_deref().unwrap_or("ros2msg");
            *schema_ids
                .entry(channel.message_type.clone())
                .or_insert_with(|| {
                    mcap_writer
                        .add_schema(&channel.message_type, encoding, schema.as_bytes())
                        .unwrap_or(0)
                })
        } else {
            0
        };

        let out_ch_id = mcap_writer.add_channel(
            schema_id,
            &channel.topic,
            &channel.encoding,
            &BTreeMap::new(),
        )?;
        channel_ids.insert(ch_id, out_ch_id);
        sequences.insert(out_ch_id, 0);
    }

    // Copy messages in time range
    let iter = reader.iter_raw()?;
    let stream = iter.into_stream()?;
    let mut written = 0;

    for result in stream {
        let (msg, _channel) = result?;

        if msg.publish_time >= start && msg.publish_time <= end {
            let out_ch_id = channel_ids.get(&msg.channel_id).copied().unwrap_or(0);

            let seq = *sequences.get(&out_ch_id).unwrap_or(&0);
            mcap_writer.write_to_known_channel(
                &mcap::records::MessageHeader {
                    channel_id: out_ch_id,
                    sequence: seq,
                    log_time: msg.log_time,
                    publish_time: msg.publish_time,
                },
                &msg.data,
            )?;

            sequences.insert(out_ch_id, seq + 1);
            written += 1;
        }
    }

    drop(mcap_writer);
    println!("Extracted {} messages to {}", written, output);

    Ok(())
}

/// Extract messages within time range from BAG.
fn extract_bag_time_range(
    input: &str,
    output: &str,
    start: u64,
    end: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let reader = robocodec::RoboReader::open(input)?;

    println!("Extracting from BAG: {}", input);
    println!("Time range: {} - {} ns", start, end);
    println!("Output: {}", output);

    let mut writer = robocodec::BagWriter::create(output)?;

    // Copy connections, preserving callerid from the original bag
    for (ch_id, channel) in reader.channels() {
        let schema = channel.schema.as_deref().unwrap_or("");
        let callerid = channel.callerid.as_deref().unwrap_or("");
        writer.add_connection_with_callerid(
            *ch_id,
            &channel.topic,
            &channel.message_type,
            schema,
            callerid,
        )?;
    }

    // Copy messages in time range
    let iter = reader.iter_raw()?;
    let stream = iter.into_stream()?;
    let mut written = 0;

    for result in stream {
        let (msg, _channel) = result?;

        if msg.publish_time >= start && msg.publish_time <= end {
            let bag_msg =
                robocodec::BagMessage::from_raw(msg.channel_id, msg.publish_time, msg.data);
            writer.write_message(&bag_msg)?;
            written += 1;
        }
    }

    writer.finish()?;
    println!("Extracted {} messages to {}", written, output);

    Ok(())
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let (input, cmd) = match parse_args(&args) {
        Ok(result) => result,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    };

    if let Err(e) = run_extract(&input, cmd) {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}
