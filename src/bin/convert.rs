//! Unified format conversion tool for robotics data files.
//!
//! Supports bidirectional conversion between MCAP and BAG formats,
//! as well as conversion from MCAP to LeRobot datasets.
//!
//! Usage:
//!   convert bag-to-mcap <input.bag> <output.mcap>    - Convert BAG to MCAP
//!   convert mcap-to-bag <input.mcap> <output.bag>    - Convert MCAP to BAG
//!   convert normalize <input> <output> <config>      - Normalize using config
//!   convert to-lerobot <input.mcap> <output_dir> <config> - Convert MCAP to LeRobot

use std::collections::{BTreeMap, HashMap};
use std::env;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;

enum Command {
    BagToMcap {
        input: String,
        output: String,
    },
    McapToBag {
        input: String,
        output: String,
    },
    Normalize {
        input: String,
        output: String,
        config: String,
    },
    ToLeRobot {
        input: String,
        output: String,
        config: String,
    },
}

fn parse_args(args: &[String]) -> Result<Command, String> {
    if args.len() < 4 {
        return Err(format!(
            "Usage: {} <command> <input> <output> [options]\n\
             Commands:\n\
               bag-to-mcap <input.bag> <output.mcap>     - Convert ROS1 BAG to MCAP\n\
               mcap-to-bag <input.mcap> <output.bag>     - Convert MCAP to ROS1 BAG\n\
               normalize <input> <output> <config>        - Normalize using config file\n\
               to-lerobot <input.mcap> <output_dir> <config> - Convert MCAP to LeRobot",
            args[0]
        ));
    }

    let command = &args[1];
    let input = args[2].clone();
    let output = args[3].clone();

    let cmd = match command.as_str() {
        "bag-to-mcap" => Command::BagToMcap { input, output },
        "mcap-to-bag" => Command::McapToBag { input, output },
        "normalize" => {
            if args.len() < 5 {
                return Err("normalize command requires a config file argument".to_string());
            }
            let config = args[4].clone();
            Command::Normalize {
                input,
                output,
                config,
            }
        }
        "to-lerobot" => {
            if args.len() < 5 {
                return Err("to-lerobot command requires a config file argument".to_string());
            }
            let config = args[4].clone();
            Command::ToLeRobot {
                input,
                output,
                config,
            }
        }
        _ => return Err(format!("Unknown command: {command}")),
    };

    Ok(cmd)
}

fn run_convert(cmd: Command) -> Result<(), Box<dyn std::error::Error>> {
    match cmd {
        Command::BagToMcap { input, output } => convert_bag_to_mcap(&input, &output),
        Command::McapToBag { input, output } => convert_mcap_to_bag(&input, &output),
        Command::Normalize {
            input,
            output,
            config,
        } => normalize_file(&input, &output, &config),
        Command::ToLeRobot {
            input,
            output,
            config,
        } => convert_to_lerobot(&input, &output, &config),
    }
}

/// Convert ROS1 BAG to MCAP format.
fn convert_bag_to_mcap(input: &str, output: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("Converting BAG to MCAP: {} -> {}", input, output);

    let reader = robocodec::RoboReader::open(input)?;
    println!("Channels: {}", reader.channels().len());

    let output_file = File::create(output)?;
    let mut mcap_writer = mcap::Writer::new(BufWriter::new(output_file))?;

    let mut schema_ids: HashMap<String, u16> = HashMap::new();
    let mut channel_ids: HashMap<u16, u16> = HashMap::new();
    let mut sequences: HashMap<u16, u32> = HashMap::new();
    let mut msg_count = 0u64;
    let mut failures = 0u64;

    // Add schemas and channels
    for (&ch_id, channel) in reader.channels() {
        let schema_id = if let Some(schema) = &channel.schema {
            let encoding = channel.schema_encoding.as_deref().unwrap_or("ros1msg");
            // Check if schema already exists
            if let Some(&id) = schema_ids.get(&channel.message_type) {
                id
            } else {
                let id = mcap_writer
                    .add_schema(&channel.message_type, encoding, schema.as_bytes())
                    .map_err(|e| {
                        format!(
                            "Failed to add schema for type {}: {}",
                            channel.message_type, e
                        )
                    })?;
                schema_ids.insert(channel.message_type.clone(), id);
                id
            }
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

    // Convert messages using raw data to avoid decode/encode issues
    let iter = reader.iter_raw()?;
    let stream = iter.into_stream()?;

    for result in stream {
        let (msg, _channel) = result?;

        let out_ch_id = match channel_ids.get(&msg.channel_id) {
            Some(&id) => id,
            None => {
                eprintln!(
                    "Warning: Unknown channel_id {}, skipping message",
                    msg.channel_id
                );
                continue;
            }
        };

        let seq = *sequences.get(&out_ch_id).unwrap_or(&0);

        // Write raw message data (preserves original encoding)
        if let Err(e) = mcap_writer.write_to_known_channel(
            &mcap::records::MessageHeader {
                channel_id: out_ch_id,
                sequence: seq,
                log_time: msg.log_time,
                publish_time: msg.publish_time,
            },
            &msg.data,
        ) {
            eprintln!("Warning: Failed to write message: {}", e);
            failures += 1;
            continue;
        }

        sequences.insert(out_ch_id, seq + 1);
        msg_count += 1;

        if msg_count.is_multiple_of(1000) {
            println!("Processed {} messages...", msg_count);
        }
    }

    drop(mcap_writer);

    println!();
    println!("=== Conversion Complete ===");
    println!("Messages processed: {}", msg_count);
    println!("Channels: {}", channel_ids.len());
    if failures > 0 {
        println!("Failures: {}", failures);
    }

    Ok(())
}

/// Convert MCAP to ROS1 BAG format.
fn convert_mcap_to_bag(input: &str, output: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("Converting MCAP to BAG: {} -> {}", input, output);

    let reader = robocodec::RoboReader::open(input)?;
    println!("Channels: {}", reader.channels().len());

    let mut writer = robocodec::BagWriter::create(output)?;
    let mut channel_ids: HashMap<u16, u16> = HashMap::new();
    let mut msg_count = 0u64;
    let mut failures = 0u64;

    // Add connections, preserving callerid
    for (conn_id, (&ch_id, channel)) in reader.channels().iter().enumerate() {
        let conn_id = conn_id as u16;
        let schema = channel.schema.as_deref().unwrap_or("");
        let callerid = channel.callerid.as_deref().unwrap_or("");
        writer.add_connection_with_callerid(
            conn_id,
            &channel.topic,
            &channel.message_type,
            schema,
            callerid,
        )?;
        channel_ids.insert(ch_id, conn_id);
    }

    // Convert messages using raw data
    let iter = reader.iter_raw()?;
    let stream = iter.into_stream()?;

    for result in stream {
        let (msg, _channel) = result?;

        let out_conn_id = match channel_ids.get(&msg.channel_id) {
            Some(&id) => id,
            None => continue,
        };

        let bag_msg = robocodec::BagMessage::from_raw(out_conn_id, msg.publish_time, msg.data);

        if let Err(e) = writer.write_message(&bag_msg) {
            eprintln!("Warning: Failed to write message: {}", e);
            failures += 1;
            continue;
        }

        msg_count += 1;

        if msg_count.is_multiple_of(1000) {
            println!("Processed {} messages...", msg_count);
        }
    }

    writer.finish()?;

    println!();
    println!("=== Conversion Complete ===");
    println!("Messages processed: {}", msg_count);
    println!("Connections: {}", channel_ids.len());
    if failures > 0 {
        println!("Failures: {}", failures);
    }

    Ok(())
}

/// Normalize a file using a config.
fn normalize_file(
    input: &str,
    output: &str,
    config_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Normalizing: {} -> {}", input, output);
    println!("Config: {}", config_path);

    // Load normalization config
    let config = robocodec::config::NormalizeConfig::from_file(config_path)?;
    let pipeline = config.to_pipeline();

    println!("Type mappings: {}", config.type_mappings.len());
    println!("Topic mappings: {}", config.topic_mappings.len());

    let output_ext = Path::new(output)
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("");

    // Determine output format
    if output_ext == "mcap" {
        normalize_to_mcap(input, &pipeline, output)?
    } else if output_ext == "bag" {
        normalize_to_bag(input, &pipeline, output)?
    } else {
        return Err(format!("Unsupported output format: .{output_ext}").into());
    }

    Ok(())
}

fn normalize_to_mcap(
    input: &str,
    pipeline: &robocodec::format::mcap::transform::TransformPipeline,
    output: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let input_path = std::path::Path::new(input);
    let input_ext = input_path
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("");

    match input_ext {
        "mcap" => mcap_to_mcap_normalized(input, pipeline, output),
        "bag" => bag_to_mcap_normalized(input, pipeline, output),
        _ => Err(format!("Unsupported input format: .{input_ext}").into()),
    }
}

/// Convert MCAP file to MCAP format with transformations.
fn mcap_to_mcap_normalized(
    input: &str,
    pipeline: &robocodec::format::mcap::transform::TransformPipeline,
    output: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use robocodec::format::mcap::{McapReader, McapRewriteEngine};

    let mcap_reader = McapReader::open(input)?;
    let mut engine = McapRewriteEngine::new();
    engine.prepare_schemas(&mcap_reader, Some(pipeline))?;

    let output_file = File::create(output)?;
    let mut mcap_writer = mcap::Writer::new(BufWriter::new(output_file))?;

    let mut schema_ids: HashMap<String, u16> = HashMap::new();
    let mut channel_ids: HashMap<u16, u16> = HashMap::new();
    let mut sequences: HashMap<u16, u32> = HashMap::new();
    let mut msg_count = 0;

    // Add transformed schemas and channels
    for (&ch_id, channel) in mcap_reader.channels() {
        let transformed_topic = engine
            .get_transformed_topic(ch_id)
            .unwrap_or(&channel.topic)
            .to_string();

        let transformed_schema = engine.get_transformed_schema(ch_id);

        let schema_id = if let Some(schema) = transformed_schema {
            let type_name = schema.type_name().to_string();
            let (schema_bytes, encoding) = match schema {
                robocodec::encoding::transform::SchemaMetadata::Cdr { schema_text, .. } => {
                    (Some(schema_text.as_bytes().to_vec()), "ros1msg")
                }
                robocodec::encoding::transform::SchemaMetadata::Protobuf {
                    file_descriptor_set,
                    ..
                } => (Some(file_descriptor_set.clone()), "protobuf"),
                robocodec::encoding::transform::SchemaMetadata::Json { schema_text, .. } => {
                    (Some(schema_text.as_bytes().to_vec()), "jsonschema")
                }
            };

            if let Some(bytes) = schema_bytes {
                // Check if schema already exists, and if not, add it with proper error handling
                if let Some(&id) = schema_ids.get(&type_name) {
                    id
                } else {
                    let id = mcap_writer
                        .add_schema(&type_name, encoding, &bytes)
                        .map_err(|e| {
                            format!("Failed to add schema for type {}: {}", type_name, e)
                        })?;
                    schema_ids.insert(type_name.clone(), id);
                    id
                }
            } else {
                0
            }
        } else {
            0
        };

        let out_ch_id = mcap_writer.add_channel(
            schema_id,
            &transformed_topic,
            &channel.encoding,
            &BTreeMap::new(),
        )?;

        channel_ids.insert(ch_id, out_ch_id);
        sequences.insert(out_ch_id, 0);
    }

    // Copy messages (data stays the same, only metadata is transformed)
    let iter = mcap_reader.iter_raw()?;
    let stream = iter.into_stream()?;

    for result in stream {
        let (msg, _channel) = result?;

        let out_ch_id = match channel_ids.get(&msg.channel_id) {
            Some(&id) => id,
            None => {
                eprintln!(
                    "Warning: Unknown channel_id {}, skipping message",
                    msg.channel_id
                );
                continue;
            }
        };

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
        msg_count += 1;
    }

    drop(mcap_writer);

    println!(
        "Normalized {} messages from MCAP to MCAP: {}",
        msg_count, output
    );

    Ok(())
}

/// Convert BAG file to MCAP format with transformations.
fn bag_to_mcap_normalized(
    input: &str,
    pipeline: &robocodec::format::mcap::transform::TransformPipeline,
    output: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use robocodec::reader::{BagFormatReader, BagRawMessageIter, FormatReader};

    println!("Converting BAG to MCAP with transforms");
    println!("  Input: {}", input);
    println!("  Output: {}", output);

    let reader = BagFormatReader::open(input)?;
    let channels = FormatReader::channels(&reader).clone();
    let conn_id_map = reader.conn_id_map().clone();

    let output_file = File::create(output)?;
    let mut mcap_writer = mcap::Writer::new(BufWriter::new(output_file))?;

    let mut schema_ids: HashMap<String, u16> = HashMap::new();
    let mut channel_ids: HashMap<u16, u16> = HashMap::new();
    let mut sequences: HashMap<u16, u32> = HashMap::new();
    let mut msg_count = 0;

    // Apply transforms and add schemas and channels
    for (&ch_id, channel) in &channels {
        let (transformed_type, transformed_schema) =
            pipeline.transform_type(&channel.message_type, channel.schema.as_deref());
        let transformed_topic = pipeline
            .transform_topic(&channel.topic)
            .unwrap_or_else(|| channel.topic.clone());

        // Use the transformed schema if available, otherwise use the original
        let schema_text = transformed_schema
            .as_deref()
            .or(channel.schema.as_deref())
            .unwrap_or("");
        let schema_bytes = schema_text.as_bytes();

        // Check if schema already exists, and if not, add it with proper error handling
        let schema_id = if !schema_text.is_empty() {
            if let Some(&id) = schema_ids.get(&transformed_type) {
                id
            } else {
                let id = mcap_writer
                    .add_schema(&transformed_type, "ros1msg", schema_bytes)
                    .map_err(|e| {
                        format!("Failed to add schema for type {}: {}", transformed_type, e)
                    })?;
                schema_ids.insert(transformed_type.clone(), id);
                id
            }
        } else {
            0
        };

        let channel_id = mcap_writer
            .add_channel(
                schema_id,
                &transformed_topic,
                &channel.encoding,
                &BTreeMap::new(),
            )
            .map_err(|e| format!("Failed to add channel: {e}"))?;

        channel_ids.insert(ch_id, channel_id);
        sequences.insert(channel_id, 0);
    }

    // Copy messages using BagRawMessageIter
    let iter = BagRawMessageIter::new(input.to_string(), channels.clone(), conn_id_map);
    let stream = iter.into_stream()?;

    for result in stream {
        let (msg, _channel) = result?;

        let out_ch_id = match channel_ids.get(&msg.channel_id) {
            Some(&id) => id,
            None => {
                eprintln!(
                    "Warning: Unknown channel_id {}, skipping message",
                    msg.channel_id
                );
                continue;
            }
        };

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
        msg_count += 1;
    }

    drop(mcap_writer);

    println!(
        "Converted {} messages from BAG to MCAP: {}",
        msg_count, output
    );
    Ok(())
}

fn normalize_to_bag(
    input: &str,
    pipeline: &robocodec::format::mcap::transform::TransformPipeline,
    output: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Detect input format
    let input_path = std::path::Path::new(input);
    let input_ext = input_path
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("");

    match input_ext {
        "mcap" => {
            // MCAP → BAG: existing code path
            mcap_to_bag(input, pipeline, output)
        }
        "bag" => {
            // BAG → BAG: use BagRewriter
            bag_to_bag(input, pipeline, output)
        }
        _ => Err(format!("Unsupported input format: .{input_ext}").into()),
    }
}

/// Convert MCAP file to BAG format.
fn mcap_to_bag(
    input: &str,
    pipeline: &robocodec::format::mcap::transform::TransformPipeline,
    output: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use robocodec::format::mcap::{McapReader, McapRewriteEngine};

    let reader = McapReader::open(input)?;
    let mut engine = McapRewriteEngine::new();
    engine.prepare_schemas(&reader, Some(pipeline))?;

    let mut writer = robocodec::BagWriter::create(output)?;
    let mut channel_ids: HashMap<u16, u16> = HashMap::new();
    let mut msg_count = 0;

    // Add transformed connections
    for (conn_id, (&ch_id, channel)) in reader.channels().iter().enumerate() {
        let conn_id = conn_id as u16;
        let transformed_topic = engine
            .get_transformed_topic(ch_id)
            .unwrap_or(&channel.topic)
            .to_string();

        let transformed_schema = engine.get_transformed_schema(ch_id);

        let (message_type, message_definition) = if let Some(schema) = transformed_schema {
            let type_name = schema.type_name().to_string();
            let definition = match schema {
                robocodec::encoding::transform::SchemaMetadata::Cdr { schema_text, .. } => {
                    schema_text.clone()
                }
                _ => channel.schema.clone().unwrap_or_default(),
            };
            (type_name, definition)
        } else {
            (
                channel.message_type.clone(),
                channel.schema.clone().unwrap_or_default(),
            )
        };

        // Preserve callerid from the original channel
        let callerid = channel.callerid.as_deref().unwrap_or("");
        writer.add_connection_with_callerid(
            conn_id,
            &transformed_topic,
            &message_type,
            &message_definition,
            callerid,
        )?;
        channel_ids.insert(ch_id, conn_id);
    }

    // Copy messages
    let iter = reader.iter_raw()?;
    let stream = iter.into_stream()?;

    for result in stream {
        let (msg, _channel) = result?;

        let out_conn_id = match channel_ids.get(&msg.channel_id) {
            Some(&id) => id,
            None => continue,
        };

        let bag_msg = robocodec::BagMessage::from_raw(out_conn_id, msg.publish_time, msg.data);
        writer.write_message(&bag_msg)?;
        msg_count += 1;
    }

    writer.finish()?;

    println!("Normalized {} messages to BAG: {}", msg_count, output);
    Ok(())
}

/// Convert BAG file to BAG format with transformations.
fn bag_to_bag(
    input: &str,
    pipeline: &robocodec::format::mcap::transform::TransformPipeline,
    output: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use robocodec::reader::{BagFormatReader, BagRawMessageIter, FormatReader};

    println!("Converting BAG to BAG with transforms");
    println!("  Input: {}", input);
    println!("  Output: {}", output);

    let reader = BagFormatReader::open(input)?;
    let channels = FormatReader::channels(&reader).clone();
    let conn_id_map = reader.conn_id_map().clone();

    let mut writer = robocodec::BagWriter::create(output)?;
    let mut channel_ids: HashMap<u16, u16> = HashMap::new();
    let mut msg_count = 0;

    // Build transformed connections
    for (conn_id, (&ch_id, channel)) in channels.iter().enumerate() {
        let conn_id = conn_id as u16;
        let (transformed_type, transformed_schema) =
            pipeline.transform_type(&channel.message_type, channel.schema.as_deref());
        let transformed_topic = pipeline
            .transform_topic(&channel.topic)
            .unwrap_or_else(|| channel.topic.clone());

        // Preserve callerid from the original channel
        let callerid = channel.callerid.as_deref().unwrap_or("");

        let schema = transformed_schema.as_deref().unwrap_or("");
        writer.add_connection_with_callerid(
            conn_id,
            &transformed_topic,
            &transformed_type,
            schema,
            callerid,
        )?;
        channel_ids.insert(ch_id, conn_id);
    }

    // Copy messages
    let iter = BagRawMessageIter::new(input.to_string(), channels.clone(), conn_id_map);
    let stream = iter.into_stream()?;

    for result in stream {
        let (msg, _channel) = result?;

        let out_conn_id = match channel_ids.get(&msg.channel_id) {
            Some(&id) => id,
            None => continue,
        };

        let bag_msg = robocodec::BagMessage::from_raw(out_conn_id, msg.publish_time, msg.data);
        writer.write_message(&bag_msg)?;
        msg_count += 1;
    }

    writer.finish()?;

    println!(
        "Rewritten {} channels, {} messages to BAG: {}",
        channel_ids.len(),
        msg_count,
        output
    );
    Ok(())
}

/// Convert MCAP to LeRobot dataset format.
fn convert_to_lerobot(
    input: &str,
    output_dir: &str,
    config_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use robocodec::format::lerobot::{LeRobotConfig, OutputFormat};

    println!("Converting MCAP to LeRobot dataset");
    println!("  Input: {}", input);
    println!("  Output: {}", output_dir);
    println!("  Config: {}", config_path);

    // Load LeRobot config
    let config_content = std::fs::read_to_string(config_path)?;
    let config: LeRobotConfig = toml::from_str(&config_content)?;

    println!("  Dataset: {}", config.dataset.name);
    println!("  Robot type: {:?}", config.dataset.robot_type);
    println!("  FPS: {}", config.dataset.fps);
    println!("  Mappings: {}", config.mappings.len());

    // Check which formats to generate
    let formats = config.output.formats.clone();
    let use_hdf5 = formats.is_empty() || formats.contains(&OutputFormat::Hdf5);
    let use_parquet = formats.is_empty() || formats.contains(&OutputFormat::Parquet);

    // Convert to HDF5
    if use_hdf5 {
        #[cfg(feature = "lerobot-hdf5")]
        {
            use robocodec::format::lerobot::Hdf5LeRobotWriter;

            println!();
            println!("Creating HDF5 format...");
            let mut writer = Hdf5LeRobotWriter::create(output_dir, 0)?;
            writer.write_from_mcap(input, &config)?;
            writer.finish(&config)?;
        }

        #[cfg(not(feature = "lerobot-hdf5"))]
        {
            eprintln!("Warning: HDF5 format requested but 'lerobot-hdf5' feature is not enabled.");
            eprintln!(
                "  Run with: cargo run --bin convert --features lerobot-hdf5 -- to-lerobot ..."
            );
        }
    }

    // Convert to Parquet+MP4
    if use_parquet {
        #[cfg(feature = "lerobot-parquet")]
        {
            use robocodec::format::lerobot::ParquetLeRobotWriter;

            println!();
            println!("Creating Parquet+MP4 format...");
            let mut writer = ParquetLeRobotWriter::create(output_dir, 0)?;
            writer.write_from_mcap(input, &config)?;
            writer.finish(&config)?;
        }

        #[cfg(not(feature = "lerobot-parquet"))]
        {
            eprintln!(
                "Warning: Parquet format requested but 'lerobot-parquet' feature is not enabled."
            );
            eprintln!(
                "  Run with: cargo run --bin convert --features lerobot-parquet -- to-lerobot ..."
            );
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

    if let Err(e) = run_convert(cmd) {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}
