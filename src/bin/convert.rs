// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Unified format conversion tool for robotics data files.
//!
//! Supports bidirectional conversion between MCAP and BAG formats,
//! as well as streaming conversion from MCAP/BAG to LeRobot datasets.
//!
//! Usage:
//!   convert bag-to-mcap <input.bag> <output.mcap>         - Convert BAG to MCAP
//!   convert mcap-to-bag <input.mcap> <output.bag>         - Convert MCAP to BAG
//!   convert normalize <input> <output> <config>          - Normalize using config
//!   convert to-lerobot <input.mcap> <output_dir> <config> - Convert MCAP to LeRobot (streaming)
//!   convert bag-to-lerobot <input.bag> <output_dir> <config> - Convert BAG to LeRobot (streaming)
//!
//! The streaming converters use bounded memory regardless of input file size.

use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;

use robocodec::mcap::ParallelMcapWriter;

#[cfg(feature = "dataset-all")]
use roboflow_storage::{RoboflowConfig, StorageConfig, StorageFactory};

// ============================================================================
// Fluent API Types
// ============================================================================

/// CLI credential options.
#[derive(Debug, Default)]
#[cfg(feature = "dataset-all")]
struct CredentialOptions {
    oss_endpoint: Option<String>,
    oss_access_key_id: Option<String>,
    oss_access_key_secret: Option<String>,
    oss_region: Option<String>,
    config_file: Option<String>,
}

/// Check if a path string is a cloud URL.
#[cfg(feature = "dataset-all")]
fn is_cloud_url(path: &str) -> bool {
    path.starts_with("oss://") || path.starts_with("s3://")
}

/// Load storage configuration from config file, environment, and CLI flags.
#[cfg(feature = "dataset-all")]
fn load_storage_config(cli_opts: &CredentialOptions) -> StorageConfig {
    // Load from config file if specified or default
    let config_file_path = cli_opts.config_file.as_ref().and_then(|p| {
        if p == "default" {
            None // Use default path in RoboflowConfig::load_default()
        } else {
            Some(std::path::PathBuf::from(p))
        }
    });

    let file_config = if let Some(path) = config_file_path {
        // If user explicitly provided a config path, report errors
        match RoboflowConfig::load_from(&path) {
            Ok(config) => config,
            Err(e) => {
                eprintln!("Error loading config file {}: {}", path.display(), e);
                return StorageConfig::from_env();
            }
        }
    } else {
        // Default config path - silently ignore if not found
        RoboflowConfig::load_default().ok().flatten()
    };

    // Start with environment variables, then merge config file, then CLI flags
    let mut config = StorageConfig::from_env().merge_with_config_file(file_config);

    // Merge CLI flag values (highest priority)
    if cli_opts.oss_access_key_id.is_some() {
        config.oss_access_key_id = cli_opts.oss_access_key_id.clone();
    }
    if cli_opts.oss_access_key_secret.is_some() {
        config.oss_access_key_secret = cli_opts.oss_access_key_secret.clone();
    }
    if cli_opts.oss_endpoint.is_some() {
        config.oss_endpoint = cli_opts.oss_endpoint.clone();
    }
    if cli_opts.oss_region.is_some() {
        config.aws_region = cli_opts.oss_region.clone();
    }

    config
}

/// Convert BAG to MCAP format using the fluent API.
///
/// # Examples
///
/// ```no_run
/// # mod convert;
/// // Simple conversion
/// convert::bag_to_mcap("input.bag", "output.mcap")
///     .run()
///     .unwrap();
/// ```
fn bag_to_mcap<'a>(input: &'a str, output: &'a str) -> ConversionBuilder<'a> {
    ConversionBuilder::BagToMcap { input, output }
}

/// Convert MCAP to BAG format using the fluent API.
///
/// # Examples
///
/// ```no_run
/// # mod convert;
/// convert::mcap_to_bag("input.mcap", "output.bag")
///     .run()
///     .unwrap();
/// ```
fn mcap_to_bag<'a>(input: &'a str, output: &'a str) -> ConversionBuilder<'a> {
    ConversionBuilder::McapToBag { input, output }
}

/// Normalize a file using the fluent API.
///
/// # Examples
///
/// ```no_run
/// # mod convert;
/// convert::normalize("input.bag", "output.mcap")
///     .config("config.toml")
///     .run()
///     .unwrap();
/// ```
fn normalize<'a>(input: &'a str, output: &'a str) -> NormalizeBuilder<'a> {
    NormalizeBuilder::new(input, output)
}

/// Convert MCAP to LeRobot dataset using the fluent API.
///
/// # Examples
///
/// ```no_run
/// # mod convert;
/// convert::to_lerobot("input.mcap", "output_dir")
///     .config("config.toml")
///     .run()
///     .unwrap();
/// ```
#[cfg(feature = "dataset-all")]
fn to_lerobot<'a>(input: &'a str, output_dir: &'a str) -> LeRobotBuilder<'a> {
    LeRobotBuilder::new(input, output_dir)
}

/// Builder for simple conversions (BAG ↔ MCAP).
enum ConversionBuilder<'a> {
    BagToMcap { input: &'a str, output: &'a str },
    McapToBag { input: &'a str, output: &'a str },
}

impl<'a> ConversionBuilder<'a> {
    /// Execute the conversion.
    fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            Self::BagToMcap { input, output } => convert_bag_to_mcap(input, output),
            Self::McapToBag { input, output } => convert_mcap_to_bag(input, output),
        }
    }
}

/// Builder for normalize conversions.
struct NormalizeBuilder<'a> {
    input: &'a str,
    output: &'a str,
    config: Option<&'a str>,
}

impl<'a> NormalizeBuilder<'a> {
    fn new(input: &'a str, output: &'a str) -> Self {
        Self {
            input,
            output,
            config: None,
        }
    }

    fn config(mut self, config: &'a str) -> Self {
        self.config = Some(config);
        self
    }

    fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let config = self.config.ok_or("normalize requires a config file")?;
        normalize_file(self.input, self.output, config)
    }
}

/// Builder for LeRobot conversions.
#[cfg(feature = "dataset-all")]
struct LeRobotBuilder<'a> {
    input: &'a str,
    output_dir: &'a str,
    config: Option<&'a str>,
}

#[cfg(feature = "dataset-all")]
impl<'a> LeRobotBuilder<'a> {
    fn new(input: &'a str, output_dir: &'a str) -> Self {
        Self {
            input,
            output_dir,
            config: None,
        }
    }

    fn config(mut self, config: &'a str) -> Self {
        self.config = Some(config);
        self
    }

    fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let config = self.config.ok_or("to-lerobot requires a config file")?;
        convert_to_lerobot(self.input, self.output_dir, config)
    }
}

// ============================================================================
// Command Line Parsing
// ============================================================================

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
    #[cfg(feature = "dataset-all")]
    ToLeRobot {
        input: String,
        output: String,
        config: String,
        credentials: CredentialOptions,
    },
    #[cfg(feature = "dataset-all")]
    BagToLeRobot {
        input: String,
        output: String,
        config: String,
        credentials: CredentialOptions,
    },
}

fn parse_args(args: &[String]) -> Result<Command, String> {
    if args.len() < 4 {
        return Err(format!(
            "Usage: {} <command> <input> <output> [options]\n\
             Commands:\n\
               bag-to-mcap <input.bag> <output.mcap>              - Convert ROS1 BAG to MCAP\n\
               mcap-to-bag <input.mcap> <output.bag>              - Convert MCAP to ROS1 BAG\n\
               normalize <input> <output> <config>                 - Normalize using config file\n\
               to-lerobot <input.mcap> <output_dir> <config> [opts] - Convert MCAP to LeRobot\n\
               bag-to-lerobot <input.bag> <output_dir> <config> [opts] - Convert BAG to LeRobot\n\
             \n\
             Input/Output Paths:\n\
               Local paths: ./input.mcap, /path/to/output/\n\
               Cloud URLs:  oss://bucket/path/input.mcap, s3://bucket/path/\n\
             \n\
             Credential Options (for cloud URLs):\n\
               --oss-endpoint <url>        - OSS endpoint (e.g., oss-cn-hangzhou.aliyuncs.com)\n\
               --oss-access-key-id <key>   - OSS access key ID\n\
               --oss-access-key-secret <key> - OSS access key secret\n\
               --oss-region <region>       - OSS region\n\
               --config <path>             - Config file path (default: ~/.roboflow/config.toml)\n\
             \n\
             Environment Variables (alternative to CLI flags):\n\
               OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET, OSS_ENDPOINT, OSS_REGION\n\
             \n\
             Examples:\n\
               # Local to local\n\
               roboflow to-lerobot input.mcap ./output config.toml\n\
               \n\
               # Cloud to local\n\
               roboflow to-lerobot oss://bucket/input.mcap ./output config.toml\n\
               \n\
               # Local to cloud with explicit credentials\n\
               roboflow to-lerobot input.mcap oss://bucket/output config.toml \\\n\
                 --oss-endpoint oss-cn-hangzhou.aliyuncs.com \\\n\
                 --oss-access-key-id LTAI... \\\n\
                 --oss-access-key-secret ...\n\
             \n\
             Deprecated Options (kept for backward compatibility):\n\
               --input-storage <url>  - Use cloud URLs directly in input path instead\n\
               --output-storage <url> - Use cloud URLs directly in output path instead",
            args[0]
        ));
    }

    let command = &args[1];
    let input = args[2].clone();
    let output = args[3].clone();

    Ok(match command.as_str() {
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
        #[cfg(feature = "dataset-all")]
        "to-lerobot" => {
            if args.len() < 5 {
                return Err("to-lerobot command requires a config file argument".to_string());
            }
            let config = args[4].clone();

            // Parse credential and optional arguments
            let mut credentials = CredentialOptions::default();
            let mut i = 5;
            while i < args.len() {
                match args[i].as_str() {
                    "--oss-endpoint" => {
                        if i + 1 >= args.len() {
                            return Err("--oss-endpoint requires a value argument".to_string());
                        }
                        credentials.oss_endpoint = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "--oss-access-key-id" => {
                        if i + 1 >= args.len() {
                            return Err("--oss-access-key-id requires a value argument".to_string());
                        }
                        credentials.oss_access_key_id = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "--oss-access-key-secret" => {
                        if i + 1 >= args.len() {
                            return Err(
                                "--oss-access-key-secret requires a value argument".to_string()
                            );
                        }
                        credentials.oss_access_key_secret = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "--oss-region" => {
                        if i + 1 >= args.len() {
                            return Err("--oss-region requires a value argument".to_string());
                        }
                        credentials.oss_region = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "--config" => {
                        if i + 1 >= args.len() {
                            return Err("--config requires a path argument".to_string());
                        }
                        credentials.config_file = Some(args[i + 1].clone());
                        i += 2;
                    }
                    // Legacy flags (kept for backward compatibility, warn but ignore)
                    "--input-storage" | "--output-storage" => {
                        eprintln!(
                            "Warning: {} flag is deprecated. Use cloud URLs directly in input/output paths.",
                            args[i]
                        );
                        if i + 1 >= args.len() {
                            return Err(format!("--{} requires a URL argument", &args[i][2..]));
                        }
                        i += 2;
                    }
                    _ => {
                        return Err(format!("Unknown argument: {}", args[i]));
                    }
                }
            }

            Command::ToLeRobot {
                input,
                output,
                config,
                credentials,
            }
        }
        #[cfg(feature = "dataset-all")]
        "bag-to-lerobot" => {
            if args.len() < 5 {
                return Err("bag-to-lerobot command requires a config file argument".to_string());
            }
            let config = args[4].clone();

            // Parse credential and optional arguments
            let mut credentials = CredentialOptions::default();
            let mut i = 5;
            while i < args.len() {
                match args[i].as_str() {
                    "--oss-endpoint" => {
                        if i + 1 >= args.len() {
                            return Err("--oss-endpoint requires a value argument".to_string());
                        }
                        credentials.oss_endpoint = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "--oss-access-key-id" => {
                        if i + 1 >= args.len() {
                            return Err("--oss-access-key-id requires a value argument".to_string());
                        }
                        credentials.oss_access_key_id = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "--oss-access-key-secret" => {
                        if i + 1 >= args.len() {
                            return Err(
                                "--oss-access-key-secret requires a value argument".to_string()
                            );
                        }
                        credentials.oss_access_key_secret = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "--oss-region" => {
                        if i + 1 >= args.len() {
                            return Err("--oss-region requires a value argument".to_string());
                        }
                        credentials.oss_region = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "--config" => {
                        if i + 1 >= args.len() {
                            return Err("--config requires a path argument".to_string());
                        }
                        credentials.config_file = Some(args[i + 1].clone());
                        i += 2;
                    }
                    // Legacy flags (kept for backward compatibility, warn but ignore)
                    "--input-storage" | "--output-storage" => {
                        eprintln!(
                            "Warning: {} flag is deprecated. Use cloud URLs directly in input/output paths.",
                            args[i]
                        );
                        if i + 1 >= args.len() {
                            return Err(format!("--{} requires a URL argument", &args[i][2..]));
                        }
                        i += 2;
                    }
                    _ => {
                        return Err(format!("Unknown argument: {}", args[i]));
                    }
                }
            }

            Command::BagToLeRobot {
                input,
                output,
                config,
                credentials,
            }
        }
        _ => return Err(format!("Unknown command: {command}")),
    })
}

fn run_convert(cmd: Command) -> Result<(), Box<dyn std::error::Error>> {
    match cmd {
        Command::BagToMcap { input, output } => bag_to_mcap(&input, &output).run(),
        Command::McapToBag { input, output } => mcap_to_bag(&input, &output).run(),
        Command::Normalize {
            input,
            output,
            config,
        } => normalize(&input, &output).config(&config).run(),
        #[cfg(feature = "dataset-all")]
        Command::ToLeRobot {
            input,
            output,
            config,
            credentials,
        } => {
            // Detect if input/output are cloud URLs
            let input_is_cloud = is_cloud_url(&input);
            let output_is_cloud = is_cloud_url(&output);

            if input_is_cloud || output_is_cloud {
                convert_to_lerobot_with_urls(&input, &output, &config, credentials)
            } else {
                to_lerobot(&input, &output).config(&config).run()
            }
        }
        #[cfg(feature = "dataset-all")]
        Command::BagToLeRobot {
            input,
            output,
            config,
            credentials,
        } => {
            // Detect if input/output are cloud URLs
            let input_is_cloud = is_cloud_url(&input);
            let output_is_cloud = is_cloud_url(&output);

            if input_is_cloud || output_is_cloud {
                convert_bag_to_lerobot_with_urls(&input, &output, &config, credentials)
            } else {
                convert_bag_to_lerobot(&input, &output, &config)
            }
        }
    }
}

// ============================================================================
// Conversion Implementations
// ============================================================================

/// Convert ROS1 BAG to MCAP format.
fn convert_bag_to_mcap(input: &str, output: &str) -> Result<(), Box<dyn std::error::Error>> {
    use robocodec::bag::BagFormat;
    use robocodec::io::traits::FormatReader;

    println!("Converting BAG to MCAP: {} -> {}", input, output);

    let reader = BagFormat::open(input)?;
    println!("Channels: {}", reader.channels().len());

    let output_file = File::create(output)?;
    let mut mcap_writer = ParallelMcapWriter::new(BufWriter::new(output_file))?;

    let mut schema_ids: HashMap<String, u16> = HashMap::new();
    let mut channel_ids: HashMap<u16, u16> = HashMap::new();
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
            &HashMap::new(),
        )?;

        channel_ids.insert(ch_id, out_ch_id);
    }

    // Convert messages using raw data to avoid decode/encode issues
    let iter = reader.iter_raw()?;
    let stream = iter;

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

        // Write raw message data (preserves original encoding)
        if let Err(e) =
            mcap_writer.write_message(out_ch_id, msg.log_time, msg.publish_time, &msg.data)
        {
            eprintln!("Warning: Failed to write message: {}", e);
            failures += 1;
            continue;
        }

        msg_count += 1;

        if msg_count.is_multiple_of(1000) {
            println!("Processed {} messages...", msg_count);
        }
    }

    mcap_writer.finish()?;

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

    let reader = robocodec::mcap::McapReader::open(input)?;
    println!("Channels: {}", reader.channels().len());

    let mut writer = robocodec::bag::BagWriter::create(output)?;
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
    let raw_iter = reader.iter_raw()?;
    let stream = raw_iter.stream()?;

    for result in stream {
        let (msg, _channel) = result?;

        let out_conn_id = match channel_ids.get(&msg.channel_id) {
            Some(&id) => id,
            None => continue,
        };

        let bag_msg = robocodec::bag::BagMessage::from_raw(out_conn_id, msg.publish_time, msg.data);

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
    let config = roboflow::config::NormalizeConfig::from_file(config_path)?;
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
    pipeline: &robocodec::transform::MultiTransform,
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
    pipeline: &robocodec::transform::MultiTransform,
    output: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use robocodec::mcap::McapReader;
    use robocodec::rewriter::engine::McapRewriteEngine;

    let mcap_reader = McapReader::open(input)?;
    let mut engine = McapRewriteEngine::new();
    engine.prepare_schemas(&mcap_reader, Some(pipeline))?;

    let output_file = File::create(output)?;
    let mut mcap_writer = ParallelMcapWriter::new(BufWriter::new(output_file))?;

    let mut schema_ids: HashMap<String, u16> = HashMap::new();
    let mut channel_ids: HashMap<u16, u16> = HashMap::new();
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
            &HashMap::new(),
        )?;

        channel_ids.insert(ch_id, out_ch_id);
    }

    // Copy messages (data stays the same, only metadata is transformed)
    let raw_iter = mcap_reader.iter_raw()?;
    let stream = raw_iter.stream()?;

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

        mcap_writer.write_message(out_ch_id, msg.log_time, msg.publish_time, &msg.data)?;

        msg_count += 1;
    }

    mcap_writer.finish()?;

    println!(
        "Normalized {} messages from MCAP to MCAP: {}",
        msg_count, output
    );

    Ok(())
}

/// Convert BAG file to MCAP format with transformations.
fn bag_to_mcap_normalized(
    input: &str,
    pipeline: &robocodec::transform::MultiTransform,
    output: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use robocodec::bag::BagFormat;
    use robocodec::io::traits::FormatReader;

    println!("Converting BAG to MCAP with transforms");
    println!("  Input: {}", input);
    println!("  Output: {}", output);

    let reader = BagFormat::open(input)?;
    let channels = FormatReader::channels(&reader).clone();

    let output_file = File::create(output)?;
    let mut mcap_writer = ParallelMcapWriter::new(BufWriter::new(output_file))?;

    let mut schema_ids: HashMap<String, u16> = HashMap::new();
    let mut channel_ids: HashMap<u16, u16> = HashMap::new();
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
                &HashMap::new(),
            )
            .map_err(|e| format!("Failed to add channel: {e}"))?;

        channel_ids.insert(ch_id, channel_id);
    }

    // Copy messages using BagRawMessageIter
    let stream = reader.iter_raw()?;

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

        mcap_writer.write_message(out_ch_id, msg.log_time, msg.publish_time, &msg.data)?;

        msg_count += 1;
    }

    mcap_writer.finish()?;

    println!(
        "Converted {} messages from BAG to MCAP: {}",
        msg_count, output
    );
    Ok(())
}

fn normalize_to_bag(
    input: &str,
    pipeline: &robocodec::transform::MultiTransform,
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
            mcap_to_bag_normalized(input, pipeline, output)
        }
        "bag" => {
            // BAG → BAG: use BagRewriter
            bag_to_bag(input, pipeline, output)
        }
        _ => Err(format!("Unsupported input format: .{input_ext}").into()),
    }
}

/// Convert MCAP file to BAG format.
fn mcap_to_bag_normalized(
    input: &str,
    pipeline: &robocodec::transform::MultiTransform,
    output: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use robocodec::mcap::McapReader;
    use robocodec::rewriter::engine::McapRewriteEngine;

    let reader = McapReader::open(input)?;
    let mut engine = McapRewriteEngine::new();
    engine.prepare_schemas(&reader, Some(pipeline))?;

    let mut writer = robocodec::bag::BagWriter::create(output)?;
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
    let raw_iter = reader.iter_raw()?;
    let stream = raw_iter.stream()?;

    for result in stream {
        let (msg, _channel) = result?;

        let out_conn_id = match channel_ids.get(&msg.channel_id) {
            Some(&id) => id,
            None => continue,
        };

        let bag_msg = robocodec::bag::BagMessage::from_raw(out_conn_id, msg.publish_time, msg.data);
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
    pipeline: &robocodec::transform::MultiTransform,
    output: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use robocodec::bag::BagFormat;
    use robocodec::io::traits::FormatReader;

    println!("Converting BAG to BAG with transforms");
    println!("  Input: {}", input);
    println!("  Output: {}", output);

    let reader = BagFormat::open(input)?;
    let channels = FormatReader::channels(&reader).clone();

    let mut writer = robocodec::bag::BagWriter::create(output)?;
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
    let stream = reader.iter_raw()?;

    for result in stream {
        let (msg, _channel) = result?;

        let out_conn_id = match channel_ids.get(&msg.channel_id) {
            Some(&id) => id,
            None => continue,
        };

        let bag_msg = robocodec::bag::BagMessage::from_raw(out_conn_id, msg.publish_time, msg.data);
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

/// Convert MCAP to LeRobot dataset format using streaming converter.
#[cfg(feature = "dataset-all")]
fn convert_to_lerobot(
    input: &str,
    output_dir: &str,
    config_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use roboflow::lerobot::LerobotConfig;
    use roboflow::streaming::StreamingDatasetConverter;

    println!("Converting MCAP to LeRobot dataset (streaming)");
    println!("  Input: {}", input);
    println!("  Output: {}", output_dir);
    println!("  Config: {}", config_path);

    // Load LeRobot config
    let config = LerobotConfig::from_file(config_path)?;

    println!("  Dataset: {}", config.dataset.name);
    println!("  Robot type: {:?}", config.dataset.robot_type);
    println!("  FPS: {}", config.dataset.fps);
    println!("  Mappings: {}", config.mappings.len());

    // Use StreamingDatasetConverter for bounded-memory streaming conversion
    let converter = StreamingDatasetConverter::new_lerobot(output_dir, config)?
        .with_completion_window(5) // 5 frames completion window
        .with_max_buffered_frames(300); // Max 10 seconds at 30fps

    let stats = converter.convert(input)?;

    println!();
    println!("=== Conversion Complete ===");
    println!("Frames written: {}", stats.frames_written);
    println!("Messages processed: {}", stats.messages_processed);
    if stats.force_completed_frames > 0 {
        println!("Force-completed frames: {}", stats.force_completed_frames);
    }
    println!("Avg buffer size: {:.1} frames", stats.avg_buffer_size);
    println!("Peak memory: {:.1} MB", stats.peak_memory_mb);
    println!("Duration: {:.2}s", stats.duration_sec);
    println!("Throughput: {:.1} frames/s", stats.throughput_fps());

    Ok(())
}

/// Convert BAG file directly to LeRobot dataset format.
///
/// This function uses the StreamingDatasetConverter for true streaming conversion:
/// BAG -> decoded messages -> AlignedFrames -> LeRobot dataset
///
/// No intermediate MCAP file is created, and memory usage is bounded.
#[cfg(feature = "dataset-all")]
fn convert_bag_to_lerobot(
    input: &str,
    output_dir: &str,
    config_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use roboflow::lerobot::LerobotConfig;
    use roboflow::streaming::StreamingDatasetConverter;

    println!("Converting BAG to LeRobot dataset (streaming)");
    println!("  Input: {}", input);
    println!("  Output: {}", output_dir);
    println!("  Config: {}", config_path);

    // Load LeRobot config
    let config = LerobotConfig::from_file(config_path)?;

    println!("  Dataset: {}", config.dataset.name);
    println!("  Robot type: {:?}", config.dataset.robot_type);
    println!("  FPS: {}", config.dataset.fps);
    println!("  Mappings: {}", config.mappings.len());

    // Use StreamingDatasetConverter for bounded-memory streaming conversion
    let converter = StreamingDatasetConverter::new_lerobot(output_dir, config)?
        .with_completion_window(5) // 5 frames completion window
        .with_max_buffered_frames(300); // Max 10 seconds at 30fps

    let stats = converter.convert(input)?;

    println!();
    println!("=== Conversion Complete ===");
    println!("Frames written: {}", stats.frames_written);
    println!("Messages processed: {}", stats.messages_processed);
    if stats.force_completed_frames > 0 {
        println!("Force-completed frames: {}", stats.force_completed_frames);
    }
    println!("Avg buffer size: {:.1} frames", stats.avg_buffer_size);
    println!("Peak memory: {:.1} MB", stats.peak_memory_mb);
    println!("Duration: {:.2}s", stats.duration_sec);
    println!("Throughput: {:.1} frames/s", stats.throughput_fps());

    Ok(())
}

/// Convert MCAP to LeRobot dataset format with cloud URL support.
#[cfg(feature = "dataset-all")]
fn convert_to_lerobot_with_urls(
    input: &str,
    output: &str,
    config_path: &str,
    credentials: CredentialOptions,
) -> Result<(), Box<dyn std::error::Error>> {
    use roboflow::lerobot::LerobotConfig;
    use roboflow::streaming::{StreamingConfig, StreamingDatasetConverter};

    println!("Converting MCAP to LeRobot dataset (cloud-enabled)");
    println!("  Input: {}", input);
    println!("  Output: {}", output);
    println!("  Config: {}", config_path);

    // Load LeRobot config
    let config = LerobotConfig::from_file(config_path)?;

    println!("  Dataset: {}", config.dataset.name);
    println!("  Robot type: {:?}", config.dataset.robot_type);
    println!("  FPS: {}", config.dataset.fps);
    println!("  Mappings: {}", config.mappings.len());

    // Detect if input/output are cloud URLs
    let input_is_cloud = is_cloud_url(input);
    let output_is_cloud = is_cloud_url(output);

    // Load credentials from file, env, and CLI flags
    let storage_config = load_storage_config(&credentials);

    // Validate credentials for cloud URLs
    if (input_is_cloud || output_is_cloud) && !storage_config.has_oss_credentials() {
        return Err(
            "OSS credentials required for cloud URLs. Set:\n\
             - Environment: OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET, OSS_ENDPOINT\n\
             - Config file: ~/.roboflow/config.toml\n\
             - CLI flags: --oss-access-key-id, --oss-access-key-secret, --oss-endpoint\n\
             \n\
             Examples:\n\
               roboflow to-lerobot oss://bucket/input.mcap ./output config.toml\n\
               roboflow to-lerobot ./input.mcap oss://bucket/output config.toml --oss-endpoint oss-cn-hangzhou.aliyuncs.com"
                .into(),
        );
    }

    // Create storage factory with loaded credentials
    let factory = StorageFactory::with_config(storage_config);

    // Create input storage backend if input is a cloud URL
    let input_storage = if input_is_cloud {
        Some(factory.create(input)?)
    } else {
        None
    };

    // Create output storage backend if output is a cloud URL
    let output_storage = if output_is_cloud {
        Some(factory.create(output)?)
    } else {
        None
    };

    // Build streaming config with temp directory for cloud downloads
    let mut streaming_config = StreamingConfig::with_fps(config.dataset.fps);
    if input_is_cloud {
        let temp_dir = std::env::var("ROBOFLOW_TEMP_DIR")
            .ok()
            .or_else(|| std::env::var("TMPDIR").ok())
            .unwrap_or_else(|| "/tmp".to_string());
        println!("  Temp directory: {}", temp_dir);
        streaming_config.temp_dir = Some(std::path::PathBuf::from(temp_dir));
    }

    // Use StreamingDatasetConverter with storage backends
    let converter = StreamingDatasetConverter::new_lerobot_with_storage(
        output,
        config,
        input_storage,
        output_storage,
    )?
    .with_completion_window(5)
    .with_max_buffered_frames(300);

    let stats = converter.convert(input)?;

    println!();
    println!("=== Conversion Complete ===");
    println!("Frames written: {}", stats.frames_written);
    println!("Messages processed: {}", stats.messages_processed);
    if stats.force_completed_frames > 0 {
        println!("Force-completed frames: {}", stats.force_completed_frames);
    }
    println!("Avg buffer size: {:.1} frames", stats.avg_buffer_size);
    println!("Peak memory: {:.1} MB", stats.peak_memory_mb);
    println!("Duration: {:.2}s", stats.duration_sec);
    println!("Throughput: {:.1} frames/s", stats.throughput_fps());

    Ok(())
}

/// Convert BAG file directly to LeRobot dataset format with cloud URL support.
#[cfg(feature = "dataset-all")]
fn convert_bag_to_lerobot_with_urls(
    input: &str,
    output: &str,
    config_path: &str,
    credentials: CredentialOptions,
) -> Result<(), Box<dyn std::error::Error>> {
    use roboflow::lerobot::LerobotConfig;
    use roboflow::streaming::{StreamingConfig, StreamingDatasetConverter};

    println!("Converting BAG to LeRobot dataset (cloud-enabled)");
    println!("  Input: {}", input);
    println!("  Output: {}", output);
    println!("  Config: {}", config_path);

    // Load LeRobot config
    let config = LerobotConfig::from_file(config_path)?;

    println!("  Dataset: {}", config.dataset.name);
    println!("  Robot type: {:?}", config.dataset.robot_type);
    println!("  FPS: {}", config.dataset.fps);
    println!("  Mappings: {}", config.mappings.len());

    // Detect if input/output are cloud URLs
    let input_is_cloud = is_cloud_url(input);
    let output_is_cloud = is_cloud_url(output);

    // Load credentials from file, env, and CLI flags
    let storage_config = load_storage_config(&credentials);

    // Validate credentials for cloud URLs
    if (input_is_cloud || output_is_cloud) && !storage_config.has_oss_credentials() {
        return Err(
            "OSS credentials required for cloud URLs. Set:\n\
             - Environment: OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET, OSS_ENDPOINT\n\
             - Config file: ~/.roboflow/config.toml\n\
             - CLI flags: --oss-access-key-id, --oss-access-key-secret, --oss-endpoint\n\
             \n\
             Examples:\n\
               roboflow bag-to-lerobot oss://bucket/input.bag ./output config.toml\n\
               roboflow bag-to-lerobot ./input.bag oss://bucket/output config.toml --oss-endpoint oss-cn-hangzhou.aliyuncs.com"
                .into(),
        );
    }

    // Create storage factory with loaded credentials
    let factory = StorageFactory::with_config(storage_config);

    // Create input storage backend if input is a cloud URL
    let input_storage = if input_is_cloud {
        Some(factory.create(input)?)
    } else {
        None
    };

    // Create output storage backend if output is a cloud URL
    let output_storage = if output_is_cloud {
        Some(factory.create(output)?)
    } else {
        None
    };

    // Build streaming config with temp directory for cloud downloads
    let mut streaming_config = StreamingConfig::with_fps(config.dataset.fps);
    if input_is_cloud {
        let temp_dir = std::env::var("ROBOFLOW_TEMP_DIR")
            .ok()
            .or_else(|| std::env::var("TMPDIR").ok())
            .unwrap_or_else(|| "/tmp".to_string());
        println!("  Temp directory: {}", temp_dir);
        streaming_config.temp_dir = Some(std::path::PathBuf::from(temp_dir));
    }

    // Use StreamingDatasetConverter with storage backends
    let converter = StreamingDatasetConverter::new_lerobot_with_storage(
        output,
        config,
        input_storage,
        output_storage,
    )?
    .with_completion_window(5)
    .with_max_buffered_frames(300);

    let stats = converter.convert(input)?;

    println!();
    println!("=== Conversion Complete ===");
    println!("Frames written: {}", stats.frames_written);
    println!("Messages processed: {}", stats.messages_processed);
    if stats.force_completed_frames > 0 {
        println!("Force-completed frames: {}", stats.force_completed_frames);
    }
    println!("Avg buffer size: {:.1} frames", stats.avg_buffer_size);
    println!("Peak memory: {:.1} MB", stats.peak_memory_mb);
    println!("Duration: {:.2}s", stats.duration_sec);
    println!("Throughput: {:.1} frames/s", stats.throughput_fps());

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
