//! Extract one message per topic from a bag file and create a new bag.

use std::path::Path;

use robocodec::format::bag::{BagMessage, BagWriter};
use robocodec::reader::{BagFormatReader, FormatReader};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: {} <input.bag> <output.bag>", args[0]);
        std::process::exit(1);
    }

    let input_path = &args[1];
    let output_path = &args[2];

    if !Path::new(input_path).exists() {
        eprintln!("Input file not found: {}", input_path);
        std::process::exit(1);
    }

    println!("Reading: {}", input_path);

    // Open the input bag
    let reader = BagFormatReader::open(input_path)?;
    let channels = FormatReader::channels(&reader);

    println!("Found {} channels:", channels.len());

    for (id, ch) in channels.iter() {
        println!("  [{}] {} -> {}", id, ch.topic, ch.message_type);
    }

    if channels.is_empty() {
        eprintln!("No channels found in bag file!");
        return Ok(());
    }

    // Create output bag writer
    let mut writer = BagWriter::create(output_path)?;

    // Add connections to output using the same channel IDs as the reader
    // Preserving callerid from the original bag file
    for (channel_id, ch) in channels.iter() {
        let callerid = ch.callerid.as_deref().unwrap_or("");
        writer.add_connection_with_callerid(
            *channel_id,
            &ch.topic,
            &ch.message_type,
            ch.schema.as_deref().unwrap_or(""),
            callerid,
        )?;
    }

    // Read messages and extract one per unique (topic, callerid) combination
    // In ROS1, multiple nodes can publish to the same topic with different callerids
    let mut topics_seen: std::collections::HashSet<(String, Option<String>)> =
        std::collections::HashSet::new();
    let mut messages_written = 0;

    let conn_id_map = reader.conn_id_map().clone();
    let channels_clone = channels.clone();

    // Create message iterator
    let path_str = input_path.clone();
    let iter = robocodec::reader::BagRawMessageIter::new(path_str, channels_clone, conn_id_map);
    let mut stream = iter.into_stream()?;

    println!("\nExtracting one message per unique (topic, callerid)...");

    while let Some(result) = stream.next() {
        let (raw_msg, channel_info) = result?;

        // Skip if we've already seen this (topic, callerid) combination
        let key = (channel_info.topic.clone(), channel_info.callerid.clone());
        if !topics_seen.insert(key) {
            continue;
        }

        // Write the message - use raw_msg.channel_id directly since it's already
        // the internal channel ID which matches what we used for add_connection
        writer.write_message(&BagMessage::from_raw(
            raw_msg.channel_id,
            raw_msg.log_time,
            raw_msg.data.clone(),
        ))?;

        messages_written += 1;
        let callerid_str = channel_info.callerid.as_deref().unwrap_or("none");
        println!(
            "  Extracted: {} (callerid: {}, type: {})",
            channel_info.topic, callerid_str, channel_info.message_type
        );

        // Stop when we've extracted one message for each unique (topic, callerid)
        // The actual count may be less than channels.len() if some channels have no messages
        if messages_written >= channels.len() {
            break;
        }
    }

    writer.finish()?;

    println!("\nWrote {} messages to {}", messages_written, output_path);

    Ok(())
}
