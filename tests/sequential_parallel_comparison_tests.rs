//! Tests to validate sequential and parallel readers produce the same results.
//!
//! These tests use the sequential readers (which rely on the proven mcap and rosbag crates)
//! as reference implementations to verify the correctness of the parallel readers.

use std::path::Path;

use robocodec::formats::{BagFormat, McapFormat, SequentialBagReader, SequentialMcapReader};
use robocodec::io::traits::FormatReader;

/// Helper to collect all messages from a sequential MCAP reader.
fn collect_mcap_messages_sequential(path: &str) -> Vec<(u16, u64, Vec<u8>)> {
    let reader = SequentialMcapReader::open(path).expect("Failed to open MCAP file");
    let iter = reader.iter_raw().expect("Failed to create iterator");
    let stream = iter.into_iter();

    stream
        .filter_map(|r: Result<_, _>| r.ok())
        .map(|(msg, _)| (msg.channel_id, msg.log_time, msg.data))
        .collect()
}

/// Helper to collect all messages from a parallel MCAP reader.
fn collect_mcap_messages_parallel(path: &str) -> Vec<(u16, u64, Vec<u8>)> {
    use robocodec::io::traits::ParallelReader;

    let reader = McapFormat::open(path).expect("Failed to open MCAP file");
    let (sender, receiver) = crossbeam_channel::unbounded();

    let config = robocodec::io::traits::ParallelReaderConfig::default();
    std::thread::spawn(move || {
        reader
            .read_parallel(config, sender)
            .expect("Failed to read parallel");
    });

    let mut messages = Vec::new();
    for chunk in receiver {
        // Access the messages field directly
        for msg in &chunk.messages {
            messages.push((msg.channel_id, msg.log_time, msg.data.as_ref().to_vec()));
        }
    }

    // Sort by timestamp for comparison
    messages.sort_by_key(|(_, t, _)| *t);
    messages
}

/// Helper to collect all messages from a sequential BAG reader.
fn collect_bag_messages_sequential(path: &str) -> Vec<(u16, u64, Vec<u8>)> {
    let reader = SequentialBagReader::open(path).expect("Failed to open BAG file");
    let iter = reader.iter_raw().expect("Failed to create iterator");

    iter.filter_map(|r: Result<_, _>| r.ok())
        .map(|(msg, _)| (msg.channel_id, msg.log_time, msg.data))
        .collect()
}

/// Helper to collect all messages from a parallel BAG reader.
fn collect_bag_messages_parallel(path: &str) -> Vec<(u16, u64, Vec<u8>)> {
    use robocodec::io::traits::ParallelReader;

    let reader = BagFormat::open(path).expect("Failed to open BAG file");
    let (sender, receiver) = crossbeam_channel::unbounded();

    let config = robocodec::io::traits::ParallelReaderConfig::default();
    std::thread::spawn(move || {
        reader
            .read_parallel(config, sender)
            .expect("Failed to read parallel");
    });

    let mut messages = Vec::new();
    for chunk in receiver {
        // Access the messages field directly
        for msg in &chunk.messages {
            messages.push((msg.channel_id, msg.log_time, msg.data.as_ref().to_vec()));
        }
    }

    // Sort by timestamp for comparison
    messages.sort_by_key(|(_, t, _)| *t);
    messages
}

#[test]
fn test_sequential_mcap_reader_reads_fixture() {
    let mcap_path = "tests/fixtures/robocodec_test_0.mcap";
    if !Path::new(mcap_path).exists() {
        eprintln!("Skipping test: fixture not found at {}", mcap_path);
        return;
    }

    let reader = SequentialMcapReader::open(mcap_path).expect("Failed to open MCAP file");
    let channels = reader.channels();

    println!("Sequential MCAP reader found {} channels", channels.len());
    for (id, ch) in channels {
        println!("  Channel {}: {} -> {}", id, ch.topic, ch.message_type);
    }

    // Count messages
    let iter = reader.iter_raw().expect("Failed to create iterator");
    let stream = iter.into_iter();
    let count: usize = stream.filter(|r: &Result<_, _>| r.is_ok()).count();

    println!("Sequential MCAP reader read {} messages", count);
    assert!(
        count > 0 || channels.is_empty(),
        "Should read messages or have no channels"
    );
}

#[test]
fn test_sequential_bag_reader_reads_fixture() {
    let bag_path = "tests/fixtures/robocodec_test_15.bag";
    if !Path::new(bag_path).exists() {
        eprintln!("Skipping test: fixture not found at {}", bag_path);
        return;
    }

    let reader = SequentialBagReader::open(bag_path).expect("Failed to open BAG file");
    let channels = reader.channels();

    println!("Sequential BAG reader found {} channels", channels.len());
    for (id, ch) in channels {
        println!("  Channel {}: {} -> {}", id, ch.topic, ch.message_type);
    }

    // Count messages
    let iter = reader.iter_raw().expect("Failed to create iterator");
    let count: usize = iter.filter(|r: &Result<_, _>| r.is_ok()).count();

    println!("Sequential BAG reader read {} messages", count);
    assert!(
        count > 0 || channels.is_empty(),
        "Should read messages or have no channels"
    );
}

#[test]
fn test_parallel_mcap_matches_sequential() {
    let mcap_path = "tests/fixtures/robocodec_test_0.mcap";
    if !Path::new(mcap_path).exists() {
        eprintln!("Skipping test: fixture not found at {}", mcap_path);
        return;
    }

    // Collect messages from both readers
    let sequential = collect_mcap_messages_sequential(mcap_path);
    let mut parallel = collect_mcap_messages_parallel(mcap_path);

    // Sort parallel messages by timestamp for comparison
    parallel.sort_by_key(|(_, t, _)| *t);

    println!("Sequential reader: {} messages", sequential.len());
    println!("Parallel reader: {} messages", parallel.len());

    // Compare counts
    assert_eq!(
        sequential.len(),
        parallel.len(),
        "Message counts should match between sequential and parallel readers"
    );

    // Compare message data
    for (i, (seq, par)) in sequential.iter().zip(parallel.iter()).enumerate() {
        assert_eq!(
            seq.1, par.1,
            "Message {} timestamp mismatch: seq={}, par={}",
            i, seq.1, par.1
        );
        assert_eq!(
            seq.2.len(),
            par.2.len(),
            "Message {} data length mismatch: seq={}, par={}",
            i,
            seq.2.len(),
            par.2.len()
        );
        assert_eq!(
            seq.2, par.2,
            "Message {} data mismatch at timestamp {}",
            i, seq.1
        );
    }

    println!(
        "All {} messages match between sequential and parallel readers",
        sequential.len()
    );
}

#[test]
fn test_parallel_bag_matches_sequential() {
    let bag_path = "tests/fixtures/robocodec_test_15.bag";
    if !Path::new(bag_path).exists() {
        eprintln!("Skipping test: fixture not found at {}", bag_path);
        return;
    }

    // Collect messages from both readers
    let sequential = collect_bag_messages_sequential(bag_path);
    let mut parallel = collect_bag_messages_parallel(bag_path);

    // Sort parallel messages by timestamp for comparison
    parallel.sort_by_key(|(_, t, _)| *t);

    println!("Sequential reader: {} messages", sequential.len());
    println!("Parallel reader: {} messages", parallel.len());

    // Compare counts
    assert_eq!(
        sequential.len(),
        parallel.len(),
        "Message counts should match between sequential and parallel readers"
    );

    // Compare message data
    for (i, (seq, par)) in sequential.iter().zip(parallel.iter()).enumerate() {
        assert_eq!(
            seq.1, par.1,
            "Message {} timestamp mismatch: seq={}, par={}",
            i, seq.1, par.1
        );
        assert_eq!(
            seq.2.len(),
            par.2.len(),
            "Message {} data length mismatch: seq={}, par={}",
            i,
            seq.2.len(),
            par.2.len()
        );
        assert_eq!(
            seq.2, par.2,
            "Message {} data mismatch at timestamp {}",
            i, seq.1
        );
    }

    println!(
        "All {} messages match between sequential and parallel readers",
        sequential.len()
    );
}

#[test]
fn test_mcap_channel_info_matches() {
    let mcap_path = "tests/fixtures/robocodec_test_0.mcap";
    if !Path::new(mcap_path).exists() {
        eprintln!("Skipping test: fixture not found at {}", mcap_path);
        return;
    }

    // Get channels from sequential reader
    let seq_reader = SequentialMcapReader::open(mcap_path).expect("Failed to open MCAP file");
    let seq_channels = seq_reader.channels();

    // Get channels from parallel reader
    let par_reader = McapFormat::open(mcap_path).expect("Failed to open MCAP file");
    let par_channels = par_reader.channels();

    println!(
        "Sequential: {} channels, Parallel: {} channels",
        seq_channels.len(),
        par_channels.len()
    );

    // Both should have the same topics
    let seq_topics: std::collections::HashSet<_> =
        seq_channels.values().map(|c| &c.topic).collect();
    let par_topics: std::collections::HashSet<_> =
        par_channels.values().map(|c| &c.topic).collect();

    assert_eq!(
        seq_topics, par_topics,
        "Topics should match between sequential and parallel readers"
    );

    println!("Channel topics match between sequential and parallel readers");
}

#[test]
fn test_bag_channel_info_matches() {
    let bag_path = "tests/fixtures/robocodec_test_15.bag";
    if !Path::new(bag_path).exists() {
        eprintln!("Skipping test: fixture not found at {}", bag_path);
        return;
    }

    // Get channels from sequential reader
    let seq_reader = SequentialBagReader::open(bag_path).expect("Failed to open BAG file");
    let seq_channels = seq_reader.channels();

    // Get channels from parallel reader
    let par_reader = BagFormat::open(bag_path).expect("Failed to open BAG file");
    let par_channels = par_reader.channels();

    println!(
        "Sequential: {} channels, Parallel: {} channels",
        seq_channels.len(),
        par_channels.len()
    );

    // Both should have the same topics
    let seq_topics: std::collections::HashSet<_> =
        seq_channels.values().map(|c| &c.topic).collect();
    let par_topics: std::collections::HashSet<_> =
        par_channels.values().map(|c| &c.topic).collect();

    assert_eq!(
        seq_topics, par_topics,
        "Topics should match between sequential and parallel readers"
    );

    println!("Channel topics match between sequential and parallel readers");
}
