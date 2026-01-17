//! Analyze all fixture files to understand their structure.

use std::collections::{BTreeMap, BTreeSet};

fn main() {
    // Analyze all robocodec test fixtures
    let fixtures: Vec<String> = (0..=16)
        .map(|i| {
            if i == 15 {
                format!("robocodec_test_{}.bag", i)
            } else {
                format!("robocodec_test_{}.mcap", i)
            }
        })
        .collect();

    // Collect all unique message types and topics
    let mut all_message_types: BTreeSet<String> = BTreeSet::new();
    let mut all_topics: BTreeSet<String> = BTreeSet::new();
    let mut fixture_info: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();

    println!("=== Fixture File Analysis ===\n");

    for fixture in fixtures {
        let path = format!("tests/fixtures/{}", fixture);
        println!("--- {} ---", fixture);

        match robocodec::RoboReader::open(&path) {
            Ok(reader) => {
                let channels: Vec<_> = reader.channels().values().cloned().collect();
                println!("  Channels: {}", channels.len());

                let mut topics_and_types = Vec::new();
                for chan in &channels {
                    println!("    Topic: {} | Type: {}", chan.topic, chan.message_type);
                    all_topics.insert(chan.topic.clone());
                    all_message_types.insert(chan.message_type.clone());
                    topics_and_types.push((chan.topic.clone(), chan.message_type.clone()));
                }
                fixture_info.insert(fixture.to_string(), topics_and_types);
            }
            Err(e) => {
                println!("  Error: {}", e);
            }
        }
        println!();
    }

    println!("=== Summary ===\n");
    println!("Unique topics ({}):", all_topics.len());
    for topic in &all_topics {
        println!("  {}", topic);
    }
    println!();

    println!("Unique message types ({}):", all_message_types.len());
    for msg_type in &all_message_types {
        println!("  {}", msg_type);
    }
}
