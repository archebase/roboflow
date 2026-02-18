use std::fs;
use std::path::Path;
use std::time::Instant;

use roboflow_sources::{BagSource, BagSourceBatched, BagSourceBlocking, Source, SourceConfig};

const TEST_BAG_PATH: &str = "../../tests/fixtures/A02-A01-37-45-77-factory_07-P4_210-leju_claw-20260104174020-v001.bag";

fn init_sources() {
    roboflow_sources::register_builtin_sources();
}

#[test]
#[ignore = "Requires real bag file - run manually"]
fn test_batched_vs_original_performance() {
    init_sources();
    
    if !Path::new(TEST_BAG_PATH).exists() {
        eprintln!("Skipping benchmark: bag file not found at {}", TEST_BAG_PATH);
        return;
    }

    let rt = tokio::runtime::Runtime::new().unwrap();
    
    let input_metadata = fs::metadata(TEST_BAG_PATH).expect("Failed to read input metadata");
    let input_size_mb = input_metadata.len() as f64 / (1024.0 * 1024.0);
    
    println!("\n{}", std::iter::repeat("=").take(80).collect::<String>());
    println!("BATCHED vs ORIGINAL PERFORMANCE COMPARISON");
    println!("{}", std::iter::repeat("=").take(80).collect::<String>());
    println!("Input: {} ({:.2} MB)\n", TEST_BAG_PATH, input_size_mb);
    
    let batch_sizes = [100, 1000, 5000, 10000];
    
    for batch_size in batch_sizes {
        println!("\nTesting with batch_size={}:", batch_size);
        println!("{}", std::iter::repeat("-").take(60).collect::<String>());
        
        let original_time = rt.block_on(run_original_bag_source());
        let batched_time = rt.block_on(run_batched_bag_source(batch_size));
        let blocking_time = rt.block_on(run_blocking_bag_source(batch_size));
        
        let speedup_batched = original_time / batched_time;
        let speedup_blocking = original_time / blocking_time;
        
        println!("  Original BagSource:  {:.2}s", original_time);
        println!("  Batched BagSource:   {:.2}s ({:.2}x)", batched_time, speedup_batched);
        println!("  Blocking BagSource:  {:.2}s ({:.2}x)", blocking_time, speedup_blocking);
    }
    
    println!("\n{}", std::iter::repeat("=").take(80).collect::<String>());
}

async fn run_original_bag_source() -> f64 {
    let start = Instant::now();
    
    let source_config = SourceConfig::bag(TEST_BAG_PATH);
    let mut source = BagSource::from_config(&source_config)
        .expect("Failed to create BagSource");
    
    let _metadata = source.initialize(&source_config).await
        .expect("Failed to initialize");
    
    let mut total_messages = 0usize;
    
    loop {
        match source.read_batch(1000).await {
            Ok(Some(messages)) => {
                total_messages += messages.len();
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }
    
    start.elapsed().as_secs_f64()
}

async fn run_batched_bag_source(batch_size: usize) -> f64 {
    let start = Instant::now();
    
    let source_config = SourceConfig::bag(TEST_BAG_PATH);
    let mut source = BagSourceBatched::from_config(&source_config, batch_size)
        .expect("Failed to create BagSourceBatched");
    
    let _metadata = source.initialize(&source_config).await
        .expect("Failed to initialize");
    
    let mut total_messages = 0usize;
    
    loop {
        match source.read_batch(1000).await {
            Ok(Some(messages)) => {
                total_messages += messages.len();
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }
    
    start.elapsed().as_secs_f64()
}

async fn run_blocking_bag_source(batch_size: usize) -> f64 {
    let start = Instant::now();
    
    let source_config = SourceConfig::bag(TEST_BAG_PATH);
    let mut source = BagSourceBlocking::from_config(&source_config, batch_size)
        .expect("Failed to create BagSourceBlocking");
    
    let _metadata = source.initialize(&source_config).await
        .expect("Failed to initialize");
    
    let mut total_messages = 0usize;
    
    loop {
        match source.read_batch(1000).await {
            Ok(Some(messages)) => {
                total_messages += messages.len();
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }
    
    start.elapsed().as_secs_f64()
}

#[test]
#[ignore = "Requires real bag file - run manually"]
fn test_batched_throughput_analysis() {
    init_sources();
    
    if !Path::new(TEST_BAG_PATH).exists() {
        eprintln!("Skipping benchmark: bag file not found at {}", TEST_BAG_PATH);
        return;
    }

    let rt = tokio::runtime::Runtime::new().unwrap();
    
    let input_metadata = fs::metadata(TEST_BAG_PATH).expect("Failed to read input metadata");
    let input_size_mb = input_metadata.len() as f64 / (1024.0 * 1024.0);
    
    println!("\n{}", std::iter::repeat("=").take(80).collect::<String>());
    println!("BATCHED SOURCE THROUGHPUT ANALYSIS");
    println!("{}", std::iter::repeat("=").take(80).collect::<String>());
    println!("Input: {} ({:.2} MB)\n", TEST_BAG_PATH, input_size_mb);
    
    let decoder_batch_size = 10000;
    let read_batch_size = 1000;
    
    rt.block_on(async {
        let source_config = SourceConfig::bag(TEST_BAG_PATH);
        let mut source = BagSourceBatched::from_config(&source_config, decoder_batch_size)
            .expect("Failed to create BagSourceBatched");
        
        let metadata = source.initialize(&source_config).await
            .expect("Failed to initialize");
        
        println!("Source initialized with {} topics", metadata.topics.len());
        println!("Decoder batch size: {}", decoder_batch_size);
        println!("Read batch size: {}\n", read_batch_size);
        
        let start = Instant::now();
        let mut total_messages = 0usize;
        let mut batch_count = 0usize;
        let mut channel_receives = 0usize;
        
        loop {
            let batch_start = Instant::now();
            match source.read_batch(read_batch_size).await {
                Ok(Some(messages)) => {
                    let batch_time = batch_start.elapsed().as_secs_f64();
                    batch_count += 1;
                    total_messages += messages.len();
                    
                    if messages.len() == read_batch_size {
                        channel_receives += 1;
                    }
                    
                    if batch_count % 100 == 0 {
                        let elapsed = start.elapsed().as_secs_f64();
                        let throughput = total_messages as f64 / elapsed;
                        println!(
                            "  Batch {}: {} messages total, {:.1} msg/s avg, {:.3}ms/batch",
                            batch_count,
                            total_messages,
                            throughput,
                            batch_time * 1000.0
                        );
                    }
                }
                Ok(None) => break,
                Err(_) => break,
            }
        }
        
        let total_time = start.elapsed().as_secs_f64();
        let throughput = total_messages as f64 / total_time;
        let channel_ops = total_messages / decoder_batch_size;
        
        println!("\n{}", std::iter::repeat("=").take(80).collect::<String>());
        println!("RESULTS:");
        println!("  Total time:        {:.2}s", total_time);
        println!("  Total messages:    {}", total_messages);
        println!("  Total batches:     {}", batch_count);
        println!("  Channel receives:  ~{}", channel_ops);
        println!("  Throughput:        {:.1} msg/s", throughput);
        println!("  MB/s:              {:.1} MB/s", input_size_mb / total_time);
        println!("{}", std::iter::repeat("=").take(80).collect::<String>());
    });
}
