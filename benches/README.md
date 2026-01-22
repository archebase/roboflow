# Benchmarks

Benchmarking and profiling tool for `robocodec` performance analysis and optimization.

## Overview

The `profiler.rs` benchmark provides three subcommands:
- **`run`** - Single conversion with metrics output
- **`bench`** - Benchmark with warmup and steady-state statistics
- **`profile`** - Profile run with flamegraph generation (requires `profiling` feature)

## Pipeline Modes

Two pipeline modes are available:

| Mode | Description | Flag |
|------|-------------|------|
| **Standard Parallel** | Rayon-based parallel processing | Default (no flag) |
| **HyperPipeline** | Async staged pipeline with higher throughput | `--hyper` |

Both modes support compression presets (`fast`, `balanced`, `slow`) and auto-detected WindowLog from CPU cache.

## Prerequisites

### Go (for pprof visualization)

```bash
# macOS
brew install go

# Linux
# Download from https://go.dev/dl/

# Verify
go version
```

### Graphviz (for flamegraphs)

```bash
# macOS
brew install graphviz

# Ubuntu/Debian
sudo apt-get install graphviz
```

## Running via cargo bench

### Basic Usage

```bash
# Standard Parallel Pipeline (default)
cargo bench --bench profiler --features profiling -- bench \
    -i /path/to/input.bag \
    -o /path/to/output.mcap

# HyperPipeline (async)
cargo bench --bench profiler --features profiling -- bench \
    -i /path/to/input.bag \
    -o /path/to/output.mcap \
    --hyper
```

**Note:** The double `--` separates cargo arguments from profiler arguments. `bench` is the subcommand name.

### Subcommands

#### `run` - Single conversion with metrics

```bash
cargo bench --bench profiler --features profiling -- run \
    -i input.bag \
    -o output.mcap \
    --preset balanced

# With HyperPipeline
cargo bench --bench profiler --features profiling -- run \
    -i input.bag \
    -o output.mcap \
    --hyper \
    --mode throughput
```

#### `bench` - Benchmark with statistics

```bash
# Defaults: 2 warmup runs, 10 measured runs
cargo bench --bench profiler --features profiling -- bench \
    -i input.bag \
    -o output.mcap

# Custom warmup and runs
cargo bench --bench profiler --features profiling -- bench \
    -i input.bag \
    -o output.mcap \
    --warmup 1 \
    --runs 5

# Verbose output (shows each run)
cargo bench --bench profiler --features profiling -- bench \
    -i input.bag \
    -o output.mcap \
    --verbose
```

**Auto-overwrite:** The `bench` command automatically removes existing output files before running.

#### `profile` - Generate flamegraph

```bash
cargo bench --bench profiler --features profiling -- profile \
    -i input.bag \
    -o output.mcap \
    --profile-output profile \
    --save-trace
```

## Options

### Compression Presets

| Preset | Level | Description |
|--------|-------|-------------|
| `fast` | 1 | Fastest compression |
| `balanced` | 3 | Default (recommended) |
| `slow` | 9 | Best compression |

```bash
--preset fast
--preset balanced  # default
--preset slow
```

### HyperPipeline Options

```bash
# Auto-configuration with performance mode
--hyper --mode throughput

# Performance modes:
#   - throughput: Maximum throughput on beefy machines
#   - balanced: Middle ground
#   - memory_efficient: Conserve memory

# Manual configuration
--hyper --batch-size 8388608 --compress-threads 6
```

### Common Options

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--input` | `-i` | required | Input BAG/MCAP file |
| `--output` | `-o` | required | Output MCAP file |
| `--preset` | `-p` | `balanced` | Compression preset |
| `--warmup` | `-w` | `2` | Warmup runs (discarded from stats) |
| `--runs` | `-r` | `10` | Measured runs (for statistics) |
| `--verbose` | | | Show individual run times |
| `--hyper` | | | Use HyperPipeline |
| `--mode` | | | Performance mode (with `--hyper`) |
| `--batch-size` | | | Batch size in bytes (with `--hyper`) |
| `--compress-threads` | | | Compression threads (with `--hyper`) |

## Using the Built Binary

```bash
# Build
cargo build --release --features profiling --bin profiler

# Run benchmark
./target/release/profiler bench \
    -i input.bag \
    -o output.mcap \
    --warmup 2 \
    --runs 10
```

## Output Examples

### Standard Pipeline
```
profiler: Balanced preset
pipeline: Parallel
input: /path/to/input.bag
input_mb: 5667.37
output: /path/to/output.mcap
warmup: 1
runs: 3
WindowLog: auto-detected from CPU cache

  1/3: 8.45s
  2/3: 8.32s
  3/3: 8.38s

steady-state:
  avg: 8.38s
  min: 8.32s
  max: 8.45s
  p50: 8.38s
  p95: 8.44s
  p99: 8.45s
  throughput: 676.2 MB/s

Final output: /path/to/output.mcap
```

### HyperPipeline
```
profiler: Balanced preset
pipeline: HyperPipeline (async)
mode: Throughput
input: /path/to/input.bag
input_mb: 5667.37
output: /path/to/output.mcap
warmup: 1
runs: 3
WindowLog: auto-detected from CPU cache

Starting compression stage with 6 worker threads...
Starting parallel BAG reader with 2 worker threads...

steady-state:
  avg: 3.02s
  min: 2.98s
  max: 3.10s
  throughput: 1876.8 MB/s
```

## Profiling with Flamegraphs

```bash
# Generate profile with flamegraph and protobuf trace
./target/release/profiler profile \
    -i input.bag \
    -o output.mcap \
    --profile-output profile \
    --freq 99 \
    --save-trace

# With HyperPipeline
./target/release/profiler profile \
    -i input.bag \
    -o output.mcap \
    --hyper \
    --mode throughput \
    --profile-output profile \
    --save-trace
```

**Generated files:**
- `profile.svg` - Flamegraph (opens in browser)
- `profile.pb` - Protobuf trace (for pprof)

### Using go tool pprof

```bash
# Interactive session
go tool pprof profile.pb

# Commands in interactive mode:
(pprof) top       # Top CPU consumers
(pprof) web       # Open call graph in browser
(pprof) pdf       # Generate PDF
(pprof) flamegraph  # Generate flamegraph
```

## Troubleshooting

**"input file not found"** - Verify the `-i` path is correct

**"output file already exists"** - Only `run` and `profile` commands check this. `bench` auto-overwrites.

**"steady-state: no data"** - You specified `--runs 0`. Use `--runs 1` or higher.

**"graphviz not found"** - Install Graphviz for PDF/PNG generation

**Empty flamegraph** - Increase `--freq` or run longer

## Tips

- **Warmup runs** fill CPU caches and stabilize measurements
- **Multiple runs** account for system load variance
- **Steady-state metrics** (p50, p95, p99) show typical vs worst-case
- **HyperPipeline** provides significantly higher throughput on multi-core systems
- **Performance modes** auto-tune batch sizes and thread counts
