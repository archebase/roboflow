# roboflow-distributed

[![License: MulanPSL-2.0](https://img.shields.io/badge/License-MulanPSL--2.0-blue.svg)](http://license.coscl.org.cn/MulanPSL2)

Distributed coordination layer using TiKV for fault-tolerant batch processing.

## Features

- **TiKV Backend**: Distributed key-value store for state management
- **Worker Coordination**: Heartbeat-based worker registration
- **Job Queue**: Distributed task claiming with optimistic locking
- **Batch Processing**: Controller-based batch lifecycle management

## Architecture

```
┌─────────────────────────────────────────────┐
│              Control Plane                   │
├─────────────────────────────────────────────┤
│  Scanner     Reaper      Finalizer           │
│  Controller  Controller Controller           │
│       │          │          │                │
│       └──────────┼──────────┘                │
│                  ▼                           │
│           ┌───────────┐                      │
│           │   TiKV    │                      │
│           └───────────┘                      │
└─────────────────────────────────────────────┘

┌─────────────────────────────────────────────┐
│               Data Plane                     │
├─────────────────────────────────────────────┤
│  Worker-1    Worker-2    Worker-3           │
│  • Claim     • Claim     • Claim            │
│  • Process   • Process   • Process          │
│  • Complete  • Complete  • Complete         │
└─────────────────────────────────────────────┘
```

## Usage

### TiKV Client

```rust
use roboflow_distributed::tikv::{TikvClient, TikvConfig};

let client = TikvClient::new(TikvConfig {
    pd_endpoints: vec!["127.0.0.1:2379".to_string()],
    ..Default::default()
}).await?;

// Claim a job
let job = client.claim_job(&worker_id).await?;

// Send heartbeat
client.heartbeat(&worker_id).await?;
```

### Batch Processing

```rust
use roboflow_distributed::batch::{BatchController, BatchSpec};

let controller = BatchController::new(client);
controller.submit_batch(BatchSpec {
    source_prefix: "s3://bucket/raw/".to_string(),
    output_prefix: "s3://bucket/output/".to_string(),
    ..Default::default()
}).await?;
```

## Key Types

| Type | Purpose |
|------|---------|
| `TikvClient` | Low-level TiKV operations |
| `WorkerStatus` | Worker state machine |
| `CheckpointState` | Job progress tracking |
| `LockRecord` | Distributed locking |
| `HeartbeatRecord` | Worker health monitoring |

## Configuration

### Environment Variables
- `TIKV_PD_ENDPOINTS`: Comma-separated PD endpoints (default: `127.0.0.1:2379`)

### Connection Settings
```rust
TikvConfig {
    pd_endpoints: vec!["pd1:2379", "pd2:2379"],
    connection_timeout_secs: 30,
    operation_timeout_secs: 60,
}
```

## License

MulanPSL-2.0
