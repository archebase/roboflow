# Roboflow

[![License: MulanPSL-2.0](https://img.shields.io/badge/License-MulanPSL--2.0-blue.svg)](http://license.coscl.org.cn/MulanPSL2)
[![Rust](https://img.shields.io/badge/rust-1.80%2B-orange.svg)](https://www.rust-lang.org)

[English](README.md) | [简体中文](README_zh.md)

**Roboflow** 是一个分布式数据转换流水线，用于将机器人 bag/MCAP 文件转换为可训练的数据集（LeRobot 格式）。

## 特性

- **水平扩展**：基于 TiKV 的分布式协调处理
- **模式驱动转换**：支持 CDR（ROS1/ROS2）、Protobuf、JSON 消息格式
- **零拷贝分配**：基于 Arena 的内存高效设计（减少约 22% 开销）
- **云存储支持**：原生支持 S3 和阿里云 OSS，用于分布式工作负载
- **高吞吐量**：并行分块处理，最高可达 ~1800 MB/s
- **LeRobot 导出**：转换为 LeRobot 数据集格式，用于机器人学习

## 架构

Roboflow 采用**受 Kubernetes 启发的分布式控制平面**，实现容错的批处理。

```
┌─────────────────────────────────────────────────────────────────────┐
│                         控制平面 (Control Plane)                    │
├─────────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │
│  │   Scanner    │  │   Reaper     │  │  Finalizer   │              │
│  │  控制器      │  │  控制器      │  │  控制器      │              │
│  │              │  │              │  │              │              │
│  │ • 发现文件   │  │ • 检测失活   │  │ • 监控批处理  │              │
│  │ • 创建作业   │  │   Pod        │  │ • 触发合并    │              │
│  │              │  │ • 回收孤儿   │  │              │              │
│  │              │  │   作业       │  │              │              │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘              │
│         │                 │                 │                       │
│         └─────────────────┼─────────────────┘                       │
│                           │                                         │
│                           ▼                                         │
│                    ┌─────────────┐                                  │
│                    │    TiKV     │                                  │
│                    │  (类似 etcd │                                  │
│                    │   状态存储) │                                  │
│                    └─────────────┘                                  │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                          数据平面 (Data Plane)                       │
├─────────────────────────────────────────────────────────────────────┤
│  Worker (pod-abc)    Worker (pod-def)    Worker (pod-xyz)           │
│  • 认领作业          • 认领作业          • 认领作业                  │
│  • 发送心跳          • 发送心跳          • 发送心跳                  │
│  • 处理数据          • 处理数据          • 处理数据                  │
│  • 保存检查点        • 保存检查点        • 保存检查点                │
└─────────────────────────────────────────────────────────────────────┘
```

### 核心模式

| Kubernetes 概念 | Roboflow 等价实现 |
|----------------|-------------------|
| Pod | 带 `pod_id` 的 `Worker` |
| etcd | TiKV 分布式存储 |
| kubelet 心跳 | `HeartbeatManager` |
| node-controller | `ZombieReaper` |
| Finalizers | `Finalizer` 控制器 |
| Job/CronJob | `JobRecord`, `BatchSpec` |
| Lease API | `LockManager` |

## 工作空间结构

| Crate | 用途 |
|-------|------|
| `roboflow-core` | 错误类型、注册表、值类型 |
| `roboflow-storage` | S3、OSS、本地存储（始终可用） |
| `roboflow-dataset` | KPS、LeRobot、流式转换器 |
| `roboflow-distributed` | TiKV 客户端、目录、控制器 |
| `roboflow-hdf5` | 可选的 HDF5 格式支持 |
| `roboflow-pipeline` | Hyper 流水线、压缩阶段 |

## 快速开始

### 提交转换任务

```bash
roboflow submit \
  --input s3://bucket/input.bag \
  --output s3://bucket/output/ \
  --config lerobot_config.toml
```

### 运行 Worker

```bash
export TIKV_PD_ENDPOINTS="127.0.0.1:2379"
export AWS_ACCESS_KEY_ID="your-key"
export AWS_SECRET_ACCESS_KEY="your-secret"

roboflow worker
```

### 运行 Scanner

```bash
export SCANNER_INPUT_PREFIX="s3://bucket/input/"
export SCANNER_OUTPUT_PREFIX="s3://bucket/jobs/"

roboflow scanner
```

### 列出任务

```bash
roboflow jobs list
roboflow jobs get <job-id>
roboflow jobs retry <job-id>
```

## 安装

### 从源码构建

```bash
git clone https://github.com/archebase/roboflow.git
cd roboflow
cargo build --release
```

### 依赖要求

- Rust 1.80+
- TiKV 4.0+（用于分布式协调）
- ffmpeg（用于 LeRobot 数据集中的视频编码）

## 配置

### LeRobot 数据集配置 (`lerobot_config.toml`)

```toml
[dataset]
name = "my_dataset"
fps = 30
robot_type = "stretch"

[[mapping]]
topic = "/camera/image_raw"
name = "observation.images.camera_0"
encoding = "ros1msg"

[[mapping]]
topic = "/joint_states"
name = "observation.joint_state"
encoding = "cdr"
```

## 环境变量

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `TIKV_PD_ENDPOINTS` | TiKV PD 端点 | `127.0.0.1:2379` |
| `AWS_ACCESS_KEY_ID` | AWS 访问密钥 | - |
| `AWS_SECRET_ACCESS_KEY` | AWS 密钥 | - |
| `AWS_REGION` | AWS 区域 | - |
| `OSS_ACCESS_KEY_ID` | 阿里云 OSS 密钥 | - |
| `OSS_ACCESS_KEY_SECRET` | 阿里云 OSS 密钥 | - |
| `OSS_ENDPOINT` | 阿里云 OSS 端点 | - |
| `WORKER_POLL_INTERVAL_SECS` | 任务轮询间隔 | `5` |
| `WORKER_MAX_CONCURRENT_JOBS` | 最大并发任务数 | `1` |
| `SCANNER_SCAN_INTERVAL_SECS` | 扫描间隔 | `60` |

## 开发

### 构建

```bash
cargo build
cargo build --features distributed
```

### 测试

```bash
cargo test
cargo test --features distributed
```

### 格式化与检查

```bash
cargo fmt
cargo clippy --all-targets -- -D warnings
```

## 贡献

详见 [CONTRIBUTING.md](CONTRIBUTING.md)。

## 许可证

本项目采用 MulanPSL v2 许可证 - 详见 [LICENSE](LICENSE) 文件。

## 相关项目

- [robocodec](https://github.com/archebase/robocodec) - I/O、编解码器、Arena 分配
- [LeRobot](https://github.com/huggingface/lerobot) - 机器人学习数据集
- [TiKV](https://github.com/tikv/tikv) - 分布式事务 KV 存储

## 链接

- [问题追踪](https://github.com/archebase/roboflow/issues)
- [更新日志](CHANGELOG.md)
