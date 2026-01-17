# Robocodec

[![Crates.io](https://img.shields.io/crates/v/robocodec)](https://crates.io/crates/robocodec)
[![PyPI](https://img.shields.io/pypi/v/robocodec)](https://pypi.org/project/robocodec/)
[![License: MulanPSL-2.0](https://img.shields.io/badge/License-MulanPSL--2.0-blue.svg)](http://mulan.cosine.org.cn/license/MulanPSL2)
[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org)

[English](README.md) | [简体中文](README_zh.md)

**Robocodec** 是一个通用的、基于模式的运行时解码引擎，用于处理机器人数据。它提供了统一的接口来解码、编码和转换不同的机器人消息格式和数据存储格式。

## 特性

- **多格式支持**：解码和编码 CDR（ROS1/ROS2）、Protobuf 和 JSON 消息
- **文件格式支持**：读写 MCAP 和 ROS1 bag 文件
- **模式解析**：解析 ROS `.msg` 文件、ROS2 IDL 和 OMG IDL 格式
- **跨语言**：提供 Rust 和 Python API，功能完全对等
- **数据转换**：内置格式转换、主题重命名和类型规范化工具
- **LeRobot 集成**：支持将机器人数据集转换为 LeRobot 格式

## 安装

### Rust

在 `Cargo.toml` 中添加 `robocodec`：

```toml
[dependencies]
robocodec = "0.1"
```

根据需要启用可选特性：

```toml
robocodec = { version = "0.1", features = ["python", "lerobot-all"] }
```

### Python

从 PyPI 安装：

```bash
pip install robocodec
```

或从源码构建：

```bash
pip install .
```

## 快速开始

### Rust API

```rust
use robocodec::Reader;

// 打开机器人数据文件
let reader = Reader::open("data.bag")?;

// 遍历消息
for result in reader.iter_messages() {
    let (topic, message) = result?;
    println!("Topic: {}, Data: {}", topic, message);
}
```

### Python API

```python
from robocodec import Reader

# 打开机器人数据文件
reader = Reader("data.bag")

# 遍历消息
for topic, message in reader:
    print(f"Topic: {topic}, Data: {message}")
```

### 命令行工具

格式转换：

```bash
# ROS bag 转 MCAP
robocodec-convert input.bag output.mcap

# 检查文件内容
robocodec-inspect data.mcap

# 提取特定主题
robocodec-extract data.bag --topics /camera/image_raw --output extracted/
```

## 支持的格式

| 格式 | 读取 | 写入 | 说明 |
|--------|------|-------|-------|
| MCAP | ✅ | ✅ | Mission Data Capture 格式 |
| ROS1 Bag | ✅ | ✅ | ROS1 rosbag 格式 |
| CDR | ✅ | ✅ | 通用数据表示（ROS1/ROS2） |
| Protobuf | ✅ | ✅ | Protocol Buffers |
| JSON | ✅ | ✅ | JSON 序列化 |

## 模式支持

- ROS `.msg` 文件（ROS1）
- ROS2 IDL（接口定义语言）
- OMG IDL（对象管理组织）

## Python 绑定

Python 绑定提供对 Rust 核心的完整访问：

```python
from robocodec import Reader, Writer, decode, encode

# 从文件读取
reader = Reader("data.mcap")

# 写入文件
writer = Writer("output.bag")

# 解码二进制消息
data = decode(b"<二进制数据>", schema)

# 编码为二进制
binary = encode(data, schema)
```

## 可选特性

- `python` - 通过 PyO3 的 Python 绑定
- `lerobot-hdf5` - LeRobot HDF5 数据集支持
- `lerobot-parquet` - LeRobot Parquet 数据集支持
- `lerobot-all` - 所有 LeRobot 特性

## 命令行工具

| 工具 | 描述 |
|------|-------------|
| `robocodec-convert` | 在 bag/MCAP 格式之间转换 |
| `robocodec-extract` | 从文件中提取数据 |
| `robocodec-inspect` | 检查文件元数据 |
| `robocodec-schema` | 处理模式定义 |
| `robocodec-search` | 搜索数据文件 |
| `robocodec-extract_sample` | 创建样本数据集 |

## 开发

### 前置要求

- Rust 1.70 或更高版本
- Python 3.11+（用于 Python 绑定）
- maturin（用于构建 Python 包）

### 构建

```bash
# 构建 Rust 库
cargo build --release

# 构建 Python 包
maturin develop

# 运行测试
cargo test

# 运行 Python 测试
pytest
```

### 运行示例

```bash
cargo run --bin convert -- input.bag output.mcap
cargo run --bin inspect -- data.mcap
```

## 贡献

我们欢迎贡献！请参阅 [贡献指南](CONTRIBUTING_zh.md) 了解详情。

## 许可证

本项目采用 MulanPSL v2 许可证 - 详见 [LICENSE](LICENSE) 文件。

## 致谢

Robocodec 最初作为 [Strata](https://github.com/archebase/strata) 机器人平台的一部分开发。

## 相关项目

- [MCAP](https://mcap.dev/) - Mission Data Capture 格式
- [LeRobot](https://github.com/huggingface/lerobot) - 机器人学习数据集
- [ROS](https://www.ros.org/) - 机器人操作系统

## 链接

- [文档](https://github.com/archebase/robocodec/wiki)
- [问题追踪](https://github.com/archebase/robocodec/issues)
- [更新日志](CHANGELOG.md)
