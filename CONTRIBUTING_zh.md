# 贡献指南

感谢您对 Roboflow 项目的关注！本文档提供了为该项目做贡献的指导原则和说明。

## 行为准则

请在所有互动中保持尊重和建设性。详情请参阅 [CODE_OF_CONDUCT_zh.md](CODE_OF_CONDUCT_zh.md)。

## 如何贡献

### 报告错误

在创建错误报告之前，请先检查现有 issue 以避免重复。创建错误报告时，请包含：

- **清晰的标题和描述**：总结问题
- **重现步骤**：重现错误的详细步骤
- **预期行为**：您期望发生什么
- **实际行为**：实际发生了什么
- **环境信息**：操作系统、Rust 版本
- **日志/错误信息**：任何相关的错误信息或堆栈跟踪
- **测试文件**：如果适用，提供可重现问题的示例数据文件

### 建议新功能

我们欢迎功能建议！请提供：

- **清晰的描述**：描述您提议的功能
- **用例**：解释用例以及为什么该功能有用
- **考虑的替代方案**：您考虑过的任何其他解决方案

### 提交 Pull Request

#### 准备工作

1. Fork 本仓库
2. 克隆您的 fork 并添加上游远程仓库：
   ```bash
   git clone https://github.com/YOUR_USERNAME/roboflow.git
   cd roboflow
   git remote add upstream https://github.com/archebase/roboflow.git
   ```

3. 为您的更改创建分支：
   ```bash
   git checkout -b feature/your-feature-name
   # 或
   git checkout -b fix/your-bug-fix
   ```

#### 进行更改

1. **遵循现有代码风格**：项目使用标准的 Rust 格式
2. **编写测试**：为新功能或错误修复添加测试
3. **更新文档**：更新相关文档、注释和 README
4. **提交信息**：使用清晰、描述性的提交信息：
   ```
   feat: 添加对 XYZ 格式的支持
   fix: 处理 CDR 解码器中的边界情况
   docs: 更新安装说明
   ```

#### 测试

提交前运行测试套件：

```bash
# 运行所有测试
cargo test

# 运行带数据集功能的测试（需要安装 HDF5）
cargo test --features dataset-all

# 运行 clippy
cargo clippy --all-features -- -D warnings

# 检查格式
cargo fmt -- --check
```

#### 提交

1. 将您的分支推送到 fork
2. 创建 Pull Request 到 `main` 分支
3. 填写 Pull Request 模板
4. 等待审核并处理反馈意见

## 开发流程

### 项目结构

```
roboflow/
├── crates/
│   ├── roboflow-core/         # 核心类型和错误处理
│   ├── roboflow-storage/      # 存储抽象（S3、OSS、本地）
│   ├── roboflow-dataset/      # 数据集写入器（KPS、LeRobot）
│   ├── roboflow-distributed/  # TiKV 分布式协调
│   ├── roboflow-hdf5/         # 可选的 HDF5 支持
│   ├── roboflow-pipeline/     # 流水线实现
│   └── roboflow/              # 主包，包含 CLI 工具
│       ├── src/
│       │   ├── pipeline/       # 流水线实现
│       │   └── bin/           # 命令行工具
└── Cargo.toml
```

**说明**：项目使用外部 `robocodec` 库进行 I/O 操作和格式处理。

### 添加功能

1. **新的编解码器支持**：在 `robocodec` 库中实现并更新注册表
2. **新的文件格式**：实现 Reader/Writer 接口
3. **新的模式格式**：将解析器添加到 schema 模块
4. **CLI 工具**：添加二进制文件到 `crates/roboflow/src/bin/` 并更新 Cargo.toml

### 测试指南

- **单元测试**：测试单个函数和模块
- **集成测试**：测试端到端功能
- **往返测试**：验证编解码一致性

## 发布流程

维护者遵循以下发布流程：

1. 更新 `Cargo.toml` 中的版本号
2. 更新 CHANGELOG.md
3. 创建 git 标签
4. 发布到 crates.io

## 有问题？

欢迎创建 issue 来询问或讨论贡献相关的问题。
