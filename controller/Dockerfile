# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0

# Dockerfile for the Roboflow Kubernetes Job Controller.
#
# This Dockerfile builds a container image for the Roboflow controller
# that manages Kubernetes Jobs for distributed data conversion.
#
# Note: The controller binary is not yet implemented.
# This Dockerfile serves as a placeholder for future implementation.
#
# Build:
#   docker build -t roboflow-controller:latest controller/

# =============================================================================
# Builder stage: Compile the Rust binary
# =============================================================================
ARG RUST_VERSION=1.83
FROM docker.io/library/rust:${RUST_VERSION}-slim AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /build

# Copy manifests and source
COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/
COPY src/ ./src/

# Build the controller binary (placeholder)
# TODO: Add controller binary target to Cargo.toml
# RUN --mount=type=cache,target=/usr/local/cargo/registry \
#     --mount=type=cache,target=/target \
#     cargo build --release --bin controller

# Placeholder: create a dummy binary for now
# Exit 0 to avoid crash loops in Kubernetes; controller is not yet implemented
RUN echo "#!/bin/sh\necho 'Controller not yet implemented'\nexit 0" > /tmp/controller \
    && chmod +x /tmp/controller

# =============================================================================
# Runtime stage: Minimal image with runtime dependencies
# =============================================================================
FROM docker.io/library/debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    libcurl4 \
    && rm -rf /var/lib/apt/lists/* \
    && update-ca-certificates

# Create non-root user
RUN groupadd -r roboflow && useradd -r -g roboflow -u 1001 roboflow

# Set working directory
WORKDIR /workspace

# Copy binary from builder
COPY --from=builder --chown=roboflow:roboflow /tmp/controller /usr/local/bin/controller

# Switch to non-root user
USER roboflow

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD controller --health || exit 1

# Default command
ENTRYPOINT ["/usr/local/bin/controller"]
