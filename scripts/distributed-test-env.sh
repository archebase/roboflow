#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0
#
# distributed-test-env.sh - Environment setup for distributed testing
#
# Usage:
#   source scripts/distributed-test-env.sh
#
# This script sets up all required environment variables for testing
# the distributed pipeline with local MinIO and TiKV.

set -euo pipefail

# =============================================================================
# Configuration
# =============================================================================

# MinIO/S3 Configuration
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}"
export AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL:-http://127.0.0.1:9000}"
export AWS_REGION="${AWS_REGION:-us-east-1}"

# TiKV Configuration
export TIKV_PD_ENDPOINTS="${TIKV_PD_ENDPOINTS:-127.0.0.1:2379}"

# Roboflow Configuration
export ROBOFLOW_USER="${ROBOFLOW_USER:-$(whoami)}"
export ROBOFLOW_OUTPUT_PREFIX="${ROBOFLOW_OUTPUT_PREFIX:-s3://roboflow-output/}"

# Logging
export RUST_LOG="${RUST_LOG:-roboflow=info,roboflow_distributed=info,roboflow_distributed::batch::controller=warn,tikv_client=warn}"

# =============================================================================
# Helper Functions
# =============================================================================

# Print current environment configuration
show-config() {
    cat <<EOF
=============================================================================
Distributed Test Environment Configuration
=============================================================================
S3/MinIO:
  Endpoint:   ${AWS_ENDPOINT_URL}
  Access Key: ${AWS_ACCESS_KEY_ID}
  Region:     ${AWS_REGION}
  Input:      s3://roboflow-raw/
  Output:     s3://roboflow-output/

TiKV:
  PD Endpoints: ${TIKV_PD_ENDPOINTS}

Roboflow:
  User:          ${ROBOFLOW_USER}
  Output Prefix: ${ROBOFLOW_OUTPUT_PREFIX}
  Config:        examples/rust/lerobot_config.toml

Logging:
  RUST_LOG: ${RUST_LOG}
=============================================================================
EOF
}

# Check if required services are running
check-services() {
    echo "Checking required services..."

    # Check MinIO
    if curl -sf "${AWS_ENDPOINT_URL}/minio/health/live" > /dev/null 2>&1; then
        echo "  ✓ MinIO is running at ${AWS_ENDPOINT_URL}"
    else
        echo "  ✗ MinIO is NOT running at ${AWS_ENDPOINT_URL}"
        echo "    Start with: docker run -p 9000:9000 -p 9001:9001 minio/minio server /data --console-address ':9001'"
        return 1
    fi

    # Check TiKV
    if nc -z "${TIKV_PD_ENDPOINTS%:*}" "${TIKV_PD_ENDPOINTS#*:}" 2>/dev/null; then
        echo "  ✓ TiKV PD is running at ${TIKV_PD_ENDPOINTS}"
    else
        echo "  ✗ TiKV PD is NOT running at ${TIKV_PD_ENDPOINTS}"
        echo "    Start with: docker-compose -f scripts/docker-compose.yml up -d tikv pd"
        return 1
    fi

    echo "All services are running!"
    return 0
}

# List buckets in MinIO
list-buckets() {
    echo "Listing S3 buckets..."
    aws configure set aws_access_key_id "${AWS_ACCESS_KEY_ID}"
    aws configure set aws_secret_access_key "${AWS_SECRET_ACCESS_KEY}"
    aws configure set default.region "${AWS_REGION}"

    AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL}" aws s3 ls --endpoint-url "${AWS_ENDPOINT_URL}" 2>/dev/null || true
}

# List input files
list-input-files() {
    echo "Listing input files in s3://roboflow-raw/..."
    aws configure set aws_access_key_id "${AWS_ACCESS_KEY_ID}"
    aws configure set aws_secret_access_key "${AWS_SECRET_ACCESS_KEY}"
    aws configure set default.region "${AWS_REGION}"

    AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL}" aws s3 ls "s3://roboflow-raw/" --endpoint-url "${AWS_ENDPOINT_URL}" 2>/dev/null || echo "  (bucket empty or not accessible)"
}

# =============================================================================
# Main
# =============================================================================

# Show configuration when sourced
show-config

# Export helper functions
export -f show-config
export -f check-services
export -f list-buckets
export -f list-input-files

echo "Environment variables set. Run 'check-services' to verify services."
echo ""
