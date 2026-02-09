#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0
#
# test-distributed.sh - One-shot distributed testing script
#
# Usage:
#   ./scripts/test-distributed.sh [command] [args...]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

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
export ROBOFLOW_OUTPUT_PREFIX="${ROBOFLOW_OUTPUT_PREFIX:-s3://roboflow-datasets/}"

# Logging
export RUST_LOG="${RUST_LOG:-roboflow=debug,roboflow_distributed=debug,tikv_client=warn}"

ROBOFLOW_BIN="${PROJECT_ROOT}/target/debug/roboflow"
CONFIG_FILE="${CONFIG_FILE:-examples/rust/lerobot_config.toml}"
OUTPUT_PREFIX="${ROBOFLOW_OUTPUT_PREFIX:-s3://roboflow-datasets/}"

# =============================================================================
# Functions
# =============================================================================

usage() {
    cat <<EOF
Roboflow Distributed Testing Script

USAGE:
    ./scripts/test-distributed.sh <command> [args...]

COMMANDS:
    env         Show environment configuration
    check       Check if required services are running
    submit      Submit a job for processing
    run         Run the worker service
    status      Show batch/job status
    list        List all batches or jobs
    logs        View logs
    reset       Reset TiKV state (dry-run by default)

OPTIONS FOR 'submit':
    ./scripts/test-distributed.sh submit <input-file>
    Example: ./scripts/test-distributed.sh submit s3://roboflow-raw/file.bag

OPTIONS FOR 'run':
    ./scripts/test-distributed.sh run [role]
    Example: ./scripts/test-distributed.sh run worker

OPTIONS FOR 'status':
    ./scripts/test-distributed.sh status [batch-id]
    Example: ./scripts/test-distributed.sh status abc123

OPTIONS FOR 'logs':
    ./scripts/test-distributed.sh logs [batch-id] [--follow]
    Example: ./scripts/test-distributed.sh logs --follow

OPTIONS FOR 'reset':
    ./scripts/test-distributed.sh reset [--execute]
    Example: ./scripts/test-distributed.sh reset --execute

EXAMPLES:
    # Check services
    ./scripts/test-distributed.sh check

    # Submit a job
    ./scripts/test-distributed.sh submit s3://roboflow-raw/file.bag

    # Run worker
    ./scripts/test-distributed.sh run

    # Watch status
    ./scripts/test-distributed.sh status

    # Watch logs
    ./scripts/test-distributed.sh logs --follow

ENVIRONMENT (can be set before running):
    AWS_ACCESS_KEY_ID       S3/MinIO access key (default: minioadmin)
    AWS_SECRET_ACCESS_KEY   S3/MinIO secret key (default: minioadmin)
    AWS_ENDPOINT_URL        S3/MinIO endpoint (default: http://127.0.0.1:9000)
    TIKV_PD_ENDPOINTS       TiKV PD endpoints (default: 127.0.0.1:2379)
    RUST_LOG                Logging level (default: roboflow=debug)
EOF
}

log-info() {
    echo "[INFO] $*"
}

log-error() {
    echo "[ERROR] $*" >&2
}

cmd-env() {
    cat <<EOF

=============================================================================
Distributed Test Environment
=============================================================================
S3/MinIO:
  Endpoint:   ${AWS_ENDPOINT_URL}
  Access Key: ${AWS_ACCESS_KEY_ID}
  Input:      s3://roboflow-raw/
  Output:     s3://roboflow-datasets/

TiKV:
  PD Endpoints: ${TIKV_PD_ENDPOINTS}

Roboflow:
  User:          ${ROBOFLOW_USER}
  Output Prefix: ${ROBOFLOW_OUTPUT_PREFIX}
  Config:        ${CONFIG_FILE}

Logging:
  RUST_LOG: ${RUST_LOG}
=============================================================================
EOF
}

cmd-check() {
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
    local pd_host="${TIKV_PD_ENDPOINTS%:*}"
    local pd_port="${TIKV_PD_ENDPOINTS#*:}"
    if nc -z "${pd_host}" "${pd_port}" 2>/dev/null; then
        echo "  ✓ TiKV PD is running at ${TIKV_PD_ENDPOINTS}"
    else
        echo "  ✗ TiKV PD is NOT running at ${TIKV_PD_ENDPOINTS}"
        echo "    Start with: docker run -p 2379:2379 pingcap/tikv:latest --addr 0.0.0.0:20160 --pd-endpoints ${TIKV_PD_ENDPOINTS}"
        return 1
    fi

    echo "All services are running!"
    return 0
}

cmd-submit() {
    if [[ $# -lt 1 ]]; then
        log-error "Usage: $0 submit <input-file>"
        exit 1
    fi

    local input="$1"

    if [[ ! -f "${ROBOFLOW_BIN}" ]]; then
        log-error "Roboflow binary not found. Build first: cargo build"
        exit 1
    fi

    log-info "Submitting job: ${input}"
    log-info "Output: ${OUTPUT_PREFIX}"
    log-info "Config: ${CONFIG_FILE}"

    "${ROBOFLOW_BIN}" submit \
        -c "${CONFIG_FILE}" \
        -o "${OUTPUT_PREFIX}" \
        --tikv-endpoints "${TIKV_PD_ENDPOINTS}" \
        "${input}"
}

cmd-run() {
    local role="${1:-unified}"

    if [[ ! -f "${ROBOFLOW_BIN}" ]]; then
        log-error "Roboflow binary not found. Build first: cargo build"
        exit 1
    fi

    log-info "Starting Roboflow worker (role: ${role})..."
    log-info "  TiKV:     ${TIKV_PD_ENDPOINTS}"
    log-info "  S3/MinIO: ${AWS_ENDPOINT_URL}"
    log-info "  Output:   ${OUTPUT_PREFIX}"
    log-info "Press Ctrl+C to stop"

    exec "${ROBOFLOW_BIN}" run --role "${role}"
}

cmd-status() {
    local batch_id="${1:-}"

    if [[ ! -f "${ROBOFLOW_BIN}" ]]; then
        log-error "Roboflow binary not found. Build first: cargo build"
        exit 1
    fi

    if [[ -n "${batch_id}" ]]; then
        "${ROBOFLOW_BIN}" batch status "${batch_id}" --tikv-endpoints "${TIKV_PD_ENDPOINTS}"
    else
        "${ROBOFLOW_BIN}" batch list --tikv-endpoints "${TIKV_PD_ENDPOINTS}"
    fi
}

cmd-list() {
    if [[ ! -f "${ROBOFLOW_BIN}" ]]; then
        log-error "Roboflow binary not found. Build first: cargo build"
        exit 1
    fi

    "${ROBOFLOW_BIN}" batch list --tikv-endpoints "${TIKV_PD_ENDPOINTS}"
}

cmd-logs() {
    if [[ ! -f "${ROBOFLOW_BIN}" ]]; then
        log-error "Roboflow binary not found. Build first: cargo build"
        exit 1
    fi

    local follow=""
    local batch_id=""

    for arg in "$@"; do
        if [[ "${arg}" == "--follow" || "${arg}" == "-f" ]]; then
            follow="true"
        elif [[ "${arg}" != -* ]]; then
            batch_id="${arg}"
        fi
    done

    if [[ "${follow}" == "true" ]]; then
        log-info "Following status (Ctrl+C to stop)..."
        if [[ -n "${batch_id}" ]]; then
            exec "${ROBOFLOW_BIN}" batch status "${batch_id}" --watch --tikv-endpoints "${TIKV_PD_ENDPOINTS}"
        else
            while true; do
                clear
                echo "==============================================================================="
                echo "Roboflow Status - $(date '+%Y-%m-%d %H:%M:%S')"
                echo "==============================================================================="
                echo ""
                "${ROBOFLOW_BIN}" batch list --tikv-endpoints "${TIKV_PD_ENDPOINTS}" 2>&1
                echo ""
                echo "Press Ctrl+C to stop. Refreshing in 3s..."
                sleep 3
            done
        fi
    else
        if [[ -n "${batch_id}" ]]; then
            "${ROBOFLOW_BIN}" batch status "${batch_id}" --tikv-endpoints "${TIKV_PD_ENDPOINTS}"
        else
            "${ROBOFLOW_BIN}" batch list --tikv-endpoints "${TIKV_PD_ENDPOINTS}"
        fi
    fi
}

cmd-reset() {
    local execute="false"

    for arg in "$@"; do
        if [[ "${arg}" == "--execute" || "${arg}" == "-x" ]]; then
            execute="true"
        fi
    done

    echo ""
    echo "==============================================================================="
    echo "TiKV State Reset"
    echo "==============================================================================="
    echo ""
    echo "Current batches:"
    "${ROBOFLOW_BIN}" batch list --tikv-endpoints "${TIKV_PD_ENDPOINTS}" 2>&1 || echo "  (no batches)"
    echo ""
    echo "==============================================================================="
    echo ""

    if [[ "${execute}" == "true" ]]; then
        echo "Reset functionality requires TiKV client tools."
        echo "For now, please manually delete batches using:"
        echo "  ./scripts/test-distributed.sh list"
        echo "  Then cancel individual batches as needed."
    else
        echo "DRY RUN - Add --execute to actually reset"
    fi
}

# =============================================================================
# Main
# =============================================================================

if [[ $# -lt 1 ]]; then
    usage
    exit 1
fi

COMMAND="$1"
shift

case "${COMMAND}" in
    env)
        cmd-env
        ;;
    check)
        cmd-check
        ;;
    submit)
        cmd-submit "$@"
        ;;
    run)
        cmd-run "$@"
        ;;
    status)
        cmd-status "$@"
        ;;
    list)
        cmd-list "$@"
        ;;
    logs)
        cmd-logs "$@"
        ;;
    reset)
        cmd-reset "$@"
        ;;
    -h|--help|help)
        usage
        ;;
    *)
        log-error "Unknown command: ${COMMAND}"
        usage
        exit 1
        ;;
esac
