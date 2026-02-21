#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0
#
# distributed-logs.sh - View and monitor distributed job logs
#
# Usage:
#   ./scripts/distributed-logs.sh [batch-id] [OPTIONS]
#
# Examples:
#   ./scripts/distributed-logs.sh              # Show recent logs from all workers
#   ./scripts/distributed-logs.sh abc123      # Show logs for specific batch
#   ./scripts/distributed-logs.sh --follow    # Follow logs in real-time

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# =============================================================================
# Configuration
# =============================================================================

ROBOFLOW_BIN="${PROJECT_ROOT}/target/debug/roboflow"
TIKV_ENDPOINTS="${TIKV_PD_ENDPOINTS:-127.0.0.1:2379}"
LOG_DIR="${LOG_DIR:-/tmp/roboflow-logs}"
LOG_LEVEL="${RUST_LOG:-roboflow=debug,roboflow_distributed=debug,tikv_client=warn}"

# =============================================================================
# Functions
# =============================================================================

usage() {
    cat <<EOF
View and monitor distributed job logs.

USAGE:
    $(basename "$0") [BATCH_ID] [OPTIONS]

ARGUMENTS:
    BATCH_ID    Optional batch ID to filter logs (default: show all)

OPTIONS:
    -f, --follow          Follow logs in real-time (like tail -f)
    -n, --lines <N>       Show last N lines (default: 100)
    -w, --worker <ID>     Filter by worker ID
    -l, --level <LEVEL>   Filter by log level (debug, info, warn, error)
    -h, --help            Show this help

EXAMPLES:
    # Show recent logs from all batches
    $(basename "$0")

    # Follow logs in real-time
    $(basename "$0") --follow

    # Show logs for specific batch
    $(basename "$0") abc123

    # Follow logs for specific batch
    $(basename "$0") abc123 --follow

    # Show logs with worker filter
    $(basename "$0") --worker roboflow-worker-1

ENVIRONMENT VARIABLES:
    TIKV_PD_ENDPOINTS    TiKV PD endpoints (default: 127.0.0.1:2379)
    RUST_LOG            Logging level for roboflow commands
EOF
}

log-info() {
    echo "[INFO] $(date '+%Y-%m-%d %H:%M:%S') $*"
}

log-error() {
    echo "[ERROR] $(date '+%Y-%m-%d %H:%M:%S') $*" >&2
}

show-batch-logs() {
    local batch_id="$1"
    local lines="${2:-100}"

    "${ROBOFLOW_BIN}" batch status "${batch_id}" \
        --tikv-endpoints "${TIKV_ENDPOINTS}" 2>&1 | tail -n "${lines}"
}

show-all-logs() {
    local lines="${1:-100}"

    "${ROBOFLOW_BIN}" batch list \
        --tikv-endpoints "${TIKV_ENDPOINTS}" 2>&1 | tail -n "${lines}"
}

follow-logs() {
    local batch_id="$1"

    if [[ -n "${batch_id}" ]]; then
        # Use the built-in --watch flag for a specific batch
        log-info "Watching batch ${batch_id} (Ctrl+C to stop)..."
        exec "${ROBOFLOW_BIN}" batch status "${batch_id}" --watch \
            --tikv-endpoints "${TIKV_ENDPOINTS}"
    fi

    log-info "Watching all batches (Ctrl+C to stop)..."
    echo ""

    while true; do
        clear
        echo "==============================================================================="
        echo "Roboflow Distributed Status - $(date '+%Y-%m-%d %H:%M:%S')"
        echo "==============================================================================="
        echo ""

        show-all-logs 50

        echo ""
        echo "Press Ctrl+C to stop. Refreshing in 3s..."
        sleep 3
    done
}

# =============================================================================
# Main
# =============================================================================

FOLLOW_MODE=""
LINES="100"
WORKER_ID=""
LOG_FILTER=""
BATCH_ID=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -f|--follow)
            FOLLOW_MODE="true"
            shift
            ;;
        -n|--lines)
            LINES="$2"
            shift 2
            ;;
        -w|--worker)
            WORKER_ID="$2"
            shift 2
            ;;
        -l|--level)
            LOG_FILTER="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        -*)
            log-error "Unknown option: $1"
            usage
            exit 1
            ;;
        *)
            BATCH_ID="$1"
            shift
            ;;
    esac
done

# Check if binary exists
if [[ ! -f "${ROBOFLOW_BIN}" ]]; then
    log-error "Roboflow binary not found at ${ROBOFLOW_BIN}"
    log-error "Build first: cargo build"
    exit 1
fi

# Run in follow mode or single shot
if [[ "${FOLLOW_MODE}" == "true" ]]; then
    follow-logs "${BATCH_ID}"
else
    if [[ -n "${BATCH_ID}" ]]; then
        show-batch-logs "${BATCH_ID}" "${LINES}"
    else
        show-all-logs "${LINES}"
    fi
fi
