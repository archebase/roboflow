#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0
#
# distributed-status.sh - Monitor job and batch status
#
# Usage:
#   ./scripts/distributed-status.sh [batch-id]
#
# Examples:
#   ./scripts/distributed-status.sh              # List all batches
#   ./scripts/distributed-status.sh abc123      # Show specific batch
#   ./scripts/distributed-status.sh --watch      # Watch mode

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# =============================================================================
# Configuration
# =============================================================================

ROBOFLOW_BIN="${PROJECT_ROOT}/target/debug/roboflow"
TIKV_ENDPOINTS="${TIKV_PD_ENDPOINTS:-127.0.0.1:2379}"
WATCH_INTERVAL=5

# =============================================================================
# Functions
# =============================================================================

usage() {
    cat <<EOF
Monitor distributed job and batch status.

USAGE:
    $(basename "$0") [BATCH_ID] [OPTIONS]

ARGUMENTS:
    BATCH_ID    Optional batch ID to show details (default: list all)

OPTIONS:
    -w, --watch          Watch mode - refresh every ${WATCH_INTERVAL}s
    -j, --jobs           Show jobs within batch
    -h, --help           Show this help

EXAMPLES:
    # List all batches
    $(basename "$0")

    # Show specific batch details
    $(basename "$0") abc123

    # Watch all batches
    $(basename "$0") --watch

    # Watch specific batch with jobs
    $(basename "$0") abc123 --watch --jobs

ENVIRONMENT VARIABLES:
    TIKV_PD_ENDPOINTS    TiKV PD endpoints (default: 127.0.0.1:2379)
EOF
}

log-info() {
    echo "[INFO] $(date '+%Y-%m-%d %H:%M:%S') $*"
}

format-phase() {
    case "$1" in
        Pending)      echo "⏳ Pending" ;;
        Discovering)  echo "🔍 Discovering" ;;
        Running)      echo "▶️  Running" ;;
        Merging)      echo "🔄 Merging" ;;
        Complete)     echo "✅ Complete" ;;
        Failed)       echo "❌ Failed" ;;
        Cancelled)    echo "🚫 Cancelled" ;;
        *)            echo "$1" ;;
    esac
}

show-batch-list() {
    "${ROBOFLOW_BIN}" batch list --tikv-endpoints "${TIKV_ENDPOINTS}" 2>&1
}

show-batch-details() {
    local batch_id="$1"
    "${ROBOFLOW_BIN}" batch status "${batch_id}" --tikv-endpoints "${TIKV_ENDPOINTS}" 2>&1
}

show-batch-jobs() {
    local batch_id="$1"
    # batch status already shows work unit details; use JSON for richer output
    "${ROBOFLOW_BIN}" batch status "${batch_id}" --json --tikv-endpoints "${TIKV_ENDPOINTS}" 2>&1
}

watch-batches() {
    local show_jobs="$1"
    local batch_filter="$2"

    log-info "Watching batches (Ctrl+C to stop)..."
    echo ""

    while true; do
        clear
        echo "==============================================================================="
        echo "Roboflow Distributed Pipeline - Status Monitor"
        echo "==============================================================================="
        echo "Last updated: $(date '+%Y-%m-%d %H:%M:%S')"
        echo "==============================================================================="
        echo ""

        if [[ -n "${batch_filter}" ]]; then
            if [[ "${show_jobs}" == "true" ]]; then
                show-batch-details "${batch_filter}"
                echo ""
                echo "-------------------------------------------------------------------------------"
                echo ""
                show-batch-jobs "${batch_filter}"
            else
                show-batch-details "${batch_filter}"
            fi
        else
            show-batch-list
        fi

        echo ""
        echo "Press Ctrl+C to stop. Refreshing in ${WATCH_INTERVAL}s..."
        sleep "${WATCH_INTERVAL}"
    done
}

# =============================================================================
# Main
# =============================================================================

WATCH_MODE=""
SHOW_JOBS=""
BATCH_ID=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -w|--watch)
            WATCH_MODE="true"
            shift
            ;;
        -j|--jobs)
            SHOW_JOBS="true"
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        -*)
            echo "Unknown option: $1" >&2
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
    echo "Error: Roboflow binary not found at ${ROBOFLOW_BIN}" >&2
    echo "Build first: cargo build" >&2
    exit 1
fi

# Run in watch mode or single shot
if [[ "${WATCH_MODE}" == "true" ]]; then
    watch-batches "${SHOW_JOBS}" "${BATCH_ID}"
else
    if [[ -n "${BATCH_ID}" ]]; then
        show-batch-details "${BATCH_ID}"
        if [[ "${SHOW_JOBS}" == "true" ]]; then
            echo ""
            show-batch-jobs "${BATCH_ID}"
        fi
    else
        show-batch-list
    fi
fi
