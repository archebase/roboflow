#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0
#
# distributed-list.sh - List batches and jobs
#
# Usage:
#   ./scripts/distributed-list.sh [OPTIONS]
#
# Examples:
#   ./scripts/distributed-list.sh              # List all batches
#   ./scripts/distributed-list.sh --jobs       # List all jobs
#   ./scripts/distributed-list.sh --failed    # Show only failed

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# =============================================================================
# Configuration
# =============================================================================

ROBOFLOW_BIN="${PROJECT_ROOT}/target/debug/roboflow"
TIKV_ENDPOINTS="${TIKV_PD_ENDPOINTS:-127.0.0.1:2379}"

# =============================================================================
# Functions
# =============================================================================

usage() {
    cat <<EOF
List batches and jobs in the distributed pipeline.

USAGE:
    $(basename "$0") [OPTIONS]

OPTIONS:
    -j, --jobs           List jobs instead of batches
    -b, --batch <ID>     List jobs for specific batch
    -f, --failed         Show only failed batches/jobs
    -r, --running        Show only running batches/jobs
    -c, --complete       Show only completed batches
    -o, --output FORMAT  Output format: table, json, csv (default: table)
    -h, --help           Show this help

EXAMPLES:
    # List all batches
    $(basename "$0")

    # List all jobs
    $(basename "$0") --jobs

    # List jobs for specific batch
    $(basename "$0") --batch abc123

    # Show only failed items
    $(basename "$0") --failed

    # Output as JSON
    $(basename "$0") --output json

ENVIRONMENT VARIABLES:
    TIKV_PD_ENDPOINTS    TiKV PD endpoints (default: 127.0.0.1:2379)
EOF
}

log-info() {
    echo "[INFO] $(date '+%Y-%m-%d %H:%M:%S') $*"
}

list-batches() {
    local filter="$1"

    case "${filter}" in
        failed)
            "${ROBOFLOW_BIN}" batch list --tikv-endpoints "${TIKV_ENDPOINTS}" 2>&1 | grep -i "failed" || true
            ;;
        running)
            "${ROBOFLOW_BIN}" batch list --tikv-endpoints "${TIKV_ENDPOINTS}" 2>&1 | grep -E "(Running|Discovering|Merging)" || true
            ;;
        complete)
            "${ROBOFLOW_BIN}" batch list --tikv-endpoints "${TIKV_ENDPOINTS}" 2>&1 | grep -i "complete" || true
            ;;
        *)
            "${ROBOFLOW_BIN}" batch list --tikv-endpoints "${TIKV_ENDPOINTS}" 2>&1
            ;;
    esac
}

list-jobs() {
    local batch_id="$1"
    local filter="$2"
    local output

    if [[ -n "${batch_id}" ]]; then
        output=$("${ROBOFLOW_BIN}" batch status "${batch_id}" --tikv-endpoints "${TIKV_ENDPOINTS}" 2>&1)
    else
        output=$("${ROBOFLOW_BIN}" batch list --tikv-endpoints "${TIKV_ENDPOINTS}" 2>&1)
    fi

    # Apply filter
    case "${filter}" in
        failed)
            echo "${output}" | grep -i "failed" || true
            ;;
        running)
            echo "${output}" | grep -E "(Running|Pending|Discovering)" || true
            ;;
        complete)
            echo "${output}" | grep -i "complete" || true
            ;;
        *)
            echo "${output}"
            ;;
    esac
}

show-summary() {
    echo "==============================================================================="
    echo "Distributed Pipeline Summary"
    echo "==============================================================================="

    # Get batch list output
    local batch_output
    batch_output=$("${ROBOFLOW_BIN}" batch list --tikv-endpoints "${TIKV_ENDPOINTS}" 2>&1)

    # Count batches by status
    local total running complete failed
    total=$(echo "${batch_output}" | grep -c "^jobs:" || echo "0")
    running=$(echo "${batch_output}" | grep -cE "(Running|Discovering|Merging)" || echo "0")
    complete=$(echo "${batch_output}" | grep -c "Complete" || echo "0")
    failed=$(echo "${batch_output}" | grep -c "Failed" || echo "0")

    echo "Total Batches:   ${total}"
    echo "Running:         ${running}"
    echo "Complete:        ${complete}"
    echo "Failed:          ${failed}"
    echo "==============================================================================="
    echo ""
}

# =============================================================================
# Main
# =============================================================================

SHOW_JOBS=""
BATCH_ID=""
FILTER=""
OUTPUT_FORMAT=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -j|--jobs)
            SHOW_JOBS="true"
            shift
            ;;
        -b|--batch)
            BATCH_ID="$2"
            shift 2
            ;;
        -f|--failed)
            FILTER="failed"
            shift
            ;;
        -r|--running)
            FILTER="running"
            shift
            ;;
        -c|--complete)
            FILTER="complete"
            shift
            ;;
        -o|--output)
            OUTPUT_FORMAT="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage
            exit 1
            ;;
    esac
done

# Check if binary exists
if [[ ! -f "${ROBOFLOW_BIN}" ]]; then
    echo "Error: Roboflow binary not found at ${ROBOFLOW_BIN}" >&2
    echo "Build first: cargo build" >&2
    exit 1
fi

# Show summary first
show-summary

# List items
if [[ "${SHOW_JOBS}" == "true" ]]; then
    list-jobs "${BATCH_ID}" "${FILTER}"
else
    list-batches "${FILTER}"
fi
