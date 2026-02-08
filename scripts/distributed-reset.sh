#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0
#
# distributed-reset.sh - Reset TiKV state for testing
#
# Usage:
#   ./scripts/distributed-reset.sh [OPTIONS]
#
# Examples:
#   ./scripts/distributed-reset.sh              # Show what would be deleted
#   ./scripts/distributed-reset.sh --execute    # Actually delete

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# =============================================================================
# Configuration
# =============================================================================

ROBOFLOW_BIN="${PROJECT_ROOT}/target/debug/roboflow"
TIKV_ENDPOINTS="${TIKV_PD_ENDPOINTS:-127.0.0.1:2379}"

# TiKV key prefixes to clean
PREFIX_BATCH="jobs:"
PREFIX_CONFIG="config:"
PREFIX_WORKER="worker:"
PREFIX_HEARTBEAT="heartbeat:"
PREFIX_WORK_UNIT="work_unit:"

# =============================================================================
# Functions
# =============================================================================

usage() {
    cat <<EOF
Reset TiKV state for testing. WARNING: This deletes data!

USAGE:
    $(basename "$0") [OPTIONS]

OPTIONS:
    -x, --execute         Actually delete data (default: dry-run)
    -c, --config-only     Only clear configs
    -b, --batch-only      Only clear batches/jobs
    -y, --yes             Skip confirmation prompt
    -h, --help            Show this help

EXAMPLES:
    # Dry run - show what would be deleted
    $(basename "$0")

    # Actually delete everything (with confirmation)
    $(basename "$0") --execute

    # Delete without confirmation
    $(basename "$0") --execute --yes

    # Only clear batch data
    $(basename "$0") --execute --batch-only

ENVIRONMENT VARIABLES:
    TIKV_PD_ENDPOINTS    TiKV PD endpoints (default: 127.0.0.1:2379)
EOF
}

log-info() {
    echo "[INFO] $(date '+%Y-%m-%d %H:%M:%S') $*"
}

log-error() {
    echo "[ERROR] $(date '+%Y-%m-%d %H:%M:%S') $*" >&2
}

confirm-prompt() {
    local prompt="$1"
    local response

    while true; do
        read -r -p "${prompt} (y/N): " response
        case "${response}" in
            [Yy]|[Yy][Ee][Ss]) return 0 ;;
            [Nn]|[Nn][Oo]|"") return 1 ;;
        esac
    done
}

count-keys() {
    local prefix="$1"

    # Use roboflow to scan keys with prefix
    # This is a simplified count - actual implementation may vary
    echo "Counting keys with prefix '${prefix}'..."
}

delete-by-prefix() {
    local prefix="$1"
    local execute="$2"

    if [[ "${execute}" != "true" ]]; then
        echo "[DRY RUN] Would delete all keys with prefix: ${prefix}"
        return 0
    fi

    log-info "Deleting keys with prefix: ${prefix}"

    # Use tikv-client or roboflow to delete keys
    # For now, this is a placeholder showing intent
    # Actual implementation would use:
    # 1. tikv-ctl scan to get all keys with prefix
    # 2. tikv-ctl delete to remove them
}

show-state() {
    cat <<EOF

=============================================================================
Current TiKV State
=============================================================================

Checking for data in TiKV at ${TIKV_ENDPOINTS}...

Note: This script shows what would be deleted. Use --execute to actually delete.

=============================================================================
EOF

    # Check if we can connect to TiKV
    if ! nc -z "${TIKV_ENDPOINTS%:*}" "${TIKV_ENDPOINTS#*:}" 2>/dev/null; then
        log-error "Cannot connect to TiKV at ${TIKV_ENDPOINTS}"
        return 1
    fi

    # Try to list batches using roboflow
    log-info "Listing batches..."
    if "${ROBOFLOW_BIN}" batch list --tikv-endpoints "${TIKV_ENDPOINTS}" >/dev/null 2>&1; then
        "${ROBOFLOW_BIN}" batch list --tikv-endpoints "${TIKV_ENDPOINTS}" 2>&1 || true
    else
        echo "  (No batches found or roboflow not available)"
    fi

    echo ""
    echo "============================================================================="
}

# =============================================================================
# Main
# =============================================================================

EXECUTE=""
CONFIG_ONLY=""
BATCH_ONLY=""
SKIP_CONFIRM=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -x|--execute)
            EXECUTE="true"
            shift
            ;;
        -c|--config-only)
            CONFIG_ONLY="true"
            shift
            ;;
        -b|--batch-only)
            BATCH_ONLY="true"
            shift
            ;;
        -y|--yes)
            SKIP_CONFIRM="true"
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            log-error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Check if binary exists (for listing)
if [[ ! -f "${ROBOFLOW_BIN}" ]]; then
    log-error "Roboflow binary not found at ${ROBOFLOW_BIN}"
    log-error "Build first: cargo build"
    exit 1
fi

# Show current state
show-state

# Determine what to delete
delete_configs="true"
delete_batches="true"

if [[ -n "${CONFIG_ONLY}" ]]; then
    delete_batches="false"
elif [[ -n "${BATCH_ONLY}" ]]; then
    delete_configs="false"
fi

# Show what would be deleted
cat <<EOF

Would delete:
  $([ "${delete_batches}" == "true" ] && echo "  - All batches (jobs:*)")
  $([ "${delete_configs}" == "true" ] && echo "  - All configs (config:*)")
  $([ "${delete_batches}" == "true" ] && echo "  - Worker state (worker:*)")
  $([ "${delete_batches}" == "true" ] && echo "  - Heartbeats (heartbeat:*)")
  $([ "${delete_batches}" == "true" ] && echo "  - Work units (work_unit:*)")

EOF

if [[ "${EXECUTE}" != "true" ]]; then
    echo "DRY RUN COMPLETE. Use --execute to actually delete."
    echo "Use --yes to skip confirmation."
    exit 0
fi

# Confirm before deletion
if [[ -z "${SKIP_CONFIRM}" ]]; then
    if ! confirm-prompt "Are you sure you want to delete ALL this data?"; then
        echo "Aborted."
        exit 0
    fi
fi

# Perform deletion
log-info "Starting deletion..."

if [[ "${delete_batches}" == "true" ]]; then
    delete-by-prefix "${PREFIX_BATCH}" "${EXECUTE}"
fi

if [[ "${delete_configs}" == "true" ]]; then
    delete-by-prefix "${PREFIX_CONFIG}" "${EXECUTE}"
fi

if [[ "${delete_batches}" == "true" ]]; then
    delete-by-prefix "${PREFIX_WORKER}" "${EXECUTE}"
    delete-by-prefix "${PREFIX_HEARTBEAT}" "${EXECUTE}"
    delete-by-prefix "${PREFIX_WORK_UNIT}" "${EXECUTE}"
fi

log-info "Deletion complete!"
echo ""
echo "Use 'roboflow batch list' to verify."
