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

# =============================================================================
# Functions
# =============================================================================

usage() {
    cat <<EOF
Reset TiKV batch jobs for testing. WARNING: This cancels all batch jobs!

USAGE:
    $(basename "$0") [OPTIONS]

OPTIONS:
    -x, --execute         Actually cancel batch jobs (default: dry-run)
    -y, --yes             Skip confirmation prompt
    -h, --help            Show this help

EXAMPLES:
    # Dry run - show what would be canceled
    $(basename "$0")

    # Actually cancel all batch jobs (with confirmation)
    $(basename "$0") --execute

    # Cancel without confirmation
    $(basename "$0") --execute --yes

NOTE:
    For a complete wipe of all TiKV data (configs, worker state, etc.),
    use 'docker compose down -v' followed by 'make dev-up'.

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

delete-batches() {
    local execute="$1"

    if [[ "${execute}" != "true" ]]; then
        echo "[DRY RUN] Would delete all batches"
        return 0
    fi

    log-info "Deleting all batches..."

    # Get list of all batch IDs and cancel them
    local batches
    batches=$("${ROBOFLOW_BIN}" batch list --tikv-endpoints "${TIKV_ENDPOINTS}" 2>/dev/null | grep -oE 'jobs:[a-f0-9]+' || true)

    if [[ -n "${batches}" ]]; then
        while IFS= read -r batch_id; do
            if [[ -n "${batch_id}" ]]; then
                log-info "Canceling batch: ${batch_id}"
                "${ROBOFLOW_BIN}" batch cancel "${batch_id}" --pd-endpoints "${TIKV_ENDPOINTS}" >/dev/null 2>&1 || true
            fi
        done <<< "${batches}"
    else
        log-info "No batches found to delete"
    fi
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
SKIP_CONFIRM=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -x|--execute)
            EXECUTE="true"
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

# Show what would be canceled
cat <<EOF

Would cancel:
  - All batch jobs

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

delete-batches "${EXECUTE}"

# Note: Configs, worker state, heartbeats, and work units are tied to batches
# and will be cleaned up automatically when batches are deleted via TiKV's
# internal garbage collection. For a complete wipe, use 'docker compose down -v'

log-info "Deletion complete!"
echo ""
echo "Use 'roboflow batch list' to verify."
