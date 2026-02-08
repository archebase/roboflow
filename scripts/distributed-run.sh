#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0
#
# distributed-run.sh - Run the distributed worker service
#
# Usage:
#   ./scripts/distributed-run.sh [OPTIONS]
#
# Examples:
#   ./scripts/distributed-run.sh                    # Run unified service
#   ./scripts/distributed-run.sh --role worker     # Run worker only
#   ./scripts/distributed-run.sh --role finalizer  # Run finalizer only

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# =============================================================================
# Configuration
# =============================================================================

ROBOFLOW_BIN="${PROJECT_ROOT}/target/debug/roboflow"
TIKV_ENDPOINTS="${TIKV_PD_ENDPOINTS:-127.0.0.1:2379}"
ROLE="${ROLE:-unified}"
POD_ID="${POD_ID:-}"

# =============================================================================
# Functions
# =============================================================================

usage() {
    cat <<EOF
Run the distributed Roboflow worker service.

USAGE:
    $(basename "$0") [OPTIONS]

OPTIONS:
    -r, --role <ROLE>       Role to run: worker, finalizer, unified (default: unified)
    -p, --pod-id <ID>       Pod ID for this instance (default: auto-generated)
    -h, --help              Show this help

ROLES:
    unified    Run all components (scanner, worker, finalizer, reaper) [default]
    worker     Run job processing only
    finalizer  Run batch finalization and merge only

EXAMPLES:
    # Run unified service (all roles)
    $(basename "$0")

    # Run as worker only
    $(basename "$0") --role worker

    # Run as finalizer with custom pod ID
    $(basename "$0") --role finalizer --pod-id finalizer-1

ENVIRONMENT VARIABLES:
    TIKV_PD_ENDPOINTS        TiKV PD endpoints (default: 127.0.0.1:2379)
    RUST_LOG                Logging level (default: roboflow=info)
    ROLE                    Default role to run
    POD_ID                  Pod ID for this instance
EOF
}

log-info() {
    echo "[INFO] $(date '+%Y-%m-%d %H:%M:%S') $*"
}

log-error() {
    echo "[ERROR] $(date '+%Y-%m-%d %H:%M:%S') $*" >&2
}

check-prereqs() {
    # Check if binary exists
    if [[ ! -f "${ROBOFLOW_BIN}" ]]; then
        log-error "Roboflow binary not found at ${ROBOFLOW_BIN}"
        log-error "Build first: cargo build"
        exit 1
    fi

    # Check TiKV connection
    local pd_host="${TIKV_ENDPOINTS%:*}"
    local pd_port="${TIKV_ENDPOINTS#*:}"

    if ! nc -z "${pd_host}" "${pd_port}" 2>/dev/null; then
        log-error "TiKV PD is not running at ${TIKV_ENDPOINTS}"
        log-error "Start TiKV first, or check TIKV_PD_ENDPOINTS"
        exit 1
    fi

    log-info "Prerequisites check passed"
}

show-banner() {
    cat <<EOF

=============================================================================
Roboflow Distributed Worker Service
=============================================================================
Role:         ${ROLE}
TiKV PD:      ${TIKV_ENDPOINTS}
Pod ID:       ${POD_ID:-auto-generated}
RUST_LOG:     ${RUST_LOG:-roboflow=info}
=============================================================================

Press Ctrl+C to stop the service
EOF
}

# =============================================================================
# Main
# =============================================================================

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -r|--role)
            ROLE="$2"
            shift 2
            ;;
        -p|--pod-id)
            POD_ID="$2"
            shift 2
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

# Validate role
case "${ROLE}" in
    worker|finalizer|unified)
        ;;
    *)
        log-error "Invalid role: ${ROLE}"
        log-error "Valid roles: worker, finalizer, unified"
        exit 1
        ;;
esac

# Check prereqs
check-prereqs

# Show banner
show-banner

# Build command
CMD=("${ROBOFLOW_BIN}" run --role "${ROLE}")

if [[ -n "${POD_ID}" ]]; then
    CMD+=(--pod-id "${POD_ID}")
fi

# Export environment for the subprocess
export TIKV_PD_ENDPOINTS="${TIKV_ENDPOINTS}"

# Run command
exec "${CMD[@]}"
