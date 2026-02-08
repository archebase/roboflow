#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0
#
# distributed-submit.sh - Submit jobs to the distributed pipeline
#
# Usage:
#   ./scripts/distributed-submit.sh [OPTIONS] <input-file>
#
# Examples:
#   ./scripts/distributed-submit.sh s3://roboflow-raw/file.bag
#   ./scripts/distributed-submit.sh --dry-run s3://roboflow-raw/*.bag
#   ./scripts/distributed-submit.sh --manifest jobs.json

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# =============================================================================
# Configuration
# =============================================================================

ROBOFLOW_BIN="${PROJECT_ROOT}/target/debug/roboflow"
CONFIG_FILE="${CONFIG_FILE:-examples/rust/lerobot_config.toml}"
OUTPUT_PREFIX="${ROBOFLOW_OUTPUT_PREFIX:-s3://roboflow-output/}"
TIKV_ENDPOINTS="${TIKV_PD_ENDPOINTS:-127.0.0.1:2379}"

# =============================================================================
# Functions
# =============================================================================

usage() {
    cat <<EOF
Submit jobs to the distributed Roboflow pipeline.

USAGE:
    $(basename "$0") [OPTIONS] <input-file>

ARGUMENTS:
    <input-file>            Input file or glob pattern (e.g., s3://roboflow-raw/file.bag)

OPTIONS:
    -o, --output <PREFIX>   Output location (default: s3://roboflow-output/)
    -c, --config <PATH>     Dataset config file (default: examples/rust/lerobot_config.toml)
    -m, --manifest <PATH>   Submit jobs from JSON manifest file
    --max-attempts <N>      Maximum retry attempts (default: 3)
    --dry-run               Show what would be submitted without submitting
    --json                  Output in JSON format
    --csv                   Output in CSV format
    -v, --verbose           Show detailed progress
    -h, --help              Show this help

EXAMPLES:
    # Submit a single file
    $(basename "$0") s3://roboflow-raw/file.bag

    # Submit multiple files with glob
    $(basename "$0") "s3://roboflow-raw/*.bag"

    # Dry run to see what would be submitted
    $(basename "$0") --dry-run s3://roboflow-raw/*.bag

    # Submit with custom config
    $(basename "$0") -c custom_config.toml s3://roboflow-raw/file.bag

    # Submit from manifest
    $(basename "$0") --manifest jobs.json

ENVIRONMENT VARIABLES:
    AWS_ACCESS_KEY_ID       S3/MinIO access key
    AWS_SECRET_ACCESS_KEY   S3/MinIO secret key
    AWS_ENDPOINT_URL        S3/MinIO endpoint URL
    TIKV_PD_ENDPOINTS       TiKV PD endpoints (default: 127.0.0.1:2379)
    RUST_LOG                Logging level (default: roboflow=info)
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

    # Check if config exists
    if [[ ! -f "${PROJECT_ROOT}/${CONFIG_FILE}" ]] && [[ "${CONFIG_FILE}" == examples/* ]]; then
        log-error "Config file not found: ${PROJECT_ROOT}/${CONFIG_FILE}"
        exit 1
    fi

    log-info "Prerequisites check passed"
}

show-submission-summary() {
    local batch_id="$1"
    local output="$2"

    cat <<EOF

=============================================================================
Job Submitted Successfully
=============================================================================
Batch ID:      ${batch_id}
Output:        ${output}
Config:        ${CONFIG_FILE}

Monitor job with:
  ./scripts/distributed-status.sh ${batch_id}

View logs with:
  ./scripts/distributed-logs.sh ${batch_id}

List all jobs:
  ./scripts/distributed-list.sh
=============================================================================
EOF
}

# =============================================================================
# Main
# =============================================================================

# Parse arguments
INPUTS=()
OUTPUT=""
CONFIG=""
MANIFEST=""
MAX_ATTEMPTS=""
DRY_RUN=""
OUTPUT_FORMAT=""
VERBOSE=""
TIKV_ENDPOINTS_ARG=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -o|--output)
            OUTPUT="$2"
            shift 2
            ;;
        -c|--config)
            CONFIG="$2"
            shift 2
            ;;
        -m|--manifest)
            MANIFEST="$2"
            shift 2
            ;;
        --max-attempts)
            MAX_ATTEMPTS="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN="--dry-run"
            shift
            ;;
        --json)
            OUTPUT_FORMAT="--json"
            shift
            ;;
        --csv)
            OUTPUT_FORMAT="--csv"
            shift
            ;;
        -v|--verbose)
            VERBOSE="--verbose"
            shift
            ;;
        --tikv-endpoints)
            TIKV_ENDPOINTS_ARG="$2"
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
            INPUTS+=("$1")
            shift
            ;;
    esac
done

# Use defaults if not set
OUTPUT="${OUTPUT:-${OUTPUT_PREFIX}}"
CONFIG="${CONFIG:-${CONFIG_FILE}}"

# Check prereqs
check-prereqs

# Build command
CMD=("${ROBOFLOW_BIN}" submit)

if [[ -n "${OUTPUT}" ]]; then
    CMD+=(-o "${OUTPUT}")
fi

if [[ -n "${CONFIG}" ]]; then
    CMD+=(-c "${CONFIG}")
fi

if [[ -n "${MANIFEST}" ]]; then
    CMD+=(--manifest "${MANIFEST}")
fi

if [[ -n "${MAX_ATTEMPTS}" ]]; then
    CMD+=(--max-attempts "${MAX_ATTEMPTS}")
fi

if [[ -n "${DRY_RUN}" ]]; then
    CMD+=(${DRY_RUN})
fi

if [[ -n "${OUTPUT_FORMAT}" ]]; then
    CMD+=(${OUTPUT_FORMAT})
fi

if [[ -n "${VERBOSE}" ]]; then
    CMD+=(${VERBOSE})
fi

if [[ -n "${TIKV_ENDPOINTS_ARG}" ]]; then
    CMD+=(--tikv-endpoints "${TIKV_ENDPOINTS_ARG}")
else
    CMD+=(--tikv-endpoints "${TIKV_ENDPOINTS}")
fi

# Add inputs
if [[ ${#INPUTS[@]} -gt 0 ]]; then
    CMD+=("${INPUTS[@]}")
fi

# Show command being run
log-info "Running: ${CMD[*]}"
echo ""

# Run command and capture output
OUTPUT_JSON=$("${CMD[@]}" 2>&1)
EXIT_CODE=$?

echo "${OUTPUT_JSON}"
echo ""

# Parse batch ID from output (if successful)
if [[ ${EXIT_CODE} -eq 0 ]] && [[ -z "${MANIFEST}" ]] && [[ -z "${DRY_RUN}" ]] && [[ ${#INPUTS[@]} -eq 1 ]]; then
    # Try to extract batch ID from output
    BATCH_ID=$(echo "${OUTPUT_JSON}" | grep -oE 'jobs:[a-f0-9]+' | head -1 || echo "")

    if [[ -n "${BATCH_ID}" ]]; then
        show-submission-summary "${BATCH_ID}" "${OUTPUT}"
    fi
fi

exit ${EXIT_CODE}
