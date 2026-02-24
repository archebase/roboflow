#!/bin/bash

set -euo pipefail

TMPFILE=$(mktemp)
trap "rm -f $TMPFILE" EXIT

cargo "$@" 2>&1 | tee "$TMPFILE"
EXIT_CODE=${PIPESTATUS[0]}

if grep -qE "502 Bad Gateway|503 Service Unavailable|cache storage failed|dns error|sccache" "$TMPFILE"; then
  echo ""
  echo "=== sccache/cache error detected, retrying without sccache ==="
  echo ""
  unset RUSTC_WRAPPER
  unset SCCACHE_GHA_ENABLED
  cargo "$@"
else
  exit $EXIT_CODE
fi
