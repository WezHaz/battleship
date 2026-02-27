#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./scripts/scan_sources.sh [recommender_base_url] [enabled_only]

Examples:
  ./scripts/scan_sources.sh
  ./scripts/scan_sources.sh http://localhost:8001 true
  ./scripts/scan_sources.sh http://localhost:8001 false

Environment:
  - RECOMMENDER_API_KEY (optional; sent as x-api-key when set)
EOF
}

if [[ $# -gt 2 ]]; then
  usage
  exit 1
fi

RECOMMENDER_BASE_URL="${1:-${RECOMMENDER_BASE_URL:-http://localhost:8001}}"
ENABLED_ONLY="${2:-true}"

if [[ "$ENABLED_ONLY" != "true" && "$ENABLED_ONLY" != "false" ]]; then
  echo "error: enabled_only must be true or false" >&2
  usage
  exit 1
fi

echo "Scanning job sources at ${RECOMMENDER_BASE_URL} (enabled_only=${ENABLED_ONLY})"

CURL_ARGS=(
  --fail
  --silent
  --show-error
  -X POST
  "${RECOMMENDER_BASE_URL}/job-sources/scan?enabled_only=${ENABLED_ONLY}"
  -H "Content-Type: application/json"
  --data "{}"
)

if [[ -n "${RECOMMENDER_API_KEY:-}" ]]; then
  CURL_ARGS+=(-H "x-api-key: ${RECOMMENDER_API_KEY}")
fi

curl "${CURL_ARGS[@]}"
echo
