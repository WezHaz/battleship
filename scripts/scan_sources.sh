#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./scripts/scan_sources.sh [recommender_base_url] [enabled_only] [trigger] [respect_backoff]

Examples:
  ./scripts/scan_sources.sh
  ./scripts/scan_sources.sh http://localhost:8001 true
  ./scripts/scan_sources.sh http://localhost:8001 false
  ./scripts/scan_sources.sh http://localhost:8001 true scheduled
  ./scripts/scan_sources.sh http://localhost:8001 true manual true

Environment:
  - RECOMMENDER_API_KEY (optional; sent as x-api-key when set)
EOF
}

if [[ $# -gt 4 ]]; then
  usage
  exit 1
fi

RECOMMENDER_BASE_URL="${1:-${RECOMMENDER_BASE_URL:-http://localhost:8001}}"
ENABLED_ONLY="${2:-true}"
TRIGGER="${3:-manual}"
RESPECT_BACKOFF="${4:-false}"

if [[ "$ENABLED_ONLY" != "true" && "$ENABLED_ONLY" != "false" ]]; then
  echo "error: enabled_only must be true or false" >&2
  usage
  exit 1
fi

if [[ "$TRIGGER" != "manual" && "$TRIGGER" != "scheduled" ]]; then
  echo "error: trigger must be manual or scheduled" >&2
  usage
  exit 1
fi

if [[ "$RESPECT_BACKOFF" != "true" && "$RESPECT_BACKOFF" != "false" ]]; then
  echo "error: respect_backoff must be true or false" >&2
  usage
  exit 1
fi

if [[ "$TRIGGER" == "scheduled" ]]; then
  RESPECT_BACKOFF="true"
  SCAN_URL="${RECOMMENDER_BASE_URL}/job-sources/scan/scheduled?enabled_only=${ENABLED_ONLY}"
else
  SCAN_URL="${RECOMMENDER_BASE_URL}/job-sources/scan?enabled_only=${ENABLED_ONLY}&respect_backoff=${RESPECT_BACKOFF}"
fi

echo "Scanning job sources at ${RECOMMENDER_BASE_URL} (enabled_only=${ENABLED_ONLY}, trigger=${TRIGGER}, respect_backoff=${RESPECT_BACKOFF})"

CURL_ARGS=(
  --fail
  --silent
  --show-error
  -X POST
  "${SCAN_URL}"
  -H "Content-Type: application/json"
  --data "{}"
)

if [[ -n "${RECOMMENDER_API_KEY:-}" ]]; then
  CURL_ARGS+=(-H "x-api-key: ${RECOMMENDER_API_KEY}")
fi

curl "${CURL_ARGS[@]}"
echo
