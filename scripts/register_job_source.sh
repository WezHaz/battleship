#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./scripts/register_job_source.sh <source_id> <source_name> <source_input> [recommender_base_url]

source_input:
  - Path to JSON file (treated as inline_json source)
  - URL beginning with http:// or https:// (treated as json_url source)

JSON file format:
  - {"postings":[...]} or raw array [...]
  - Posting objects should include title and description (id optional)

Environment:
  - RECOMMENDER_API_KEY (optional; sent as x-api-key when set)
EOF
}

if [[ $# -lt 3 || $# -gt 4 ]]; then
  usage
  exit 1
fi

SOURCE_ID="$1"
SOURCE_NAME="$2"
SOURCE_INPUT="$3"
RECOMMENDER_BASE_URL="${4:-${RECOMMENDER_BASE_URL:-http://localhost:8001}}"

if ! command -v python3 >/dev/null 2>&1; then
  echo "error: python3 is required" >&2
  exit 1
fi

TMP_PAYLOAD="$(mktemp)"
trap 'rm -f "$TMP_PAYLOAD"' EXIT

if [[ "$SOURCE_INPUT" =~ ^https?:// ]]; then
  python3 - "$SOURCE_ID" "$SOURCE_NAME" "$SOURCE_INPUT" >"$TMP_PAYLOAD" <<'PY'
import json
import sys

source_id, source_name, source_url = sys.argv[1:4]
payload = {
    "source_id": source_id,
    "name": source_name,
    "source_type": "json_url",
    "url": source_url,
    "enabled": True,
}
json.dump(payload, sys.stdout)
PY
else
  if [[ ! -f "$SOURCE_INPUT" ]]; then
    echo "error: file not found: $SOURCE_INPUT" >&2
    exit 1
  fi
  python3 - "$SOURCE_ID" "$SOURCE_NAME" "$SOURCE_INPUT" >"$TMP_PAYLOAD" <<'PY'
import json
import sys

source_id, source_name, file_path = sys.argv[1:4]
with open(file_path, "r", encoding="utf-8") as handle:
    data = json.load(handle)

if isinstance(data, list):
    postings = data
elif isinstance(data, dict):
    postings = data.get("postings", [])
else:
    raise SystemExit("source JSON must be an object or list")

if not isinstance(postings, list):
    raise SystemExit("source postings must be a list")

payload = {
    "source_id": source_id,
    "name": source_name,
    "source_type": "inline_json",
    "postings": postings,
    "enabled": True,
}
json.dump(payload, sys.stdout)
PY
fi

echo "Registering source '$SOURCE_ID' at ${RECOMMENDER_BASE_URL}"

CURL_ARGS=(
  --fail
  --silent
  --show-error
  -X POST
  "${RECOMMENDER_BASE_URL}/job-sources"
  -H "Content-Type: application/json"
  --data-binary "@${TMP_PAYLOAD}"
)

if [[ -n "${RECOMMENDER_API_KEY:-}" ]]; then
  CURL_ARGS+=(-H "x-api-key: ${RECOMMENDER_API_KEY}")
fi

curl "${CURL_ARGS[@]}"
echo
