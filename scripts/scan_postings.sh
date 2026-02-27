#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./scripts/scan_postings.sh <postings_json_file> [recommender_base_url]

Input format:
  - Either {"postings":[...]} or a raw JSON array [...]
  - Each posting should include: id, title, description

Examples:
  ./scripts/scan_postings.sh ./scripts/example_postings.json
  ./scripts/scan_postings.sh ./scripts/example_postings.json http://localhost:8001
EOF
}

if [[ $# -lt 1 || $# -gt 2 ]]; then
  usage
  exit 1
fi

POSTINGS_FILE="$1"
RECOMMENDER_BASE_URL="${2:-${RECOMMENDER_BASE_URL:-http://localhost:8001}}"

if [[ ! -f "$POSTINGS_FILE" ]]; then
  echo "error: postings file not found: $POSTINGS_FILE" >&2
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "error: python3 is required to validate and normalize posting JSON" >&2
  exit 1
fi

TMP_PAYLOAD="$(mktemp)"
trap 'rm -f "$TMP_PAYLOAD"' EXIT

python3 - "$POSTINGS_FILE" >"$TMP_PAYLOAD" <<'PY'
import json
import sys

file_path = sys.argv[1]
with open(file_path, "r", encoding="utf-8") as handle:
    payload = json.load(handle)

if isinstance(payload, list):
    payload = {"postings": payload}

if not isinstance(payload, dict) or not isinstance(payload.get("postings"), list):
    raise SystemExit("Input JSON must be an object with a postings list or a raw postings list.")

for index, posting in enumerate(payload["postings"], start=1):
    if not isinstance(posting, dict):
        raise SystemExit(f"Posting #{index} is not an object.")
    missing = [key for key in ("id", "title", "description") if not posting.get(key)]
    if missing:
        raise SystemExit(f"Posting #{index} is missing required fields: {', '.join(missing)}")

json.dump(payload, sys.stdout)
PY

echo "Scanning postings into recommender at: ${RECOMMENDER_BASE_URL}"
curl --fail --silent --show-error \
  -X POST "${RECOMMENDER_BASE_URL}/postings" \
  -H "Content-Type: application/json" \
  --data-binary "@${TMP_PAYLOAD}"
echo
