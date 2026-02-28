#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./scripts/push_remotes.sh [branch] [--tags]

Examples:
  ./scripts/push_remotes.sh
  ./scripts/push_remotes.sh main
  ./scripts/push_remotes.sh main --tags

Behavior:
  - pushes the selected branch to Gitea first
  - then mirrors the same branch to GitHub
  - optionally pushes tags to both remotes

Notes:
  - this repo treats Gitea as the CI/CD source of truth
  - GitHub remains a parallel mirror remote
EOF
}

BRANCH=""
PUSH_TAGS="false"

for arg in "$@"; do
  case "$arg" in
    --tags)
      PUSH_TAGS="true"
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      if [[ -n "$BRANCH" ]]; then
        echo "error: only one branch argument is supported" >&2
        usage
        exit 1
      fi
      BRANCH="$arg"
      ;;
  esac
done

if [[ -z "$BRANCH" ]]; then
  BRANCH="$(git branch --show-current)"
fi

if [[ -z "$BRANCH" ]]; then
  echo "error: unable to determine branch" >&2
  exit 1
fi

echo "Pushing ${BRANCH} to Gitea..."
git push gitea "${BRANCH}"

echo "Pushing ${BRANCH} to GitHub..."
git push origin "${BRANCH}"

if [[ "$PUSH_TAGS" == "true" ]]; then
  echo "Pushing tags to Gitea..."
  git push gitea --tags
  echo "Pushing tags to GitHub..."
  git push origin --tags
fi

echo "Push complete."
