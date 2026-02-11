#!/usr/bin/env bash
set -euo pipefail

uv sync --all-packages --group dev
uv lock

echo "Workspace bootstrapped."
