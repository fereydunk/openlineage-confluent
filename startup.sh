#!/usr/bin/env bash
# startup.sh — launch the openlineage-confluent setup wizard
#
# What this does:
#   1. Kills any old wizard process on port 8892
#   2. Starts web/server.py (blocking — Ctrl-C to stop)
#   3. Opens the browser to http://localhost:8892
#
# The wizard handles:
#   - Sign in to Confluent Cloud (email + password, non-interactive
#     via CONFLUENT_CLOUD_EMAIL / CONFLUENT_CLOUD_PASSWORD env vars)
#   - List environments visible to that account
#   - Pick an environment → write CONFLUENT_ENV_ID to config.yaml
#
# Other config.yaml fields (CONFLUENT_CLUSTER_ID, API keys, SR, etc.) are
# still set by the user editing config.yaml directly — this wizard only
# helps with the email/password → env-id step.

set -euo pipefail
REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
PORT=8892

# Kill any existing wizard server
pkill -f "web/server.py" 2>/dev/null && echo "  Stopped old wizard server." || true
sleep 1

# Open browser after a short delay
(sleep 2 && open "http://localhost:${PORT}") &

# Pick a Python — prefer the project venv (consistent with installed deps)
if [[ -x "${REPO_DIR}/.venv/bin/python3" ]]; then
  PY="${REPO_DIR}/.venv/bin/python3"
else
  PY="$(command -v python3)"
fi

echo ""
echo "Starting setup wizard on http://localhost:${PORT} ..."
"${PY}" "${REPO_DIR}/web/server.py"
