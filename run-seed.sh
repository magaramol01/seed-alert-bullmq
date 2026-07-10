#!/usr/bin/env bash
# Wrapper used by cron: cd into this dir so .env / data.csv resolve, then run once.
set -euo pipefail
cd "$(dirname "$0")"
# Use the service venv if present, otherwise the system python3.
PYTHON="../defect-identification-service/.venv/bin/python"
[ -x "$PYTHON" ] || PYTHON="$(command -v python3)"
exec "$PYTHON" seed.py "$@" >> seed-alert.log 2>&1
