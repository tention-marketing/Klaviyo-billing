#!/usr/bin/env bash
# Wrapper for scheduled runs.
#
# index.py loads .env by itself, so this script only fixes the working
# directory and picks the interpreter. No business logic lives here, and
# n8n is optional - the Klaviyo check runs with or without a webhook.
#
#   crontab -e
#   0 9 * * *  /full/path/to/Klaviyo-Billing/run.sh >> /full/path/to/Klaviyo-Billing/run.log 2>&1
#
# PYTHON_BIN may be exported to choose an interpreter, e.g.
#   PYTHON_BIN=/opt/homebrew/bin/python3 ./run.sh
set -euo pipefail

cd "$(dirname "$0")"

exec "${PYTHON_BIN:-python3}" index.py "$@"
