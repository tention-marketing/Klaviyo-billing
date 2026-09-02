#!/usr/bin/env bash
#
# Railway container entrypoint.
#
# Applies the database schema, then starts the monitor. Runs INSIDE Railway,
# where postgres.railway.internal resolves - that hostname is private IPv6
# DNS and deliberately does not resolve from a developer machine.
#
# `set -e` plus the ordering below guarantees index.py never starts if the
# migration fails: a broken schema must surface as a failed deploy, not as a
# monitoring run that quietly checks nothing.
#
# migrate.py is idempotent (every statement is CREATE ... IF NOT EXISTS or a
# guarded DO block) so this is safe on every restart. It never drops or
# rewrites data, and it never imports clients - that is a deliberate one-time
# operation run through a tunnel.
set -euo pipefail

cd "$(dirname "$0")"

if [ -n "${DATABASE_URL:-}" ]; then
  echo "==> Applying database schema"
  python3 migrate.py
else
  echo "==> DATABASE_URL not set; skipping migration (local JSON client config)"
fi

echo "==> Starting Klaviyo billing monitor"
exec python3 index.py "$@"
