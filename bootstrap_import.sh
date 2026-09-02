#!/usr/bin/env bash
#
# One-time bootstrap: import a local clients.json into Railway Postgres.
#
#   ./bootstrap_import.sh [path/to/clients.json]
#
# Railway's Postgres is on a private IPv6 network, so its hostname
# (postgres.railway.internal) does not resolve from your machine. This opens
# Railway's SSH tunnel and talks to the database over localhost - the database
# is never exposed publicly.
#
# The master key is read straight from the Railway service into a shell
# variable and never printed, so the import always encrypts under the SAME key
# the deployed service will use to decrypt.
#
# Requires: railway CLI, logged in and linked to the project.
set -euo pipefail

cd "$(dirname "$0")"

CLIENTS_FILE="${1:-clients.json}"
DB_SERVICE="${DB_SERVICE:-Postgres}"
APP_SERVICE="${APP_SERVICE:-Klaviyo-billing}"
LOCAL_PORT="${LOCAL_PORT:-45432}"

# 0600 temp file: the tunnel prints the database password into it.
TUNNEL_LOG="$(mktemp -t klaviyo-tunnel)"
chmod 600 "$TUNNEL_LOG"

command -v railway >/dev/null 2>&1 || {
  echo "ERROR: railway CLI not found. npm i -g @railway/cli" >&2; exit 1; }
[ -f "$CLIENTS_FILE" ] || {
  echo "ERROR: $CLIENTS_FILE not found." >&2; exit 1; }

echo "==> Reading KLAVIYO_MASTER_KEY from the $APP_SERVICE service"
MASTER_KEY="$(railway variables --service "$APP_SERVICE" --kv 2>/dev/null \
  | sed -n 's/^KLAVIYO_MASTER_KEY=//p')"
if [ -z "$MASTER_KEY" ]; then
  echo "ERROR: KLAVIYO_MASTER_KEY is not set on the $APP_SERVICE service." >&2
  echo "Set it there FIRST. Importing under a different key would store" >&2
  echo "credentials the deployed service cannot decrypt." >&2
  exit 1
fi
echo "    got a ${#MASTER_KEY}-character key (value not shown)"

# Validate before touching the database.
export KLAVIYO_MASTER_KEY="$MASTER_KEY"
python3 -c "
import crypto_store, sys
try:
    crypto_store.load_master_key()
except crypto_store.MasterKeyError as exc:
    print('ERROR:', exc, file=sys.stderr); sys.exit(1)
print('    master key validated (32 bytes)')
"

echo "==> Opening tunnel to $DB_SERVICE on localhost:$LOCAL_PORT"
railway connect "$DB_SERVICE" --tunnel-only --port "$LOCAL_PORT" \
  > "$TUNNEL_LOG" 2>&1 &
TUNNEL_PID=$!

cleanup() {
  echo "==> Closing tunnel"
  kill "$TUNNEL_PID" 2>/dev/null || true
  wait "$TUNNEL_PID" 2>/dev/null || true
  # The tunnel log contains the database password - do not leave it on disk.
  rm -f "$TUNNEL_LOG"
  unset KLAVIYO_MASTER_KEY DATABASE_URL
}
trap cleanup EXIT

# Wait for the tunnel to accept connections.
for _ in $(seq 1 40); do
  if python3 -c "
import socket,sys
try: socket.create_connection(('127.0.0.1',$LOCAL_PORT),0.4).close()
except OSError: sys.exit(1)
" 2>/dev/null; then TUNNEL_UP=1; break; fi
  sleep 0.5
done
if [ -z "${TUNNEL_UP:-}" ]; then
  echo "ERROR: tunnel did not come up. Log (credentials stripped):" >&2
  # The tunnel prints Host/Port/User/Password lines AND may print a URL.
  # Drop every line that could carry a credential rather than trying to
  # pattern-match the secret out of it.
  grep -viE 'password|user|postgres(ql)?://' "$TUNNEL_LOG" >&2 || true
  exit 1
fi
echo "    tunnel is up"

# Build the local DATABASE_URL from the tunnel's own connection details,
# so no password is ever typed or echoed.
DATABASE_URL="$(railway variables --service "$DB_SERVICE" --kv 2>/dev/null \
  | sed -n 's/^DATABASE_URL=//p' \
  | sed -E "s#@[^:/]+:[0-9]+#@127.0.0.1:$LOCAL_PORT#" )"
if [ -z "$DATABASE_URL" ]; then
  echo "ERROR: could not read DATABASE_URL from the $DB_SERVICE service." >&2
  exit 1
fi
export DATABASE_URL
echo "    using localhost:$LOCAL_PORT (private host rewritten, not exposed)"

echo
echo "==> Applying schema"
python3 migrate.py
echo
echo "==> Importing $CLIENTS_FILE"
python3 manage_clients.py import-json "$CLIENTS_FILE"
echo
echo "==> Stored clients"
python3 manage_clients.py list
echo
echo "==> Verifying every stored key decrypts"
python3 manage_clients.py verify
echo
echo "Done. Redeploy the $APP_SERVICE service to pick up the imported clients."
