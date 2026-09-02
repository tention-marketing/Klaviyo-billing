#!/usr/bin/env python3
"""
Client credential management.

    python3 manage_clients.py list
    python3 manage_clients.py add [--name "Steven Denn"]
    python3 manage_clients.py update-key --name "Steven Denn"
    python3 manage_clients.py disable --name "Steven Denn"
    python3 manage_clients.py enable  --name "Steven Denn"
    python3 manage_clients.py remove  --name "Steven Denn"
    python3 manage_clients.py import-json clients.json
    python3 manage_clients.py verify

API keys are read from a hidden prompt (never argv, so they stay out of shell
history) and are never printed back. Requires DATABASE_URL and
KLAVIYO_MASTER_KEY.
"""

import argparse
import getpass
import json
import os
import sys
from typing import Optional

import crypto_store
import db

# index.py loads .env; reuse that so this CLI behaves the same locally.
try:
    import index as _index  # noqa: F401  (import triggers .env loading)
except Exception:  # pragma: no cover - the CLI must work even if index.py can't import
    pass


def _fail(message: str) -> int:
    print("ERROR: {}".format(message), file=sys.stderr)
    return 1


def _require_config() -> bytes:
    """Validate both secrets up front so we never half-apply a change."""
    if not db.is_configured():
        raise SystemExit(_fail(
            "DATABASE_URL is not set. Point it at your Railway Postgres database."
        ))
    try:
        return crypto_store.load_master_key()
    except crypto_store.MasterKeyError as exc:
        raise SystemExit(_fail(str(exc)))


def _prompt_key(label: str = "Klaviyo private API key") -> Optional[str]:
    """Hidden, confirmed input. Returns None if the two entries differ."""
    first = getpass.getpass("{}: ".format(label)).strip()
    if not first:
        print("No key entered.")
        return None
    if not first.startswith("pk_"):
        print("Warning: Klaviyo private keys normally start with 'pk_'.")
    second = getpass.getpass("Confirm key: ").strip()
    if first != second:
        print("Keys did not match.")
        return None
    return first


def _read_key(args) -> Optional[str]:
    """
    Where the plaintext key comes from.

    --key-stdin exists for scripted/non-interactive use; passing a key as a
    command-line argument is deliberately not supported.
    """
    if getattr(args, "key_stdin", False):
        key = sys.stdin.readline().strip()
        return key or None
    return _prompt_key()


# ------------------------------------------------------------------
# Commands
# ------------------------------------------------------------------

def cmd_list(args) -> int:
    if not db.is_configured():
        return _fail("DATABASE_URL is not set.")
    try:
        rows = db.fetch_clients(active_only=False)
    except db.DatabaseUnavailable as exc:
        return _fail(str(exc))

    if not rows:
        print("No clients stored. Add one with: python3 manage_clients.py add")
        return 0

    print("{:<4} {:<34} {:<9} {}".format("ID", "CLIENT", "STATUS", "UPDATED"))
    for row in rows:
        print("{:<4} {:<34} {:<9} {}".format(
            row["id"], row["client_name"],
            "active" if row["active"] else "inactive",
            row["updated_at"].strftime("%Y-%m-%d %H:%M"),
        ))

    active = sum(1 for row in rows if row["active"])
    print("\n{} client(s): {} active, {} inactive".format(
        len(rows), active, len(rows) - active))
    return 0


def cmd_add(args) -> int:
    master_key = _require_config()

    name = (args.name or input("Client name: ")).strip()
    if not name:
        return _fail("Client name is required.")

    key = _read_key(args)
    if not key:
        return _fail("Aborted - no API key provided.")

    ciphertext, nonce = crypto_store.encrypt_api_key(key, master_key)
    del key

    try:
        client_id, inserted = db.upsert_client(name, ciphertext, nonce, active=True)
    except db.DatabaseUnavailable as exc:
        return _fail(str(exc))

    print("{:<34} {} (id {})".format(
        name, "added" if inserted else "key replaced", client_id))
    return 0


def cmd_update_key(args) -> int:
    master_key = _require_config()

    name = (args.name or input("Client name: ")).strip()
    if not name:
        return _fail("Client name is required.")

    try:
        existing = {row["client_name"] for row in db.fetch_clients(active_only=False)}
    except db.DatabaseUnavailable as exc:
        return _fail(str(exc))
    if name not in existing:
        return _fail("No client named {!r}. Use `add` to create it.".format(name))

    key = _read_key(args)
    if not key:
        return _fail("Aborted - no API key provided.")

    ciphertext, nonce = crypto_store.encrypt_api_key(key, master_key)
    del key

    current = [r for r in db.fetch_clients(active_only=False) if r["client_name"] == name]
    db.upsert_client(name, ciphertext, nonce, active=current[0]["active"])
    print("{:<34} key replaced".format(name))
    return 0


def _set_active(name: str, active: bool) -> int:
    if not db.is_configured():
        return _fail("DATABASE_URL is not set.")
    try:
        changed = db.set_client_active(name, active)
    except db.DatabaseUnavailable as exc:
        return _fail(str(exc))
    if not changed:
        return _fail("No client named {!r}.".format(name))
    print("{:<34} {}".format(name, "enabled" if active else "disabled"))
    return 0


def cmd_disable(args) -> int:
    return _set_active((args.name or "").strip(), False)


def cmd_enable(args) -> int:
    return _set_active((args.name or "").strip(), True)


def cmd_remove(args) -> int:
    if not db.is_configured():
        return _fail("DATABASE_URL is not set.")
    name = (args.name or "").strip()
    if not name:
        return _fail("--name is required.")

    if not args.yes:
        confirm = input(
            "Permanently delete {!r} and its stored key? [y/N] ".format(name))
        if confirm.strip().lower() not in ("y", "yes"):
            print("Cancelled.")
            return 0

    try:
        removed = db.delete_client(name)
    except db.DatabaseUnavailable as exc:
        return _fail(str(exc))
    if not removed:
        return _fail("No client named {!r}.".format(name))
    print("{:<34} removed".format(name))
    return 0


def cmd_import_json(args) -> int:
    """One-time migration of clients.json into encrypted Postgres storage."""
    master_key = _require_config()

    path = args.path
    if not os.path.exists(path):
        return _fail("{} not found.".format(path))

    try:
        with open(path, "r") as handle:
            entries = json.load(handle)
    except ValueError as exc:
        return _fail("{} is not valid JSON: {}".format(path, exc))

    if isinstance(entries, dict):
        entries = entries.get("clients", [])
    if not isinstance(entries, list):
        return _fail("{} must contain a JSON array of clients.".format(path))

    print("Importing {} client(s) from {} ...\n".format(len(entries), path))

    imported = failed = 0
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            print("{:<34} FAILED (entry #{} is not an object)".format("?", index))
            failed += 1
            continue

        name = str(entry.get("client_name") or "").strip()
        key = str(entry.get("api_key") or "").strip()
        label = name or "(entry #{})".format(index)

        if not name:
            print("{:<34} FAILED (missing client_name)".format(label))
            failed += 1
            continue
        if not key or "REPLACE" in key.upper() or not key.startswith("pk_"):
            # Never echo the value - only that it was unusable.
            print("{:<34} FAILED (missing or placeholder api_key)".format(label))
            failed += 1
            continue

        try:
            ciphertext, nonce = crypto_store.encrypt_api_key(key, master_key)
            _, inserted = db.upsert_client(name, ciphertext, nonce, active=True)
            print("{:<34} {}".format(name, "imported" if inserted else "updated"))
            imported += 1
        except (db.DatabaseUnavailable, crypto_store.MasterKeyError) as exc:
            print("{:<34} FAILED ({})".format(name, exc))
            failed += 1
        finally:
            key = ""

    print("\n{} imported\n{} failed".format(imported, failed))
    print("\n{} was NOT modified or deleted.".format(path))
    if imported and not failed:
        print("Production no longer needs it - keep it only as a local backup.")
    return 1 if failed and not imported else 0


def cmd_verify(args) -> int:
    """
    Prove every stored credential decrypts with the current master key.

    Reports names and pass/fail only - never a key, or any part of one.
    """
    master_key = _require_config()
    try:
        rows = db.fetch_clients(active_only=False)
    except db.DatabaseUnavailable as exc:
        return _fail(str(exc))

    if not rows:
        print("No clients stored.")
        return 0

    ok = bad = 0
    for row in rows:
        try:
            plaintext = crypto_store.decrypt_api_key(
                row["api_key_ciphertext"], row["api_key_nonce"], master_key)
            usable = plaintext.startswith("pk_")
            del plaintext
            print("{:<34} {}".format(
                row["client_name"],
                "OK" if usable else "decrypts, but does not look like a pk_ key"))
            ok += 1
        except crypto_store.DecryptionError as exc:
            print("{:<34} FAILED ({})".format(row["client_name"], exc))
            bad += 1

    print("\n{} decrypted, {} failed".format(ok, bad))
    return 1 if bad else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Manage encrypted Klaviyo client credentials",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    def with_name(cmd, need_name=False, allow_stdin=False):
        cmd.add_argument("--name", required=need_name, help="client name")
        if allow_stdin:
            cmd.add_argument(
                "--key-stdin", action="store_true",
                help="read the API key from stdin instead of prompting",
            )
        return cmd

    sub.add_parser("list", help="list stored clients").set_defaults(func=cmd_list)
    with_name(sub.add_parser("add", help="add a client"), allow_stdin=True
              ).set_defaults(func=cmd_add)
    with_name(sub.add_parser("update-key", help="replace a client's API key"),
              allow_stdin=True).set_defaults(func=cmd_update_key)
    with_name(sub.add_parser("disable", help="stop monitoring a client"),
              need_name=True).set_defaults(func=cmd_disable)
    with_name(sub.add_parser("enable", help="resume monitoring a client"),
              need_name=True).set_defaults(func=cmd_enable)

    remove = with_name(sub.add_parser("remove", help="delete a client and its key"),
                       need_name=True)
    remove.add_argument("--yes", action="store_true", help="skip confirmation")
    remove.set_defaults(func=cmd_remove)

    importer = sub.add_parser("import-json", help="one-time clients.json import")
    importer.add_argument("path", nargs="?", default="clients.json")
    importer.set_defaults(func=cmd_import_json)

    sub.add_parser("verify", help="check every stored key decrypts"
                   ).set_defaults(func=cmd_verify)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
