"""
Tests for encrypted credential storage and Postgres-backed alert history.

Crypto tests always run. Database tests need a throwaway Postgres:

    export TEST_DATABASE_URL=postgresql://postgres:PASSWORD@127.0.0.1:5432/klaviyo_test
    python3 test_storage.py

WARNING: the database tests TRUNCATE klaviyo_clients and klaviyo_alert_history.
Never point TEST_DATABASE_URL at production. Every key here is fake.
"""

import base64
import io
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone

import crypto_store
import db as database
import index as m

# Obviously fake, and not Klaviyo-shaped beyond the prefix.
FAKE_KEY_A = "pk_TESTONLY_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa01"
FAKE_KEY_B = "pk_TESTONLY_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb02"

FAILURES = []
TEST_URL = (os.getenv("TEST_DATABASE_URL") or "").strip()


def check(label, got, want):
    ok = got == want
    if not ok:
        FAILURES.append("{}: got {!r}, want {!r}".format(label, got, want))
    print("  {}  {:<56} {}".format(
        "ok  " if ok else "FAIL", label,
        "" if ok else "got={!r} want={!r}".format(got, want)))


def section(name):
    print("\n== {} ==".format(name))


def _reset_db():
    with database.connection(TEST_URL) as conn:
        conn.execute("TRUNCATE klaviyo_alert_history, klaviyo_clients RESTART IDENTITY")


def _cli(*args, env_extra=None, stdin=None):
    env = dict(os.environ)
    env["DATABASE_URL"] = TEST_URL
    env.update(env_extra or {})
    proc = subprocess.run(
        [sys.executable, "manage_clients.py"] + list(args),
        capture_output=True, text=True, env=env, input=stdin,
        cwd=os.path.dirname(os.path.abspath(__file__)),
    )
    return proc.returncode, proc.stdout + proc.stderr


# ------------------------------------------------------------------
# TESTS 1-3: encryption
# ------------------------------------------------------------------

def test_crypto():
    section("TEST 1-3: encryption")
    key = crypto_store.load_master_key(crypto_store.generate_master_key())
    check("master key is 32 bytes", len(key), 32)

    ct, nonce = crypto_store.encrypt_api_key(FAKE_KEY_A, key)
    check("TEST 1: round trip", crypto_store.decrypt_api_key(ct, nonce, key), FAKE_KEY_A)
    check("nonce is 12 bytes", len(nonce), 12)
    check("ciphertext is not the plaintext", FAKE_KEY_A.encode() in ct, False)

    ct2, nonce2 = crypto_store.encrypt_api_key(FAKE_KEY_A, key)
    check("TEST 2: fresh nonce each time", nonce == nonce2, False)
    check("TEST 2: ciphertext differs", ct == ct2, False)
    check("TEST 2: both still decrypt",
          crypto_store.decrypt_api_key(ct2, nonce2, key), FAKE_KEY_A)

    other = crypto_store.load_master_key(crypto_store.generate_master_key())
    try:
        crypto_store.decrypt_api_key(ct, nonce, other)
        check("TEST 3: wrong master key rejected", False, True)
    except crypto_store.DecryptionError:
        check("TEST 3: wrong master key rejected", True, True)

    tampered = bytearray(ct)
    tampered[0] ^= 0x01
    try:
        crypto_store.decrypt_api_key(bytes(tampered), nonce, key)
        check("tampered ciphertext rejected (GCM tag)", False, True)
    except crypto_store.DecryptionError:
        check("tampered ciphertext rejected (GCM tag)", True, True)

    section("master key validation")
    for bad, why in [("", "missing"),
                     ("not base64!!", "not base64"),
                     (base64.b64encode(b"short").decode(), "wrong length")]:
        try:
            crypto_store.load_master_key(bad)
            check("rejects {} master key".format(why), False, True)
        except crypto_store.MasterKeyError as exc:
            leaks = bad and len(bad) > 8 and bad in str(exc)
            check("rejects {} master key".format(why), not leaks, True)

    real = crypto_store.generate_master_key()
    try:
        crypto_store.load_master_key(real[:-4] + "AAAA")
    except crypto_store.MasterKeyError as exc:
        check("error text never contains a key", real in str(exc), False)


# ------------------------------------------------------------------
# TESTS 4-5: secrets never leak
# ------------------------------------------------------------------

def test_no_leaks():
    section("TEST 4-5: secrets never leak")
    record = m.ClientRecord(client_name="Acme", client_id=7, plaintext=FAKE_KEY_A)
    check("TEST 4: repr() hides the key", FAKE_KEY_A in repr(record), False)
    check("TEST 4: str() hides the key", FAKE_KEY_A in str(record), False)
    check("record still reveals on demand", record.reveal(), FAKE_KEY_A)

    check("TEST 4: redact() strips it", FAKE_KEY_A in m.redact("used " + FAKE_KEY_A), False)
    check("TEST 4: KlaviyoError redacts",
          FAKE_KEY_A in str(m.KlaviyoError("boom " + FAKE_KEY_A)), False)

    payload = m.build_payload("Acme")
    payload["client_id"] = 7
    wire = m._for_wire(payload)
    check("TEST 5: no api_key field on the wire", "api_key" in wire, False)
    check("TEST 5: client_id stays internal", "client_id" in wire, False)
    check("TEST 5: client_name is kept for humans", "client_name" in wire, True)
    check("TEST 5: no ciphertext/nonce on the wire",
          any(k in wire for k in
              ("api_key_ciphertext", "api_key_nonce", "encrypted_api_key")), False)
    check("TEST 5: no db/master secrets on the wire",
          any(k in wire for k in ("DATABASE_URL", "KLAVIYO_MASTER_KEY",
                                  "database_url", "master_key")), False)
    check("TEST 5: payload JSON has no key material",
          FAKE_KEY_A in json.dumps(wire), False)

    buffer = io.StringIO()
    import contextlib
    good = dict(payload, success=True, should_remind=False)
    with contextlib.redirect_stdout(buffer):
        m.print_summary([good], "not_configured", "n8n not configured")
    check("TEST 4: terminal summary has no key",
          FAKE_KEY_A in buffer.getvalue(), False)
    check("TEST 4: terminal summary has no client_id row",
          "client_id" in buffer.getvalue(), False)

    check("DATABASE_URL password never in safe_target",
          "FAKE-PW-hunter2" in database.safe_target(
              "postgresql://user:FAKE-PW-hunter2@db.example.com:5432/app"), False)
    check("safe_target still identifies the database",
          database.safe_target("postgresql://u:p@db.example.com:5432/app"),
          "db.example.com:5432/app")
    check("_scrub removes the password",
          "FAKE-PW-hunter2" in database._scrub(
              "connection to postgresql://user:FAKE-PW-hunter2@h:5432/a failed",
              "postgresql://user:FAKE-PW-hunter2@h:5432/a"), False)


# ------------------------------------------------------------------
# TESTS 6-12: client store
# ------------------------------------------------------------------

def test_client_store():
    section("TEST 6-12: client store (Postgres)")
    _reset_db()
    master = crypto_store.generate_master_key()
    key = crypto_store.load_master_key(master)

    code, out = _cli("add", "--name", "Alpha Co", "--key-stdin",
                     env_extra={"KLAVIYO_MASTER_KEY": master},
                     stdin=FAKE_KEY_A + "\n")
    check("TEST 6: add via CLI", code, 0)
    check("TEST 6: CLI confirms without echoing the key",
          "added" in out and FAKE_KEY_A not in out, True)

    _cli("add", "--name", "Beta Co", "--key-stdin",
         env_extra={"KLAVIYO_MASTER_KEY": master}, stdin=FAKE_KEY_B + "\n")

    rows = database.fetch_clients(active_only=True, url=TEST_URL)
    check("TEST 7: reads active clients",
          sorted(r["client_name"] for r in rows), ["Alpha Co", "Beta Co"])
    check("TEST 7: decrypts to the stored key",
          crypto_store.decrypt_api_key(
              rows[0]["api_key_ciphertext"], rows[0]["api_key_nonce"], key),
          FAKE_KEY_A)

    stored = database.fetch_clients(active_only=False, url=TEST_URL)
    check("no plaintext key in any column",
          any(FAKE_KEY_A.encode() in bytes(r["api_key_ciphertext"]) for r in stored),
          False)

    code, out = _cli("disable", "--name", "Beta Co",
                     env_extra={"KLAVIYO_MASTER_KEY": master})
    active = [r["client_name"] for r in database.fetch_clients(True, url=TEST_URL)]
    check("TEST 8: disabled client is excluded", active, ["Alpha Co"])
    check("TEST 8: but is still stored",
          len(database.fetch_clients(False, url=TEST_URL)), 2)

    _cli("enable", "--name", "Beta Co", env_extra={"KLAVIYO_MASTER_KEY": master})
    check("re-enable works",
          len(database.fetch_clients(True, url=TEST_URL)), 2)

    code, out = _cli("update-key", "--name", "Alpha Co", "--key-stdin",
                     env_extra={"KLAVIYO_MASTER_KEY": master},
                     stdin=FAKE_KEY_B + "\n")
    row = [r for r in database.fetch_clients(False, url=TEST_URL)
           if r["client_name"] == "Alpha Co"][0]
    check("TEST 9: key replaced",
          crypto_store.decrypt_api_key(
              row["api_key_ciphertext"], row["api_key_nonce"], key), FAKE_KEY_B)
    check("TEST 9: no duplicate row created",
          len(database.fetch_clients(False, url=TEST_URL)), 2)
    check("TEST 9: updated_at moved past created_at",
          row["updated_at"] > row["created_at"], True)

    section("TEST 10: import-json")
    _reset_db()
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
        json.dump([
            {"client_name": "Import One", "api_key": FAKE_KEY_A},
            {"client_name": "Import Two", "api_key": FAKE_KEY_B},
            {"client_name": "Bad Placeholder", "api_key": "pk_REPLACE_ME"},
            {"client_name": "", "api_key": FAKE_KEY_A},
        ], handle)
        temp_path = handle.name

    code, out = _cli("import-json", temp_path,
                     env_extra={"KLAVIYO_MASTER_KEY": master})
    check("TEST 10: valid clients imported",
          sorted(r["client_name"] for r in database.fetch_clients(True, url=TEST_URL)),
          ["Import One", "Import Two"])
    check("TEST 10: placeholder rejected", "Bad Placeholder" in out and "FAILED" in out, True)
    check("TEST 10: reports counts", "2 imported" in out, True)
    check("TEST 10: never prints a key", FAKE_KEY_A in out or FAKE_KEY_B in out, False)
    check("TEST 10: source file untouched", os.path.exists(temp_path), True)
    os.unlink(temp_path)

    code, out = _cli("list", env_extra={"KLAVIYO_MASTER_KEY": master})
    check("list shows names and status", "Import One" in out and "active" in out, True)
    check("list never shows a key", FAKE_KEY_A in out or FAKE_KEY_B in out, False)

    section("TEST 12: one bad credential does not stop the run")
    _reset_db()
    ct, nonce = crypto_store.encrypt_api_key(FAKE_KEY_A, key)
    database.upsert_client("Good Client", ct, nonce, True, url=TEST_URL)
    # A row encrypted under a DIFFERENT master key: undecryptable here.
    foreign = crypto_store.load_master_key(crypto_store.generate_master_key())
    bad_ct, bad_nonce = crypto_store.encrypt_api_key(FAKE_KEY_B, foreign)
    database.upsert_client("Broken Client", bad_ct, bad_nonce, True, url=TEST_URL)

    m._MASTER_KEY_CACHE[:] = [key]
    records = [
        m.ClientRecord(client_name=r["client_name"], client_id=r["id"],
                       ciphertext=r["api_key_ciphertext"], nonce=r["api_key_nonce"])
        for r in database.fetch_clients(True, url=TEST_URL)
    ]
    broken = [r for r in records if r.client_name == "Broken Client"][0]
    payload = m.process_client(broken)
    check("TEST 12: broken credential reported per client",
          (payload["success"], payload["error_type"]), (False, "credential"))
    check("TEST 12: error mentions authentication, not the key",
          FAKE_KEY_B in str(payload["error"]), False)
    good = [r for r in records if r.client_name == "Good Client"][0]
    check("TEST 12: good client's key still resolves", good.reveal(), FAKE_KEY_A)
    m._MASTER_KEY_CACHE[:] = []


# ------------------------------------------------------------------
# TESTS 13-16: alert history in Postgres
# ------------------------------------------------------------------

def test_alert_history():
    section("TEST 13-16: alert history in Postgres")
    _reset_db()
    store = m.PostgresAlertStore()
    store._db = database

    def payload(period_end="2026-09-12T21:07:00+00:00",
                types=("high_usage:sms_credits",), name="Acme"):
        return {
            "client_name": name, "client_id": None, "success": True,
            "should_remind": True, "billing_period_end": period_end,
            "alert_types": list(types),
        }

    original_url = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = TEST_URL
    try:
        first = [payload()]
        keys = m.assign_alert_identity(first)
        state = store.load(keys)
        m.mark_duplicates(first, state, force=False)
        check("TEST 13: first alert is not a duplicate",
              first[0]["suppressed_duplicate"], False)

        m.record_alert(state, first[0])
        store.persist(first, state)

        with database.connection(TEST_URL) as conn:
            row = conn.execute(
                "SELECT alert_key, client_name, alert_types, billing_period_end "
                "FROM klaviyo_alert_history").fetchone()
        check("TEST 13: row persisted in Postgres",
              row["client_name"], "Acme")
        check("TEST 13: alert_types stored",
              list(row["alert_types"]), ["high_usage:sms_credits"])
        check("TEST 13: billing period stored",
              row["billing_period_end"].isoformat(), "2026-09-12T21:07:00+00:00")

        second = [payload()]
        keys = m.assign_alert_identity(second)
        state2 = store.load(keys)
        m.mark_duplicates(second, state2, force=False)
        check("TEST 14: identical alert suppressed",
              second[0]["suppressed_duplicate"], True)
        check("TEST 14: reports when it last fired",
              bool(second[0]["previously_alerted_at"]), True)

        newer = [payload(period_end="2026-10-12T21:07:00+00:00")]
        keys = m.assign_alert_identity(newer)
        m.mark_duplicates(newer, store.load(keys), force=False)
        check("TEST 15: new billing period alerts again",
              newer[0]["suppressed_duplicate"], False)
        check("TEST 15: and gets a different key",
              newer[0]["alert_key"] != second[0]["alert_key"], True)

        escalated = [payload(types=("over_limit:sms_credits",))]
        keys = m.assign_alert_identity(escalated)
        m.mark_duplicates(escalated, store.load(keys), force=False)
        check("TEST 16: high usage -> over limit alerts again",
              escalated[0]["suppressed_duplicate"], False)

        forced = [payload()]
        m.assign_alert_identity(forced)
        m.mark_duplicates(forced, store.load(m.assign_alert_identity(forced)), force=True)
        check("--force overrides suppression",
              forced[0]["suppressed_duplicate"], False)

        # Concurrency: the same key recorded twice must not duplicate rows.
        again = [payload()]
        st = {}
        m.assign_alert_identity(again)
        again[0]["suppressed_duplicate"] = False
        m.record_alert(st, again[0])
        store.persist(again, st)
        store.persist(again, st)
        with database.connection(TEST_URL) as conn:
            count = conn.execute(
                "SELECT count(*) AS n FROM klaviyo_alert_history "
                "WHERE alert_key = %s", (again[0]["alert_key"],)).fetchone()["n"]
        check("repeat persist upserts, never duplicates", count, 1)

        quiet = [{"client_name": "Quiet", "success": True, "should_remind": False}]
        keys = m.assign_alert_identity(quiet)
        check("quiet client claims no alert key", keys, [])

        check("store describes itself without credentials",
              "testpw" in store.describe(), False)
    finally:
        if original_url is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = original_url


def test_db_failure():
    section("TEST 19: database failure is loud, not silent")
    # Fake credentials on a closed port: the point is to prove the password
    # is stripped from the error, so one has to be present.
    unreachable = "postgresql://postgres:FAKE-PW-NOT-REAL@127.0.0.1:1/none"
    try:
        database.fetch_clients(url=unreachable)
        check("unreachable database raises", False, True)
    except database.DatabaseUnavailable as exc:
        check("unreachable database raises DatabaseUnavailable", True, True)
        check("message says database unavailable", "Database unavailable" in str(exc), True)
        check("message does not leak the password", "secretpw" in str(exc), False)
        check("message identifies host/port/db", "127.0.0.1:1/none" in str(exc), True)

    check("is_configured() false when unset",
          database.is_configured() if os.getenv("DATABASE_URL") else False,
          bool(os.getenv("DATABASE_URL")))


def main():
    m.MAX_RETRIES = 2
    m.REQUEST_DELAY = 0.0
    m.REQUEST_TIMEOUT = 5

    test_crypto()
    test_no_leaks()

    if TEST_URL:
        test_client_store()
        test_alert_history()
        test_db_failure()
    else:
        print("\n!! TEST_DATABASE_URL not set - database tests SKIPPED")
        FAILURES.append("database tests skipped (TEST_DATABASE_URL unset)")

    print("\n" + "=" * 70)
    if FAILURES:
        print("{} FAILURE(S):".format(len(FAILURES)))
        for failure in FAILURES:
            print("  - " + failure)
        return 1
    print("All storage checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
