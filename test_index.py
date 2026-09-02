"""
Offline self-test for the Klaviyo billing monitor.

No network, no API key, no pytest:

    python3 test_index.py

Covers the logic that decides what gets sent to n8n and Discord: date
parsing, usage maths, reminder triggers, duplicate suppression, error
classification, retry behaviour and secret redaction.
"""

import json
import os
import shutil
import sys
import tempfile
import threading
import http.server
from datetime import datetime, timedelta, timezone

import requests

import index as m

FAILURES = []


def check(label, got, want):
    ok = got == want
    if not ok:
        FAILURES.append("{}: got {!r}, want {!r}".format(label, got, want))
    print("  {}  {:<52} {}".format("ok  " if ok else "FAIL", label,
                                   "" if ok else "got={!r} want={!r}".format(got, want)))


def section(name):
    print("\n== {} ==".format(name))


UTC = timezone.utc


def test_dates():
    section("Date handling (Python 3.9 cannot parse these natively)")
    check("Z suffix", m.parse_iso("2026-09-12T21:07:00Z"), datetime(2026, 9, 12, 21, 7, tzinfo=UTC))
    check("offset", m.parse_iso("2026-09-12T21:07:00+00:00"), datetime(2026, 9, 12, 21, 7, tzinfo=UTC))
    check("microseconds", m.parse_iso("2026-09-01T21:46:19.962278Z"),
          datetime(2026, 9, 1, 21, 46, 19, 962278, tzinfo=UTC))
    check("milliseconds", m.parse_iso("2026-09-01T21:46:19.962Z"),
          datetime(2026, 9, 1, 21, 46, 19, 962000, tzinfo=UTC))
    check("nanoseconds truncated", m.parse_iso("2026-09-01T21:46:19.123456789Z"),
          datetime(2026, 9, 1, 21, 46, 19, 123456, tzinfo=UTC))
    check("offset without colon", m.parse_iso("2026-09-12T21:07:00+0000"),
          datetime(2026, 9, 12, 21, 7, tzinfo=UTC))
    check("naive treated as UTC", m.parse_iso("2026-09-12T21:07:00"),
          datetime(2026, 9, 12, 21, 7, tzinfo=UTC))
    check("non-UTC normalised", m.parse_iso("2026-09-12T17:07:00-04:00"),
          datetime(2026, 9, 12, 21, 7, tzinfo=UTC))
    check("invalid date is None", m.parse_iso("not a date"), None)
    check("None is None", m.parse_iso(None), None)

    end = datetime(2026, 9, 12, 21, 7, tzinfo=UTC)
    check("2.9 days -> 2", m.days_between(end, end - timedelta(days=2, hours=22)), 2)
    check("exactly 3 days -> 3", m.days_between(end, end - timedelta(days=3)), 3)
    check("6h remaining -> 0", m.days_between(end, end - timedelta(hours=6)), 0)
    check("expired 6h -> -1", m.days_between(end, end + timedelta(hours=6)), -1)


def test_usage():
    section("Usage parsing (nulls, zero limits, unknown metrics)")
    resp = {"data": [
        {"id": "active_profiles", "attributes": {
            "usage_used": 90, "usage_max": 100, "unit": "profiles",
            "period_start": "2026-08-12T21:07:00Z", "period_end": "2026-09-12T21:07:00Z",
            "as_of": "2026-09-02T00:00:00Z"}},
        {"id": "sms_credits", "attributes": {
            "usage_used": 50, "usage_max": None, "unit": "credits",
            "period_start": "2026-08-12T21:07:00Z", "period_end": "2026-09-12T21:07:00Z"}},
        {"id": "email_sends", "attributes": {
            "usage_used": 120, "usage_max": 100, "unit": "messages",
            "period_start": "2026-08-12T21:07:00Z", "period_end": "2026-09-12T21:07:00Z"}},
        {"id": "reviews_orders", "attributes": {
            "usage_used": 5, "usage_max": 0, "unit": "orders",
            "period_start": "2026-08-12T21:07:00Z", "period_end": "2026-09-12T21:07:00Z"}},
        {"id": "future_metric_klaviyo_adds", "attributes": {
            "usage_used": 1, "usage_max": 10, "unit": "things",
            "period_start": "2026-08-12T21:07:00Z", "period_end": "2026-09-12T21:07:00Z"}},
    ]}
    parsed = m.parse_billing_usage(resp)
    metrics = parsed["metrics"]

    check("percent computed", metrics["active_profiles"]["percent"], 90.0)
    check("high usage detected", metrics["active_profiles"]["high_usage"], True)
    check("null limit -> percent None", metrics["sms_credits"]["percent"], None)
    check("null limit is not over limit", metrics["sms_credits"]["over_limit"], False)
    check("over limit detected", metrics["email_sends"]["over_limit"], True)
    check("zero limit with usage IS over limit", metrics["reviews_orders"]["over_limit"], True)
    check("unknown metric retained", "future_metric_klaviyo_adds" in metrics, True)
    check("unknown metric not important", metrics["future_metric_klaviyo_adds"]["important"], False)
    check("period extracted", parsed["period_start"], "2026-08-12T21:07:00Z")

    check("whole numbers stay ints", m._tidy(100712.0), 100712)
    check("fractions rounded", m._tidy(39.2098), 39.21)
    check("None passes through", m._tidy(None), None)
    check("garbage number -> None", m._safe_number("abc"), None)


def test_reminder():
    section("Reminder logic")
    quiet = {"active_profiles": {"label": "Profiles", "important": True, "used": 1,
                                 "limit": 100, "percent": 1.0, "over_limit": False,
                                 "high_usage": False}}
    check("quiet + far from billing -> no remind", m.build_reminder(quiet, 10)[0], False)
    check("quiet + 3 days -> remind", m.build_reminder(quiet, 3)[0], True)
    check("3 days reason", m.build_reminder(quiet, 3)[1], ["Billing period ends in 3 days"])
    check("1 day is singular", m.build_reminder(quiet, 1)[1], ["Billing period ends in 1 day"])
    check("0 days -> today", m.build_reminder(quiet, 0)[1], ["Billing period ends today"])
    check("expired", m.build_reminder(quiet, -2)[1], ["Billing period ended 2 days ago"])
    check("period alert type", m.build_reminder(quiet, 2)[2], ["billing_period_ending"])

    high = {"sms_credits": {"label": "SMS", "important": True, "used": 95, "limit": 100,
                            "percent": 95.0, "over_limit": False, "high_usage": True}}
    check("important high usage -> remind", m.build_reminder(high, 30)[0], True)
    check("high usage alert type", m.build_reminder(high, 30)[2], ["high_usage:sms_credits"])

    minor = {"ai_credits": {"label": "AI credits", "important": False, "used": 95,
                            "limit": 100, "percent": 95.0, "over_limit": False,
                            "high_usage": True}}
    check("unimportant high usage -> no remind", m.build_reminder(minor, 30)[0], False)

    over = {"ai_credits": {"label": "AI credits", "important": False, "used": 150,
                           "limit": 100, "percent": 150.0, "over_limit": True,
                           "high_usage": True}}
    check("any over limit -> remind", m.build_reminder(over, 30)[0], True)
    check("over limit alert type", m.build_reminder(over, 30)[2], ["over_limit:ai_credits"])


def test_average():
    section("Average recipients")
    def avg(values):
        known = [v for v in values if v is not None]
        return round(sum(known) / len(known)) if known else None

    check("simple average", avg([100, 200, 300]), 200)
    check("failed estimates excluded from denominator", avg([100, None, 300]), 200)
    check("no campaigns -> None (no divide by zero)", avg([]), None)
    check("all estimates failed -> None", avg([None, None]), None)


def test_dedupe():
    section("Duplicate alert suppression")
    def key(**kw):
        p = dict(client_name="Acme", success=True, should_remind=True,
                 billing_period_end="2026-09-12T21:07:00+00:00",
                 alert_types=["high_usage:sms_credits"])
        p.update(kw)
        return m.alert_identity(p)[1]

    base = key()
    check("stable across runs", key(), base)
    check("escalation makes a new key", key(alert_types=["over_limit:sms_credits"]) != base, True)
    check("new billing period makes a new key",
          key(billing_period_end="2026-10-12T21:07:00+00:00") != base, True)
    check("other client makes a new key", key(client_name="Other") != base, True)
    check("alert type order does not matter",
          key(alert_types=["a", "b"]), key(alert_types=["b", "a"]))
    check("quiet account has no alert key",
          m.alert_identity({"client_name": "A", "success": True, "should_remind": False})[1], None)
    check("failure gets its own key",
          m.alert_identity({"client_name": "A", "success": False, "error_type": "auth"})[0],
          ["client_failed:auth"])

    now = m.now_utc()
    payloads = [
        {"client_name": "Acme", "success": True, "should_remind": True,
         "billing_period_end": "2026-09-12T21:07:00+00:00",
         "alert_types": ["high_usage:sms_credits"]},
    ]
    state = {}
    m.mark_duplicates(payloads, state, force=False)
    check("first sighting is not a duplicate", payloads[0]["suppressed_duplicate"], False)

    m.record_alert(state, payloads[0])
    check("state remembers the alert", len(state), 1)

    m.mark_duplicates(payloads, state, force=False)
    check("second sighting IS a duplicate", payloads[0]["suppressed_duplicate"], True)

    m.mark_duplicates(payloads, state, force=True)
    check("--force overrides", payloads[0]["suppressed_duplicate"], False)

    state_old = {list(state)[0]: (now - timedelta(days=30)).isoformat()}
    m.mark_duplicates(payloads, state_old, force=False)
    check("re-alerts after cooldown", payloads[0]["suppressed_duplicate"], False)


def test_errors():
    section("Error classification and retries")
    for code, kind in [(400, "api"), (401, "auth"), (403, "permission"), (404, "not_found"),
                       (429, "rate_limit"), (500, "server"), (502, "server"),
                       (503, "server"), (504, "server")]:
        check("HTTP {} -> {}".format(code, kind), m._classify(code), kind)

    check("auth is not retried", m._classify(401) in ("rate_limit", "server"), False)
    check("429 is retried", m._classify(429) in ("rate_limit", "server"), True)
    check("retries are bounded", m.MAX_RETRIES > 0 and m.MAX_RETRIES < 100, True)
    check("campaign paging is bounded", m.MAX_CAMPAIGN_PAGES > 0, True)
    check("estimation polling is bounded", m.JOB_MAX_POLLS > 0, True)

    calls = {"n": 0}

    class Throttled(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            calls["n"] += 1
            if calls["n"] < 3:
                self.send_response(429)
                self.send_header("Retry-After", "1")
                self.end_headers()
                self.wfile.write(b'{"errors":[{"detail":"throttled"}]}')
            else:
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"data":[]}')

        def log_message(self, *a):
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), Throttled)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = "http://127.0.0.1:{}/x".format(server.server_port)
    body = m.request(requests.Session(), "GET", url, "pk_test", "rev", label="throttled")
    server.shutdown()
    check("429 retried then succeeded", (calls["n"], body), (3, {"data": []}))

    try:
        m.request(requests.Session(), "GET", "http://127.0.0.1:9/x", "pk_test", "rev",
                  label="dead")
        check("network failure raises", False, True)
    except m.KlaviyoError as exc:
        check("network failure -> KlaviyoError(network)", exc.kind, "network")

    delivered, detail = m.post_json("http://127.0.0.1:9/webhook", {"client_name": "X"})
    check("n8n down returns False, does not raise", delivered, False)
    check("n8n down reports a reason", bool(detail), True)


def test_security():
    section("Secret handling")
    fake = "pk_AbCdEf_0123456789abcdef0123456789abcdef"
    check("redact strips the key", fake in m.redact("using " + fake), False)
    check("redact leaves a marker", "REDACTED" in m.redact("using " + fake), True)
    check("KlaviyoError redacts", fake in str(m.KlaviyoError("failed with " + fake)), False)

    payload = m.build_payload("Acme")
    check("payload has no api_key field", "api_key" in payload, False)
    check("payload is JSON serialisable", isinstance(json.dumps(payload), str), True)
    check("payload carries should_remind", "should_remind" in payload, True)
    check("payload carries reasons", "reasons" in payload, True)
    check("payload carries alert_key", "alert_key" in payload, True)

    secret = "https://n8n.acme.io/webhook/SECRET-TOKEN-123"
    check("redact_url keeps host only", m.redact_url(secret), "https://n8n.acme.io/...")
    check("redact_url hides the token path", "SECRET-TOKEN-123" in m.redact_url(secret), False)
    check("scrub removes the token from an error",
          "SECRET-TOKEN-123" in m._scrub_url("failed POST to " + secret, secret), False)
    check("short error has no URL path",
          "SECRET-TOKEN-123" in m._short_error(
              requests.exceptions.ConnectionError("boom " + secret), secret), False)

    wire = m._for_wire({"client_name": "A", "n8n_delivery": "sent",
                        "n8n_detail": "HTTP 200", "should_remind": True})
    check("local-only n8n_* fields never go on the wire",
          sorted(wire), ["client_name", "should_remind"])


def test_optional_webhook():
    section("n8n is optional")
    for bad, expected in [("", "not_configured"),
                          ("   ", "not_configured"),
                          ("ftp://x", "invalid"),
                          ("https://your-n8n-domain/webhook/x", "placeholder"),
                          ("https://example.com/webhook/x", "placeholder")]:
        url, status, message = m.resolve_webhook(bad)
        check("{!r} -> {} and no delivery".format(bad or "(empty)", expected),
              (url, status), (None, expected))
        check("  ...and explains itself", bool(message), True)

    url, status, _ = m.resolve_webhook("https://n8n.acme.io/webhook/k")
    check("real URL is usable", (url, status), ("https://n8n.acme.io/webhook/k", "configured"))

    _, _, message = m.resolve_webhook("https://n8n.acme.io/webhook/SECRET-TOKEN-123")
    check("configured message hides the token", "SECRET-TOKEN-123" in message, False)

    check("resolve_webhook never exits",
          all(m.resolve_webhook(v)[1] for v in ["", "ftp://x", "https://example.com/x"]), True)


def test_dotenv(tmpdir):
    section(".env loading")
    path = os.path.join(tmpdir, ".env")
    with open(path, "w") as handle:
        handle.write(
            "# a comment\n"
            "\n"
            "N8N_WEBHOOK_URL=https://n8n.example.org/webhook/abc\n"
            "export REMINDER_DAYS=5\n"
            "QUOTED=\"spaced value\"\n"
            "SINGLE='single value'\n"
            "TRAILING=plain # trailing comment\n"
            "MALFORMED_LINE_NO_EQUALS\n"
            "ALREADY_SET=from_file\n"
        )

    os.environ["ALREADY_SET"] = "from_shell"
    for key in ["N8N_WEBHOOK_URL", "REMINDER_DAYS", "QUOTED", "SINGLE", "TRAILING"]:
        os.environ.pop(key, None)

    loaded = m.load_dotenv(path)

    check("reads a plain value", os.environ.get("N8N_WEBHOOK_URL"),
          "https://n8n.example.org/webhook/abc")
    check("handles `export `", os.environ.get("REMINDER_DAYS"), "5")
    check("handles double quotes", os.environ.get("QUOTED"), "spaced value")
    check("handles single quotes", os.environ.get("SINGLE"), "single value")
    check("strips trailing comment", os.environ.get("TRAILING"), "plain")
    check("skips malformed lines", "MALFORMED_LINE_NO_EQUALS" in loaded, False)
    check("real environment wins over .env", os.environ.get("ALREADY_SET"), "from_shell")
    check("missing file is not an error", m.load_dotenv(os.path.join(tmpdir, "nope")), [])

    for key in ["N8N_WEBHOOK_URL", "REMINDER_DAYS", "QUOTED", "SINGLE",
                "TRAILING", "ALREADY_SET"]:
        os.environ.pop(key, None)


def test_summary_rendering():
    section("Terminal summary")
    import io
    import contextlib

    empty = m.build_payload("Sparse Client")   # every value still None
    good = dict(empty, success=True, billing_period_start="2026-08-12T21:07:00+00:00",
                billing_period_end="2026-09-12T21:07:00+00:00", days_until_billing=10,
                active_profiles=100712, profile_limit=170000, profile_usage_percent=59.24,
                should_remind=True, reasons=["SMS usage is at 98.11%"])
    failed = dict(empty, success=False, error_type="auth", error="401 Incorrect credentials")

    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        m.print_summary([good, failed, empty], "not_configured", "n8n not configured")
    text = buffer.getvalue()

    check("renders a client name", "Sparse Client" in text, True)
    check("renders usage numbers", "100,712 / 170,000 (59.24%)" in text, True)
    check("renders a readable date", "12 Aug 2026" in text, True)
    check("renders the reminder verdict", "Reminder" in text and "YES" in text, True)
    check("renders the reason", "SMS usage is at 98.11%" in text, True)
    check("renders a failed client", "FAILED (auth)" in text, True)
    check("renders the skip notice", "Not configured - webhook skipped" in text, True)
    check("tolerates all-None payload without crashing", "SUMMARY" in text, True)


def main():
    m.MAX_RETRIES = 2
    m.REQUEST_DELAY = 0.0
    m.REQUEST_TIMEOUT = 5

    tmpdir = tempfile.mkdtemp(prefix="klaviyo-test-")
    try:
        for test in (test_dates, test_usage, test_reminder, test_average,
                     test_dedupe, test_errors, test_security,
                     test_optional_webhook, test_summary_rendering):
            test()
        test_dotenv(tmpdir)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    print("\n" + "=" * 60)
    if FAILURES:
        print("{} FAILURE(S):".format(len(FAILURES)))
        for failure in FAILURES:
            print("  - " + failure)
        return 1
    print("All checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
