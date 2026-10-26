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
    check("90% is below the 95% alert threshold", metrics["active_profiles"]["high_usage"], False)
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


def _billing_item(metric_id, used, limit, unit="profiles",
                  start="2026-10-01T00:00:00Z", end="2026-11-01T00:00:00Z"):
    return {"id": metric_id, "attributes": {
        "usage_used": used, "usage_max": limit, "unit": unit,
        "period_start": start, "period_end": end}}


def _remind(items):
    return m.build_reminder(m.parse_billing_usage({"data": items})["metrics"])


# Core metrics comfortably below 95%.
SAFE = [_billing_item("active_profiles", 3250, 6500),
        _billing_item("email_sends", 2000, 10000, "messages"),
        _billing_item("sms_credits", 10, 100, "USD")]


def test_reminder():
    section("Reminder logic: core usage >= 95% only")
    check("threshold is 95%", m.USAGE_ALERT_PERCENT, 95.0)

    check("1. profiles 94.99% -> no alert",
          _remind([_billing_item("active_profiles", 9499, 10000)]), (False, [], []))
    check("   profiles 94.996% does not round up into an alert",
          _remind([_billing_item("active_profiles", 94996, 100000)])[0], False)
    check("2. profiles 95.00% -> alert",
          _remind([_billing_item("active_profiles", 9500, 10000)]),
          (True, ["Profiles usage is at 95.0%"], ["usage:active_profiles"]))
    check("   profiles 95.2% reason",
          _remind([_billing_item("active_profiles", 9520, 10000)]),
          (True, ["Profiles usage is at 95.2%"], ["usage:active_profiles"]))
    check("3. profiles 98% -> alert",
          _remind([_billing_item("active_profiles", 6370, 6500)] + SAFE[1:]),
          (True, ["Profiles usage is at 98.0%"], ["usage:active_profiles"]))
    check("   profiles 105% -> same alert type, no separate over-limit",
          _remind([_billing_item("active_profiles", 6825, 6500)] + SAFE[1:]),
          (True, ["Profiles usage is at 105.0%"], ["usage:active_profiles"]))
    check("   email 95% -> alert",
          _remind([SAFE[0], _billing_item("email_sends", 9500, 10000, "messages"), SAFE[2]]),
          (True, ["Email usage is at 95.0%"], ["usage:email_sends"]))
    check("   sms 96% -> alert",
          _remind(SAFE[:2] + [_billing_item("sms_credits", 96, 100, "USD")]),
          (True, ["SMS usage is at 96.0%"], ["usage:sms_credits"]))
    check("   all three -> three independent alert types",
          _remind([_billing_item("active_profiles", 96, 100),
                   _billing_item("email_sends", 97, 100, "messages"),
                   _billing_item("sms_credits", 98, 100, "USD")])[2],
          ["usage:active_profiles", "usage:email_sends", "usage:sms_credits"])
    check("   core zero allowance with usage -> alert",
          _remind([_billing_item("sms_credits", 5, 0, "USD")]),
          (True, ["SMS usage is over its zero allowance"], ["usage:sms_credits"]))
    check("   unlimited core metric -> no alert",
          _remind([_billing_item("sms_credits", 5000, None, "USD")]), (False, [], []))
    check("example 1: 78% / 25% / 10% -> quiet",
          _remind([_billing_item("active_profiles", 78, 100),
                   _billing_item("email_sends", 25, 100, "messages"),
                   _billing_item("sms_credits", 10, 100, "USD")]), (False, [], []))
    check("build_reminder no longer takes days remaining",
          m.build_reminder.__code__.co_argcount, 1)
    check("REMINDER_DAYS countdown setting is gone", hasattr(m, "REMINDER_DAYS"), False)


def test_excluded_metrics():
    section("15. Only Profiles / Email / SMS raise usage alerts")
    # 6405 / 250 = 2562% - the real-world numbers that caused noisy alerts.
    hub = _billing_item("customer_hub_active_profiles", 6405, 250)
    social = _billing_item("k_social_active_profiles", 6405, 250)

    check("allowlist is exactly the core metrics", m.ALERTABLE_USAGE_METRICS,
          frozenset({"active_profiles", "email_sends", "sms_credits"}))
    check("ai credits 500% -> no alert",
          _remind(SAFE + [_billing_item("ai_credits", 500, 100, "credits")]), (False, [], []))
    check("reviews 500% -> no alert",
          _remind(SAFE + [_billing_item("reviews_orders", 500, 100, "orders")]), (False, [], []))
    check("hub / k social 20,000% -> no alert",
          _remind(SAFE + [_billing_item("customer_hub_active_profiles", 50000, 250),
                          _billing_item("k_social_active_profiles", 50000, 250)]),
          (False, [], []))
    check("helpdesk / ai resolutions over -> no alert",
          _remind(SAFE + [_billing_item("helpdesk_conversations", 900, 800, "conversations"),
                          _billing_item("ai_resolutions", 900, 750, "resolutions")]),
          (False, [], []))
    check("unknown future metric over limit -> no alert",
          _remind(SAFE + [_billing_item("future_metric_klaviyo_adds", 10, 0, "things")]),
          (False, [], []))
    check("2562% is computed for hub",
          m.parse_billing_usage({"data": [hub]})["metrics"]["customer_hub_active_profiles"]["percent"],
          2562.0)
    check("main profiles 98.95% alongside hub -> profiles only",
          _remind([_billing_item("active_profiles", 6432, 6500), hub, social]),
          (True, ["Profiles usage is at 98.95%"], ["usage:active_profiles"]))
    check("customer hub 2562% alone -> no alert", _remind([hub]), (False, [], []))
    check("k social 2562% alone -> no alert", _remind([social]), (False, [], []))
    check("sms 95% + optional high -> only sms",
          _remind(SAFE[:2] + [_billing_item("sms_credits", 95, 100, "USD"), hub, social]),
          (True, ["SMS usage is at 95.0%"], ["usage:sms_credits"]))


def test_excluded_metrics_preserved():
    section("Customer Hub / K Social still reported in other_usage")
    response = {"data": [
        _billing_item("active_profiles", 3250, 6500),
        _billing_item("email_sends", 2000, 10000, "messages"),
        _billing_item("sms_credits", 10, 100, "USD"),
        _billing_item("customer_hub_active_profiles", 6405, 250),
        _billing_item("k_social_active_profiles", 6405, 250),
        _billing_item("ai_credits", 500, 100, "credits"),
        _billing_item("reviews_orders", 500, 100, "orders"),
        _billing_item("future_metric_klaviyo_adds", 10, 5, "things"),
    ]}

    class FakeRecord:
        client_name = "Excluded Metrics Client"
        client_id = None

        def reveal(self):
            return "pk_test_not_a_real_key"

    originals = (m.get_billing_usage, m.get_sent_email_campaigns, m.collect_recipients)
    m.get_billing_usage = lambda session, api_key: response
    m.get_sent_email_campaigns = lambda *a, **k: []
    m.collect_recipients = lambda *a, **k: ({}, [])
    try:
        payload = m.process_client(FakeRecord())
    finally:
        m.get_billing_usage, m.get_sent_email_campaigns, m.collect_recipients = originals

    other = payload.get("other_usage") or {}
    check("client succeeded", payload["success"], True)
    check("hub kept in other_usage", other.get("customer_hub_active_profiles"),
          {"used": 6405, "limit": 250, "usage_percent": 2562.0, "unit": "profiles"})
    check("k social kept in other_usage", other.get("k_social_active_profiles"),
          {"used": 6405, "limit": 250, "usage_percent": 2562.0, "unit": "profiles"})
    check("payload should_remind false", payload["should_remind"], False)
    check("payload reasons empty", payload["reasons"], [])
    check("payload alert_types empty", payload["alert_types"], [])
    check("ai credits still in payload", (payload["ai_used"], payload["ai_usage_percent"]),
          (500, 500.0))
    check("reviews still in payload",
          (payload["reviews_used"], payload["reviews_usage_percent"]), (500, 500.0))
    check("unknown metric kept in other_usage",
          other.get("future_metric_klaviyo_adds", {}).get("usage_percent"), 200.0)


def test_average():
    section("Average recipients")
    def avg(values):
        known = [v for v in values if v is not None]
        return round(sum(known) / len(known)) if known else None

    check("simple average", avg([100, 200, 300]), 200)
    check("failed estimates excluded from denominator", avg([100, None, 300]), 200)
    check("no campaigns -> None (no divide by zero)", avg([]), None)
    check("all estimates failed -> None", avg([None, None]), None)


def _usage_payload(types, period_end="2026-11-21T00:00:00+00:00", name="FortCore",
                   reasons=None):
    reasons = reasons or ["{} reason".format(t) for t in types]
    return {"client_name": name, "success": True, "should_remind": bool(types),
            "billing_period_end": period_end, "alert_types": list(types),
            "reasons": list(reasons),
            "usage_alerts": [{"alert_type": t, "reason": r} for t, r in zip(types, reasons)]}


def test_dedupe():
    section("Duplicate suppression: one alert per metric per billing period")

    def keys(payload):
        return dict(m.alert_identity(payload))

    base = keys(_usage_payload(["usage:active_profiles"]))["usage:active_profiles"]
    check("key is stable across runs",
          keys(_usage_payload(["usage:active_profiles"]))["usage:active_profiles"], base)
    check("key ignores the percentage / reason text",
          keys(_usage_payload(["usage:active_profiles"],
                              reasons=["Profiles usage is at 105.0%"]))["usage:active_profiles"],
          base)
    check("key = client + period end + metric",
          base, m._alert_key("FortCore", "2026-11-21", "usage:active_profiles"))
    check("key ignores period-end timestamp formatting",
          keys(_usage_payload(["usage:active_profiles"],
                              period_end="2026-11-21T00:00:00.000Z"))["usage:active_profiles"],
          base)
    check("new billing period -> new key",
          keys(_usage_payload(["usage:active_profiles"],
                              period_end="2026-12-21T00:00:00+00:00"))["usage:active_profiles"]
          != base, True)
    check("other client -> new key",
          keys(_usage_payload(["usage:active_profiles"], name="Other"))["usage:active_profiles"]
          != base, True)
    both = keys(_usage_payload(["usage:active_profiles", "usage:sms_credits"]))
    check("each metric has its own key",
          (both["usage:active_profiles"], both["usage:sms_credits"] != base), (base, True))
    check("quiet account has no alert identity",
          m.alert_identity({"client_name": "A", "success": True, "should_remind": False}), [])
    check("failure keeps its existing key format",
          m.alert_identity({"client_name": "A", "success": False, "error_type": "auth"}),
          [("client_failed:auth", m._alert_key("A", "failure", "client_failed:auth"))])

    # --- the daily sequence from the spec, against one state ---------------
    state = {}

    def run(types, reasons, period_end="2026-11-21T00:00:00+00:00", force=False):
        payload = _usage_payload(types, period_end=period_end, reasons=reasons)
        m.mark_duplicates([payload], state, force=force)
        return payload

    day2 = run(["usage:active_profiles"], ["Profiles usage is at 95.1%"])
    check("example 2: first 95.1% -> should_remind", day2["should_remind"], True)
    check("example 2: reason", day2["reasons"], ["Profiles usage is at 95.1%"])
    check("example 2: not a duplicate", day2["suppressed_duplicate"], False)
    m.record_alert(state, day2)
    check("18. delivery records exactly one key", list(state), [base])

    day3 = run(["usage:active_profiles"], ["Profiles usage is at 97.8%"])
    check("4. next day 97.8% -> should_remind false", day3["should_remind"], False)
    check("4. ...reasons empty", day3["reasons"], [])
    check("4. ...suppressed", day3["suppressed_duplicate"], True)
    check("4. ...lists what already alerted",
          [a["alert_type"] for a in day3["already_alerted"]], ["usage:active_profiles"])
    check("4. ...says when", day3["previously_alerted_at"], state[base])

    day5 = run(["usage:active_profiles"], ["Profiles usage is at 105.0%"])
    check("5. 105% after 95% alert -> still suppressed",
          (day5["should_remind"], day5["suppressed_duplicate"]), (False, True))

    old = dict(state)
    state[base] = (m.now_utc() - timedelta(days=60)).isoformat()
    check("usage alerts never re-fire on a cooldown",
          run(["usage:active_profiles"], ["Profiles usage is at 110%"])["should_remind"], False)
    state.clear()
    state.update(old)

    sms = run(["usage:active_profiles", "usage:sms_credits"],
              ["Profiles usage is at 99.0%", "SMS usage is at 96.0%"])
    check("6. profiles alerted, SMS reaches 96% -> alert", sms["should_remind"], True)
    check("6. ...only the SMS reason is sent", sms["reasons"], ["SMS usage is at 96.0%"])
    check("6. ...only the SMS type is sent", sms["alert_types"], ["usage:sms_credits"])
    check("6. ...profiles not resent",
          [a["alert_type"] for a in sms["already_alerted"]], ["usage:active_profiles"])
    check("6. ...not marked as a duplicate", sms["suppressed_duplicate"], False)
    m.record_alert(state, sms)
    check("6. ...recording adds only the SMS key", len(state), 2)

    for pct in ("96.2", "96.9", "97.5"):
        again = run(["usage:active_profiles", "usage:sms_credits"],
                    ["Profiles usage is at 99.0%", "SMS usage is at {}%".format(pct)])
        check("7. SMS {}% after SMS alert -> no new alert".format(pct),
              again["should_remind"], False)

    email = run(["usage:active_profiles", "usage:email_sends", "usage:sms_credits"],
                ["Profiles usage is at 99.0%", "Email usage is at 95.5%", "SMS usage is at 97.0%"])
    check("8. email alerts independently",
          (email["should_remind"], email["alert_types"]), (True, ["usage:email_sends"]))
    m.record_alert(state, email)
    check("8. three alerts max for this period", len(state), 3)
    check("8. all three alerted -> silent",
          run(["usage:active_profiles", "usage:email_sends", "usage:sms_credits"],
              ["p", "e", "s"])["should_remind"], False)

    nxt = run(["usage:active_profiles"], ["Profiles usage is at 95.0%"],
              period_end="2026-12-21T00:00:00+00:00")
    check("9. new billing period -> profiles may alert again",
          (nxt["should_remind"], nxt["suppressed_duplicate"]), (True, False))

    forced = run(["usage:active_profiles"], ["Profiles usage is at 99.0%"], force=True)
    check("--force overrides", forced["should_remind"], True)

    check("mark_duplicates is repeatable (candidates untouched)",
          len(day3["usage_alerts"]), 1)
    m.mark_duplicates([day3], {}, force=False)
    check("...re-evaluating with empty history re-qualifies", day3["should_remind"], True)

    # --- client failures keep cooldown suppression ------------------------
    fail = {"client_name": "Broken", "success": False, "error_type": "auth"}
    fstate = {}
    m.mark_duplicates([fail], fstate, force=False)
    check("16. failure alerts", fail["suppressed_duplicate"], False)
    m.record_alert(fstate, fail)
    m.mark_duplicates([fail], fstate, force=False)
    check("identical failure next day suppressed", fail["suppressed_duplicate"], True)
    old_fail = {k: (m.now_utc() - timedelta(days=30)).isoformat() for k in fstate}
    m.mark_duplicates([fail], old_fail, force=False)
    check("failure re-alerts after cooldown", fail["suppressed_duplicate"], False)


class _FakeN8n:
    """Local stand-in for the n8n webhook that records every POST body."""

    def __init__(self):
        received = self.received = []
        self.status = 200
        owner = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                received.append(body.decode("utf-8"))
                self.send_response(owner.status)
                self.end_headers()

            def log_message(self, *a):
                pass

        self._server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self._server.serve_forever, daemon=True).start()
        self.url = "http://127.0.0.1:{}/webhook/SECRET-TOKEN".format(self._server.server_port)

    def take(self):
        bodies = [json.loads(b) for b in self.received]
        del self.received[:]
        return bodies

    def close(self):
        self._server.shutdown()


SECRET_KEY = "pk_TESTONLY_never_log_this_0123456789"


def test_daily_runs(tmpdir):
    section("End to end: daily main() runs, n8n delivery and alert history")
    import io
    import contextlib
    import logging

    n8n = _FakeN8n()
    state_file = os.path.join(tmpdir, "alert_state.json")
    accounts = {}   # client name -> billing-usage response, or an exception

    class FakeRecord:
        client_id = None

        def __init__(self, name):
            self.client_name = name

        def reveal(self):
            return SECRET_KEY

    def fake_billing(session, api_key):
        assert api_key == SECRET_KEY
        account = accounts[current["name"]]
        if isinstance(account, Exception):
            raise account
        return account

    current = {}
    real_process = m.process_client

    def process(record):
        current["name"] = record.client_name
        return real_process(record)

    # Klaviyo reports a fixed period boundary; it must not drift between runs.
    period_now = m.now_utc().replace(microsecond=0)
    period = ((period_now - timedelta(days=10)).isoformat(),
              (period_now + timedelta(days=20)).isoformat())

    def account(profiles=30, email=20, sms=10, start=None, end=None, extra=()):
        start = start or period[0]
        end = end or period[1]
        items = [_billing_item("active_profiles", profiles, 100, start=start, end=end),
                 _billing_item("email_sends", email, 100, "messages", start=start, end=end),
                 _billing_item("sms_credits", sms, 100, "USD", start=start, end=end)]
        return {"data": items + [_billing_item(i, u, l, start=start, end=end)
                                 for i, u, l in extra]}

    logs = io.StringIO()
    log_handler = logging.StreamHandler(logs)

    def run(*flags):
        """One daily run. Returns (POST bodies, stdout)."""
        sys.argv = ["index.py", "--state-file", state_file] + list(flags)
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            m.main()
        return n8n.take(), out.getvalue()

    originals = (m.load_clients, m.process_client, m.get_billing_usage,
                 m.get_sent_email_campaigns, m.collect_recipients, m.N8N_WEBHOOK_URL,
                 sys.argv, os.environ.pop("DATABASE_URL", None))
    m.load_clients = lambda: [FakeRecord(name) for name in sorted(accounts)]
    m.process_client = lambda record: process(record)
    m.get_billing_usage = fake_billing
    m.get_sent_email_campaigns = lambda *a, **k: []
    m.collect_recipients = lambda *a, **k: ({}, [])
    m.N8N_WEBHOOK_URL = n8n.url
    m.MAX_WORKERS = 1   # `current` is shared; keep clients sequential
    m.logger.addHandler(log_handler)
    try:
        # Example 1 / 14: normal client.
        accounts["FortCore"] = account(profiles=78, email=25, sms=10)
        sent, _ = run()
        check("14. normal client -> nothing POSTed to n8n", sent, [])

        # 1: 94.99% (day 1).
        accounts["FortCore"] = account(profiles=94.99)
        sent, _ = run()
        check("1. profiles 94.99% -> nothing POSTed", sent, [])

        # 10-13: billing countdown with normal usage.
        for label, end in [
            ("10. billing 3 days remaining", timedelta(days=3, hours=2)),
            ("11. billing 1 day remaining", timedelta(days=1, hours=2)),
            ("12. billing ends today", timedelta(hours=6)),
        ]:
            accounts["FortCore"] = account(end=(m.now_utc() + end).isoformat())
            sent, _ = run()
            check(label + " -> nothing POSTed", sent, [])
        accounts["FortCore"] = account(start=(m.now_utc() - timedelta(hours=1)).isoformat(),
                                       end=(m.now_utc() + timedelta(days=30)).isoformat())
        sent, _ = run()
        check("13. new billing period alone -> nothing POSTed", sent, [])

        # 15: excluded metrics wildly over, core fine.
        accounts["FortCore"] = account(extra=[
            ("customer_hub_active_profiles", 6405, 250), ("k_social_active_profiles", 6405, 250),
            ("ai_credits", 500, 100), ("reviews_orders", 500, 100),
            ("helpdesk_conversations", 900, 800), ("ai_resolutions", 900, 750),
            ("future_metric_klaviyo_adds", 10, 0)])
        sent, _ = run()
        check("15. hub/K social/AI/reviews/unknown over -> nothing POSTed", sent, [])
        check("15. nothing recorded in alert history",
              m.load_alert_state(state_file), {})

        # 17: profiles hits 95.1% but n8n is down.
        accounts["FortCore"] = account(profiles=95.1)
        n8n.status = 500
        sent, _ = run()
        check("17. failed delivery was attempted", len(sent) >= 1, True)
        check("17. failed delivery is NOT recorded", m.load_alert_state(state_file), {})

        # Dry run: shows the qualifying client, sends nothing, writes nothing.
        n8n.status = 200
        before = open(state_file).read() if os.path.exists(state_file) else None
        sent, out = run("--dry-run")
        after = open(state_file).read() if os.path.exists(state_file) else None
        check("dry run POSTs nothing", sent, [])
        check("dry run leaves alert history untouched", after, before)
        check("dry run shows the qualifying alert", "Profiles usage is at 95.1%" in out, True)

        # 2 / 18: retried next run, n8n back up.
        sent, _ = run()
        check("17. retried on the next run -> one POST", len(sent), 1)
        check("2. first 95.1% alert reason",
              sent[0]["reasons"] if sent else None, ["Profiles usage is at 95.1%"])
        check("2. alert payload should_remind", sent[0]["should_remind"] if sent else None, True)
        check("18. successful delivery recorded", len(m.load_alert_state(state_file)), 1)
        first_alert = sent[0] if sent else {}

        # 4 / 5: following days.
        for pct in (96, 99, 103):
            accounts["FortCore"] = account(profiles=pct)
            sent, out = run()
            check("4/5. profiles {}% after alert -> nothing POSTed".format(pct), sent, [])
        check("5. summary shows it already alerted",
              "Already alerted" in out and "usage:active_profiles" in out, True)

        # 6 / 7: SMS reaches 96%, then drifts.
        accounts["FortCore"] = account(profiles=103, sms=96)
        sent, _ = run()
        check("6. SMS 96% -> exactly one POST", len(sent), 1)
        check("6. ...carrying only the SMS reason",
              sent[0]["reasons"] if sent else None, ["SMS usage is at 96.0%"])
        for sms in (96.4, 97.2):
            accounts["FortCore"] = account(profiles=104, sms=sms)
            sent, _ = run()
            check("7. SMS {}% after SMS alert -> nothing POSTed".format(sms), sent, [])

        # 8: Email.
        accounts["FortCore"] = account(profiles=104, sms=97, email=95)
        sent, _ = run()
        check("8. Email 95% -> one POST with only Email",
              [s["alert_types"] for s in sent], [["usage:email_sends"]])
        check("8. three alerts recorded for this period",
              len(m.load_alert_state(state_file)), 3)

        # 9: next billing period.
        accounts["FortCore"] = account(profiles=95, start=m.now_utc().isoformat(),
                                       end=(m.now_utc() + timedelta(days=31)).isoformat())
        sent, _ = run()
        check("9. new billing period -> profiles alerts again",
              [s["alert_types"] for s in sent], [["usage:active_profiles"]])

        # 16: client / API failure still alerts, once.
        accounts["Broken Co"] = m.KlaviyoError("GET billing-usage -> 401 bad key", 401, "auth")
        sent, _ = run()
        failures = [s for s in sent if s["client_name"] == "Broken Co"]
        check("16. invalid API key -> failure POSTed",
              [(s["success"], s["error_type"]) for s in failures], [(False, "auth")])
        check("16. ...and FortCore is not re-sent",
              [s["client_name"] for s in sent], ["Broken Co"])
        sent, _ = run()
        check("16. same failure next day is suppressed", sent, [])

        # 19: raw usage data still in the delivered payload.
        check("19. raw usage fields still delivered",
              (first_alert.get("active_profiles"), first_alert.get("profile_limit"),
               first_alert.get("email_used"), first_alert.get("sms_used"),
               "days_until_billing" in first_alert, "billing_period_start" in first_alert),
              (95.1, 100, 20, 10, True, True))

        # 20: no secrets anywhere.
        everything = json.dumps(first_alert) + logs.getvalue()
        check("20. API key never in a payload or log line", SECRET_KEY in everything, False)
        check("20. webhook token never in a log line", "SECRET-TOKEN" in logs.getvalue(), False)
    finally:
        (m.load_clients, m.process_client, m.get_billing_usage, m.get_sent_email_campaigns,
         m.collect_recipients, m.N8N_WEBHOOK_URL, sys.argv, db_url) = originals
        if db_url is not None:
            os.environ["DATABASE_URL"] = db_url
        m.logger.removeHandler(log_handler)
        n8n.close()


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
        for test in (test_dates, test_usage, test_reminder, test_excluded_metrics,
                     test_excluded_metrics_preserved, test_average,
                     test_dedupe, test_errors, test_security,
                     test_optional_webhook, test_summary_rendering):
            test()
        test_daily_runs(tmpdir)
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
