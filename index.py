"""
Klaviyo billing monitor.

Checks every configured client's Klaviyo account, computes billing-period
usage and campaign recipient stats, decides whether the account needs a
human review, prints a readable summary, and - when a webhook is
configured - posts one clean JSON payload per client to n8n.

API keys never leave this process: they are used only for Klaviyo requests
and are stripped from logs and from the n8n payload.

Verified against the live Klaviyo API (see README.md for the findings that
drove the revision / filter / endpoint choices).
"""

import argparse
import hashlib
import json
import logging
import math
import os
import re
import sys
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import requests


# ============================================================
# ENVIRONMENT (.env is loaded before any config is read)
# ============================================================

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))


def _project_path(name: str) -> str:
    """Resolve a default filename next to this script, not the caller's cwd."""
    return name if os.path.isabs(name) else os.path.join(PROJECT_DIR, name)


def load_dotenv(path: Optional[str] = None) -> List[str]:
    """
    Minimal .env loader - no third-party dependency needed.

    Supports `KEY=value`, `export KEY=value`, quoted values, `#` comments and
    blank lines. Real environment variables always win, so an explicit
    `N8N_WEBHOOK_URL=... python3 index.py` still overrides the file.
    """
    target = path or os.getenv("DOTENV_PATH") or _project_path(".env")
    if not os.path.exists(target):
        return []

    loaded: List[str] = []
    try:
        with open(target, "r") as handle:
            for raw in handle:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("export "):
                    line = line[len("export "):].lstrip()
                if "=" not in line:
                    continue

                key, _, value = line.partition("=")
                key = key.strip()
                if not key:
                    continue

                value = value.strip()
                if value[:1] in ('"', "'"):
                    quote = value[0]
                    closing = value.find(quote, 1)
                    value = value[1:closing] if closing > 0 else value[1:]
                else:
                    # Trim a trailing ` # comment` on unquoted values.
                    value = value.split(" #", 1)[0].strip()

                if key not in os.environ:
                    os.environ[key] = value
                    loaded.append(key)
    except OSError:
        return []

    return loaded


# Must run before the constants below read the environment.
_DOTENV_PATH = os.getenv("DOTENV_PATH") or _project_path(".env")
_DOTENV_LOADED = load_dotenv(_DOTENV_PATH)


# ============================================================
# CONFIG
# ============================================================

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, "").strip() or default)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, "").strip() or default)
    except ValueError:
        return default


N8N_WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL", "").strip()

# "per_client" -> one POST per client (matches the documented payload).
# "batch"      -> a single POST containing every client's payload.
N8N_SEND_MODE = os.getenv("N8N_SEND_MODE", "per_client").strip().lower()

# Where client credentials come from. Either a JSON file:
#   [{"client_name": "Steven Denn", "api_key": "pk_..."}, ...]
# or the same JSON inline in KLAVIYO_CLIENTS.
CLIENTS_FILE = _project_path(
    os.getenv("KLAVIYO_CLIENTS_FILE", "").strip() or "clients.json"
)

# A core metric at or above this percentage raises its one usage alert for the
# billing period. Days remaining in the period never trigger an alert.
USAGE_ALERT_PERCENT = _env_float("USAGE_ALERT_PERCENT", 95.0)

# Clients are processed in parallel; each has its own Klaviyo rate-limit
# bucket, so this is bounded by local resources, not by Klaviyo.
MAX_WORKERS = _env_int("MAX_WORKERS", 6)

# Politeness delay between requests on the same client session.
REQUEST_DELAY = _env_float("REQUEST_DELAY", 0.12)

MAX_RETRIES = _env_int("MAX_RETRIES", 4)
REQUEST_TIMEOUT = _env_float("REQUEST_TIMEOUT", 60.0)

# Campaign pages to walk before giving up (safety valve for huge accounts).
MAX_CAMPAIGN_PAGES = _env_int("MAX_CAMPAIGN_PAGES", 20)

# `send_time` is not a filterable field, so campaigns are fetched by
# `scheduled_at` and filtered locally on `send_time`. Those two can differ by
# a few hours (observed on smart-send campaigns), so widen the server-side
# window and let the local filter do the exact work.
CAMPAIGN_LOOKBACK_BUFFER_DAYS = _env_int("CAMPAIGN_LOOKBACK_BUFFER_DAYS", 2)

# Recipient estimation jobs need the `campaigns:write` scope. They are only
# used as a fallback for campaigns the values report did not cover.
USE_ESTIMATION_FALLBACK = os.getenv(
    "USE_ESTIMATION_FALLBACK", "false"
).strip().lower() in {"1", "true", "yes"}

# --- duplicate alert suppression -----------------------------------------
# A usage alert is identified by client + billing period end + metric, so
# each core metric alerts at most once per billing period no matter how far
# usage climbs afterwards; a new billing period produces a new key. Client
# failures are identified by client + error type.
ALERT_STATE_FILE = _project_path(
    os.getenv("ALERT_STATE_FILE", "").strip() or ".alert_state.json"
)

# Hours before an unchanged client-failure alert may fire again. 0 = never
# repeat. Usage alerts ignore this: they never repeat within a period.
ALERT_COOLDOWN_HOURS = _env_float("ALERT_COOLDOWN_HOURS", 168.0)  # weekly

# Drop remembered alerts older than this so the state file cannot grow forever.
ALERT_STATE_RETENTION_DAYS = _env_int("ALERT_STATE_RETENTION_DAYS", 90)

JOB_POLL_SECONDS = _env_float("JOB_POLL_SECONDS", 1.5)
JOB_MAX_POLLS = _env_int("JOB_MAX_POLLS", 12)

# --- API revisions -------------------------------------------------------
# These are NOT interchangeable and must not be unified:
#   billing-usage           exists only on the pre-release revision
#   campaigns / reporting   exist only on the stable revision
BILLING_REVISION = os.getenv("KLAVIYO_BILLING_REVISION", "2026-07-15.pre")
CAMPAIGN_REVISION = os.getenv("KLAVIYO_CAMPAIGN_REVISION", "2026-07-15")

KLAVIYO_BASE = "https://a.klaviyo.com/api"

# Placeholder webhook hosts we refuse to POST to.
_PLACEHOLDER_PATTERNS = (
    "your-n8n-domain",
    "your_n8n_domain",
    "yourdomain",
    "example.com",
    "changeme",
    "replace-me",
    "replace_me",
)

# billing-usage resource id -> (payload prefix, human label, important?)
# Only ALERTABLE_USAGE_METRICS below can raise a usage alert.
USAGE_METRICS: Dict[str, Tuple[str, str, bool]] = {
    "active_profiles": ("profile", "Profiles", True),
    "email_sends": ("email", "Email", True),
    "sms_credits": ("sms", "SMS", True),
    "ai_credits": ("ai", "AI credits", False),
    "reviews_orders": ("reviews", "Reviews", False),
}

# The only billing-usage ids that can raise a usage alert (>= 95% or over a
# zero allowance). Everything else - AI credits, reviews, Customer Hub, K Social and any
# metric Klaviyo adds later - is still reported but never flags an account.
# Add-on allowances are often tiny (e.g. 250) and routinely read thousands of
# percent "over", which drowned out the real billing warnings.
ALERTABLE_USAGE_METRICS = frozenset({
    "active_profiles",
    "email_sends",
    "sms_credits",
})


# ============================================================
# LOGGING (with API-key redaction)
# ============================================================

_KEY_RE = re.compile(r"\bpk_[A-Za-z0-9_\-]{4,}")


def redact(text: Any) -> str:
    """Strip anything that looks like a Klaviyo private key."""
    return _KEY_RE.sub("pk_***REDACTED***", str(text))


def _redact_database_url(text: str) -> str:
    """Strip the DATABASE_URL password out of any log line."""
    url = os.getenv("DATABASE_URL", "").strip()
    if not url or "@" not in url:
        return text
    try:
        credentials = url.split("://", 1)[1].split("@", 1)[0]
        if ":" in credentials:
            password = credentials.split(":", 1)[1]
            if password:
                text = text.replace(password, "***")
        text = text.replace(url, "<database>")
    except (IndexError, ValueError):
        pass
    return text


class _RedactFilter(logging.Filter):
    """Last line of defence: no credential reaches a log handler."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            record.msg = _redact_database_url(redact(record.getMessage()))
            record.args = ()
        except Exception:
            pass
        return True


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)-7s | %(message)s",
)
logger = logging.getLogger("klaviyo-billing")
for _handler in logging.getLogger().handlers:
    _handler.addFilter(_RedactFilter())

if _DOTENV_LOADED:
    # Names only - values may be secrets.
    logger.debug("Loaded %d setting(s) from %s", len(_DOTENV_LOADED), _DOTENV_PATH)


# ============================================================
# TIME HELPERS
# ============================================================

_ISO_RE = re.compile(
    r"^(?P<base>\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2})"
    r"(?:\.(?P<frac>\d+))?"
    r"(?P<tz>Z|z|[+-]\d{2}:?\d{2})?$"
)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso(value: Optional[str]) -> Optional[datetime]:
    """
    Parse a Klaviyo ISO-8601 timestamp into an aware UTC datetime.

    Written by hand because Python 3.9's `datetime.fromisoformat` rejects
    'Z' suffixes and fractional seconds that are not exactly 3 or 6 digits.
    Naive timestamps are assumed to be UTC, which is what Klaviyo returns.
    """
    if not value:
        return None

    match = _ISO_RE.match(str(value).strip())
    if not match:
        return None

    base = match.group("base").replace(" ", "T")

    frac = match.group("frac")
    fraction = "." + frac[:6].ljust(6, "0") if frac else ""

    tz = match.group("tz") or "+00:00"
    if tz in ("Z", "z"):
        tz = "+00:00"
    elif len(tz) == 5:  # +0000 -> +00:00
        tz = tz[:3] + ":" + tz[3:]

    try:
        return datetime.fromisoformat(base + fraction + tz).astimezone(timezone.utc)
    except ValueError:
        return None


def days_between(later: datetime, earlier: datetime) -> int:
    """
    Whole days remaining, rounded down.

    2.9 days left -> 2 ("ends in 2 days"), and a period that ended 6 hours
    ago -> -1 rather than 0, so an expired period never reads as current.
    """
    return int(math.floor((later - earlier).total_seconds() / 86400.0))


# ============================================================
# ERRORS
# ============================================================

class KlaviyoError(Exception):
    """A Klaviyo request failed. `kind` classifies it for reporting."""

    def __init__(self, message: str, status: Optional[int] = None, kind: str = "api"):
        super().__init__(redact(message))
        self.status = status
        self.kind = kind


def _classify(status: int) -> str:
    if status == 401:
        return "auth"
    if status == 403:
        return "permission"
    if status == 404:
        return "not_found"
    if status == 429:
        return "rate_limit"
    if status >= 500:
        return "server"
    return "api"


# ============================================================
# HTTP LAYER
# ============================================================

def klaviyo_headers(api_key: str, revision: str) -> Dict[str, str]:
    return {
        "Authorization": "Klaviyo-API-Key {}".format(api_key),
        "Accept": "application/vnd.api+json",
        "Content-Type": "application/vnd.api+json",
        "revision": revision,
    }


def _retry_after_seconds(response: requests.Response) -> float:
    header = response.headers.get("Retry-After")
    if header:
        try:
            return max(1.0, float(header))
        except ValueError:
            pass
    # Klaviyo also states the wait in the error body.
    match = re.search(r"available in (\d+) seconds", response.text or "")
    if match:
        return float(match.group(1)) + 1.0
    return 5.0


def _error_detail(response: requests.Response) -> str:
    try:
        errors = response.json().get("errors") or []
        if errors:
            first = errors[0]
            title = (first.get("title") or "").strip()
            detail = (first.get("detail") or "").strip()
            if detail and detail != title:
                return "{} {}".format(title, detail).strip()
            return title or detail
    except Exception:
        pass
    return (response.text or "")[:300]


def request(
    session: requests.Session,
    method: str,
    url: str,
    api_key: str,
    revision: str,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    label: str = "",
) -> Dict[str, Any]:
    """
    Perform one Klaviyo request and return the decoded JSON body.

    Retries 429 (honouring Retry-After) and 5xx/network errors. Auth,
    permission and validation errors are raised immediately - retrying them
    only burns rate limit.
    """
    attempt = 0
    while True:
        attempt += 1
        time.sleep(REQUEST_DELAY)

        try:
            response = session.request(
                method=method,
                url=url,
                headers=klaviyo_headers(api_key, revision),
                params=params,
                json=json_body,
                timeout=REQUEST_TIMEOUT,
            )
        except requests.RequestException as exc:
            if attempt > MAX_RETRIES:
                raise KlaviyoError(
                    "network error on {}: {}".format(label or url, exc), kind="network"
                )
            time.sleep(min(2 ** attempt, 20))
            continue

        if response.status_code < 400:
            if not response.content:
                return {}
            try:
                return response.json()
            except ValueError:
                raise KlaviyoError(
                    "non-JSON response from {}".format(label or url),
                    status=response.status_code,
                )

        kind = _classify(response.status_code)

        if kind in ("rate_limit", "server") and attempt <= MAX_RETRIES:
            wait = (
                _retry_after_seconds(response)
                if kind == "rate_limit"
                else min(2 ** attempt, 20)
            )
            logger.debug(
                "%s %s -> %s, retrying in %.0fs (attempt %d/%d)",
                method, label or url, response.status_code, wait, attempt, MAX_RETRIES,
            )
            time.sleep(wait)
            continue

        raise KlaviyoError(
            "{} {} -> {} {}".format(
                method, label or url, response.status_code, _error_detail(response)
            ),
            status=response.status_code,
            kind=kind,
        )


def paginate(
    session: requests.Session,
    url: str,
    api_key: str,
    revision: str,
    params: Dict[str, Any],
    label: str,
    max_pages: int = MAX_CAMPAIGN_PAGES,
) -> Iterable[List[Dict[str, Any]]]:
    """Yield each page's `data` array, following JSON:API cursor links."""
    next_url: Optional[str] = url
    next_params: Optional[Dict[str, Any]] = params
    pages = 0

    while next_url and pages < max_pages:
        body = request(
            session, "GET", next_url, api_key, revision,
            params=next_params, label=label,
        )
        pages += 1
        yield body.get("data") or []

        # `links.next` already carries the encoded query string.
        next_url = (body.get("links") or {}).get("next")
        next_params = None


# ============================================================
# BILLING USAGE
# ============================================================

def _safe_number(value: Any) -> Optional[float]:
    """Klaviyo returns null for unlimited allowances; keep that distinct from 0."""
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _tidy(value: Optional[float]) -> Optional[Any]:
    """Render whole numbers as ints so counts do not serialise as 100712.0."""
    if value is None:
        return None
    if float(value).is_integer():
        return int(value)
    return round(float(value), 2)


def _usage_keys(prefix: str) -> Tuple[str, str, str]:
    """Payload field names for a usage metric."""
    used = "active_profiles" if prefix == "profile" else "{}_used".format(prefix)
    return used, "{}_limit".format(prefix), "{}_usage_percent".format(prefix)


def get_billing_usage(session: requests.Session, api_key: str) -> Dict[str, Any]:
    return request(
        session, "GET", KLAVIYO_BASE + "/billing-usage",
        api_key, BILLING_REVISION, label="billing-usage",
    )


def parse_billing_usage(billing_response: Dict[str, Any]) -> Dict[str, Any]:
    """
    Turn the billing-usage collection into per-metric numbers plus the
    billing period. Every item carries the same period; the first item that
    supplies both bounds wins.
    """
    metrics: Dict[str, Dict[str, Any]] = {}
    period_start: Optional[str] = None
    period_end: Optional[str] = None
    as_of: Optional[str] = None

    for item in billing_response.get("data") or []:
        metric_id = item.get("id")
        attributes = item.get("attributes") or {}
        if not metric_id:
            continue

        used = _safe_number(attributes.get("usage_used")) or 0.0
        limit = _safe_number(attributes.get("usage_max"))

        if limit is not None and limit > 0:
            percent = round((used / limit) * 100.0, 2)
            over_limit = used > limit
            # Compare unrounded, so 94.996% does not round its way to an alert.
            high_usage = (used / limit) * 100.0 >= USAGE_ALERT_PERCENT
        elif limit == 0:
            # A published allowance of zero: any usage at all is an overage.
            percent = None
            over_limit = used > 0
            high_usage = over_limit
        else:
            # usage_max is null -> unlimited or not applicable.
            percent = None
            over_limit = False
            high_usage = False

        _, label, important = USAGE_METRICS.get(
            metric_id, (metric_id, metric_id.replace("_", " ").title(), False)
        )

        metrics[metric_id] = {
            "label": label,
            "important": important,
            "used": round(used, 4),
            "limit": limit,
            "unit": attributes.get("unit"),
            "percent": percent,
            "over_limit": over_limit,
            "high_usage": high_usage,
        }

        if period_start is None:
            period_start = attributes.get("period_start")
        if period_end is None:
            period_end = attributes.get("period_end")
        if as_of is None:
            as_of = attributes.get("as_of")

    return {
        "metrics": metrics,
        "period_start": period_start,
        "period_end": period_end,
        "as_of": as_of,
    }


# ============================================================
# CAMPAIGNS
# ============================================================

def _campaign_sent_at(attributes: Dict[str, Any]) -> Optional[datetime]:
    """
    The moment a campaign actually went out.

    `send_time` is authoritative but can be absent; `scheduled_at` is the
    fallback. (On smart-send campaigns the two legitimately differ by hours,
    which is why the server-side window is padded.)
    """
    return parse_iso(attributes.get("send_time")) or parse_iso(
        attributes.get("scheduled_at")
    )


def get_sent_email_campaigns(
    session: requests.Session,
    api_key: str,
    period_start: datetime,
    period_end: datetime,
) -> List[Dict[str, Any]]:
    """
    Fetch email campaigns sent inside the billing period.

    A channel filter is mandatory on this endpoint, and `messages.channel`
    IS filterable on the stable revision. Status and scheduled_at are
    filtered server-side to keep the number of pages small; the exact
    send-time window is applied locally.
    """
    cutoff = period_start - timedelta(days=CAMPAIGN_LOOKBACK_BUFFER_DAYS)
    cutoff_iso = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")

    campaigns: Dict[str, Dict[str, Any]] = {}

    # Archived campaigns are excluded by default but still consumed billing,
    # so both buckets are queried.
    for archived in ("false", "true"):
        campaign_filter = (
            "equals(messages.channel,'email'),"
            "equals(status,'Sent'),"
            "equals(archived,{}),"
            "greater-or-equal(scheduled_at,{})".format(archived, cutoff_iso)
        )
        params = {
            "filter": campaign_filter,
            "page[size]": 100,
            "sort": "-scheduled_at",
        }

        for page in paginate(
            session, KLAVIYO_BASE + "/campaigns", api_key, CAMPAIGN_REVISION,
            params, label="campaigns(archived={})".format(archived),
        ):
            oldest: Optional[datetime] = None

            for campaign in page:
                attributes = campaign.get("attributes") or {}
                campaign_id = campaign.get("id")
                if not campaign_id:
                    continue

                # Defensive: never trust the server filter alone.
                if attributes.get("status") != "Sent":
                    continue

                sent_at = _campaign_sent_at(attributes)
                if sent_at is None:
                    continue

                scheduled_at = parse_iso(attributes.get("scheduled_at")) or sent_at
                if oldest is None or scheduled_at < oldest:
                    oldest = scheduled_at

                if not (period_start <= sent_at < period_end):
                    continue

                campaigns[campaign_id] = {
                    "campaign_id": campaign_id,
                    "campaign_name": attributes.get("name") or "Unnamed campaign",
                    "sent_at": sent_at.isoformat(),
                    "archived": bool(attributes.get("archived")),
                }

            # Results are newest-first: once a page ends before the padded
            # window there is nothing older worth fetching.
            if oldest is not None and oldest < cutoff:
                break

    return sorted(campaigns.values(), key=lambda item: item["sent_at"], reverse=True)


# ============================================================
# RECIPIENTS
# ============================================================

def get_conversion_metric_id(session: requests.Session, api_key: str) -> Optional[str]:
    """
    The campaign values report requires a conversion metric id even when we
    only ask for `recipients`. Prefer "Placed Order"; otherwise any metric
    will do, since the conversion columns are not requested.
    """
    body = request(
        session, "GET", KLAVIYO_BASE + "/metrics",
        api_key, CAMPAIGN_REVISION, label="metrics",
    )
    metrics = body.get("data") or []

    for metric in metrics:
        if ((metric.get("attributes") or {}).get("name") or "") == "Placed Order":
            return metric.get("id")

    return metrics[0].get("id") if metrics else None


def get_campaign_recipients(
    session: requests.Session,
    api_key: str,
    period_start: datetime,
    period_end: datetime,
) -> Dict[str, int]:
    """
    Actual recipients per email campaign for the billing period.

    Uses the campaign values report: one request covering every campaign,
    real numbers rather than estimates, and it works with a read-only key
    (recipient estimation jobs require `campaigns:write`).
    """
    conversion_metric_id = get_conversion_metric_id(session, api_key)
    if not conversion_metric_id:
        raise KlaviyoError("no metrics available for conversion_metric_id", kind="api")

    # The report cannot look into the future; cap at "now".
    end = min(period_end, now_utc())
    if end <= period_start:
        return {}

    body = {
        "data": {
            "type": "campaign-values-report",
            "attributes": {
                "timeframe": {
                    "start": period_start.isoformat(),
                    "end": end.isoformat(),
                },
                "statistics": ["recipients"],
                "conversion_metric_id": conversion_metric_id,
            },
        }
    }

    response = request(
        session, "POST", KLAVIYO_BASE + "/campaign-values-reports",
        api_key, CAMPAIGN_REVISION, json_body=body, label="campaign-values-report",
    )

    results = ((response.get("data") or {}).get("attributes") or {}).get("results") or []

    recipients: Dict[str, int] = {}
    for row in results:
        groupings = row.get("groupings") or {}
        if groupings.get("send_channel") != "email":
            continue

        campaign_id = groupings.get("campaign_id")
        if not campaign_id:
            continue

        count = _safe_number((row.get("statistics") or {}).get("recipients"))
        if count is None:
            continue

        # A/B campaigns report one row per message; sum them per campaign.
        recipients[campaign_id] = recipients.get(campaign_id, 0) + int(round(count))

    return recipients


def estimate_recipients(
    session: requests.Session, api_key: str, campaign_id: str
) -> Optional[int]:
    """
    Fallback for campaigns the values report did not cover.

    Requires the `campaigns:write` scope; callers disable it for the whole
    client on the first permission error.
    """
    created = request(
        session, "POST", KLAVIYO_BASE + "/campaign-recipient-estimation-jobs",
        api_key, CAMPAIGN_REVISION,
        json_body={
            "data": {"type": "campaign-recipient-estimation-job", "id": campaign_id}
        },
        label="recipient-estimation-job",
    )

    job_id = (created.get("data") or {}).get("id") or campaign_id

    for _ in range(JOB_MAX_POLLS):
        job = request(
            session, "GET",
            "{}/campaign-recipient-estimation-jobs/{}".format(KLAVIYO_BASE, job_id),
            api_key, CAMPAIGN_REVISION, label="recipient-estimation-job",
        )
        attributes = (job.get("data") or {}).get("attributes") or {}
        status = (attributes.get("status") or "").lower()

        if status in ("complete", "completed"):
            count = _safe_number(attributes.get("estimated_recipient_count"))
            return int(count) if count is not None else None

        if status in ("cancelled", "canceled", "failed", "error"):
            return None

        time.sleep(JOB_POLL_SECONDS)

    return None


def collect_recipients(
    session: requests.Session,
    api_key: str,
    campaigns: List[Dict[str, Any]],
    period_start: datetime,
    period_end: datetime,
) -> Tuple[Dict[str, Optional[int]], List[str]]:
    """Map campaign_id -> recipients (None when unknown), plus any warnings."""
    warnings: List[str] = []
    recipients: Dict[str, Optional[int]] = {c["campaign_id"]: None for c in campaigns}

    if not campaigns:
        return recipients, warnings

    try:
        reported = get_campaign_recipients(session, api_key, period_start, period_end)
        for campaign_id in recipients:
            if campaign_id in reported:
                recipients[campaign_id] = reported[campaign_id]
    except KlaviyoError as exc:
        warnings.append("recipient report unavailable ({})".format(exc))

    if not USE_ESTIMATION_FALLBACK:
        return recipients, warnings

    for campaign_id, value in list(recipients.items()):
        if value is not None:
            continue
        try:
            recipients[campaign_id] = estimate_recipients(session, api_key, campaign_id)
        except KlaviyoError as exc:
            if exc.kind in ("permission", "auth"):
                warnings.append("recipient estimation unavailable ({})".format(exc))
                break  # Scope is missing for the whole account; stop asking.
            warnings.append(
                "estimation failed for {} ({})".format(campaign_id, exc)
            )

    return recipients, warnings


# ============================================================
# REMINDER LOGIC
# ============================================================

def build_reminder(
    metrics: Dict[str, Dict[str, Any]]
) -> Tuple[bool, List[str], List[str]]:
    """
    Flag every metric in ALERTABLE_USAGE_METRICS at or above
    USAGE_ALERT_PERCENT. How many days are left in the billing period plays
    no part: only usage alerts.

    Returns (should_remind, human reasons, machine-readable alert types).
    There is one alert type per metric - `usage:<metric_id>` - whatever the
    percentage, so 95%, 99% and 105% are the same condition and duplicate
    suppression lets each metric alert only once per billing period.
    """
    reasons: List[str] = []
    alert_types: List[str] = []

    # Stable order so Discord messages do not shuffle between runs.
    for metric_id in sorted(
        metrics, key=lambda key: list(USAGE_METRICS).index(key)
        if key in USAGE_METRICS else len(USAGE_METRICS)
    ):
        if metric_id not in ALERTABLE_USAGE_METRICS:
            continue
        metric = metrics[metric_id]
        if not metric["high_usage"]:
            continue
        alert_types.append("usage:{}".format(metric_id))
        if metric["percent"] is None:
            reasons.append("{} usage is over its zero allowance".format(metric["label"]))
        else:
            reasons.append(
                "{} usage is at {}%".format(metric["label"], metric["percent"])
            )

    return bool(alert_types), reasons, alert_types


# ============================================================
# PROCESS ONE CLIENT
# ============================================================

def build_payload(client_name: str) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "client_name": client_name,
        "success": False,
        "error": None,
        "error_type": None,
        "checked_at": now_utc().isoformat(),
        "billing_period_start": None,
        "billing_period_end": None,
        "usage_as_of": None,
        "days_until_billing": None,
    }

    # Insertion order is the payload order n8n/Discord will see.
    for prefix, _, _ in USAGE_METRICS.values():
        for key in _usage_keys(prefix):
            payload[key] = None

    payload.update({
        "sent_campaign_count": None,
        "campaigns_with_recipient_data": None,
        "total_estimated_recipients": None,
        "average_recipients_per_email": None,
        # should_remind / reasons / alert_types describe only alerts that are
        # new this billing period; usage_alerts is every core metric at or
        # above the threshold, already alerted or not.
        "should_remind": False,
        "reasons": [],
        "alert_types": [],
        "usage_alerts": [],
        "alert_key": None,
        "alert_keys": [],
        "already_alerted": [],
        "suppressed_duplicate": False,
        "previously_alerted_at": None,
        "warnings": [],
    })
    return payload


def process_client(record: "ClientRecord") -> Dict[str, Any]:
    """
    Check one client. Never raises: every failure is reported in the payload
    so one bad account (or one undecryptable credential) cannot stop the run.

    The plaintext API key is produced here, used for this client's requests,
    and dropped before returning. It is never placed in the payload.
    """
    client_name = record.client_name
    logger.info("[%s] checking", client_name)

    payload = build_payload(client_name)
    payload["client_id"] = record.client_id

    api_key = None
    session = requests.Session()
    try:
        try:
            api_key = record.reveal()
        except Exception as exc:
            # A broken credential is this client's problem, not the run's.
            payload["error"] = redact(exc)
            payload["error_type"] = "credential"
            logger.error("[%s] credential unavailable: %s", client_name, redact(exc))
            return payload

        # --- 1. Billing usage and period ---------------------------------
        billing = parse_billing_usage(get_billing_usage(session, api_key))
        metrics = billing["metrics"]

        period_start = parse_iso(billing["period_start"])
        period_end = parse_iso(billing["period_end"])
        if period_start is None or period_end is None:
            raise KlaviyoError("Klaviyo did not return a billing period", kind="data")

        days_until_billing = days_between(period_end, now_utc())

        payload.update({
            "billing_period_start": period_start.isoformat(),
            "billing_period_end": period_end.isoformat(),
            "usage_as_of": billing["as_of"],
            "days_until_billing": days_until_billing,
        })

        # --- 2. Flatten metrics into the documented payload shape --------
        extra_usage: Dict[str, Any] = {}
        for metric_id, metric in metrics.items():
            mapped = USAGE_METRICS.get(metric_id)
            if not mapped:
                extra_usage[metric_id] = {
                    "used": _tidy(metric["used"]),
                    "limit": _tidy(metric["limit"]),
                    "usage_percent": metric["percent"],
                    "unit": metric["unit"],
                }
                continue

            used_key, limit_key, percent_key = _usage_keys(mapped[0])
            payload[used_key] = _tidy(metric["used"])
            payload[limit_key] = _tidy(metric["limit"])
            payload[percent_key] = metric["percent"]

        if extra_usage:
            payload["other_usage"] = extra_usage

        # --- 3. Sent email campaigns in the period -----------------------
        campaigns = get_sent_email_campaigns(session, api_key, period_start, period_end)

        # --- 4. Recipients ------------------------------------------------
        recipients, warnings = collect_recipients(
            session, api_key, campaigns, period_start, period_end
        )

        known = [value for value in recipients.values() if value is not None]
        total_recipients = sum(known)
        average_recipients = round(total_recipients / len(known)) if known else None

        payload.update({
            "sent_campaign_count": len(campaigns),
            "campaigns_with_recipient_data": len(known),
            "total_estimated_recipients": total_recipients if known else None,
            "average_recipients_per_email": average_recipients,
        })

        # --- 5. Reminder decision -----------------------------------------
        # Candidates only: mark_duplicates() later narrows these to the
        # metrics that have not yet alerted this billing period.
        should_remind, reasons, alert_types = build_reminder(metrics)

        payload.update({
            "success": True,
            "should_remind": should_remind,
            "reasons": reasons,
            "alert_types": alert_types,
            "usage_alerts": [
                {"alert_type": alert_type, "reason": reason}
                for alert_type, reason in zip(alert_types, reasons)
            ],
            "warnings": warnings,
        })

        logger.info(
            "[%s] ok | %s days left | %d campaigns | avg recipients %s | remind=%s",
            client_name, days_until_billing, len(campaigns),
            average_recipients, should_remind,
        )
        return payload

    except KlaviyoError as exc:
        payload["error"] = str(exc)
        payload["error_type"] = exc.kind
        if exc.kind == "auth":
            logger.error("[%s] INVALID API KEY - skipping this client", client_name)
        else:
            logger.error("[%s] failed (%s): %s", client_name, exc.kind, exc)
        return payload

    except Exception as exc:  # never let one client kill the run
        payload["error"] = redact(exc)
        payload["error_type"] = "unexpected"
        logger.exception("[%s] unexpected failure", client_name)
        return payload

    finally:
        session.close()
        # Drop the plaintext credential as soon as this client is done.
        api_key = None
        del api_key


# ============================================================
# CLIENT CONFIG
# ============================================================

class ClientRecord:
    """
    One monitored client.

    Holds either a plaintext key (local JSON config) or ciphertext + nonce
    (Postgres). `reveal()` produces the plaintext only at the moment a worker
    needs it, so decrypted credentials never sit in the parent thread's
    client list.

    __repr__/__str__ deliberately omit the credential: these objects appear in
    tracebacks and log lines.
    """

    __slots__ = ("client_id", "client_name", "_plaintext", "_ciphertext", "_nonce")

    def __init__(
        self,
        client_name: str,
        client_id: Optional[int] = None,
        plaintext: Optional[str] = None,
        ciphertext: Optional[bytes] = None,
        nonce: Optional[bytes] = None,
    ):
        self.client_name = client_name
        self.client_id = client_id
        self._plaintext = plaintext
        self._ciphertext = ciphertext
        self._nonce = nonce

    def reveal(self) -> str:
        """The plaintext API key. Callers must not store or log the result."""
        if self._plaintext is not None:
            return self._plaintext

        if self._ciphertext is None or self._nonce is None:
            raise RuntimeError(
                "No credential stored for {}".format(self.client_name)
            )

        import crypto_store  # local import: only needed in database mode
        return crypto_store.decrypt_api_key(
            self._ciphertext, self._nonce, _master_key()
        )

    def __repr__(self) -> str:
        return "<ClientRecord {!r} id={}>".format(self.client_name, self.client_id)

    __str__ = __repr__


_MASTER_KEY_CACHE: List[bytes] = []


def _master_key() -> bytes:
    """Validated master key, loaded once per process."""
    if not _MASTER_KEY_CACHE:
        import crypto_store
        _MASTER_KEY_CACHE.append(crypto_store.load_master_key())
    return _MASTER_KEY_CACHE[0]


def _clients_from_database() -> List[ClientRecord]:
    """
    Active clients from Postgres, credentials still encrypted.

    A database that is configured but unreachable is fatal: reporting "zero
    clients" would look like a clean run while monitoring nothing.
    """
    import crypto_store
    import db as database

    try:
        crypto_store.load_master_key()
    except crypto_store.MasterKeyError as exc:
        raise SystemExit("ERROR: {}".format(exc))

    try:
        rows = database.fetch_clients(active_only=True)
    except database.DatabaseUnavailable as exc:
        raise SystemExit("ERROR: {}".format(exc))

    records = [
        ClientRecord(
            client_name=row["client_name"],
            client_id=row["id"],
            ciphertext=row["api_key_ciphertext"],
            nonce=row["api_key_nonce"],
        )
        for row in rows
    ]

    if not records:
        raise SystemExit(
            "No active clients in the database. Add one with:\n"
            "  python3 manage_clients.py add"
        )

    logger.info(
        "Loaded %d active client(s) from Postgres (%s)",
        len(records), database.safe_target(),
    )
    return records


def load_clients() -> List[ClientRecord]:
    """
    Resolve the client list from exactly one source, in priority order:

      1. DATABASE_URL  -> encrypted credentials in Postgres (production)
      2. KLAVIYO_CLIENTS -> inline JSON
      3. clients.json  -> local file (development / pre-migration)

    Sources are never merged, so a half-finished migration cannot silently
    monitor a client twice or use a stale key.
    """
    import db as database

    if database.is_configured():
        return _clients_from_database()

    return _clients_from_json()


def _clients_from_json() -> List[ClientRecord]:
    """Load clients from KLAVIYO_CLIENTS (inline JSON) or a JSON file."""
    raw = os.getenv("KLAVIYO_CLIENTS", "").strip()
    source = "KLAVIYO_CLIENTS"

    if not raw:
        if not os.path.exists(CLIENTS_FILE):
            raise SystemExit(
                "No client config found. Create {} (see clients.example.json) "
                "or set KLAVIYO_CLIENTS.".format(CLIENTS_FILE)
            )
        with open(CLIENTS_FILE, "r") as handle:
            raw = handle.read()
        source = CLIENTS_FILE

    try:
        parsed = json.loads(raw)
    except ValueError as exc:
        raise SystemExit("{} is not valid JSON: {}".format(source, exc))

    if isinstance(parsed, dict):
        parsed = parsed.get("clients", [])
    if not isinstance(parsed, list):
        raise SystemExit("{} must contain a JSON array of clients.".format(source))

    clients: List[ClientRecord] = []
    seen: Set[str] = set()

    for index, entry in enumerate(parsed):
        if not isinstance(entry, dict):
            logger.warning("Skipping client #%d: not an object", index)
            continue

        name = str(entry.get("client_name") or "").strip()
        key = str(entry.get("api_key") or "").strip()

        if not name:
            logger.warning("Skipping client #%d: missing client_name", index)
            continue
        if not key or "REPLACE" in key.upper() or not key.startswith("pk_"):
            logger.warning("Skipping %s: missing or placeholder api_key", name)
            continue
        if name in seen:
            logger.warning("Skipping duplicate client name %s", name)
            continue

        seen.add(name)
        clients.append(ClientRecord(client_name=name, plaintext=key))

    if not clients:
        raise SystemExit("No usable clients in {}.".format(source))

    logger.info("Loaded %d client(s) from %s", len(clients), source)
    return clients


# ============================================================
# DUPLICATE ALERT SUPPRESSION
# ============================================================

def _alert_key(client_name: str, scope: str, alert_type: str) -> str:
    raw = "{}|{}|{}".format(client_name, scope, alert_type)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def _usage_candidates(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Every core usage condition on this payload, already alerted or not."""
    if "usage_alerts" in payload:
        return list(payload.get("usage_alerts") or [])
    # Payloads built before usage_alerts existed carry the types alone.
    return [{"alert_type": t, "reason": t} for t in payload.get("alert_types") or []]


def alert_identity(payload: Dict[str, Any]) -> List[Tuple[str, str]]:
    """
    (alert_type, alert_key) for every alert this payload could raise.

    A usage alert's key is client + billing period end + alert type, and the
    alert type names only the metric (`usage:active_profiles`), never the
    percentage. So 95%, 99% and 105% share one key and alert once, while a
    new billing period has a new key and may alert again. Each metric has
    its own key, so Profiles alerting does not suppress SMS.

    A client failure's key is client + error type.

    Successful-and-quiet accounts have no alert identity at all - they are
    never notified, so they must never occupy a dedupe slot.
    """
    client_name = payload.get("client_name", "")

    if not payload.get("success"):
        alert_type = "client_failed:{}".format(payload.get("error_type") or "unknown")
        return [(alert_type, _alert_key(client_name, "failure", alert_type))]

    # The period-end date, so timestamp formatting jitter between daily runs
    # can never look like a new billing period.
    period_end = parse_iso(payload.get("billing_period_end"))
    scope = (
        period_end.date().isoformat() if period_end
        else payload.get("billing_period_end") or "unknown-period"
    )
    return [
        (alert["alert_type"], _alert_key(client_name, scope, alert["alert_type"]))
        for alert in _usage_candidates(payload)
    ]


def load_alert_state(path: str) -> Dict[str, str]:
    if not path or not os.path.exists(path):
        return {}
    try:
        with open(path, "r") as handle:
            data = json.load(handle)
        return data if isinstance(data, dict) else {}
    except (ValueError, OSError) as exc:
        logger.warning("Could not read alert state %s (%s); starting fresh", path, exc)
        return {}


def save_alert_state(path: str, state: Dict[str, str]) -> None:
    if not path:
        return

    cutoff = now_utc() - timedelta(days=ALERT_STATE_RETENTION_DAYS)
    pruned = {}
    for key, stamp in state.items():
        parsed = parse_iso(stamp)
        if parsed is None or parsed >= cutoff:
            pruned[key] = stamp

    try:
        # On a host like Railway the state belongs on a mounted volume, so the
        # directory may not exist on a fresh container.
        parent = os.path.dirname(os.path.abspath(path))
        if parent and not os.path.isdir(parent):
            os.makedirs(parent, exist_ok=True)

        temp = path + ".tmp"
        with open(temp, "w") as handle:
            json.dump(pruned, handle, indent=2, sort_keys=True)
        os.replace(temp, path)  # atomic; a crash cannot truncate the state
    except OSError as exc:
        logger.warning("Could not write alert state %s (%s)", path, exc)


def assign_alert_identity(results: List[Dict[str, Any]]) -> List[str]:
    """Return every alert key on these payloads that needs a duplicate check."""
    keys: List[str] = []
    for payload in results:
        keys.extend(key for _, key in alert_identity(payload))
    return keys


def mark_duplicates(
    results: List[Dict[str, Any]], state: Dict[str, str], force: bool
) -> None:
    """
    Decide which alerts are new and narrow each payload to them.

    `state` maps alert_key -> ISO timestamp of the last delivery, whether it
    came from Postgres or the local file.

    Usage alerts: a metric already alerted this billing period is dropped
    from `reasons` / `alert_types` / `alert_keys` and listed in
    `already_alerted` instead; `should_remind` is true only when at least one
    metric is new. There is no cooldown - the billing period is the window.

    Client failures keep their cooldown (ALERT_COOLDOWN_HOURS).

    Safe to call repeatedly: the candidates in `usage_alerts` are never
    modified, only the fields derived from them.
    """
    now = now_utc()

    for payload in results:
        identities = alert_identity(payload)

        if not payload.get("success"):
            alert_type, key = identities[0]
            previous = parse_iso(state.get(key))
            if force or previous is None:
                suppressed = False
            elif ALERT_COOLDOWN_HOURS <= 0:
                suppressed = True
            else:
                elapsed_hours = (now - previous).total_seconds() / 3600.0
                suppressed = elapsed_hours < ALERT_COOLDOWN_HOURS
            payload.update({
                "alert_types": [alert_type],
                "alert_keys": [key],
                "alert_key": key,
                "already_alerted": [],
                "suppressed_duplicate": suppressed,
                "previously_alerted_at": previous.isoformat() if previous else None,
            })
            continue

        new: List[Dict[str, Any]] = []
        seen: List[Dict[str, Any]] = []
        for alert, (_, key) in zip(_usage_candidates(payload), identities):
            previous = parse_iso(state.get(key))
            entry = dict(alert, alert_key=key,
                         alerted_at=previous.isoformat() if previous else None)
            (seen if previous is not None and not force else new).append(entry)

        keys = [alert["alert_key"] for alert in new]
        payload.update({
            "should_remind": bool(new),
            "reasons": [alert["reason"] for alert in new],
            "alert_types": [alert["alert_type"] for alert in new],
            "alert_keys": keys,
            "alert_key": ",".join(keys) or None,
            "already_alerted": [
                {"alert_type": a["alert_type"], "alerted_at": a["alerted_at"]}
                for a in seen
            ],
            # Every usage condition on this client has already been alerted.
            "suppressed_duplicate": bool(seen) and not new,
            "previously_alerted_at": max(
                (a["alerted_at"] for a in seen), default=None
            ),
        })


def record_alert(state: Dict[str, str], payload: Dict[str, Any]) -> None:
    """Remember an alert only once it has actually reached n8n."""
    if payload.get("suppressed_duplicate"):
        return
    stamp = now_utc().isoformat()
    for key in payload.get("alert_keys") or []:
        state[key] = stamp


class FileAlertStore:
    """Alert history in a local JSON file (development, or no database)."""

    backend = "file"

    def __init__(self, path: str):
        self.path = path

    def describe(self) -> str:
        return self.path

    def load(self, alert_keys: List[str]) -> Dict[str, str]:
        return load_alert_state(self.path)

    def persist(self, results: List[Dict[str, Any]], state: Dict[str, str]) -> None:
        save_alert_state(self.path, state)


class PostgresAlertStore:
    """
    Alert history in Postgres.

    Removes the need for a Railway volume, and makes suppression correct when
    more than one run could overlap: writes UPSERT on alert_key.
    """

    backend = "postgres"

    def __init__(self):
        import db as database
        self._db = database

    def describe(self) -> str:
        return "postgres ({})".format(self._db.safe_target())

    def load(self, alert_keys: List[str]) -> Dict[str, str]:
        try:
            return self._db.fetch_alert_times(alert_keys)
        except self._db.DatabaseUnavailable as exc:
            # Fail closed: without history we cannot tell new from repeat, and
            # re-alerting 100 clients is worse than skipping this run's sends.
            raise SystemExit("ERROR: {}".format(exc))

    def persist(self, results: List[Dict[str, Any]], state: Dict[str, str]) -> None:
        entries = []
        for payload in results:
            # One row per alert: each usage metric is remembered separately.
            for alert_type, key in alert_identity(payload):
                if key not in state:
                    continue
                entries.append({
                    "alert_key": key,
                    "client_id": payload.get("client_id"),
                    "client_name": payload.get("client_name") or "",
                    "billing_period_end": parse_iso(payload.get("billing_period_end")),
                    "alert_types": [alert_type],
                })

        try:
            self._db.record_alerts(entries)
            if ALERT_STATE_RETENTION_DAYS > 0:
                self._db.prune_alert_history(ALERT_STATE_RETENTION_DAYS)
        except self._db.DatabaseUnavailable as exc:
            logger.error(
                "Could not record alert history (%s) - these alerts may repeat", exc
            )


def build_alert_store(state_file: str, disabled: bool = False):
    """
    Pick the alert-history backend.

    Postgres when DATABASE_URL is set, otherwise the local file, and None when
    suppression is switched off entirely.
    """
    if disabled:
        return None

    import db as database
    if database.is_configured():
        return PostgresAlertStore()
    return FileAlertStore(state_file) if state_file else None


# ============================================================
# N8N
# ============================================================

def redact_url(url: str) -> str:
    """
    Host only. An n8n webhook path contains a token, so it is a secret and
    must never reach a log line, the terminal, or an error message.
    """
    if not url:
        return "<not configured>"
    try:
        parts = urllib.parse.urlsplit(url)
        if not parts.netloc:
            return "<webhook>"
        return "{}://{}/...".format(parts.scheme, parts.netloc)
    except ValueError:
        return "<webhook>"


def _scrub_url(text: Any, url: str) -> str:
    """Strip a webhook URL (and its secret path) out of an error message."""
    out = str(text)
    if not url:
        return out
    try:
        parts = urllib.parse.urlsplit(url)
        if parts.path and len(parts.path) > 1:
            out = out.replace(parts.path, "/...")
    except ValueError:
        pass
    return out.replace(url, redact_url(url))


def _short_error(exc: Exception, url: str) -> str:
    """
    A one-line, URL-free reason. requests' own messages embed the full URL
    (secret path included) and run to several hundred characters.
    """
    if isinstance(exc, requests.exceptions.ConnectTimeout):
        return "connection timed out"
    if isinstance(exc, requests.exceptions.ReadTimeout):
        return "read timed out"
    if isinstance(exc, requests.exceptions.Timeout):
        return "timed out"
    if isinstance(exc, requests.exceptions.SSLError):
        return "TLS error"
    if isinstance(exc, requests.exceptions.ConnectionError):
        return "could not connect to {}".format(redact_url(url))
    if isinstance(exc, requests.exceptions.TooManyRedirects):
        return "too many redirects"
    return "{}: {}".format(type(exc).__name__, _scrub_url(exc, url))[:120]


def resolve_webhook(url: str) -> Tuple[Optional[str], str, str]:
    """
    Decide whether n8n delivery is possible.

    n8n is an optional delivery layer: a missing or unusable webhook is a
    reason to skip delivery, never a reason to fail the Klaviyo run. Returns
    (usable url or None, status, human message).
    """
    url = (url or "").strip()

    if not url:
        return None, "not_configured", (
            "n8n webhook not configured - webhook delivery skipped."
        )

    if not url.startswith(("http://", "https://")):
        return None, "invalid", (
            "N8N_WEBHOOK_URL must start with http:// or https:// "
            "- webhook delivery skipped."
        )

    lowered = url.lower()
    for pattern in _PLACEHOLDER_PATTERNS:
        if pattern in lowered:
            return None, "placeholder", (
                "N8N_WEBHOOK_URL still looks like a placeholder ({}) "
                "- webhook delivery skipped.".format(redact_url(url))
            )

    return url, "configured", "n8n configured ({})".format(redact_url(url))


def post_json(url: str, payload: Dict[str, Any]) -> Tuple[bool, str]:
    """POST one payload. Returns (delivered, sanitised detail)."""
    detail = "unknown error"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
            if response.status_code < 400:
                return True, "HTTP {}".format(response.status_code)

            if response.status_code < 500:
                # A 4xx is the webhook rejecting us; retrying will not help.
                detail = "HTTP {} {}".format(
                    response.status_code,
                    _scrub_url((response.text or "").strip()[:200], url),
                )
                logger.error(
                    "n8n rejected payload for %s: %s",
                    payload.get("client_name"), detail,
                )
                return False, detail

            detail = "HTTP {}".format(response.status_code)
        except requests.RequestException as exc:
            detail = _short_error(exc, url)
            logger.warning(
                "n8n POST error for %s (attempt %d/%d): %s",
                payload.get("client_name"), attempt, MAX_RETRIES, detail,
            )

        if attempt < MAX_RETRIES:
            time.sleep(min(2 ** attempt, 15))

    logger.error("n8n POST failed for %s: %s", payload.get("client_name"), detail)
    return False, detail


def _for_wire(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Strip local-only annotations before sending.

    `n8n_delivery` / `n8n_detail` describe this run's delivery outcome and
    exist purely for the terminal summary; they must never be transmitted.
    """
    internal = {"client_id"}   # database identity is ours, not n8n's
    return {
        k: v for k, v in payload.items()
        if not k.startswith("n8n_") and k not in internal
    }


def send_to_n8n(
    url: str, results: List[Dict[str, Any]], state: Dict[str, str]
) -> bool:
    """
    Deliver payloads and remember which alerts actually landed.

    State is only updated for payloads that were delivered, so a webhook
    outage means the alert is retried on the next run rather than lost.
    Each payload is annotated with its delivery outcome for the terminal
    summary; that annotation happens after the POST, so it is never sent.
    """
    if N8N_SEND_MODE == "batch":
        ok, detail = post_json(url, {
            "generated_at": now_utc().isoformat(),
            "client_count": len(results),
            "clients": [_for_wire(result) for result in results],
        })
        for result in results:
            result["n8n_delivery"] = "sent" if ok else "failed"
            result["n8n_detail"] = detail
            if ok:
                record_alert(state, result)
        logger.info(
            "Batch payload sent to n8n: %s", "ok" if ok else "FAILED (" + detail + ")"
        )
        return ok

    sent = 0
    for result in results:
        ok, detail = post_json(url, _for_wire(result))
        result["n8n_delivery"] = "sent" if ok else "failed"
        result["n8n_detail"] = detail
        if ok:
            sent += 1
            record_alert(state, result)

    logger.info("Sent %d/%d client payloads to n8n", sent, len(results))
    return sent == len(results)


# ============================================================
# TERMINAL SUMMARY
# ============================================================

_LINE = "=" * 62


def _fmt_number(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float) and not float(value).is_integer():
        return "{:,.2f}".format(value)
    return "{:,}".format(int(value))


def _fmt_usage(used: Any, limit: Any, percent: Any) -> str:
    if used is None and limit is None:
        return "-"
    if limit is None:
        return "{} (no limit)".format(_fmt_number(used))
    if percent is None:
        return "{} / {}".format(_fmt_number(used), _fmt_number(limit))
    return "{} / {} ({:.2f}%)".format(
        _fmt_number(used), _fmt_number(limit), percent
    )


def _fmt_date(value: Optional[str]) -> str:
    parsed = parse_iso(value)
    return parsed.strftime("%d %b %Y") if parsed else "-"


def _row(label: str, value: str) -> str:
    return "  {:<26}{}".format(label, value)


def _delivery_line(payload: Dict[str, Any], webhook_status: str) -> str:
    delivery = payload.get("n8n_delivery")

    if delivery == "sent":
        return "Configured - payload sent"
    if delivery == "failed":
        return "Configured - DELIVERY FAILED ({})".format(
            str(payload.get("n8n_detail") or "unknown")[:90]
        )
    if delivery == "dry_run":
        return "Dry run - nothing sent"
    if delivery == "not_sent":
        return "Configured - not sent (nothing to deliver for this client)"

    return {
        "not_configured": "Not configured - webhook skipped",
        "placeholder": "Placeholder URL - webhook skipped",
        "invalid": "Invalid URL - webhook skipped",
        "configured": "Configured - not sent (nothing to deliver)",
        "dry_run": "Dry run - nothing sent",
    }.get(webhook_status, "Not sent")


def print_summary(
    results: List[Dict[str, Any]], webhook_status: str, webhook_message: str
) -> None:
    """
    Always-on human-readable report.

    This runs whether or not n8n is configured, and whether or not delivery
    worked, so the Klaviyo results are never lost behind a webhook problem.
    """
    print()
    for payload in results:
        print(_LINE)
        print(payload.get("client_name", "(unnamed client)"))
        print(_LINE)

        if not payload.get("success"):
            print(_row("Status", "FAILED ({})".format(
                payload.get("error_type") or "unknown")))
            print(_row("Error", str(payload.get("error") or "-")[:200]))
            print(_row("n8n", _delivery_line(payload, webhook_status)))
            print()
            continue

        days = payload.get("days_until_billing")
        print(_row("Billing period", "{} -> {}".format(
            _fmt_date(payload.get("billing_period_start")),
            _fmt_date(payload.get("billing_period_end")))))
        print(_row("Days remaining", "-" if days is None else str(days)))
        print(_row("Profiles", _fmt_usage(
            payload.get("active_profiles"), payload.get("profile_limit"),
            payload.get("profile_usage_percent"))))
        print(_row("Email usage", _fmt_usage(
            payload.get("email_used"), payload.get("email_limit"),
            payload.get("email_usage_percent"))))
        print(_row("SMS usage", _fmt_usage(
            payload.get("sms_used"), payload.get("sms_limit"),
            payload.get("sms_usage_percent"))))
        print(_row("AI credits", _fmt_usage(
            payload.get("ai_used"), payload.get("ai_limit"),
            payload.get("ai_usage_percent"))))
        print(_row("Reviews", _fmt_usage(
            payload.get("reviews_used"), payload.get("reviews_limit"),
            payload.get("reviews_usage_percent"))))
        print(_row("Sent email campaigns",
                   _fmt_number(payload.get("sent_campaign_count"))))
        print(_row("Campaigns with data",
                   _fmt_number(payload.get("campaigns_with_recipient_data"))))
        print(_row("Avg recipients/email",
                   _fmt_number(payload.get("average_recipients_per_email"))))
        print(_row("Reminder", "YES" if payload.get("should_remind") else "NO"))

        for index, reason in enumerate(payload.get("reasons") or []):
            print(_row("Reasons" if index == 0 else "", "- " + reason))

        for alert in payload.get("already_alerted") or []:
            print(_row("Already alerted", "{} ({})".format(
                alert["alert_type"], alert.get("alerted_at") or "earlier")))

        for warning in payload.get("warnings") or []:
            print(_row("Warning", warning))

        print(_row("n8n", _delivery_line(payload, webhook_status)))
        print()

    succeeded = [r for r in results if r.get("success")]
    failed = [r for r in results if not r.get("success")]
    flagged = [r for r in succeeded if r.get("should_remind")]
    suppressed = [r for r in results if r.get("suppressed_duplicate")]
    delivered = [r for r in results if r.get("n8n_delivery") == "sent"]
    undelivered = [r for r in results if r.get("n8n_delivery") == "failed"]

    print(_LINE)
    print("SUMMARY")
    print(_LINE)
    print(_row("Clients checked", str(len(results))))
    print(_row("Succeeded", str(len(succeeded))))
    print(_row("Failed", str(len(failed))))
    print(_row("New usage alerts", str(len(flagged))))
    print(_row("Duplicates suppressed", str(len(suppressed))))
    if delivered or undelivered:
        print(_row("Delivered to n8n", str(len(delivered))))
        if undelivered:
            print(_row("Delivery failures", str(len(undelivered))))
    print(_row("n8n", webhook_message))
    print(_LINE)
    print()


# ============================================================
# MAIN
# ============================================================

def main() -> int:
    parser = argparse.ArgumentParser(description="Klaviyo billing monitor")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="print payloads instead of posting to n8n",
    )
    parser.add_argument(
        "--client", action="append", default=[],
        help="only process this client (repeatable)",
    )
    parser.add_argument(
        "--remind-only", action="store_true",
        help="no-op, kept for existing schedules: only clients with a new "
             "alert are ever sent",
    )
    parser.add_argument(
        "--out", help="also write all payloads to this JSON file",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="re-send alerts even if an identical one was already sent",
    )
    parser.add_argument(
        "--no-dedupe", action="store_true",
        help="disable duplicate-alert suppression entirely",
    )
    parser.add_argument(
        "--state-file", default=ALERT_STATE_FILE,
        help="where sent-alert fingerprints are remembered",
    )
    args = parser.parse_args()

    clients = load_clients()

    if args.client:
        wanted = {name.lower() for name in args.client}
        clients = [c for c in clients if c.client_name.lower() in wanted]
        if not clients:
            raise SystemExit("No configured client matched --client.")

    # n8n is an optional delivery layer. An unusable webhook is reported and
    # skipped - it never stops the Klaviyo run or hides the results.
    webhook, webhook_status, webhook_message = resolve_webhook(N8N_WEBHOOK_URL)
    if args.dry_run:
        webhook, webhook_status = None, "dry_run"
        webhook_message = "Dry run - nothing sent"
    elif webhook_status != "configured":
        logger.warning(webhook_message)

    logger.info(
        "Checking %d client(s) with %d worker(s)", len(clients), MAX_WORKERS
    )

    results: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, MAX_WORKERS)) as pool:
        futures = {
            pool.submit(process_client, record): record.client_name
            for record in clients
        }
        for future in as_completed(futures):
            results.append(future.result())

    results.sort(key=lambda item: item["client_name"].lower())

    # --- duplicate suppression -------------------------------------------
    store = build_alert_store(args.state_file, disabled=args.no_dedupe)
    keys = assign_alert_identity(results)
    state = store.load(keys) if store else {}
    if store:
        logger.info("Alert history: %s", store.describe())
    mark_duplicates(results, state, force=args.force or args.no_dedupe)

    succeeded = [r for r in results if r["success"]]
    failed = [r for r in results if not r["success"]]
    flagged = [r for r in succeeded if r.get("should_remind")]
    actionable = [
        r for r in results
        if (r.get("should_remind") or not r["success"])
        and not r.get("suppressed_duplicate")
    ]
    suppressed = [r for r in results if r.get("suppressed_duplicate")]

    logger.info(
        "Done: %d ok, %d failed, %d new usage alert(s), %d will notify, "
        "%d suppressed as duplicates",
        len(succeeded), len(failed), len(flagged), len(actionable), len(suppressed),
    )
    for result in failed:
        logger.warning(
            "  failed: %s (%s)", result["client_name"], result["error_type"]
        )
    for result in flagged:
        logger.info(
            "  new alert: %s - %s", result["client_name"], "; ".join(result["reasons"])
        )
    for result in results:
        for alert in result.get("already_alerted") or []:
            logger.info(
                "  already alerted this period: %s %s (%s)",
                result["client_name"], alert["alert_type"], alert["alerted_at"],
            )

    # Quiet clients, and alerts already delivered this billing period, are
    # never sent: every POST to n8n is a Discord-worthy alert.
    outgoing = actionable

    # ------------------------------------------------------------------
    # Optional delivery. Any failure here is reported, never fatal, and
    # never prevents the results below from being shown.
    # ------------------------------------------------------------------
    if args.dry_run:
        for result in results:
            result["n8n_delivery"] = "dry_run"
        logger.info("Dry run: nothing sent, alert state not updated.")

    elif webhook and outgoing:
        try:
            send_to_n8n(webhook, outgoing, state)
        except Exception:
            logger.exception("Unexpected error while delivering to n8n")
            for result in outgoing:
                result.setdefault("n8n_delivery", "failed")
                result.setdefault("n8n_detail", "unexpected error")
        if store:
            store.persist(results, state)

    elif webhook:
        logger.info("Nothing to send to n8n.")

    # ------------------------------------------------------------------
    # Results are always shown, in every mode.
    # ------------------------------------------------------------------
    print_summary(results, webhook_status, webhook_message)

    if args.out:
        with open(args.out, "w") as handle:
            json.dump(results, handle, indent=2)
        logger.info("Wrote %s", args.out)

    if args.dry_run:
        print(json.dumps(outgoing, indent=2))

    # Exit non-zero only for a real Klaviyo/system failure. A missing or
    # unreachable webhook is not a Klaviyo processing failure.
    return 1 if failed and not succeeded else 0


if __name__ == "__main__":
    sys.exit(main())
