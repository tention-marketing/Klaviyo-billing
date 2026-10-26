"""
PostgreSQL access for the Klaviyo billing monitor.

Holds encrypted client credentials and duplicate-alert history. Deliberately
plain SQL over psycopg3 - no ORM, since the whole schema is two tables.

Thread safety: every call takes its own short-lived connection from a small
pool. Connections and cursors are never shared across threads. Client records
are loaded once on the main thread before workers start, so worker threads do
no database I/O at all.
"""

import os
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import psycopg
from psycopg.rows import dict_row

DATABASE_URL_ENV = "DATABASE_URL"


class DatabaseUnavailable(RuntimeError):
    """
    Postgres is configured but unreachable.

    Callers must treat this as fatal rather than "zero clients" - silently
    monitoring nothing is the worst possible failure mode here.
    """


def database_url() -> str:
    return (os.getenv(DATABASE_URL_ENV, "") or "").strip()


def is_configured() -> bool:
    return bool(database_url())


def _normalise(url: str) -> str:
    """
    Railway hands out `postgres://`; psycopg wants `postgresql://`.
    Also force TLS for non-local hosts unless the URL already says otherwise.
    """
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]

    if "sslmode=" not in url and not any(
        host in url for host in ("@localhost", "@127.0.0.1", "@::1")
    ):
        url += ("&" if "?" in url else "?") + "sslmode=require"

    return url


def safe_target(url: Optional[str] = None) -> str:
    """
    A loggable description of the database: host, port and name only.

    Never the user or password. Used in error messages so a connection failure
    is diagnosable without leaking DATABASE_URL.
    """
    raw = url if url is not None else database_url()
    if not raw:
        return "<not configured>"
    try:
        parsed = psycopg.conninfo.conninfo_to_dict(_normalise(raw))
        return "{}:{}/{}".format(
            parsed.get("host", "?"), parsed.get("port", "?"),
            parsed.get("dbname", "?"),
        )
    except Exception:
        return "<unparseable DATABASE_URL>"


class connection:
    """
    Context manager yielding a psycopg connection.

    One short-lived connection per use, so nothing is shared between threads.
    No pool: a whole run makes about three queries (load clients, read alert
    history, record alerts), all from the main thread, and worker threads make
    none - so a pool would add moving parts and noisy logging for no gain.

    Commits on clean exit, rolls back on error, and turns any connection
    problem into DatabaseUnavailable with the credentials stripped out.
    """

    def __init__(self, url: Optional[str] = None):
        self._url = url if url is not None else database_url()
        self._conn = None

    def __enter__(self):
        if not self._url:
            raise DatabaseUnavailable(
                "{} is not set - no database configured".format(DATABASE_URL_ENV)
            )
        try:
            self._conn = psycopg.connect(
                _normalise(self._url),
                row_factory=dict_row,
                connect_timeout=int(float(os.getenv("DB_CONNECT_TIMEOUT", "10"))),
            )
            return self._conn
        except Exception as exc:
            raise DatabaseUnavailable(
                "Database unavailable - unable to reach {} ({})".format(
                    safe_target(self._url), _scrub(exc, self._url),
                )
            )

    def __exit__(self, exc_type, exc, tb):
        if self._conn is None:
            return False
        try:
            if exc_type is None:
                self._conn.commit()
            else:
                self._conn.rollback()
        finally:
            self._conn.close()
            self._conn = None
        return False


def _scrub(text: Any, url: str) -> str:
    """Strip the raw DATABASE_URL (and its password) out of a message."""
    out = str(text)
    if url:
        out = out.replace(url, safe_target(url))
        out = out.replace(_normalise(url), safe_target(url))
        try:
            password = psycopg.conninfo.conninfo_to_dict(_normalise(url)).get("password")
            if password:
                out = out.replace(str(password), "***")
        except Exception:
            pass
    return out[:300]


# ------------------------------------------------------------------
# Schema
# ------------------------------------------------------------------

SCHEMA: Sequence[Tuple[str, str]] = (
    (
        "klaviyo_clients",
        """
        CREATE TABLE IF NOT EXISTS klaviyo_clients (
            id                 BIGSERIAL PRIMARY KEY,
            client_name        TEXT        NOT NULL,
            api_key_ciphertext BYTEA       NOT NULL,
            api_key_nonce      BYTEA       NOT NULL,
            active             BOOLEAN     NOT NULL DEFAULT TRUE,
            created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT klaviyo_clients_name_key UNIQUE (client_name)
        )
        """,
    ),
    (
        "klaviyo_clients_active_idx",
        """
        CREATE INDEX IF NOT EXISTS klaviyo_clients_active_idx
            ON klaviyo_clients (active) WHERE active
        """,
    ),
    (
        "klaviyo_alert_history",
        """
        CREATE TABLE IF NOT EXISTS klaviyo_alert_history (
            id                 BIGSERIAL PRIMARY KEY,
            alert_key          TEXT        NOT NULL,
            client_id          BIGINT      REFERENCES klaviyo_clients(id) ON DELETE SET NULL,
            client_name        TEXT        NOT NULL,
            billing_period_end TIMESTAMPTZ,
            alert_types        TEXT[]      NOT NULL DEFAULT '{}',
            alerted_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
            created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT klaviyo_alert_history_key_key UNIQUE (alert_key)
        )
        """,
    ),
    (
        "klaviyo_alert_history_alerted_idx",
        """
        CREATE INDEX IF NOT EXISTS klaviyo_alert_history_alerted_idx
            ON klaviyo_alert_history (alerted_at)
        """,
    ),
    (
        "klaviyo_clients_touch_updated_at",
        """
        CREATE OR REPLACE FUNCTION klaviyo_clients_touch_updated_at()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = now();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
        """,
    ),
    (
        "klaviyo_clients_updated_at_trg",
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_trigger WHERE tgname = 'klaviyo_clients_updated_at_trg'
            ) THEN
                CREATE TRIGGER klaviyo_clients_updated_at_trg
                    BEFORE UPDATE ON klaviyo_clients
                    FOR EACH ROW EXECUTE FUNCTION klaviyo_clients_touch_updated_at();
            END IF;
        END $$
        """,
    ),
)



def migrate(url: Optional[str] = None) -> List[Tuple[str, str]]:
    """
    Apply the schema. Idempotent - every statement is CREATE ... IF NOT EXISTS
    or guarded, so running it repeatedly is safe and never destructive.
    """
    results: List[Tuple[str, str]] = []
    with connection(url) as conn:
        for name, statement in SCHEMA:
            existed = _object_exists(conn, name)
            conn.execute(statement)
            results.append((name, "already exists" if existed else "created"))
    return results


def _object_exists(conn, name: str) -> bool:
    row = conn.execute(
        """
        SELECT
          EXISTS (SELECT 1 FROM pg_class     WHERE relname = %(n)s)
          OR EXISTS (SELECT 1 FROM pg_proc   WHERE proname = %(n)s)
          OR EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = %(n)s) AS present
        """,
        {"n": name},
    ).fetchone()
    return bool(row and row["present"])


# ------------------------------------------------------------------
# Clients
# ------------------------------------------------------------------

def fetch_clients(
    active_only: bool = True, url: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Rows as stored - ciphertext stays encrypted until a worker needs it."""
    sql = (
        "SELECT id, client_name, api_key_ciphertext, api_key_nonce, active, "
        "       created_at, updated_at "
        "FROM klaviyo_clients "
        "{} ORDER BY client_name"
    ).format("WHERE active" if active_only else "")

    with connection(url) as conn:
        return [dict(row) for row in conn.execute(sql).fetchall()]


def upsert_client(
    client_name: str,
    ciphertext: bytes,
    nonce: bytes,
    active: bool = True,
    url: Optional[str] = None,
) -> Tuple[int, bool]:
    """Insert or replace a client's credential. Returns (id, was_insert)."""
    with connection(url) as conn:
        row = conn.execute(
            """
            INSERT INTO klaviyo_clients
                (client_name, api_key_ciphertext, api_key_nonce, active)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (client_name) DO UPDATE
                SET api_key_ciphertext = EXCLUDED.api_key_ciphertext,
                    api_key_nonce      = EXCLUDED.api_key_nonce,
                    active             = EXCLUDED.active
            RETURNING id, (created_at = updated_at) AS was_insert
            """,
            (client_name, ciphertext, nonce, active),
        ).fetchone()
    return row["id"], bool(row["was_insert"])


def set_client_active(
    client_name: str, active: bool, url: Optional[str] = None
) -> bool:
    with connection(url) as conn:
        row = conn.execute(
            "UPDATE klaviyo_clients SET active = %s WHERE client_name = %s "
            "RETURNING id",
            (active, client_name),
        ).fetchone()
    return row is not None


def delete_client(client_name: str, url: Optional[str] = None) -> bool:
    with connection(url) as conn:
        row = conn.execute(
            "DELETE FROM klaviyo_clients WHERE client_name = %s RETURNING id",
            (client_name,),
        ).fetchone()
    return row is not None


# ------------------------------------------------------------------
# Alert history (duplicate suppression)
# ------------------------------------------------------------------

def fetch_alert_times(
    alert_keys: Iterable[str], url: Optional[str] = None
) -> Dict[str, str]:
    """When each of these alerts last fired: {alert_key: ISO timestamp}."""
    keys = [key for key in alert_keys if key]
    if not keys:
        return {}

    with connection(url) as conn:
        rows = conn.execute(
            "SELECT alert_key, alerted_at FROM klaviyo_alert_history "
            "WHERE alert_key = ANY(%s)",
            (keys,),
        ).fetchall()

    return {row["alert_key"]: row["alerted_at"].isoformat() for row in rows}


def record_alerts(
    entries: Sequence[Dict[str, Any]], url: Optional[str] = None
) -> int:
    """
    Remember delivered alerts.

    UPSERT on alert_key, so two concurrent runs cannot create duplicate rows
    or deadlock on the same key.
    """
    if not entries:
        return 0

    with connection(url) as conn:
        for entry in entries:
            conn.execute(
                """
                INSERT INTO klaviyo_alert_history
                    (alert_key, client_id, client_name, billing_period_end,
                     alert_types, alerted_at)
                VALUES (%(alert_key)s, %(client_id)s, %(client_name)s,
                        %(billing_period_end)s, %(alert_types)s, now())
                ON CONFLICT (alert_key) DO UPDATE
                    SET alerted_at  = now(),
                        alert_types = EXCLUDED.alert_types,
                        client_id   = COALESCE(EXCLUDED.client_id,
                                               klaviyo_alert_history.client_id)
                """,
                entry,
            )
    return len(entries)


def prune_alert_history(days: int, url: Optional[str] = None) -> int:
    """
    Drop old alerts, but never one whose billing period is still running:
    that row is what stops a usage alert repeating within the period, and a
    period (e.g. an annual plan) can outlast the retention window.
    """
    with connection(url) as conn:
        row = conn.execute(
            "WITH gone AS (DELETE FROM klaviyo_alert_history "
            "  WHERE alerted_at < now() - make_interval(days => %s) "
            "    AND (billing_period_end IS NULL OR billing_period_end < now()) "
            "  RETURNING 1) "
            "SELECT count(*) AS n FROM gone",
            (int(days),),
        ).fetchone()
    return int(row["n"]) if row else 0
