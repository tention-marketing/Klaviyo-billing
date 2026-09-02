# Klaviyo Billing Monitor

Checks every client Klaviyo account, computes billing-period usage and campaign
recipient stats, decides whether the account needs a human review, and posts one
clean JSON payload per client to an n8n webhook.

Python does all the Klaviyo work. n8n only reads `should_remind` and notifies Discord.

## Verified API behaviour

Everything below was confirmed against the live API, not taken from docs.

| Finding | Detail |
|---|---|
| **Two different revisions are required** | `billing-usage` exists **only** on `2026-07-15.pre`. On stable `2026-07-15` it returns `404 No valid revisions found for method`. Campaigns and reporting exist **only** on stable `2026-07-15`. |
| **The `messages.channel` filter is not broken** | On `.pre` the campaigns resource has a different shape and rejects the filter (`'messages.channel' is not a filterable field`). On stable `2026-07-15` a channel filter is **mandatory** — omitting it returns `400 A channel filter is required to list campaigns`. |
| **`send_time` is not filterable** | Filterable: `scheduled_at`, `created_at`, `updated_at`, `status`, `archived`. Sortable: `-scheduled_at`, `-created_at`, `-updated_at` (not `-send_time`). |
| **`send_time` ≠ `scheduled_at`** | On smart-send campaigns they differ by hours in both directions. Campaigns are fetched by `scheduled_at` with a 2-day pad, then filtered locally on `send_time`. |
| **Recipient estimation jobs need `campaigns:write`** | A read-only monitoring key gets `403 missing required scopes: campaigns:write`. |
| **The values report is the right tool** | `POST /api/campaign-values-reports` returns *actual* recipients for every campaign in the period in **one** request, with a read-only key. |
| **`conversion_metric_id` is required** | Even when only `recipients` is requested. The script looks up "Placed Order" (or any metric) once per client. |
| **The report endpoint is throttled hard** | ~1/min. 429 responses carry `Retry-After`, which the client honours. |
| **Statuses seen in real data** | `Draft`, `Sending`, `Cancelled`, `Sent`. Only `Sent` is counted. |

## Required Klaviyo API key scopes

Each client's **private** key needs read access to:

| Scope | Used for | Required? |
|---|---|---|
| `usage:read` | `GET /api/billing-usage` | **yes** |
| `campaigns:read` | `GET /api/campaigns` | **yes** |
| `metrics:read` | `GET /api/metrics` (conversion metric id) | **yes** |
| `campaigns:write` | recipient *estimation jobs* only | no — leave off |

Scope names confirmed by the API itself: a key without the first one returns
`403 ... missing required scopes: usage:read`. A key missing a scope fails with
`error_type: "permission"`, is reported per client, and does not stop the run.

Verified: with a read-only key, billing/campaigns/metrics all return 200 and the
estimation job returns `403 missing required scopes: campaigns:write`. The
default recipient path does not need write access.

## Setup

```bash
pip3 install -r requirements.txt

cp clients.example.json clients.json   # add your 100+ clients here
chmod 600 clients.json                 # gitignored

cp .env.example .env                   # optional settings, gitignored
```

`index.py` loads `.env` from its own directory automatically — no
`source .env` needed, and it works from any working directory (so cron is
safe). Real environment variables override the file, and there is no
third-party dependency: the loader is ~30 lines in `index.py`.

**n8n is optional.** With `N8N_WEBHOOK_URL` empty, `python3 index.py` still
checks every account and prints the full summary; it just skips delivery.

`clients.json`:

```json
[
  { "client_name": "Steven Denn",     "api_key": "pk_..." },
  { "client_name": "AutoLEDTech.com", "api_key": "pk_..." }
]
```

Clients can also be supplied inline via the `KLAVIYO_CLIENTS` env var (same JSON),
which is easier in CI. Entries with a missing or placeholder key are skipped with
a warning rather than failing the run.

## Running

```bash
python3 index.py                      # check everyone, POST to n8n
python3 index.py --dry-run            # print payloads, contact no webhook
python3 index.py --client "Steven Denn"   # one client (repeatable)
python3 index.py --remind-only        # only POST accounts that need review
python3 index.py --out results.json   # also write every payload to disk
python3 index.py --force              # re-send alerts already sent
python3 index.py --no-dedupe          # disable duplicate suppression
python3 test_index.py                 # offline self-test, no API key needed
```

### Scheduling

`run.sh` loads `.env` and runs the monitor. Daily at 09:00:

```
crontab -e
0 9 * * *  /full/path/to/Klaviyo-Billing/run.sh >> /full/path/to/Klaviyo-Billing/run.log 2>&1
```

On macOS, edit the paths in `com.klaviyo.billing.plist` then:

```bash
cp com.klaviyo.billing.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.klaviyo.billing.plist
```

Business logic lives entirely in `index.py`; the scheduler only invokes it.
The script is safe to run repeatedly — see duplicate suppression below.

## Client credentials

Two sources, chosen in strict priority order. **They are never merged**, so a
half-finished migration cannot monitor a client twice or use a stale key.

| Priority | Source | When |
|---|---|---|
| 1 | **Postgres** (`DATABASE_URL`) | production |
| 2 | `KLAVIYO_CLIENTS` inline JSON | ad-hoc |
| 3 | `clients.json` | local development / before migrating |

In Postgres, keys are stored **encrypted with AES-256-GCM**. Plaintext exists
only inside the worker thread handling that client, for the duration of its
Klaviyo requests.

### Generate the master key

```bash
python3 -c "import crypto_store; print(crypto_store.generate_master_key())"
```

That is 32 random bytes, base64-encoded. Put it in `KLAVIYO_MASTER_KEY`.

**If you lose this key, every stored API key is unrecoverable** and you will
have to re-enter all of them. Keep a copy in your password manager. It is
validated at startup — missing, malformed, or wrong-length fails immediately,
before any credential is touched.

### Managing clients

```bash
python3 manage_clients.py list                          # names + status, never keys
python3 manage_clients.py add                           # prompts for name + hidden key
python3 manage_clients.py add --name "Steven Denn"
python3 manage_clients.py update-key --name "Steven Denn"
python3 manage_clients.py disable --name "Old Client"   # stops monitoring, keeps the row
python3 manage_clients.py enable  --name "Old Client"
python3 manage_clients.py remove  --name "Old Client"   # deletes the row and its key
python3 manage_clients.py import-json clients.json      # one-time migration
python3 manage_clients.py verify                        # every stored key decrypts?
```

API keys are typed into a hidden, confirmed prompt — never passed as a
command-line argument, so they stay out of shell history. No command ever
prints a key back.

## Deploying to Railway

You need **three** variables, not one per client.

### 1. Create the Postgres database

1. Open your project on [railway.app](https://railway.app).
2. Click **+ New** → **Database** → **Add PostgreSQL**. Railway provisions it
   and creates a `DATABASE_URL` variable on the Postgres service.
3. Open your **Klaviyo Billing** service → **Variables** → **+ New Variable**
   → **Add Reference** → pick the Postgres service's `DATABASE_URL`. This
   shares the URL without you ever copying the password.

### 2. Set the variables

On the Klaviyo Billing service → **Variables**:

| Variable | Value |
|---|---|
| `DATABASE_URL` | the reference from step 1 |
| `KLAVIYO_MASTER_KEY` | output of the generate command above |
| `N8N_WEBHOOK_URL` | `https://<your-n8n-host>/webhook/Klaviyo-Billing` |

That is the whole production configuration. **No client API keys live in
Railway variables**, and no `/data` volume is needed — alert history is in
Postgres.

### 3. Deploy and migrate

Push the code, then run the migration. The easiest way is the Railway CLI from
your own machine, which injects the service's variables into a local command:

```bash
npm i -g @railway/cli
railway login
railway link                     # pick your project + service

railway run python3 migrate.py
```

Expected:

```
Creating klaviyo_clients... created
Creating klaviyo_alert_history... created
```

Running it again prints `already exists`. It is idempotent and never drops or
rewrites data.

### 4. Import your existing clients

Still using `railway run`, so it talks to Railway's Postgres while reading your
**local** `clients.json`:

```bash
railway run python3 manage_clients.py import-json clients.json
```

```
Importing 2 client(s) from clients.json ...

Steven Denn                        imported
AutoLEDTech.com                    imported

2 imported
0 failed
```

Your local file is **not** modified or deleted. Verify:

```bash
railway run python3 manage_clients.py list
railway run python3 manage_clients.py verify
```

### 5. Test, then schedule

```bash
railway run python3 index.py --dry-run     # no webhook call, no state change
railway run python3 index.py --force       # real n8n -> Discord test
```

Then set the service's **Cron Schedule** to `0 9 * * *`. `railway.json` keeps
`restartPolicyType: NEVER` so the container runs once and exits rather than
being treated as a crashed web service.

After this, production no longer needs `clients.json` at all. Keep it locally
as a backup, or delete it once you trust the database.

## Database schema

```
klaviyo_clients
  id                  BIGSERIAL PRIMARY KEY
  client_name         TEXT NOT NULL UNIQUE
  api_key_ciphertext  BYTEA NOT NULL      -- AES-256-GCM, includes the auth tag
  api_key_nonce       BYTEA NOT NULL      -- fresh 96-bit nonce per key
  active              BOOLEAN NOT NULL DEFAULT TRUE
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()   -- trigger-maintained
  INDEX ON (active) WHERE active

klaviyo_alert_history
  id                  BIGSERIAL PRIMARY KEY
  alert_key           TEXT NOT NULL UNIQUE
  client_id           BIGINT REFERENCES klaviyo_clients(id) ON DELETE SET NULL
  client_name         TEXT NOT NULL
  billing_period_end  TIMESTAMPTZ
  alert_types         TEXT[] NOT NULL DEFAULT '{}'
  alerted_at          TIMESTAMPTZ NOT NULL DEFAULT now()
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
  INDEX ON (alerted_at)
```

## How encryption works

```
KLAVIYO_MASTER_KEY (base64, 32 bytes)   <- Railway variable, never in the DB
        |
        v
AES-256-GCM  +  fresh 96-bit nonce per key  +  AAD "klaviyo_client_api_key:v1"
        |
        v
api_key_ciphertext (+auth tag)  |  api_key_nonce      <- Postgres
```

- **Authenticated**: GCM verifies the tag before returning plaintext, so a
  wrong master key or a modified row raises instead of yielding garbage.
- **Fresh nonce per encryption**: storing the same key twice produces different
  ciphertext, so rows reveal nothing by comparison.
- **AAD binds the purpose**, so a row cannot be replayed as a different secret.
- The master key is only ever in the environment. A database dump on its own is
  useless.

Verified: a full `pg_dump` of the clients table contains **zero** occurrences
of `pk_`.

## Configuration

| Env var | Default | Purpose |
|---|---|---|
| `N8N_WEBHOOK_URL` | *(optional)* | Empty/placeholder → delivery skipped, run still succeeds |
| `N8N_SEND_MODE` | `per_client` | `per_client` or `batch` |
| `KLAVIYO_CLIENTS_FILE` | `clients.json` | Client config path |
| `KLAVIYO_CLIENTS` | – | Inline JSON, overrides the file |
| `REMINDER_DAYS` | `3` | Flag when the period ends within N days |
| `HIGH_USAGE_PERCENT` | `90` | Flag when an important metric hits this |
| `MAX_WORKERS` | `6` | Clients checked in parallel |
| `MAX_RETRIES` | `4` | Retries for 429/5xx/network |
| `USE_ESTIMATION_FALLBACK` | `false` | Only enable if keys have `campaigns:write` |
| `DATABASE_URL` | *(optional)* | Set → encrypted Postgres client store + alert history |
| `KLAVIYO_MASTER_KEY` | *(required with DB)* | base64 32 bytes; validated at startup |
| `ALERT_STATE_FILE` | `.alert_state.json` | Local fallback only; unused when `DATABASE_URL` is set |
| `ALERT_COOLDOWN_HOURS` | `168` | Unchanged alert may repeat after this; `0` = never |
| `ALERT_STATE_RETENTION_DAYS` | `90` | Prune remembered alerts older than this |
| `LOG_LEVEL` | `INFO` | `DEBUG` shows retries |

## Delivery modes

| `N8N_WEBHOOK_URL` | Klaviyo check | Terminal summary | Delivery | Exit code |
|---|---|---|---|---|
| unset / empty | runs | shown | skipped, clearly reported | 0 |
| placeholder or non-http | runs | shown | skipped, warning logged | 0 |
| valid, n8n reachable | runs | shown | sent | 0 |
| valid, n8n **down** | runs | shown | reported as failed, bounded retries | 0 |

A webhook problem is never treated as a Klaviyo failure, and results are never
hidden behind one. A non-zero exit means a genuine Klaviyo/system failure
(every client failed).

When delivery fails, the alert is **not** recorded in the dedupe state, so it
is retried on the next run rather than silently lost.

### Webhook URL is treated as a secret

An n8n webhook path contains a token, so only the host is ever printed
(`https://n8n.example.com/...`). The path is scrubbed out of exception text
too, and `n8n_delivery` / `n8n_detail` are local-only fields stripped before
anything goes on the wire.

## Duplicate alert protection

Running daily must not re-notify Discord about a situation that has not changed.
Each alerting payload gets a stable fingerprint:

```
alert_key = sha1(client_name | billing_period_end | sorted(alert_types))
```

Sent keys are remembered in `.alert_state.json` (gitignored, written
atomically, pruned after `ALERT_STATE_RETENTION_DAYS`). A repeat is marked
`suppressed_duplicate: true`, and the n8n Code node will not build a Discord
message for it.

A **new** alert still fires, because the key changes when:

- the billing period rolls over (`billing_period_end` changes)
- usage escalates (`high_usage:sms_credits` -> `over_limit:sms_credits`)
- another metric joins the alert
- a different client is affected

Failures get their own key (`client_failed:auth`), so a broken key alerts once
rather than every morning. `ALERT_COOLDOWN_HOURS` (default 168 = weekly) lets an
unchanged condition re-alert eventually; set it to `0` to never repeat.
`--force` re-sends regardless. State is only written after n8n accepts the
payload, so a webhook outage retries next run instead of losing the alert.

## n8n + Discord

Import `n8n_workflow.json` (Workflows -> Import from File). Three nodes:

```
Webhook (POST /klaviyo-billing)
  -> Code: "Check should_remind & Build Discord Message"
  -> HTTP Request: "Send to Discord"
```

The Code node is the gate. It notifies only when:

```js
!c.suppressed_duplicate && (c.success === false || c.should_remind === true)
```

and it handles both `N8N_SEND_MODE` values (a single client object, or a batch
`{clients: [...]}`). It emits a Discord embed carrying client name, billing end
date and days remaining, profiles/email/SMS usage, campaign count, average
recipients and the reasons — red for over-limit or a failed client, orange for
a warning. AI credits and reviews appear only when non-zero. No secret ever
reaches the payload, so none can reach Discord.

The HTTP node paces sends at one per 1.2s so a large flagged batch does not trip
Discord's webhook rate limit, and retries 3 times.

**You must edit the `Send to Discord` node URL** — it ships as
`REPLACE_WITH_YOUR_DISCORD_WEBHOOK_URL`.

## Reminder logic

`should_remind` is true when **any** of these hold:

1. `days_until_billing <= REMINDER_DAYS`
2. **any** metric is over its limit (overage is always billable)
3. an **important** metric is at or above `HIGH_USAGE_PERCENT`

Important metrics: profiles, email, SMS. AI credits and reviews are reported and
trigger on over-limit, but their high-usage alone does not flag an account.

The script only *identifies* accounts to review. It never decides a downgrade or
a savings amount — that is the human's call.

## Cost per client

4–5 Klaviyo requests regardless of campaign volume:
1 billing-usage, 2 campaign pages (archived false + true), 1 metrics lookup,
1 values report. Campaign paging stops early once results fall outside the window.

## Security

- API keys live only in `clients.json` / `KLAVIYO_CLIENTS`, both gitignored.
- Keys are used only for Klaviyo requests. The n8n payload has no key field.
- A logging filter strips anything matching `pk_…` from every log line and error.

## Known environment note

macOS system Python 3.9 prints:

```
NotOpenSSLWarning: urllib3 v2 only supports OpenSSL 1.1.1+, currently the 'ssl'
module is compiled with 'LibreSSL 2.8.3'
```

This is cosmetic — all Klaviyo TLS requests succeed. To silence it, run under a
Homebrew/pyenv Python, or `pip3 install 'urllib3<2'`.
