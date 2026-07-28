"""
Refund abuse / velocity guard (AN-192)
======================================
The per-ticket refund guards (window, amount, dispute, refundable, x-host, LLM)
decide whether ONE refund is correct. They do NOT protect against ABUSE BY VOLUME
— a bad actor (or a bug, or a flood of tickets) driving MANY auto-refunds. Refunds
go back to the original payment method, so funds can't be redirected; the real
exposure is mass in-window refund farming and runaway execution.

This module adds three velocity controls, evaluated against the BigQuery refund
log just before a refund executes:

  1. Per-brand daily circuit breaker — cap executed auto-refunds per brand / 24h.
     Stops mass abuse AND a runaway bug from draining a brand in one day.
  2. Per-customer velocity — a single email may not be auto-refunded more than
     N times in a rolling window (default: once / 30 days). Blocks farming across
     renewals and repeated same-customer abuse.
  3. (amount ceiling stays in main._amount_guard — ±band of the standard price.)

FAIL-CLOSED: if the check cannot run (BQ error, no client), it returns NOT-ok →
the refund is escalated to a human rather than executed. A money guard must never
fail open. Everything is env-tunable; disable entirely with
REFUND_ABUSE_GUARD_ENABLED=false (not recommended once refunds are live).
"""

import os
import logging

log = logging.getLogger("refund_abuse")

# ── Tunables (conservative canary defaults) ─────────────────────────────── #
# Per-brand protection is two-tier: a rolling-hour RATE limit (catches a sudden
# burst — attack or runaway bug — within the hour, regardless of brand size) plus
# a per-day COUNT backstop (hard ceiling). The rate limit is the "flexible but
# reliable" control: it lets steady legitimate volume through but slams the brakes
# on a spike. A per-currency daily SUM cap is the recommended next upgrade
# (see docs/refund_abuse_guards.md).
MAX_PER_HOUR_PER_BRAND = int(os.getenv("REFUND_MAX_PER_HOUR_PER_BRAND", "5"))
MAX_PER_DAY_PER_BRAND  = int(os.getenv("REFUND_MAX_PER_DAY_PER_BRAND", "30"))
# Per-customer: at most N refunds per rolling window (default 2 per 24h).
MAX_PER_EMAIL          = int(os.getenv("REFUND_MAX_PER_EMAIL", "2"))
EMAIL_WINDOW_HOURS     = int(os.getenv("REFUND_EMAIL_WINDOW_HOURS", "24"))
_TABLE = os.getenv("REFUND_ABUSE_TABLE",
                   f'{os.getenv("GCP_PROJECT", "powerful-vine-426615-r2")}.zendesk_bot.cancellation_logs')

_client = None


def is_enabled() -> bool:
    return os.getenv("REFUND_ABUSE_GUARD_ENABLED", "true").lower() == "true"


def _bq():
    """Lazy BigQuery client (same project as the logger)."""
    global _client
    if _client is None:
        from google.cloud import bigquery
        _client = bigquery.Client(project=os.getenv("GCP_PROJECT", "powerful-vine-426615-r2"))
    return _client


def check(brand: str, email: str, client=None) -> "tuple[bool, str]":
    """(ok, reason). Count executed auto-refunds (refund_execution_status='refunded')
    and block if this refund would exceed: the per-brand rolling-hour RATE, the
    per-brand daily COUNT backstop, or the per-email velocity. FAIL-CLOSED: any
    error → (False, reason)."""
    if not is_enabled():
        return True, ""
    try:
        from google.cloud import bigquery
        cli = client or _bq()
        q = f"""
        SELECT
          COUNTIF(refund_execution_status = 'refunded' AND refund_brand = @brand
                  AND logged_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)) AS brand_hour,
          COUNTIF(refund_execution_status = 'refunded' AND refund_brand = @brand
                  AND DATE(logged_at) = CURRENT_DATE()) AS brand_today,
          COUNTIF(refund_execution_status = 'refunded' AND email = @email
                  AND logged_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @ewin HOUR)) AS email_recent
        FROM `{_TABLE}`
        WHERE logged_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)
        """
        cfg = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("brand", "STRING", brand or ""),
            bigquery.ScalarQueryParameter("email", "STRING", email or ""),
            bigquery.ScalarQueryParameter("ewin", "INT64", EMAIL_WINDOW_HOURS),
        ])
        row = list(cli.query(q, job_config=cfg).result())[0]
        brand_hour   = int(row["brand_hour"] or 0)
        brand_today  = int(row["brand_today"] or 0)
        email_recent = int(row["email_recent"] or 0)
        if brand_hour >= MAX_PER_HOUR_PER_BRAND:
            return False, f"brand_rate:{brand_hour}>={MAX_PER_HOUR_PER_BRAND}/h"
        if brand_today >= MAX_PER_DAY_PER_BRAND:
            return False, f"brand_daily_cap:{brand_today}>={MAX_PER_DAY_PER_BRAND}"
        if email_recent >= MAX_PER_EMAIL:
            return False, f"email_velocity:{email_recent}>={MAX_PER_EMAIL}/{EMAIL_WINDOW_HOURS}h"
        return True, ""
    except Exception as e:  # noqa: BLE001 — money guard must fail closed
        log.warning("refund abuse check failed (fail-closed → escalate): %s", e)
        return False, "abuse_guard_error"
