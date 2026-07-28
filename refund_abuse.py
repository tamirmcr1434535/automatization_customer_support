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

# ── Tunables ─────────────────────────────────────────────────────────────── #
# Per-brand protection is two-tier:
#   1. rolling-hour RATE limit — a fixed burst breaker; a spike within one hour is
#      abnormal for ANY brand (attack / runaway bug), independent of brand size.
#   2. ADAPTIVE daily cap — the "normal" volume is LEARNED from the brand's own
#      recent history (trailing-window average of WOULD_BE_REFUNDED) × a factor,
#      with a floor so tiny/new brands still allow a handful and an absolute hard
#      ceiling as the ultimate backstop. This auto-tunes per brand: a brand that
#      normally does 1 refund/day trips at ~a handful, a busy brand scales up.
MAX_PER_HOUR_PER_BRAND = int(os.getenv("REFUND_MAX_PER_HOUR_PER_BRAND", "20"))
BASELINE_DAYS   = int(os.getenv("REFUND_BASELINE_DAYS", "14"))   # trailing window to learn "normal"
DAILY_FACTOR    = float(os.getenv("REFUND_DAILY_FACTOR", "3"))   # allow up to N× the brand's normal
DAILY_FLOOR     = int(os.getenv("REFUND_DAILY_FLOOR", "40"))     # min daily allowance (tiny/new brands)
DAILY_HARD_MAX  = int(os.getenv("REFUND_DAILY_HARD_MAX", "150")) # absolute ceiling regardless of baseline
# Per-customer: at most N refunds per rolling window (default 3 per 24h).
MAX_PER_EMAIL          = int(os.getenv("REFUND_MAX_PER_EMAIL", "3"))
EMAIL_WINDOW_HOURS     = int(os.getenv("REFUND_EMAIL_WINDOW_HOURS", "24"))
_TABLE = os.getenv("REFUND_ABUSE_TABLE",
                   f'{os.getenv("GCP_PROJECT", "powerful-vine-426615-r2")}.zendesk_bot.cancellation_logs')


def adaptive_daily_cap(baseline_total: float, baseline_days: float) -> int:
    """Learned daily cap = max(floor, round(avg_daily_normal × factor)), clamped to
    the absolute hard max. avg_daily_normal is the brand's trailing WOULD_BE rate."""
    avg = (baseline_total / baseline_days) if baseline_days else 0.0
    cap = max(DAILY_FLOOR, round(avg * DAILY_FACTOR))
    return min(cap, DAILY_HARD_MAX)

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
                  AND logged_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @ewin HOUR)) AS email_recent,
          -- learned baseline: the brand's normal auto-refund demand (WOULD_BE) before today
          COUNTIF(refund_reason_code = 'WOULD_BE_REFUNDED' AND refund_brand = @brand
                  AND DATE(logged_at) < CURRENT_DATE()) AS wb_baseline_total,
          COUNT(DISTINCT IF(refund_reason_code = 'WOULD_BE_REFUNDED' AND refund_brand = @brand
                            AND DATE(logged_at) < CURRENT_DATE(), DATE(logged_at), NULL)) AS wb_baseline_days
        FROM `{_TABLE}`
        WHERE logged_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @bdays DAY)
        """
        cfg = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("brand", "STRING", brand or ""),
            bigquery.ScalarQueryParameter("email", "STRING", email or ""),
            bigquery.ScalarQueryParameter("ewin", "INT64", EMAIL_WINDOW_HOURS),
            bigquery.ScalarQueryParameter("bdays", "INT64", BASELINE_DAYS),
        ])
        row = list(cli.query(q, job_config=cfg).result())[0]
        brand_hour   = int(row["brand_hour"] or 0)
        brand_today  = int(row["brand_today"] or 0)
        email_recent = int(row["email_recent"] or 0)
        daily_cap = adaptive_daily_cap(float(row["wb_baseline_total"] or 0),
                                       float(row["wb_baseline_days"] or 0))
        if brand_hour >= MAX_PER_HOUR_PER_BRAND:
            return False, f"brand_rate:{brand_hour}>={MAX_PER_HOUR_PER_BRAND}/h"
        if brand_today >= daily_cap:
            return False, f"brand_daily_adaptive:{brand_today}>={daily_cap}"
        if email_recent >= MAX_PER_EMAIL:
            return False, f"email_velocity:{email_recent}>={MAX_PER_EMAIL}/{EMAIL_WINDOW_HOURS}h"
        return True, ""
    except Exception as e:  # noqa: BLE001 — money guard must fail closed
        log.warning("refund abuse check failed (fail-closed → escalate): %s", e)
        return False, "abuse_guard_error"
