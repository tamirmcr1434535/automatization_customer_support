"""
BigQuery Logger for Cancellation Bot
=====================================
Logs every ticket processing result to BQ for monitoring and accuracy analysis.

In SHADOW_MODE, this is the primary testing tool — every ticket gets a fully
enriched row with classification, WC/Stripe lookup results, reply preview,
and shadow decision so you can query accuracy directly from BQ Console:

    SELECT ticket_id, email, shadow_decision, intent, confidence, language,
           cancel_source, subscription_found, reply_text, status, logged_at
    FROM `zendesk_bot.cancellation_logs`
    WHERE shadow_mode = TRUE
    ORDER BY logged_at DESC
"""

import os
import logging
from datetime import datetime, timezone

from google.cloud import bigquery

log = logging.getLogger("bq")

PROJECT  = os.getenv("GCP_PROJECT", "powerful-vine-426615-r2")
BQ_TABLE = f"{PROJECT}.zendesk_bot.cancellation_logs"

_bq = None

# ── Schema — all fields the bot can log ──────────────────────────────── #
# New fields added for shadow mode testing are marked with # SHADOW

SCHEMA = [
    # Core ticket info
    bigquery.SchemaField("ticket_id",        "STRING"),
    bigquery.SchemaField("email",            "STRING"),          # SHADOW
    bigquery.SchemaField("status",           "STRING"),
    bigquery.SchemaField("action",           "STRING"),

    # Classification
    bigquery.SchemaField("intent",           "STRING"),
    bigquery.SchemaField("language",         "STRING"),
    bigquery.SchemaField("confidence",       "FLOAT"),
    bigquery.SchemaField("chargeback_risk",  "STRING"),
    bigquery.SchemaField("reasoning",        "STRING"),

    # Subscription lookup results
    bigquery.SchemaField("cancel_source",    "STRING"),          # SHADOW: woocommerce / stripe / none
    bigquery.SchemaField("subscription_found", "BOOLEAN"),       # SHADOW: was a sub found anywhere?
    bigquery.SchemaField("subscription_type","STRING"),           # SHADOW: trial / subscription
    bigquery.SchemaField("order_count",      "INTEGER"),         # SHADOW: WC order count

    # Reply
    bigquery.SchemaField("reply_text",       "STRING"),

    # Flags
    bigquery.SchemaField("dry_run",          "BOOLEAN"),
    bigquery.SchemaField("shadow_mode",      "BOOLEAN"),         # SHADOW
    bigquery.SchemaField("shadow_decision",  "STRING"),          # SHADOW: would_cancel / would_escalate / etc.
    bigquery.SchemaField("refund_requested", "BOOLEAN"),         # SHADOW: refund_also_requested flag

    # Refund would-be evaluation (AN-192) — dataset for learning. Nexus-only,
    # disputes NOT checked; a YES is a DRAFT signal, not a payout permission.
    bigquery.SchemaField("refund_decision",       "STRING"),   # YES / NO (would_be_refunded)
    bigquery.SchemaField("refund_reason_code",    "STRING"),   # level where it stopped
    bigquery.SchemaField("refund_amount",         "NUMERIC"),  # verified amount (never customer-stated)
    bigquery.SchemaField("refund_currency",       "STRING"),
    bigquery.SchemaField("refund_amount_is_split","BOOLEAN"),  # A/B "150+150" summed
    bigquery.SchemaField("refund_customer_stated_amount", "NUMERIC"),  # AUDIT ONLY, never paid
    bigquery.SchemaField("refund_source",         "STRING"),   # Nexus `source` (brand/site)
    bigquery.SchemaField("refund_charge_id",      "STRING"),   # candidate charge matched
    bigquery.SchemaField("refund_charge_type",    "STRING"),   # first_sale / cross_sale / subscription
    bigquery.SchemaField("refund_engine_version", "STRING"),
    bigquery.SchemaField("refund_guard_trail",    "STRING"),   # JSON list of guard levels

    # Error info
    bigquery.SchemaField("error",            "STRING"),

    bigquery.SchemaField("logged_at",        "TIMESTAMP"),
]


def _client():
    global _bq
    if not _bq:
        _bq = bigquery.Client(project=PROJECT)
    return _bq


def ensure_log_table():
    """Create table + dataset if they don't exist. Safe to call multiple times."""
    c = _client()
    try:
        c.get_table(BQ_TABLE)
    except Exception:
        dataset = BQ_TABLE.split(".")[1]
        try:
            c.create_dataset(f"{PROJECT}.{dataset}")
        except Exception:
            pass
        c.create_table(bigquery.Table(BQ_TABLE, schema=SCHEMA))
        log.info(f"Created {BQ_TABLE}")


def _safe_str(val) -> str:
    """Convert anything to a safe string for BQ."""
    if val is None:
        return ""
    if isinstance(val, bool):
        return str(val).lower()
    return str(val)


def log_result(result: dict):
    """
    Log a ticket processing result to BigQuery.

    Accepts any dict — unknown keys are silently ignored.
    Missing keys get sensible defaults (empty string, None, False).
    """
    try:
        # Determine subscription_found from cancel_source and status
        cancel_source = result.get("cancel_source") or ""
        status = result.get("status") or ""
        subscription_found = cancel_source not in ("", "none", "unknown", None) and status not in (
            "not_found_anywhere", "not_found_closed",
        )

        row = {
            # Core
            "ticket_id":          str(result.get("ticket_id", "")),
            "email":              result.get("email") or "",
            "status":             status,
            "action":             result.get("action") or "",

            # Classification
            "intent":             result.get("intent") or "",
            "language":           result.get("language") or "",
            "confidence":         (
                float(result["confidence"])
                if result.get("confidence") is not None
                else None
            ),
            "chargeback_risk":    _safe_str(result.get("chargeback_risk", "")),
            "reasoning":          result.get("reasoning") or "",

            # Subscription
            "cancel_source":      cancel_source,
            "subscription_found": subscription_found,
            "subscription_type":  result.get("subscription_type") or "",
            "order_count":        (
                int(result["order_count"])
                if result.get("order_count") is not None
                else None
            ),

            # Reply
            "reply_text":         result.get("reply_text") or "",

            # Flags
            "dry_run":            result.get("dry_run", True),
            "shadow_mode":        result.get("shadow_mode", False),
            "shadow_decision":    result.get("shadow_decision") or "",
            "refund_requested":   result.get("refund_also_requested", False),

            # Refund would-be evaluation (AN-192)
            "refund_decision":       result.get("refund_decision") or "",
            "refund_reason_code":    result.get("refund_reason_code") or "",
            "refund_amount":         result.get("refund_amount"),          # str/NUMERIC or None
            "refund_currency":       result.get("refund_currency") or "",
            "refund_amount_is_split": bool(result.get("refund_amount_is_split", False)),
            "refund_customer_stated_amount": result.get("refund_customer_stated_amount"),
            "refund_source":         result.get("refund_source") or "",
            "refund_charge_id":      result.get("refund_charge_id") or "",
            "refund_charge_type":    result.get("refund_charge_type") or "",
            "refund_engine_version": result.get("refund_engine_version") or "",
            "refund_guard_trail":    result.get("refund_guard_trail") or "",

            # Error
            "error":              result.get("error") or "",

            "logged_at":          datetime.now(timezone.utc).isoformat(),
        }

        errors = _client().insert_rows_json(BQ_TABLE, [row])
        if errors:
            log.warning(f"BQ insert errors: {errors}")
    except Exception as e:
        log.warning(f"BQ log failed (non-critical): {e}")
