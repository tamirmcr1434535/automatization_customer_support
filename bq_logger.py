import os, logging
from datetime import datetime, timezone
from google.cloud import bigquery

log = logging.getLogger("bq")

PROJECT   = os.getenv("GCP_PROJECT", "powerful-vine-426615-r2")
BQ_TABLE  = f"{PROJECT}.zendesk_bot.cancellation_logs"
_bq       = None

SCHEMA = [
    bigquery.SchemaField("ticket_id",       "STRING"),
    bigquery.SchemaField("intent",          "STRING"),
    bigquery.SchemaField("language",        "STRING"),
    bigquery.SchemaField("action",          "STRING"),
    bigquery.SchemaField("status",          "STRING"),
    bigquery.SchemaField("dry_run",         "BOOLEAN"),
    bigquery.SchemaField("error",           "STRING"),
    bigquery.SchemaField("reply_text",      "STRING"),
    bigquery.SchemaField("confidence",      "FLOAT"),
    bigquery.SchemaField("chargeback_risk", "STRING"),
    bigquery.SchemaField("reasoning",       "STRING"),
    bigquery.SchemaField("logged_at",       "TIMESTAMP"),
]

def _client():
    global _bq
    if not _bq:
        _bq = bigquery.Client(project=PROJECT)
    return _bq

def ensure_log_table():
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

def log_result(result: dict):
    try:
        row = {
            "ticket_id":       str(result.get("ticket_id", "")),
            "intent":          result.get("intent") or "",
            "language":        result.get("language") or "",
            "action":          result.get("action") or "",
            "status":          result.get("status") or "",
            "dry_run":         result.get("dry_run", True),
            "error":           result.get("error") or "",
            "reply_text":      result.get("reply_text") or "",
            "confidence":      float(result["confidence"]) if result.get("confidence") is not None else None,
            "chargeback_risk": str(result.get("chargeback_risk") or "").lower() if isinstance(result.get("chargeback_risk"), bool) else (result.get("chargeback_risk") or ""),
            "reasoning":       result.get("reasoning") or "",
            "logged_at":       datetime.now(timezone.utc).isoformat(),
        }
        errors = _client().insert_rows_json(BQ_TABLE, [row])
        if errors:
            log.warning(f"BQ insert errors: {errors}")
    except Exception as e:
        log.warning(f"BQ log failed (non-critical): {e}")
