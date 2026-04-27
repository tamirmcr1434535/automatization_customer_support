"""
Zendesk Cancellation Bot — Google Cloud Function (Gen 2)
=========================================================
Cancellation flow (TRIAL_CANCELLATION / SUB_CANCELLATION only):
 1. Check by Zendesk email → WooCommerce cancel
    └─ Found → cancel in WC → reply confirming cancellation ✅
 2. WC: not found (clean miss, no lookup errors)
    → extract any emails mentioned in ticket body → try each one in WC
    └─ Found by alt email → cancel in WC → reply ✅
 3. Still not found in WC → Stripe fallback by email
    └─ Stripe found and cancelled → reply ✅
 4. Still not found anywhere → Slack escalation only (NO customer reply).
 5. WC lookup error (auth / timeout / 5xx) → Slack escalation only.
 6. All other intents (REFUND, UNKNOWN, DELETE_ACCOUNT, …) → bot does NOT touch
    the ticket (tag + Slack at most; never a public reply).

Guarantee: the bot ONLY sends a public reply when a subscription was
actually located and cancelled. Every error and every not-found path
escalates silently to Slack so a human takes over.

The legacy "ask for last 4 card digits" fallback has been retired —
any ticket still carrying the old awaiting_card_digits* tags is now
silently escalated to Slack instead of re-entering the card-digits
loop.

NOTE: WooCommerce is the primary cancellation target. Stripe is used as
an email-based fallback when WC cannot find the subscription.

FIX-C: Added API health check at startup; UNKNOWN escalation instead of skip;
       confidence gate before _finish_cancellation; validate_reply before sending.
FIX-A: Wired Slack alert callbacks for classifier and reply_generator API failures.
"""

import os
import re
import json
import logging
import functions_framework

from classifier import classify_ticket
from zendesk_client import ZendeskClient, TicketNotWritableError
from woocommerce_client import WooCommerceClient
from stripe_client import StripeClient
from slack_client import SlackClient
from reply_generator import (
    generate_reply,
    validate_reply,
    english_fallback_reply,
)
from bq_logger import log_result as _bq_log_result


# ── Email normalization ────────────────────────────────────────────── #

def _normalize_email(raw: str) -> str:
    """
    Fix common email typos from Zendesk form submissions:
      - consecutive dots in domain  (co..jp → co.jp)
      - leading/trailing dots in domain (.gmail.com → gmail.com)
      - leading/trailing whitespace
      - uppercase → lowercase
    Does NOT touch the local part before @ (dots can be meaningful there).
    Returns empty string if input is clearly invalid.
    """
    raw = raw.strip().lower()
    if not raw or "@" not in raw:
        return raw
    local, domain = raw.rsplit("@", 1)
    # Remove consecutive dots in domain
    while ".." in domain:
        domain = domain.replace("..", ".")
    # Strip leading/trailing dots from domain
    domain = domain.strip(".")
    if not domain or "." not in domain:
        return raw  # don't mangle beyond repair
    return f"{local}@{domain}"


def log_result(result: dict) -> None:
    """
    No-op.

    BQ logging is centralized in the webhook handler, AFTER `_process`
    returns and AFTER `_enrich_result_if_missing` fills in intent /
    confidence / language for tickets that exited early (before the
    classifier ran). This keeps BQ entries symmetric between shadow and
    prod — every row has the same fields regardless of where `_process`
    returned.

    We keep `log_result(result)` calls at every terminal return inside
    `_process` so the code still reads as "log then return", and so that
    if the centralized logger ever needs per-path customization we have
    the hooks in place.
    """
    return

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger("bot")

SHADOW_MODE        = os.getenv("SHADOW_MODE", "false").lower() == "true"
DRY_RUN            = os.getenv("DRY_RUN", "true").lower() == "true"
TEST_MODE          = os.getenv("TEST_MODE", "true").lower() == "true"
TEST_TAG           = "automation_test"

# Pause at the start of _process so the Zendesk-side merger has time to
# consolidate duplicate tickets from the same requester before the bot
# fetches and writes. Without this, the bot can win the race against the
# merger, leaving two parallel threads with the same customer.
MERGE_DELAY_SECONDS = int(os.getenv("MERGE_DELAY_SECONDS", "30"))

# SHADOW_MODE: process ALL tickets, skip ALL writes, send Slack report per ticket.
# Overrides: DRY_RUN=true (no writes), TEST_MODE=false (all tickets), Slack stays live.
if SHADOW_MODE:
    DRY_RUN   = True
    TEST_MODE = False
    logging.info("🔍 SHADOW_MODE enabled — processing all tickets, no writes, Slack reports ON")
HANDLED_INTENTS = {
    "TRIAL_CANCELLATION",
    "SUB_CANCELLATION",
}

# Tags set by the retired card-digits flow. A ticket carrying any of these
# is an old-pipeline leftover — we escalate it to a human instead of
# re-entering the flow, which would have sent a public reply to the customer.
_LEGACY_CARD_DIGITS_TAGS = frozenset({
    "awaiting_card_digits",
    "awaiting_card_digits_retry",
    "card_digits_timeout",
})

# Max order count for bot to handle. Subscriptions with >= this many orders
# are renewals that require manual review (refund assessment, etc.).
MAX_BOT_ORDERS = 3

# ── Cancel signal keywords (used in safety net + refund override guard) ── #
# Rule 1a (updated): if cancel + refund signals both present:
#   - STRONG refund (fraud, explicit refund demand, amount+refund, etc.)
#     → REFUND wins (human must handle the charge dispute)
#   - WEAK refund (just a keyword mention) → CANCEL wins, bot auto-cancels
_CANCEL_SIGNALS = [
    # English
    "cancel", "unsubscribe",
    "cancel my subscription", "cancel subscription", "cancel immediately",
    "stop all future charges", "stop future charges", "stop charging",
    "stop my subscription", "end my subscription",
    # Japanese
    "キャンセル", "解約", "解除", "退会", "止めたい", "やめたい", "取り消",
    "解約したい", "退会したい", "解約してください", "退会してください",
    # Korean
    "취소", "해지", "탈퇴",
    "구독 취소", "구독취소", "해지 요청", "해지요청",
    # German
    "kündigen", "stornieren", "kündigung", "abo kündigen", "abonnement kündigen",
    # French
    "annuler", "annuler mon abonnement", "résilier", "résiliation",
    # Spanish
    "cancelar", "cancelar suscripción", "cancelar mi suscripción", "darse de baja",
    # Dutch — including typo/space variants (e.g. "op zeggen" = "opzeggen")
    "opzeggen", "op zeggen", "beëindigen", "stopzetten", "uitschrijven", "annuleren",
    "abonnement opzeggen", "abonnement annuleren", "abroment",
    # Norwegian
    "avbryte", "avslutte", "kansellere",
    # Swedish
    "avboka", "avsluta", "säga upp",
    # Danish
    "annullere", "opsige",
    # Indonesian
    "batalkan", "hentikan langganan", "berhenti berlangganan",
    # Ukrainian / Russian
    "отменить", "відмінити", "скасувати", "отписаться",
]


def _contains_cancel_signal(text: str) -> bool:
    """Return True if text contains any cancel/stop-subscription keyword."""
    text_lower = text.lower()
    return any(kw in text_lower for kw in _CANCEL_SIGNALS)

ZENDESK_SUBDOMAIN = os.getenv("ZENDESK_SUBDOMAIN", "")

# ── Zendesk "Topic" custom field ──────────────────────────────────────── #
# Set ONLY on successful bot cancellations, so the Topic dropdown in the
# Zendesk UI reflects what the bot actually did (Trial Cancellation or
# Sub Cancellation). Escalations, errors, refunds, renewal reviews etc.
# deliberately leave the field alone — a human will fill it in.
#
# The numeric field id and option tags for the iqbooster Zendesk are
# baked in as defaults (confirmed via /api/v2/ticket_fields.json —
# field_id=16656154392220, type=tagger, options include
# "Trial Cancellation" → trial_cancellation and
# "Sub Cancellation" → sub_cancellation). Env vars still override, so
# a different Zendesk account can redirect to its own field without a
# code change. Set ZENDESK_TOPIC_FIELD_ID="" explicitly to disable.
_ZENDESK_TOPIC_FIELD_ID = os.getenv(
    "ZENDESK_TOPIC_FIELD_ID", "16656154392220",
).strip()

_TOPIC_BY_INTENT: dict[str, str] = {
    "TRIAL_CANCELLATION": os.getenv(
        "ZENDESK_TOPIC_TRIAL_CANCELLATION", "trial_cancellation",
    ),
    "SUB_CANCELLATION": os.getenv(
        "ZENDESK_TOPIC_SUB_CANCELLATION", "sub_cancellation",
    ),
}


def _set_topic_for_intent(ticket_id: str, intent: str) -> None:
    """
    Set the Zendesk Topic custom field after a successful cancellation.

    Only called from the success branch of `_finish_cancellation`, so it only
    fires when the bot actually cancelled the subscription with high
    confidence. Escalation / error / refund paths must NOT call this — we
    don't want the bot deciding the Topic when a human is taking over.

    Silently skipped if ZENDESK_TOPIC_FIELD_ID is not configured or if the
    intent isn't one of the two mapped cancellation intents. Never raises —
    topic is a reporting aid, not part of the cancellation guarantee.
    """
    if not _ZENDESK_TOPIC_FIELD_ID:
        return
    value = _TOPIC_BY_INTENT.get(intent)
    if not value:
        return
    try:
        zendesk.set_custom_field(ticket_id, int(_ZENDESK_TOPIC_FIELD_ID), value)
        log.info(f"[{ticket_id}] Topic set to '{value}' (intent={intent})")
    except Exception as e:
        log.warning(f"[{ticket_id}] Failed to set topic for intent {intent}: {e}")

# ── Webhook deduplication (Firestore-backed distributed lock) ────────── #
# Zendesk fires 5-15 webhooks per ticket (creation, agent reply, tag
# change, status change, etc.). Each webhook is a separate HTTP request.
#
# CRITICAL: this dedup runs in ALL modes (production + shadow), not just
# shadow. Without it, 3-5 concurrent webhooks all pass the tag-based
# guards (because no tag is set yet) → duplicate replies + internal notes.
#
# In-memory dedup only works within a single Cloud Run instance.
# Cloud Functions Gen 2 can spin up MULTIPLE instances → each has its
# own empty Python dict → duplicates leak through.
#
# Fix: Firestore atomic create() as distributed mutex.
#   - create() fails with ALREADY_EXISTS if another instance already claimed
#   - Two-layer approach: in-memory fast path + Firestore distributed lock
#   - Firestore TTL policy auto-deletes old entries (set in GCP Console)
#   - Production uses shorter TTL (5 min) so card-digits re-processing works
#
# One-time Firestore TTL setup (run once in GCP Console or gcloud):
#   gcloud firestore fields ttls update expire_at \
#     --collection-group=webhook_dedup --enable-ttl

import threading as _threading, time as _time
from datetime import datetime, timedelta, timezone

# Layer 1: in-memory cache (fast path, avoids Firestore call for same-instance dupes)
_dedup_seen: dict[str, float] = {}   # ticket_id → timestamp
_dedup_lock = _threading.Lock()
# Shadow: 2h TTL (each ticket processed exactly once).
# Production: 5min TTL (allows re-processing when customer replies later,
# but blocks the 3-5 concurrent webhooks that arrive within seconds).
_DEDUP_TTL = 7200 if SHADOW_MODE else 300

# Layer 2: Firestore (distributed across all Cloud Run instances)
_firestore_db = None

def _get_firestore_db():
    """Lazy-init Firestore client (reused across requests in same instance)."""
    global _firestore_db
    if _firestore_db is None:
        from google.cloud import firestore as _fs
        _firestore_db = _fs.Client()
    return _firestore_db


def _webhook_dedup(ticket_id: str) -> bool:
    """
    Return True if this ticket was already processed (duplicate).
    Two-layer dedup:
      1. In-memory dict — instant, handles same-instance dupes (no network call)
      2. Firestore create() — atomic distributed lock across all instances
         For production: checks if existing doc is expired (manual TTL check)
    """
    now = _time.time()
    tid = str(ticket_id)

    # ── Layer 1: in-memory (fast path, atomic check+claim under lock) ──
    with _dedup_lock:
        if len(_dedup_seen) > 500:
            stale = [k for k, v in _dedup_seen.items() if now - v > _DEDUP_TTL]
            for k in stale:
                del _dedup_seen[k]
        if tid in _dedup_seen and (now - _dedup_seen[tid]) < _DEDUP_TTL:
            return True  # duplicate (same instance hit, within TTL)

    # ── Layer 2: Firestore (distributed lock) ──
    try:
        db = _get_firestore_db()
        doc_ref = db.collection("webhook_dedup").document(tid)
        # Atomic create — raises AlreadyExists if another instance claimed first
        doc_ref.create({
            "ticket_id": tid,
            "created_at": datetime.now(timezone.utc),
            "expire_at": datetime.now(timezone.utc) + timedelta(seconds=_DEDUP_TTL),
        })
        # Success — we are the first instance to claim this ticket
        with _dedup_lock:
            _dedup_seen[tid] = now
        log.info(f"[{tid}] Webhook dedup: claimed in Firestore (first)")
        return False
    except Exception as e:
        err_str = str(e)
        if "already exists" in err_str.lower() or "ALREADY_EXISTS" in err_str:
            # Document exists — check if it's expired (Firestore TTL deletion
            # can be delayed hours, so we check manually for production).
            try:
                doc = doc_ref.get()
                if doc.exists:
                    created_at = doc.to_dict().get("created_at")
                    if created_at:
                        age = (datetime.now(timezone.utc) - created_at).total_seconds()
                        if age > _DEDUP_TTL:
                            # Expired — reclaim (overwrite with new timestamp)
                            doc_ref.set({
                                "ticket_id": tid,
                                "created_at": datetime.now(timezone.utc),
                                "expire_at": datetime.now(timezone.utc) + timedelta(seconds=_DEDUP_TTL),
                            })
                            with _dedup_lock:
                                _dedup_seen[tid] = now
                            log.info(f"[{tid}] Webhook dedup: reclaimed expired doc ({age:.0f}s old)")
                            return False
            except Exception as inner_e:
                log.warning(f"[{tid}] Firestore expiry check failed: {inner_e}")

            # Not expired or check failed — it's a real duplicate
            with _dedup_lock:
                _dedup_seen[tid] = now  # cache for future fast-path
            return True
        # Firestore error (network, permissions, etc.) — fail open with in-memory only
        log.warning(f"[{tid}] Firestore dedup error, falling back to in-memory: {e}")
        with _dedup_lock:
            if tid in _dedup_seen and (now - _dedup_seen[tid]) < _DEDUP_TTL:
                return True
            _dedup_seen[tid] = now
            return False


def _dedup_clear(ticket_id: str):
    """
    Remove dedup lock so future webhooks can re-process this ticket.
    Called for non-terminal states where the bot intentionally deferred
    (e.g. TEST_MODE skip, agent currently handling) and the next webhook
    for the same ticket should get a fresh processing pass.
    """
    tid = str(ticket_id)
    with _dedup_lock:
        _dedup_seen.pop(tid, None)
    try:
        db = _get_firestore_db()
        db.collection("webhook_dedup").document(tid).delete()
        log.info(f"[{tid}] Dedup lock cleared (non-terminal state)")
    except Exception as e:
        log.warning(f"[{tid}] Failed to clear Firestore dedup: {e}")


# ── Clients ──────────────────────────────────────────────────────────── #

zendesk = ZendeskClient(
    subdomain=ZENDESK_SUBDOMAIN,
    email=os.getenv("ZENDESK_EMAIL"),
    api_token=os.getenv("ZENDESK_API_TOKEN"),
    dry_run=DRY_RUN,
    shadow_mode=SHADOW_MODE,
)
woo = WooCommerceClient(
    site_url=os.getenv("WOO_SITE_URL", "https://iqbooster.org"),
    consumer_key=os.getenv("WOO_CONSUMER_KEY", ""),
    consumer_secret=os.getenv("WOO_CONSUMER_SECRET", ""),
    dry_run=DRY_RUN,
)
stripe_cli = StripeClient(
    api_key=os.getenv("STRIPE_SECRET_KEY"),
    dry_run=DRY_RUN,
)
slack = SlackClient(
    bot_token=os.getenv("SLACK_BOT_TOKEN", ""),
    target_email=os.getenv("SLACK_TARGET_EMAIL", ""),
    dry_run=DRY_RUN,  # In SHADOW_MODE: dry_run=True suppresses alerts inside _process
)
# Always-live Slack client used exclusively for the per-ticket post-process
# report emitted at the end of the webhook handler. It is live in BOTH
# shadow and prod so operators can watch every ticket decision in real time
# (the point of shadow is observation; we mirror that in prod during
# rollout so issues are caught immediately).
_report_slack = SlackClient(
    bot_token=os.getenv("SLACK_BOT_TOKEN", ""),
    target_email=os.getenv("SLACK_TARGET_EMAIL", ""),
    dry_run=False,
)

# ── FIX-A: Wire Slack alert callbacks for classifier + reply_generator ── #
# A live Slack client specifically for API failure alerts (always live, not dry_run).
# This ensures we get Slack notifications even when bot runs in DRY_RUN mode.
_alert_slack = SlackClient(
    bot_token=os.getenv("SLACK_BOT_TOKEN", ""),
    target_email=os.getenv("SLACK_TARGET_EMAIL", ""),
    dry_run=False,  # Always live — API failures must always be reported
)

# Dedup: avoid flooding Slack with the same alert on every ticket
_api_alert_sent: set[str] = set()
_api_alert_lock = _threading.Lock()


# ── Startup WooCommerce health check ─────────────────────────────────── #
# Runs once on cold start. If WC credentials are broken (401/403), we
# Slack-alert and exit hard — deploy becomes visibly broken so ops knows
# immediately instead of every incoming ticket silently failing.
# Timeouts / 5xx are logged but do NOT block startup (server is just slow).
# Set SKIP_WC_HEALTHCHECK=true to bypass (useful for local tests).
_WC_HEALTHCHECK_DONE = False

def _run_wc_healthcheck_once() -> None:
    global _WC_HEALTHCHECK_DONE
    if _WC_HEALTHCHECK_DONE:
        return
    _WC_HEALTHCHECK_DONE = True

    if os.getenv("SKIP_WC_HEALTHCHECK", "").lower() == "true":
        log.info("WC health check: SKIP_WC_HEALTHCHECK=true — bypassed")
        return

    log.info("WC health check: GET /customers?per_page=1 ...")
    hc = woo.health_check()
    if hc.get("ok"):
        log.info(f"WC health check: OK — {hc.get('detail')}")
        return

    kind = hc.get("status", "api_error")
    detail = hc.get("detail", "")
    if kind == "auth_error":
        # Hard fail — credentials are broken. Alert and crash.
        log.error(
            f"WC health check: AUTH FAILED ({detail}) — bot cannot function. "
            "Check WOO_CONSUMER_KEY / WOO_CONSUMER_SECRET / WOO_SITE_URL env vars."
        )
        try:
            _alert_slack.notify_startup_failure(
                service="WooCommerce",
                error_kind=kind,
                error_detail=(
                    f"{detail}\n\n"
                    f"site_url={os.getenv('WOO_SITE_URL', 'https://iqbooster.org')}"
                ),
            )
        except Exception:
            log.exception("WC health check: failed to send Slack alert")
        # sys.exit here kills the Cloud Function cold-start. The platform
        # will retry the next request on a fresh instance, but deploy logs
        # make the failure loudly visible.
        import sys as _sys
        _sys.exit(1)
    else:
        # timeout / api_error — don't block startup, but record a warning
        # and Slack-alert so ops knows WC is degraded.
        log.warning(
            f"WC health check: {kind} ({detail}) — WC is degraded but bot will "
            "continue. Ticket lookups may return timeout_error / api_error."
        )
        try:
            _alert_slack.notify_startup_failure(
                service="WooCommerce",
                error_kind=kind,
                error_detail=f"{detail}\n\nBot continued startup (non-auth errors are not fatal).",
            )
        except Exception:
            log.exception("WC health check: failed to send Slack alert (non-fatal)")


# Run health check at import time (module load = cold start on Cloud Functions)
try:
    _run_wc_healthcheck_once()
except SystemExit:
    raise
except Exception:
    log.exception("WC health check: unexpected error (bot will continue)")


def _send_api_failure_alert(error_msg: str):
    """
    FIX-A: Send a Slack alert when Claude API fails (classifier or reply_generator).
    Deduplicates so we don't send 100 identical alerts when API is down.
    """
    # Dedup key: first 80 chars of the error (captures the error type)
    dedup_key = error_msg[:80]
    with _api_alert_lock:
        if dedup_key in _api_alert_sent:
            return  # already alerted
        _api_alert_sent.add(dedup_key)

    log.critical(f"API FAILURE ALERT: {error_msg}")
    try:
        _alert_slack._post(
            f"🚨 *Claude API Failure* | {error_msg[:300]}",
            blocks=[
                {
                    "type": "header",
                    "text": {"type": "plain_text", "text": "🚨 Claude API Failure"},
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            f"*Error:*\n```{error_msg[:500]}```\n\n"
                            "Bot is degraded: classifier returning UNKNOWN, "
                            "translations falling back to English. "
                            "Check ANTHROPIC_API_KEY in Secret Manager and "
                            "Anthropic status page."
                        ),
                    },
                },
                {"type": "divider"},
            ],
        )
    except Exception as e:
        log.error(f"Failed to send API failure Slack alert: {e}")


# Register the callback with classifier and reply_generator
import classifier as _classifier_module
import reply_generator as _reply_generator_module
_classifier_module.set_alert_callback(_send_api_failure_alert)
_reply_generator_module.set_alert_callback(_send_api_failure_alert)

# ── FIX-C: API health check at startup ──────────────────────────────── #
# If the API key is missing, the bot will be in degraded mode from the start.
# Log a critical warning and send a Slack alert immediately.
_ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
if not _ANTHROPIC_API_KEY or not _ANTHROPIC_API_KEY.strip():
    log.critical(
        "⚠️ ANTHROPIC_API_KEY is EMPTY — classifier and reply generator will NOT work. "
        "Bot will operate in degraded mode: keyword safety net only, EN templates only."
    )
    _send_api_failure_alert(
        "ANTHROPIC_API_KEY is empty or not set at startup. "
        "Bot is running in DEGRADED MODE — no AI classification, no translations. "
        "All tickets will use keyword fallback or be escalated to humans."
    )


# ── HTTP handler ──────────────────────────────────────────────────────── #

@functions_framework.http
def zendesk_webhook(request):
    if request.method == "GET":
        return json.dumps({
            "status": "ok", "dry_run": DRY_RUN, "shadow_mode": SHADOW_MODE,
            "test_mode": TEST_MODE, "handles": list(HANDLED_INTENTS),
        }), 200, {"Content-Type": "application/json"}

    if request.method != "POST":
        return "Method not allowed", 405

    try:
        payload = request.get_json(silent=True) or {}
    except Exception:
        return "Invalid JSON", 400

    ticket_id = str(payload.get("ticket_id") or payload.get("id", ""))
    if not ticket_id:
        return "No ticket_id", 400

    log.info(f"[{ticket_id}] Webhook received")

    # ── Webhook deduplication (ALL modes) ─────────────────────────────── #
    # Zendesk fires 5-15 webhooks per ticket (creation, agent reply, tag
    # change, status change — EACH triggers a new webhook).
    # Zendesk tags do NOT work as a lock:
    #   - add_tag itself triggers ANOTHER webhook (infinite loop)
    #   - get_ticket_tags has eventual consistency (stale reads)
    #   - Multiple Cloud Function instances read tags concurrently
    #
    # Fix: Two-layer dedup (in-memory + Firestore distributed lock).
    # Runs in ALL modes — production was missing dedup entirely, causing
    # 3-5 duplicate replies + internal notes per ticket.
    if _webhook_dedup(ticket_id):
        log.info(f"[{ticket_id}] Duplicate webhook — skip")
        return json.dumps({"ticket_id": ticket_id, "status": "skipped_duplicate"}), 200, {"Content-Type": "application/json"}

    try:
        result = _process(ticket_id)
    except TicketNotWritableError as e:
        # The ticket was merged or closed between the bot's initial fetch
        # and a subsequent write (tag / note / reply / status change).
        # This is a benign race — an agent merged the ticket out from
        # under us. Skip cleanly instead of reporting "Exception: 422
        # Client Error: Unprocessable Entity" in Slack.
        cancel_outcome = getattr(e, "cancel_outcome", None)
        if cancel_outcome == "success":
            # Subscription was already cancelled in WC before the merge —
            # preserve that in the report so support sees "cancel succeeded,
            # ticket merged" instead of a bare skip that hides the action.
            log.info(
                f"[{ticket_id}] Ticket was merged AFTER cancel succeeded "
                f"({e.method} {e.url} → 422) — reporting partial success"
            )
            result = {
                "ticket_id": ticket_id,
                "status": "success_zendesk_failed",
                "action": "cancelled_then_merged",
                "cancel_outcome": "success",
                "zendesk_outcome": "merged_mid_flight",
                "zendesk_failed_at": getattr(e, "zendesk_step", "unknown"),
                "subscription_id": getattr(e, "subscription_id", None),
                "reason": (
                    "Subscription was cancelled in WooCommerce, but the "
                    "ticket was merged/closed before the bot could post the "
                    "reply or close it. No bot retry needed; the cancel is "
                    "already done."
                ),
            }
        else:
            log.info(
                f"[{ticket_id}] Ticket was merged/closed mid-flight "
                f"({e.method} {e.url} → 422) — skipping cleanly"
            )
            result = {
                "ticket_id": ticket_id,
                "status": "skipped_merged",
                "reason": (
                    "Ticket was merged or closed by an agent between the "
                    "bot's initial fetch and a subsequent write — no action "
                    "needed from the bot."
                ),
            }
    except Exception as e:
        log.exception(f"[{ticket_id}] Unhandled error: {e}")
        # The per-ticket report below renders status=error + the exception
        # message in a dedicated section, so no separate Slack alert is
        # needed here.
        result = {"ticket_id": ticket_id, "status": "error", "error": str(e)}

    # ── Clear dedup lock for non-terminal states ─────────────────────── #
    # States that expect the customer to reply later need the dedup lock
    # removed so the next webhook can re-process.
    # Terminal states (success, bot_handled, manual_review, error) keep
    # the lock as an extra anti-spam layer.
    _NON_TERMINAL_STATUSES = {
        "skipped_no_test_tag",              # TEST_MODE skip — not a real processing
        "skipped_agent_already_replied",    # agent handling — might change
        # NOTE: "error" intentionally NOT here — clearing dedup on error
        # causes infinite retry loops (especially in shadow mode).
    }
    if result.get("status") in _NON_TERMINAL_STATUSES:
        _dedup_clear(ticket_id)

    # ── Enrich + log + Slack report (symmetric for shadow AND prod) ─── #
    # Every ticket — whether _process ran to completion or exited early —
    # gets classified if intent is missing, then logged once to BQ, and
    # one Slack report per ticket is always emitted (shadow and prod).
    #
    # The per-ticket Slack report is intentionally always-on during the
    # prod rollout so operators can watch every bot decision live. This
    # mirrors the shadow-mode visibility we relied on before. It is
    # independent of the per-decision escalation alerts (refund skip,
    # wc_lookup_error, spam_detected) which continue to fire as before.
    #
    # NOTE: NO Zendesk tag writes here — each add_tag triggers a new
    # webhook, which caused the infinite duplication loop.
    #
    # `skipped_already_handled` stays silent — duplicate webhook, nothing
    # new to report. `skipped_merged` used to be silent too, but operators
    # asked for a visible card in Slack so they can see the bot recognised
    # the merge (vs. it just vanishing from the stream), so it now runs
    # the normal enrich + BQ + Slack path with the 🔀 emoji.
    _SKIP_POST_PROCESS = {"skipped_already_handled"}
    if result.get("status") not in _SKIP_POST_PROCESS:
        # 1. Enrich: classify tickets that hit early exits (before classifier)
        _enrich_result_if_missing(ticket_id, result)

        # 2. Always record the run mode on the result so BQ rows can be
        # filtered by `shadow_mode` regardless of the branch that set them.
        shadow_tag = _shadow_tag_for_status(result.get("status", ""))
        result["shadow_mode"] = SHADOW_MODE
        result["shadow_decision"] = shadow_tag.replace("shadow_", "")

        # 3. Log enriched result to BQ (authoritative entry, prod + shadow)
        try:
            _bq_log_result(result)
        except Exception:
            log.exception(f"[{ticket_id}] BQ log failed")

        # 4. Send ONE Slack report per ticket (prod + shadow).
        try:
            _report_slack.notify_ticket_result(
                ticket_id=ticket_id,
                result=result,
                zendesk_subdomain=ZENDESK_SUBDOMAIN,
                shadow=SHADOW_MODE,
            )
        except Exception:
            log.exception(f"[{ticket_id}] Failed to send per-ticket Slack report")

        # NOTE: no zendesk.add_tag here — tags trigger new webhooks!

    return json.dumps(result), 200, {"Content-Type": "application/json"}


# ── Shadow mode helpers ───────────────────────────────────────────────── #

_SHADOW_STATUS_TO_TAG = {
    "success":                      "shadow_would_cancel",
    "success_zendesk_failed":       "shadow_would_cancel_zendesk_partial",
    "success_en_fallback":          "shadow_would_cancel_en_fallback",
    "manual_review_required":       "shadow_would_escalate",
    "escalated_low_confidence":     "shadow_would_escalate",
    "escalated_delete_account":     "shadow_would_escalate",
    "escalated_explanation_question":"shadow_would_escalate",
    "escalated_no_results_received":"shadow_would_escalate",
    "skipped_refund_request":       "shadow_would_skip_refund",
    "escalated_unknown":            "shadow_would_escalate",     # FIX-C: new status
    "skipped_not_handled":          "shadow_would_skip",
    "skipped_followup":             "shadow_would_skip",
    "skipped_closed":               "shadow_would_skip",
    "escalated_not_found":          "shadow_would_escalate",
    "wc_lookup_error":              "shadow_would_escalate_wc_error",
    "escalated_legacy_card_digits": "shadow_would_escalate_legacy",
    "skipped_agent_already_replied":"shadow_agent_handling",
    "skipped_merge_candidate":      "shadow_merge_candidate",
    "skipped_spam_detected":        "shadow_spam",
    "error":                        "shadow_error",
}

def _shadow_tag_for_status(status: str) -> str:
    """Map a processing status to a shadow decision tag for daily comparison."""
    return _SHADOW_STATUS_TO_TAG.get(status, "shadow_other")


def _enrich_result_if_missing(ticket_id: str, result: dict) -> None:
    """
    If `_process` exited before the classifier ran (e.g. duplicate webhook,
    follow-up, refund in subject, agent already replied), re-fetch the
    ticket and classify it so the BQ log has intent / confidence / language
    for every ticket.

    Runs for BOTH shadow and prod — keeps BQ rows symmetric between the
    two modes. Previously this only ran in shadow, which meant prod rows
    for early-exit tickets lacked classification and could not be
    compared against shadow rows for the same ticket.
    """
    if result.get("confidence") is not None:
        return  # already classified — nothing to enrich

    try:
        ticket = zendesk.get_ticket(ticket_id)
        if not ticket:
            return
        subject = ticket.get("subject", "")
        body = ticket.get("description", "")
        # Same logic as _process: Messaging tickets have agent greeting as description,
        # so always fetch ALL customer comments for classification (not just the first).
        _is_msg = subject.lower().startswith("conversation with")
        if _is_msg or len(body.strip()) < 30:
            all_customer_text = zendesk.get_all_customer_comments_text(ticket_id)
            if all_customer_text:
                body = all_customer_text
            else:
                first_comment = zendesk.get_first_customer_comment(ticket_id)
                if first_comment:
                    body = first_comment

        classification = classify_ticket(subject, body)
        # Keep existing intent if it was already set (e.g. REFUND_REQUEST from keyword check)
        result.update({
            "intent": result.get("intent") or classification["intent"],
            "language": classification["language"],
            "confidence": classification["confidence"],
            "reasoning": classification.get("reasoning", ""),
            "chargeback_risk": classification.get("chargeback_risk", ""),
        })
        log.info(
            f"[{ticket_id}] Shadow enriched: {classification['intent']} "
            f"({classification['confidence']:.0%}) lang={classification['language']}"
        )
    except Exception:
        log.exception(f"[{ticket_id}] Shadow enrichment failed")


# ── Core logic ────────────────────────────────────────────────────────── #

def _process(ticket_id: str) -> dict:
    result = {
        "ticket_id": ticket_id,
        "status": "skipped",
        "intent": None,
        "language": None,
        "action": None,
        "dry_run": DRY_RUN,
        "cancel_source": None,
        "email": None,
    }

    # Give the Zendesk-side merger time to fold duplicate tickets from
    # the same requester into a single thread before we fetch and write.
    # The merge tag check below will then short-circuit any ticket that
    # got merged during the wait.
    if MERGE_DELAY_SECONDS > 0:
        log.info(
            f"[{ticket_id}] Waiting {MERGE_DELAY_SECONDS}s for merger to settle"
        )
        _time.sleep(MERGE_DELAY_SECONDS)

    # 1. Fetch ticket
    ticket = zendesk.get_ticket(ticket_id)
    if not ticket:
        log.warning(f"[{ticket_id}] Not found in Zendesk")
        result["status"] = "not_found"
        return result

    subject    = ticket.get("subject", "")
    body       = ticket.get("description", "")
    tags       = ticket.get("tags", [])
    ticket_status = ticket.get("status", "open")  # FIX: used for pending-check anti-spam
    requester  = ticket.get("requester", {})
    raw_email  = requester.get("email", "")
    email      = _normalize_email(raw_email)
    name       = requester.get("name", "")
    result["email"] = email

    if email != raw_email:
        log.warning(f"[{ticket_id}] Email normalized: {raw_email!r} → {email!r}")

    log.info(f"[{ticket_id}] Subject: {subject[:60]} | Email: {email}")

    # ── 2. Guard checks (cheapest first, no API calls) ──────────────── #

    # 2a. Idempotency — bot_handled tag blocks all re-processing.
    # Must be FIRST to prevent duplicate Slack alerts from parallel webhooks.
    if "bot_handled" in tags:
        log.info(f"[{ticket_id}] Already handled by bot (tag: bot_handled) — skipping")
        result["status"] = "skipped_already_handled"
        return result

    # 2b. Skip merged-away tickets.
    if "merge" in tags:
        log.info(f"[{ticket_id}] Skipping — ticket was merged into another (tag: merge)")
        result["status"] = "skipped_merged"
        return result

    if ticket_status == "closed":
        log.info(f"[{ticket_id}] Skipping — ticket status is 'closed' (likely merged)")
        result["status"] = "skipped_closed"
        return result

    # ── 2c. Early subject/body signals (still no API calls) ──────────── #

    # Follow-up ticket detection.
    # Zendesk auto-prepends "This is a follow-up to your previous request #XXXXX"
    # when a customer replies to a closed/solved ticket. These are escalations
    # that an agent already touched — the bot must not interfere.
    _FOLLOWUP_SIGNALS = [
        "this is a follow-up to your previous request",
        "follow-up to your previous request",
        "follow up to your previous request",
        "following up on my previous request",
        "following up on ticket",
        "in reference to ticket",
        "regarding my previous request",
    ]
    body_lower_early = body.lower()
    if any(sig in body_lower_early for sig in _FOLLOWUP_SIGNALS):
        log.info(
            f"[{ticket_id}] Follow-up ticket detected (references previous request) "
            "→ skipping, sending Slack alert for manual review"
        )
        zendesk.add_tag(ticket_id, "bot_handled")  # block parallel webhook
        result["status"] = "skipped_followup"
        result["intent"] = "FOLLOWUP"
        result["reason"] = "Follow-up to a previous request — agent already handled or will handle this thread"
        log_result(result)
        return result

    # Subject refund check — if the email subject itself contains refund
    # keywords, this is an escalation/dispute and should go straight to a human.
    if _contains_refund_request(subject):
        log.info(
            f"[{ticket_id}] Refund keyword in subject line: '{subject[:60]}' "
            "→ skipping, sending Slack alert"
        )
        zendesk.add_tag(ticket_id, "bot_handled")  # block parallel webhook
        result["intent"] = "REFUND_REQUEST"
        result["status"] = "skipped_refund_request"
        result["reason"] = "Refund keyword detected in subject — refund disputes require a human"
        log_result(result)
        return result

    # 2c. Skip if a human agent already replied publicly.
    # If the last public comment is from our team, they are already handling it —
    # no need for the bot to classify, escalate, or interfere.
    if zendesk.last_public_comment_is_from_agent(ticket_id):
        log.info(
            f"[{ticket_id}] Last public comment is from an agent — "
            "skipping (human already replied)"
        )
        result["status"] = "skipped_agent_already_replied"
        return result

    # 2c-bis. Merge-candidate guard.
    # If the customer already has ANOTHER active (new/open/pending/hold)
    # ticket within the last 14 days, this new one is almost certainly a
    # follow-up that a human will merge into the existing thread (cf.
    # #103787 → merged into #103735). If the bot tags / adds notes /
    # escalates, those writes land on a ticket that is about to disappear
    # into the parent, confuse agents, and steal merge authorship from
    # Volodymyr et al. — so we stay completely hands-off here: NO tags,
    # NO internal notes, NO reply. Just the one-per-ticket Slack report
    # emitted by the webhook handler, which now carries the sibling ids.
    if email:
        active_siblings = zendesk.find_active_tickets_for_email(
            email, exclude_ticket_id=ticket_id, days=14,
        )
        if active_siblings:
            sibling_ids = [str(t.get("id")) for t in active_siblings if t.get("id")]
            sibling_subjects = [
                (t.get("subject") or "")[:80] for t in active_siblings
            ]
            log.info(
                f"[{ticket_id}] Not first ticket from {email} — "
                f"{len(sibling_ids)} other active ticket(s) open: "
                f"{', '.join('#' + s for s in sibling_ids)}. "
                "Skipping bot action so humans can merge."
            )
            result["status"] = "skipped_merge_candidate"
            result["intent"] = "MERGE_CANDIDATE"
            result["active_siblings"] = sibling_ids
            result["reason"] = (
                f"Not the first ticket from {email}. Other active: "
                + ", ".join(f"#{sid}" for sid in sibling_ids)
                + ". Bot stayed hands-off — please merge manually."
            )
            if sibling_subjects:
                result["sibling_subjects"] = sibling_subjects
            log_result(result)
            return result

    # 2d. Spam detection — if bot already replied 2+ times, stop and alert.
    # Tag guard prevents duplicate Slack alerts when Zendesk fires webhook twice
    # simultaneously (both would otherwise see reply_count >= 2 and both alert).
    if "bot_spam_guard" in tags:
        log.info(f"[{ticket_id}] Spam guard tag present — skipping duplicate spam webhook")
        result["status"] = "skipped_spam_detected"
        return result

    bot_reply_count = zendesk.count_bot_replies(ticket_id)
    if bot_reply_count >= 2:
        log.warning(
            f"[{ticket_id}] Bot already replied {bot_reply_count} times — "
            "possible spam loop, skipping and alerting"
        )
        # Race condition guard: re-fetch tags to check if a parallel webhook
        # already set bot_spam_guard (same pattern as bot_handled checks).
        current_tags = zendesk.get_ticket_tags(ticket_id)
        if "bot_spam_guard" in current_tags:
            log.info(f"[{ticket_id}] Spam guard: parallel webhook already handled — skip")
            result["status"] = "skipped_spam_detected"
            return result

        zendesk.add_tag(ticket_id, "bot_spam_guard")  # blocks other webhook re-fires
        result["status"] = "skipped_spam_detected"
        result["reply_count"] = bot_reply_count
        result["reason"] = (
            f"Bot has already posted {bot_reply_count} replies to this ticket — "
            "possible webhook loop, stopping and escalating"
        )
        log_result(result)
        return result

    # 3. TEST_MODE gate
    if TEST_MODE and TEST_TAG not in tags:
        log.info(f"[{ticket_id}] Skip — test mode, missing tag '{TEST_TAG}'")
        result["status"] = "skipped_no_test_tag"
        log_result(result)
        return result

    # 2b. Messaging/chat tickets: description = agent auto-greeting, NOT customer message.
    # For Messaging tickets (subject "Conversation with ..."), ALWAYS fetch ALL
    # customer comments — the first one may be just a form submission ("Unsubscribe")
    # while follow-ups contain the real intent ("refund 39900 won").
    # For other tickets, fallback if description is very short (< 30 chars).
    #
    # FIX: Messaging conversations arrive in bursts — the customer submits a form,
    # then immediately types 1-2 follow-up messages with details. The webhook fires
    # on ticket creation when only the first message exists. We wait briefly so
    # follow-up messages are available via the Zendesk API.
    _is_messaging = subject.lower().startswith("conversation with")
    if _is_messaging or len(body.strip()) < 30:
        # Delay for messaging tickets to capture follow-up messages
        if _is_messaging:
            import time as _t
            _MESSAGING_DELAY = int(os.getenv("MESSAGING_CLASSIFY_DELAY_SEC", "45"))
            log.info(
                f"[{ticket_id}] Messaging ticket — waiting {_MESSAGING_DELAY}s "
                f"for follow-up messages before classification"
            )
            _t.sleep(_MESSAGING_DELAY)

        # Fetch ALL customer comments (oldest first), not just the first one.
        # This captures follow-up messages that clarify intent (e.g. refund requests
        # that arrive after the initial "Unsubscribe" form submission).
        all_customer_text = zendesk.get_all_customer_comments_text(ticket_id)
        if all_customer_text:
            if _is_messaging:
                log.info(
                    f"[{ticket_id}] Messaging ticket — using ALL customer comments "
                    f"for classification ({len(all_customer_text)} chars)"
                )
            else:
                log.info(
                    f"[{ticket_id}] Short description — using ALL customer comments "
                    f"for classification"
                )
            body = all_customer_text
        else:
            # Fallback: if no comments found, try first comment only
            first_comment = zendesk.get_first_customer_comment(ticket_id)
            if first_comment:
                body = first_comment
                log.info(f"[{ticket_id}] No aggregated comments — using first customer comment")

    # 3. Classify (needed for language even in card-lookup flows)
    classification = classify_ticket(subject, body)
    intent     = classification["intent"]
    language   = classification["language"]
    confidence = classification["confidence"]

    # 3a. Cancellation-verification questions ("I cancelled, did it work?"
    # / "解約できていますか" / "취소가 되었나요") — route as a normal
    # cancellation: look up the sub and reply. If the sub is already
    # cancelled, the standard "your subscription has been cancelled"
    # reply doubles as a verification confirmation. If the sub is
    # somehow still active, the bot cancels it (which is what the
    # customer thought they had already done). If WC has no record at
    # all, the not_found_anywhere path escalates to a human (no card-
    # digits prompt, since the legacy card-digits flow was retired).
    if intent == "CANCELLATION_VERIFICATION":
        log.info(
            f"[{ticket_id}] CANCELLATION_VERIFICATION → handling as "
            "TRIAL_CANCELLATION (verify-and-confirm flow)"
        )
        result["original_intent"] = "CANCELLATION_VERIFICATION"
        result["is_verification_request"] = True
        intent = "TRIAL_CANCELLATION"

    # 3b. Safety net: if classifier returned UNKNOWN but body contains clear cancel/refund
    # signals, override to prevent valid tickets from being silently skipped.
    # This catches edge cases where Claude misclassifies (e.g. image attachment noise,
    # short body with strong signals like "解約したのに...返金してほしい").
    if intent == "UNKNOWN":
        full_text_lower = (subject + " " + body).lower()
        _DELETE_ACCOUNT_KEYWORD_FALLBACK = [
            "delete my account", "delete account", "remove my account",
            "close my account", "deactivate my account",
            "アカウント削除", "アカウントの削除", "アカウントを削除",
            "アカウントを消して", "アカウントを消去",
            "계정 삭제", "계정삭제",
            # Korean — 탈퇴 (withdrawal/leave) when paired with 계정/회원 means
            # "delete account", not just "cancel subscription". The bare 탈퇴
            # is in _CANCEL_SIGNALS, but these multi-word forms are stronger
            # delete-account signals and should win.
            "계정 탈퇴", "계정탈퇴",
            "회원 탈퇴", "회원탈퇴",
            "탈퇴해주세요", "탈퇴 해주세요",
            "탈퇴하고 싶", "탈퇴하고싶",
            "konto löschen", "supprimer mon compte",
            "видалити акаунт", "удалить аккаунт",
            "account verwijderen",
            # Spanish
            "eliminar mi cuenta", "eliminar cuenta", "borrar mi cuenta",
            "borrar cuenta", "cerrar mi cuenta", "cerrar cuenta",
            "eliminar mi información", "eliminar mi informacion",
            "eliminar mi información de pago", "eliminar mi informacion de pago",
            "eliminar mis datos", "eliminar datos personales",
            "borrar mis datos", "borrar mi información", "borrar mi informacion",
        ]
        _REFUND_KEYWORD_FALLBACK = [
            "refund", "返金", "払い戻し", "クーリングオフ", "お金を返して", "geld zurück",
            "rückerstattung", "widerruf", "remboursement", "환불", "reembolso", "возврат",
            "rimborso", "money back", "chargeback",
        ]
        has_cancel = any(kw in full_text_lower for kw in _CANCEL_SIGNALS)
        has_delete = any(kw in full_text_lower for kw in _DELETE_ACCOUNT_KEYWORD_FALLBACK)
        has_refund = any(kw in full_text_lower for kw in _REFUND_KEYWORD_FALLBACK)

        # ORDER MATTERS: refund > delete-account > cancel.
        # Refund and delete-account both end in human escalation, while
        # cancel auto-handles. Phrases like "계정 탈퇴 해주세요" contain the
        # bare cancel signal "탈퇴" but the customer's actual intent is
        # account deletion — must NOT auto-cancel + reply. Same for any
        # ticket that mixes a refund word with a cancel word.
        if has_refund:
            log.info(
                f"[{ticket_id}] Classifier returned UNKNOWN but refund signal found "
                "→ overriding to REFUND_REQUEST (safety net)"
            )
            intent = "REFUND_REQUEST"
            classification["intent"] = intent
            classification["reasoning"] = (
                f"classifier fallback: UNKNOWN overridden — "
                f"refund keyword detected in body"
            )
        elif has_delete:
            log.info(
                f"[{ticket_id}] Classifier returned UNKNOWN but delete-account signal found "
                "→ overriding to DELETE_ACCOUNT (safety net)"
            )
            intent = "DELETE_ACCOUNT"
            classification["intent"] = intent
            classification["reasoning"] = (
                f"classifier fallback: UNKNOWN overridden — "
                f"delete-account keyword detected in body"
            )
        elif has_cancel:
            log.info(
                f"[{ticket_id}] Classifier returned UNKNOWN but cancel signal found "
                "→ overriding to TRIAL_CANCELLATION (safety net)"
            )
            intent = "TRIAL_CANCELLATION"
            classification["intent"] = intent
            classification["reasoning"] = (
                f"classifier fallback: UNKNOWN overridden — "
                f"cancel keyword detected in body"
            )
        elif email:
            # No signals in body — check sibling/merged tickets from same requester.
            # Merged tickets often have empty/stub body while the original has the
            # actual message. Also catches contact-form tickets where the body is
            # wrapped in form metadata that obscures the customer's intent.
            try:
                sibling_tickets = zendesk.search_tickets(
                    f"requester:{email} type:ticket created>-7days"
                )
                sibling_texts = []
                for t in sibling_tickets:
                    if str(t.get("id")) != str(ticket_id):
                        sibling_texts.append(
                            (t.get("subject") or "") + " "
                            + (t.get("description") or "")[:500]
                        )
                if sibling_texts:
                    combined = " ".join(sibling_texts).lower()
                    sib_cancel = any(kw in combined for kw in _CANCEL_SIGNALS)
                    sib_refund = any(kw in combined for kw in _REFUND_KEYWORD_FALLBACK)
                    sib_strong_refund = _contains_strong_refund_signal(" ".join(sibling_texts))

                    if sib_cancel and sib_strong_refund:
                        log.info(
                            f"[{ticket_id}] UNKNOWN safety net: sibling tickets "
                            f"from {email} have cancel + strong refund → REFUND_REQUEST"
                        )
                        intent = "REFUND_REQUEST"
                        classification["intent"] = intent
                        classification["reasoning"] = (
                            "classifier fallback: UNKNOWN overridden — "
                            "sibling ticket has cancel + strong refund signals"
                        )
                    elif sib_cancel:
                        log.info(
                            f"[{ticket_id}] UNKNOWN safety net: sibling tickets "
                            f"from {email} have cancel signal → TRIAL_CANCELLATION"
                        )
                        intent = "TRIAL_CANCELLATION"
                        classification["intent"] = intent
                        classification["reasoning"] = (
                            "classifier fallback: UNKNOWN overridden — "
                            "sibling ticket has cancel keyword"
                        )
                    elif sib_refund:
                        log.info(
                            f"[{ticket_id}] UNKNOWN safety net: sibling tickets "
                            f"from {email} have refund signal → REFUND_REQUEST"
                        )
                        intent = "REFUND_REQUEST"
                        classification["intent"] = intent
                        classification["reasoning"] = (
                            "classifier fallback: UNKNOWN overridden — "
                            "sibling ticket has refund keyword"
                        )
            except Exception:
                log.warning(
                    f"[{ticket_id}] UNKNOWN cross-ticket lookup failed — ignoring"
                )

    result.update({
        "intent": intent,
        "language": language,
        "confidence": confidence,
        "chargeback_risk": classification.get("chargeback_risk", ""),
        "reasoning": classification.get("reasoning", ""),
    })
    log.info(f"[{ticket_id}] Intent: {intent} ({confidence:.0%}) | Lang: {language}")

    # ── 3c. Refund keyword override ──────────────────────────────────────── #
    # If the classifier returned a handled intent (TRIAL / SUB cancellation)
    # but the customer text contains refund keywords (返金, refund, etc.)
    # → override to REFUND_REQUEST and skip.  Refund tickets always need
    # human review; the bot should not auto-cancel when a refund is requested.
    #
    # Check subject + body + ALL customer comments — some help-form tickets
    # have a generic description but the actual refund complaint is only in
    # the first customer comment or follow-up replies.
    _all_text_for_refund = subject + " " + body
    _customer_text_only = body  # body + comments, WITHOUT subject (for cancel check)
    if intent in HANDLED_INTENTS:
        try:
            all_comments = zendesk.get_all_customer_comments_text(ticket_id)
            if all_comments:
                _all_text_for_refund += " " + all_comments
                _customer_text_only += " " + all_comments
        except Exception:
            log.warning(f"[{ticket_id}] Failed to fetch comments for refund check")
    _has_refund_kw = intent in HANDLED_INTENTS and _contains_refund_request(_all_text_for_refund)
    # IMPORTANT: check cancel signals in BODY + COMMENTS only (not subject).
    # Subjects like "Re: Your Subscription Cancellation Code" are system-generated
    # and contain "cancel" even when the customer's actual request is a pure refund.
    _has_cancel_kw = _contains_cancel_signal(_customer_text_only) if _has_refund_kw else False

    _has_strong_refund = (
        _has_refund_kw
        and _has_cancel_kw
        and _contains_strong_refund_signal(_all_text_for_refund)
    )

    if _has_refund_kw:
        # ANY refund signal (weak or strong, with or without cancel) → human must handle.
        # Bot never auto-cancels when customer mentions refund — even casually.
        # This is the safe default: refund = money question = human decides.
        _refund_context = (
            "strong refund" if _has_strong_refund else "weak refund"
        ) + (
            " + cancel signal" if _has_cancel_kw else ", no cancel signal"
        )
        log.info(
            f"[{ticket_id}] {intent}: refund keywords detected ({_refund_context}) "
            "→ overriding to REFUND_REQUEST (human must handle any refund)"
        )
        intent = "REFUND_REQUEST"
        result["intent"] = intent
        result["status"] = "skipped_refund_request"
        result["reason"] = (
            f"Refund keywords detected in body ({_refund_context}) — human must handle any refund"
        )
        zendesk.add_tag(ticket_id, "bot_handled")
        log_result(result)
        return result

    # ── Explanation-question override ────────────────────────────────────── #
    # Customer asks to cancel AND asks "what is this charge / payment?" —
    # they don't recognise something they were charged (or nearly charged)
    # for. Auto-cancelling is not enough: an agent must explain the charge
    # before closing the ticket. Seen in real tickets like:
    #   "I paid 199 yen — is it a subscription? If so, cancel it. Also
    #    1990 yen was nearly debited, what is this?"
    # The cancel part is clear, but "what is 1990 yen?" can't be answered
    # by the bot.
    if intent in HANDLED_INTENTS and _contains_explanation_question(_all_text_for_refund):
        log.info(
            f"[{ticket_id}] {intent}: customer asks explanation question about a "
            f"charge → escalating to human (bot cannot identify the charge)"
        )

        current_tags = zendesk.get_ticket_tags(ticket_id)
        if "bot_handled" in current_tags:
            log.info(f"[{ticket_id}] Race condition: bot_handled already set — skip")
            result["status"] = "skipped_race_condition"
            return result

        zendesk.add_tag(ticket_id, "bot_handled")
        zendesk.add_tag(ticket_id, "needs_manual_review")
        zendesk.add_tag(ticket_id, "ai_bot_failed")
        zendesk.add_internal_note(
            ticket_id,
            f"🤖 Bot: customer asks an explanation question about a charge "
            f"(\"what is this?\" / \"что это?\" / \"これなに?\") alongside a "
            f"{intent} request.\n\n"
            f"Auto-cancelling would leave the customer's real question "
            f"(\"what is this payment?\") unanswered. Please identify the "
            f"charge(s) they are asking about and reply manually.",
        )
        zendesk.set_open(ticket_id)
        result.update({
            "status": "escalated_explanation_question",
            "action": "escalated_to_agent_explanation_question",
            "reason": (
                "Customer asks 'what is this charge?' alongside cancel request — "
                "a human must identify the charge before replying."
            ),
        })
        log_result(result)
        return result

    # ── "No results received" override ──────────────────────────────────── #
    # Customer says they haven't received their IQ test results / full
    # report. Even when combined with "please cancel", the bot's generic
    # cancellation reply leaves the missing-delivery complaint unanswered
    # and often turns into a refund dispute later. Policy: any such
    # phrasing → human must look at the account and explain/deliver
    # before closing the ticket.
    if intent in HANDLED_INTENTS and _contains_no_results_received_complaint(_all_text_for_refund):
        log.info(
            f"[{ticket_id}] {intent}: customer says they have not received "
            f"their results → escalating to human (delivery complaint)"
        )

        current_tags = zendesk.get_ticket_tags(ticket_id)
        if "bot_handled" in current_tags:
            log.info(f"[{ticket_id}] Race condition: bot_handled already set — skip")
            result["status"] = "skipped_race_condition"
            return result

        zendesk.add_tag(ticket_id, "bot_handled")
        zendesk.add_tag(ticket_id, "needs_manual_review")
        zendesk.add_tag(ticket_id, "ai_bot_failed")
        zendesk.add_internal_note(
            ticket_id,
            f"🤖 Bot: customer says they have not received their IQ test "
            f"results / full report alongside a {intent} request.\n\n"
            f"Auto-cancelling would ignore the missing-delivery complaint. "
            f"Please check the account: was the result ever delivered, "
            f"and reply to the customer manually before closing.",
        )
        zendesk.set_open(ticket_id)
        result.update({
            "status": "escalated_no_results_received",
            "action": "escalated_to_agent_no_results_received",
            "reason": (
                "Customer says they have not received their results — "
                "human must investigate delivery before replying."
            ),
        })
        log_result(result)
        return result

    # ── REFUND / PAYMENT DISPUTE intents ─────────────────────────────────── #
    #
    # Policy: CANCELLATION IS ALWAYS THE PRIORITY.
    #
    # If the customer asks to cancel AND mentions refund → cancel first.
    # The refund part can be handled by a human afterwards.
    # Only skip if the ticket is a PURE payment dispute with zero cancel intent.
    #
    _PURE_DISPUTE_INTENTS = {
        "CHARGEBACK_THREAT",  # customer threatening / filing chargeback
        "PAYPAL_DISPUTE",     # PayPal dispute already opened
    }
    if intent in _PURE_DISPUTE_INTENTS:
        log.info(f"[{ticket_id}] {intent} — skipping (active payment dispute, human must handle)")
        result["status"] = "skipped_refund_request"
        result["reason"] = f"{intent} — active payment dispute, human must handle"
        zendesk.add_tag(ticket_id, "bot_handled")  # block parallel webhook
        log_result(result)
        return result

    # REFUND_REQUEST / SUB_RENEWAL_REFUND — always human.
    # Any refund intent = money question = human decides. No exceptions.
    if intent in ("REFUND_REQUEST", "SUB_RENEWAL_REFUND"):
        log.info(
            f"[{ticket_id}] {intent} — refund intent, always escalate to human"
        )
        result["status"] = "skipped_refund_request"
        result["reason"] = f"{intent} — refund intents always go to a human"
        zendesk.add_tag(ticket_id, "bot_handled")
        log_result(result)
        return result

    # ── LEGACY CARD DIGITS CLEANUP ──────────────────────────────────── #
    # ── Legacy card-digits tickets — hard escalate, NO customer reply ──── #
    # The old "ask for last 4 card digits" flow has been retired. Any ticket
    # still carrying these tags is an old-pipeline leftover. We do NOT re-enter
    # the card-digits loop (which could send a public reply); we tag the
    # ticket for manual review and Slack-alert a human.
    #
    # Non-cancellation intents get silently skipped (no tags, no Slack) so
    # unrelated re-openings of old tickets don't generate noise.
    if any(t in _LEGACY_CARD_DIGITS_TAGS for t in tags):
        if intent not in HANDLED_INTENTS:
            log.info(
                f"[{ticket_id}] Legacy card-digits tag + non-cancel intent "
                f"({intent}) — silent skip"
            )
            result["status"] = "skipped_not_handled"
            log_result(result)
            return result
        return _escalate_legacy_card_digits_ticket(ticket_id, email, tags, result)

    # ── NORMAL CANCELLATION FLOW ──────────────────────────────────────── #

    # 3d. DELETE_ACCOUNT — customer wants account/data deletion (GDPR/privacy).
    # Bot cannot handle this automatically — escalate to human agent.
    if intent == "DELETE_ACCOUNT":
        log.info(f"[{ticket_id}] DELETE_ACCOUNT — escalating to agent for data deletion")

        current_tags = zendesk.get_ticket_tags(ticket_id)
        if "bot_handled" in current_tags:
            log.info(f"[{ticket_id}] Race condition: bot_handled already set — skip")
            result["status"] = "skipped_race_condition"
            return result

        zendesk.add_tag(ticket_id, "bot_handled")
        zendesk.add_tag(ticket_id, "delete_account")
        zendesk.add_tag(ticket_id, "needs_manual_review")
        zendesk.add_internal_note(
            ticket_id,
            f"🤖 Bot: customer requests account deletion (DELETE_ACCOUNT, "
            f"confidence {confidence:.0%}). Requires manual handling — "
            f"data deletion per privacy policy.",
        )
        zendesk.set_open(ticket_id)
        result.update({
            "status": "escalated_delete_account",
            "action": "escalated_to_agent_delete_account",
            "reason": "Customer requests account/data deletion — handled per privacy policy",
        })
        log_result(result)
        return result

    # 4. FIX-C: UNKNOWN after all safety nets → escalate to human, don't silently skip.
    # The safety net above tried keyword matching and sibling ticket cross-referencing.
    # If intent is STILL UNKNOWN, a human must look at this ticket.
    if intent == "UNKNOWN":
        log.warning(
            f"[{ticket_id}] Intent UNKNOWN after all safety nets — escalating to human"
        )
        zendesk.add_tag(ticket_id, "bot_handled")       # mark that bot touched it
        zendesk.add_tag(ticket_id, "needs_manual_review")
        zendesk.add_tag(ticket_id, "ai_bot_failed")
        zendesk.add_internal_note(
            ticket_id,
            f"🤖 Bot: could not determine intent (UNKNOWN after safety nets).\n"
            f"Confidence: {confidence:.0%}\n"
            f"Reasoning: {classification.get('reasoning', 'N/A')}\n\n"
            f"Please review this ticket manually."
        )
        zendesk.set_open(ticket_id)
        result["status"] = "escalated_unknown"
        result["reason"] = (
            f"UNKNOWN intent after all safety nets — confidence={confidence:.0%}, "
            f"reasoning: {classification.get('reasoning', 'N/A')}"
        )
        log_result(result)
        return result

    # 4b. Skip other unhandled intents (GENERAL_QUESTION, EXPLANATION, SPAM, etc.)
    if intent not in HANDLED_INTENTS:
        log.info(f"[{ticket_id}] Skip — not a cancellation ({intent})")
        result["status"] = "skipped_not_handled"
        log_result(result)
        return result

    # 5. Low confidence → always escalate to human.
    # Threshold: 80%. If the classifier is not confident enough, a human must review.
    # The bot tells the agent what it THINKS the intent is, so they have a head start.
    # No auto-actions below 80% — strict rule, can be softened later with keyword boost.
    if confidence < 0.80:
        # Build a hint for the agent: what did the bot detect + any keyword signals
        _keyword_hint_parts = []
        if _contains_cancel_signal(subject + " " + body):
            _keyword_hint_parts.append("cancel keyword found in text")
        if _contains_refund_request(subject + " " + body):
            _keyword_hint_parts.append("refund keyword found in text")
        _keyword_hint = (" | " + ", ".join(_keyword_hint_parts)) if _keyword_hint_parts else ""

        log.info(
            f"[{ticket_id}] Low confidence {confidence:.0%} → escalate to agent "
            f"(potential intent: {intent}{_keyword_hint})"
        )

        # Race condition guard: re-fetch tags to prevent duplicate Slack alerts
        # when Zendesk fires multiple webhooks in rapid succession.
        current_tags = zendesk.get_ticket_tags(ticket_id)
        if "bot_handled" in current_tags:
            log.info(f"[{ticket_id}] Race condition: bot_handled already set — skipping duplicate")
            result["status"] = "skipped_race_condition"
            return result

        zendesk.add_tag(ticket_id, "bot_handled") # first — blocks webhook re-fires
        zendesk.add_tag(ticket_id, "bot_low_confidence")
        zendesk.add_tag(ticket_id, "ai_bot_failed")
        zendesk.add_internal_note(
            ticket_id,
            f"🤖 Bot: confidence too low to act automatically ({confidence:.0%}).\n\n"
            f"Potential intent: {intent}\n"
            f"Language: {language}\n"
            f"Reasoning: {classification.get('reasoning', 'N/A')}\n"
            f"{('Keywords detected: ' + ', '.join(_keyword_hint_parts)) if _keyword_hint_parts else 'No keyword signals detected.'}\n\n"
            f"Please review and handle this ticket manually.",
        )
        zendesk.set_open(ticket_id)
        result["status"] = "escalated_low_confidence"
        result["reason"] = (
            f"Confidence {confidence:.0%} below 80% threshold. "
            f"Potential intent: {intent}. "
            f"Reasoning: {classification.get('reasoning', 'N/A')}"
            + (f". Keywords: {', '.join(_keyword_hint_parts)}" if _keyword_hint_parts else "")
        )
        log_result(result)
        return result

    # 6. Cancel by email (WooCommerce → Stripe)
    cancel_result  = _cancel_by_email(email, ticket_id)
    cancel_status  = cancel_result.get("status", "")
    result["cancel_source"] = cancel_result.get("source", "unknown")

    # Enrich result with WC/Stripe lookup data for BQ logging
    result["subscription_type"] = cancel_result.get("subscription_type", "")
    result["order_count"] = cancel_result.get("order_count")

    log.info(
        f"[{ticket_id}] Cancel result: {cancel_status} "
        f"via {result['cancel_source']} | type={cancel_result.get('subscription_type')}"
    )

    # 7-ERR. WooCommerce lookup errored out (auth / timeout / api_error).
    # We CANNOT tell whether the customer has a subscription — do NOT reply
    # to the customer and do NOT fall through to Stripe (which would hide
    # the real cause). Escalate to Slack and stop.
    if cancel_status == "wc_lookup_error":
        error_kind   = cancel_result.get("error_kind", "api_error")
        error_detail = cancel_result.get("error_detail", "")
        error_step   = cancel_result.get("error_step", "")
        log.error(
            f"[{ticket_id}] WC lookup error ({error_kind} at {error_step}): "
            f"{error_detail[:200]} — escalating to Slack, no customer reply"
        )

        # ── Override intent with WC ground truth when available ────────── #
        # The text classifier guesses TRIAL vs SUB from the customer's words;
        # WooCommerce order count is the source of truth. If the sub lookup
        # succeeded before the PUT failed (typical auth_error on write), use
        # those fields to label the ticket correctly — a customer with 5
        # orders is NOT a trial cancellation no matter what the text said.
        wc_sub_type = cancel_result.get("subscription_type")
        wc_order_count = cancel_result.get("order_count")
        wc_sub_id = cancel_result.get("subscription_id")
        if wc_sub_type or wc_order_count is not None:
            resolved_intent = _resolve_intent(intent, cancel_result)
            if (
                wc_order_count is not None
                and wc_order_count >= MAX_BOT_ORDERS
            ):
                resolved_intent = "SUB_RENEWAL_CANCELLATION"
            if resolved_intent != intent:
                log.info(
                    f"[{ticket_id}] Overriding intent {intent} → "
                    f"{resolved_intent} based on WC data "
                    f"(sub_type={wc_sub_type}, orders={wc_order_count})"
                )
                intent = resolved_intent
                result["intent"] = intent

        # Race condition guard — another parallel webhook may have already escalated
        current_tags = zendesk.get_ticket_tags(ticket_id)
        if "bot_handled" in current_tags:
            log.info(f"[{ticket_id}] Race condition: bot_handled already set — skip")
            result["status"] = "skipped_race_condition"
            return result

        zendesk.add_tag(ticket_id, "bot_handled")
        zendesk.add_tag(ticket_id, "needs_manual_review")
        zendesk.add_tag(ticket_id, "ai_bot_failed")
        zendesk.add_tag(ticket_id, f"wc_{error_kind}")
        if intent == "SUB_RENEWAL_CANCELLATION":
            zendesk.add_tag(ticket_id, "sub_renewal_cancellation")

        sub_info_human = ""
        if wc_sub_id is not None or wc_sub_type or wc_order_count is not None:
            sub_info_human = (
                f"\n(Partial match before the error: subscription "
                f"#{wc_sub_id}, type={wc_sub_type or 'unknown'}, "
                f"orders={wc_order_count if wc_order_count is not None else 'unknown'}.)"
            )

        # Pick the human-facing summary based on the actual failure mode.
        # The previous wording ("did not find a subscription") was misleading
        # for transient gateway errors (504/503/502) and timeouts — in those
        # cases the bot never got an answer from WC at all. Saying "no sub"
        # made support waste time treating a real customer as unknown.
        if error_kind in ("transient_error", "timeout_error"):
            human_summary = (
                "🤖 WooCommerce was temporarily unreachable — the bot could not "
                "determine whether this customer has a subscription. Please "
                f"retry in a few minutes or check manually.{sub_info_human}"
            )
        elif error_kind == "auth_error":
            human_summary = (
                "🤖 WooCommerce credentials rejected the bot — the bot cannot "
                "look up subscriptions until ops fixes the API keys. Please "
                f"check manually.{sub_info_human}"
            )
        elif error_kind == "api_error":
            human_summary = (
                "🤖 WooCommerce returned an error during lookup — the bot "
                "cannot tell whether this customer has a subscription. Please "
                f"check manually.{sub_info_human}"
            )
        else:
            human_summary = (
                "🤖 Bot did not find a subscription for this customer — this "
                "may simply mean the user does not have one. Please check "
                f"manually.{sub_info_human}"
            )

        zendesk.add_internal_note(
            ticket_id,
            f"{human_summary}\n\n"
            f"---\n"
            f"For developer:\n"
            f"WooCommerce lookup FAILED for {email}\n"
            f"Error: {error_kind} at step `{error_step}`\n"
            f"Detail: {error_detail[:300]}",
        )
        zendesk.set_open(ticket_id)
        result.update({
            "status": "wc_lookup_error",
            "action": "slack_alerted_wc_error",
            "reason": human_summary.replace("🤖 ", "").split("\n")[0],
            "error_kind": error_kind,
            "error_detail": error_detail[:300],
            "error_step": error_step,
            "subscription_type": wc_sub_type or "",
            "subscription_id": wc_sub_id,
            "order_count": wc_order_count,
        })
        log_result(result)
        return result

    # 7a. Customer found but no active subscription → try alt emails first, then Slack alert
    if cancel_status == "found_no_active_sub":
        found_in = cancel_result.get("found_in", "system")
        log.info(
            f"[{ticket_id}] Found in {found_in} but no active sub → "
            "searching all comments for alt emails before Slack alert"
        )
        # Search ALL customer comments (not just description) — the customer may have
        # mentioned a different email in a follow-up reply.
        all_comments = zendesk.get_all_customer_comments_text(ticket_id)
        search_text  = f"{body}\n{all_comments}"
        alt_found = _try_alt_emails(ticket_id, email, search_text, intent, name, language, result)
        if alt_found:
            return alt_found

        # ── Stripe fallback for no_active_sub ─────────────────────────── #
        # WC found the customer but no active subscription. Try Stripe —
        # the sub might be managed in Stripe but not reflected in WC.
        # Try primary email first, then alt emails from ticket body/comments.
        emails_to_try_stripe = [email] + _extract_emails(search_text, exclude=email)
        stripe_result = None
        stripe_status = ""
        tried_stripe_email = email
        for stripe_email in emails_to_try_stripe:
            log.info(f"[{ticket_id}] No alt email in WC → trying Stripe by email: {stripe_email}")
            stripe_result = stripe_cli.cancel_subscription(stripe_email)
            stripe_status = stripe_result.get("status", "")
            tried_stripe_email = stripe_email
            if stripe_status not in ("not_found", "no_active_sub", "error"):
                break  # found it

        if stripe_status not in ("not_found", "no_active_sub", "error"):
            alt_note = ""
            if tried_stripe_email != email:
                alt_note = f" (via alt email {tried_stripe_email} found in ticket)"
            log.info(
                f"[{ticket_id}] ✅ Stripe fallback: cancelled {stripe_result.get('subscription_type')} "
                f"sub {stripe_result.get('subscription_id')} for {tried_stripe_email}"
            )
            cancel_result = {**stripe_result, "source": "stripe"}
            result["cancel_source"] = "stripe"
            final_intent = _resolve_intent(intent, cancel_result)
            result["intent"] = final_intent
            zendesk.add_internal_note(
                ticket_id,
                f"🤖 Bot: found in WooCommerce but no active sub. "
                f"Cancelled in Stripe directly{alt_note} "
                f"(sub={stripe_result.get('subscription_id')}).",
            )
            return _finish_cancellation(
                ticket_id, name, language, final_intent, cancel_result, result
            )

        # No working alt email and Stripe didn't help → Slack alert for manual review
        log.info(f"[{ticket_id}] No alt email / Stripe worked → Slack alert + escalate to agent")

        # Race condition guard: re-fetch tags to prevent duplicate Slack alerts
        # when Zendesk fires multiple webhooks in rapid succession (same fix as card-digits).
        current_tags = zendesk.get_ticket_tags(ticket_id)
        if "bot_handled" in current_tags:
            log.info(f"[{ticket_id}] Race condition: bot_handled already set — skipping duplicate")
            result["status"] = "skipped_race_condition"
            return result

        zendesk.add_tag(ticket_id, "bot_handled") # first — blocks webhook re-fires
        zendesk.add_tag(ticket_id, "needs_manual_review")
        zendesk.add_tag(ticket_id, "ai_bot_failed")
        zendesk.add_internal_note(
            ticket_id,
            f"🤖 Bot did not find a subscription for this customer — this "
            f"may simply mean the user does not have one. Please check "
            f"manually.\n\n"
            f"---\n"
            f"For developer:\n"
            f"Customer email {email} found in {found_in}, no active subscription.",
        )
        zendesk.set_open(ticket_id)
        result.update({
            "status": "manual_review_required",
            "action": "slack_alerted_no_active_sub",
            "reason": (
                "Bot did not find a subscription for this customer — "
                "this may simply mean the user does not have one. "
                "Please check manually."
            ),
        })
        log_result(result)
        return result

    # 7b. Email not found → try alternative emails from ALL comments (not just description)
    if cancel_status == "not_found_anywhere":
        # Fetch all customer comments to cover emails mentioned in follow-up replies,
        # not only the initial ticket description.
        all_comments = zendesk.get_all_customer_comments_text(ticket_id)
        search_text  = f"{body}\n{all_comments}"
        alt_found = _try_alt_emails(ticket_id, email, search_text, intent, name, language, result)
        if alt_found:
            return alt_found

        # ── Stripe email-based fallback ──────────────────────────────── #
        # Before asking for card digits, try Stripe directly by email.
        # Stripe Customer.list(email=) is fast and reliable, and may find
        # the subscription even when WooCommerce billing_email lookup fails
        # (common when WC stores the email only in _billing_email post meta).
        # Try primary email first, then alt emails from ticket body/comments.
        emails_to_try_stripe = [email] + _extract_emails(search_text, exclude=email)
        stripe_result = None
        stripe_status = ""
        tried_stripe_email = email
        for stripe_email in emails_to_try_stripe:
            log.info(f"[{ticket_id}] WC not found → trying Stripe by email: {stripe_email}")
            stripe_result = stripe_cli.cancel_subscription(stripe_email)
            stripe_status = stripe_result.get("status", "")
            tried_stripe_email = stripe_email
            if stripe_status not in ("not_found", "no_active_sub", "error"):
                break  # found it

        if stripe_status not in ("not_found", "no_active_sub", "error"):
            # ✅ Stripe found and cancelled the subscription
            alt_note = ""
            if tried_stripe_email != email:
                alt_note = f" (via alt email {tried_stripe_email} found in ticket)"
            log.info(
                f"[{ticket_id}] ✅ Stripe fallback: cancelled {stripe_result.get('subscription_type')} "
                f"sub {stripe_result.get('subscription_id')} for {tried_stripe_email}"
            )
            cancel_result = {**stripe_result, "source": "stripe"}
            result["cancel_source"] = "stripe"
            final_intent = _resolve_intent(intent, cancel_result)
            result["intent"] = final_intent
            zendesk.add_internal_note(
                ticket_id,
                f"🤖 Bot: not found in WooCommerce by email ({email}). "
                f"Found and cancelled in Stripe directly{alt_note} "
                f"(sub={stripe_result.get('subscription_id')}).",
            )
            return _finish_cancellation(
                ticket_id, name, language, final_intent, cancel_result, result
            )

        if stripe_status == "no_active_sub":
            log.info(f"[{ticket_id}] Stripe: no active sub for any email tried")

        # ── Not found anywhere → escalate to human (Slack only, NO customer reply) ──
        # Previously this asked the customer for last 4 card digits, but that flow
        # was unreliable and spammy. Now we just alert the team in Slack and let
        # a human handle it. The customer receives NO message from the bot.
        log.info(
            f"[{ticket_id}] Not found by email anywhere → escalating to Slack "
            "(no customer reply)"
        )

        # Race condition guard
        current_tags = zendesk.get_ticket_tags(ticket_id)
        if "bot_handled" in current_tags:
            log.info(
                f"[{ticket_id}] Race condition: bot_handled already set — skip"
            )
            result["status"] = "skipped_race_condition"
            return result

        zendesk.add_tag(ticket_id, "bot_handled")
        zendesk.add_tag(ticket_id, "needs_manual_review")
        zendesk.add_tag(ticket_id, "ai_bot_failed")
        zendesk.add_internal_note(
            ticket_id,
            f"🤖 Bot did not find a subscription for this customer — this "
            f"may simply mean the user does not have one. Please check "
            f"manually.\n\n"
            f"---\n"
            f"For developer:\n"
            f"Email {email} not found in WooCommerce or Stripe. "
            f"All lookup paths exhausted (primary email, alt emails from ticket, "
            f"Stripe fallback).",
        )
        zendesk.set_open(ticket_id)
        result.update({
            "status": "escalated_not_found",
            "action": "slack_alerted_not_found",
            "reason": (
                "Bot did not find a subscription for this customer — "
                "this may simply mean the user does not have one. "
                "Please check manually."
            ),
        })
        log_result(result)
        return result

    # 8. Override intent from actual subscription data (trial vs active sub)
    # Text classifier gives a hint, but the source of truth is WooCommerce/Stripe.
    if cancel_status == "already_cancelled":
        log.info(
            f"[{ticket_id}] Subscription already cancelled in WC — "
            f"type={cancel_result.get('subscription_type')} → confirming to customer"
        )
        intent = _resolve_intent(intent, cancel_result)
        result["intent"] = intent
        log.info(f"[{ticket_id}] Final intent after data lookup: {intent}")

    # 9. Found → generate reply, tag, solve
    # (order count gate is inside _finish_cancellation — covers all paths)
    return _finish_cancellation(ticket_id, name, language, intent, cancel_result, result)


# ── Card digits handler ───────────────────────────────────────────────── #

def _escalate_legacy_card_digits_ticket(
    ticket_id: str,
    email: str,
    tags: list,
    result: dict,
) -> dict:
    """
    The card-digits flow has been retired. Any ticket still tagged with
    `awaiting_card_digits`, `awaiting_card_digits_retry`, or
    `card_digits_timeout` is an old-pipeline leftover.

    We do NOT re-enter the old flow (which would send a public reply).
    Instead we tag for manual review, strip the legacy tags so the ticket
    does not re-loop through this path, and Slack-alert a human.
    """
    matched = sorted(t for t in tags if t in _LEGACY_CARD_DIGITS_TAGS)
    log.warning(
        f"[{ticket_id}] Legacy card-digits ticket (tags={matched}) — "
        "escalating to Slack, NO customer reply"
    )

    # Race condition guard: if a parallel webhook already escalated, skip.
    current_tags = zendesk.get_ticket_tags(ticket_id)
    if "bot_handled" in current_tags:
        log.info(f"[{ticket_id}] Race condition: bot_handled already set — skip")
        result["status"] = "skipped_race_condition"
        return result

    zendesk.add_tag(ticket_id, "bot_handled")
    zendesk.add_tag(ticket_id, "needs_manual_review")
    zendesk.add_tag(ticket_id, "ai_bot_failed")
    zendesk.add_tag(ticket_id, "legacy_card_digits")
    for t in _LEGACY_CARD_DIGITS_TAGS:
        zendesk.remove_tag(ticket_id, t)

    zendesk.add_internal_note(
        ticket_id,
        f"🤖 Bot: legacy card-digits ticket (tags={matched}).\n"
        "The card-digits flow has been retired. Bot did NOT reply to the "
        "customer. Please locate the subscription manually and handle "
        "this ticket.",
    )
    zendesk.set_open(ticket_id)
    result.update({
        "status": "escalated_legacy_card_digits",
        "action": "slack_alerted_legacy_card_digits",
        "legacy_tags": matched,
        "reason": f"Legacy card-digits ticket (tags={matched}) — card-digits flow retired, human review required",
    })
    log_result(result)
    return result


# ── Helpers ───────────────────────────────────────────────────────────── #

def _try_alt_emails(
    ticket_id: str,
    primary_email: str,
    search_text: str,
    intent: str,
    name: str,
    language: str,
    result: dict,
) -> dict | None:
    """
    Extract email addresses from search_text (ticket body + all comments),
    try to cancel by each one in turn.

    Returns a completed result dict if any alt email succeeded,
    or None if none of them worked (caller should then fall back to card digits / Slack).
    """
    alt_emails = _extract_emails(search_text, exclude=primary_email)
    # Normalize all extracted emails (fix double-dots, whitespace, etc.)
    alt_emails = [_normalize_email(e) for e in alt_emails]
    alt_emails = [e for e in alt_emails if e and e != primary_email]
    if not alt_emails:
        return None

    log.info(f"[{ticket_id}] Trying alt emails found in comments: {alt_emails}")
    for alt_email in alt_emails:
        alt_result = _cancel_by_email(alt_email, ticket_id)
        alt_status = alt_result.get("status", "")
        if alt_status not in ("not_found_anywhere", "found_no_active_sub", "error", "wc_lookup_error"):
            # ✅ Found by alternative email — cancel and close
            log.info(f"[{ticket_id}] ✅ Cancelled via alt email: {alt_email}")
            zendesk.add_internal_note(
                ticket_id,
                f"🤖 Bot: primary email ({primary_email}) not matched. "
                f"Cancelled using alt email found in message: {alt_email}",
            )
            final_intent = _resolve_intent(intent, alt_result)
            result["intent"] = final_intent
            result["cancel_source"] = alt_result.get("source", "unknown")
            return _finish_cancellation(ticket_id, name, language, final_intent, alt_result, result)
        elif alt_status == "found_no_active_sub":
            log.info(f"[{ticket_id}] Alt email {alt_email} found but no active sub → try next")
            continue  # keep trying remaining alt emails

    return None


def _extract_emails(text: str, exclude: str = "") -> list[str]:
    """
    Extract email addresses from ticket text.
    Handles both ASCII @ and full-width ＠ (common in Japanese tickets).
    Returns unique emails excluding the one already tried (Zendesk email).
    """
    # Normalise full-width ＠ → @
    normalized = text.replace("＠", "@")
    found = re.findall(r'[a-zA-Z0-9._\%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}', normalized)
    seen = set()
    result = []
    exclude_lower = exclude.lower()
    for email in found:
        e = email.lower()
        if e != exclude_lower and e not in seen:
            seen.add(e)
            result.append(email)
    return result


def _resolve_intent(text_intent: str, cancel_result: dict) -> str:
    """
    Determine final TRIAL_CANCELLATION vs SUB_CANCELLATION using actual data
    returned by WooCommerce / Stripe — not just the text classifier.

    Rules:
    - subscription_type == "trial" → TRIAL_CANCELLATION
    - subscription_type == "subscription" → SUB_CANCELLATION
    - anything else → keep text_intent as fallback
    """
    sub_type = cancel_result.get("subscription_type", "")
    if sub_type == "trial":
        return "TRIAL_CANCELLATION"
    if sub_type in ("subscription", "active"):
        return "SUB_CANCELLATION"
    # Fallback: use whatever the text classifier said
    return text_intent


def _finish_cancellation(
    ticket_id: str,
    name: str,
    language: str,
    intent: str,
    cancel_result: dict,
    result: dict,
) -> dict:
    """Generate reply, tag, and solve the ticket after a successful cancellation.

    Includes an order-count gate: subscriptions with >= MAX_BOT_ORDERS orders
    are renewals that need manual review. The bot does NOT auto-cancel these.
    """
    # ── Resolve intent from actual subscription data ─────────────────── #
    # Text classifier guesses trial vs sub, but WC order count is the source of truth.
    intent = _resolve_intent(intent, cancel_result)
    result["intent"] = intent

    # ── Enrich result with subscription data for BQ logging ──────────── #
    result["subscription_type"] = cancel_result.get("subscription_type", "")
    result["order_count"] = cancel_result.get("order_count")

    # ── Order count gate ─────────────────────────────────────────────── #
    order_count = cancel_result.get("order_count")
    if order_count is not None and order_count >= MAX_BOT_ORDERS:
        # ── Override intent: this is a renewal, not a simple cancellation ── #
        intent = "SUB_RENEWAL_CANCELLATION"
        result["intent"] = intent

        email = result.get("email", "unknown")
        log.info(
            f"[{ticket_id}] Renewal: intent={intent}, {order_count} orders "
            f"(>= {MAX_BOT_ORDERS}) → escalate to agent (not auto-cancelling)"
        )

        current_tags = zendesk.get_ticket_tags(ticket_id)
        if "bot_handled" in current_tags:
            log.info(f"[{ticket_id}] Race condition: bot_handled already set — skip")
            result["status"] = "skipped_race_condition"
            return result

        zendesk.add_tag(ticket_id, "bot_handled")
        zendesk.add_tag(ticket_id, "sub_renewal_cancellation")
        zendesk.add_tag(ticket_id, "needs_manual_review")
        zendesk.add_tag(ticket_id, "ai_bot_failed")
        zendesk.add_internal_note(
            ticket_id,
            f"🤖 Bot: subscription found (#{cancel_result.get('subscription_id')}, "
            f"intent={intent}, orders={order_count}). "
            f"Renewal subscription (>= {MAX_BOT_ORDERS} orders) — requires manual review.",
        )
        zendesk.set_open(ticket_id)
        result.update({
            "status": "manual_review_required",
            "action": "skipped_renewal_too_many_orders",
            "order_count": order_count,
            "reason": (
                f"Subscription has {order_count} orders (>= {MAX_BOT_ORDERS} threshold) — "
                "renewal subscription, bot does not auto-cancel, human review required."
            ),
        })
        log_result(result)
        return result

    reply_text = generate_reply(
        intent=intent,
        language=language,
        customer_name=name,
        cancel_result=cancel_result,
    )

    # FIX-A: Validate reply before sending to customer.
    # Catches hallucinations, prompt leakage, garbage output from Claude.
    is_valid, reason = validate_reply(reply_text, language)
    if not is_valid:
        # The WC cancel has ALREADY happened — leaving the customer with no
        # confirmation while tagging the ticket "ai_bot_failed" produces the
        # exact "subscription cancelled but no message + failed tag" pattern
        # that operators flagged. Fall back to the English master template
        # (no Claude, no translation, hard-coded text) so the customer at
        # least sees that their cancellation went through. An internal note
        # still asks support to follow up in the customer's language.
        log.error(
            f"[{ticket_id}] Reply failed validation ({reason}) → "
            "sending English fallback so the customer is notified, then "
            "flagging for manual follow-up in their language"
        )
        fallback_text = english_fallback_reply(intent, cancel_result)
        zendesk.add_tag(ticket_id, "bot_handled")
        try:
            zendesk.post_reply(ticket_id, fallback_text)
            result["reply_posted"] = True
            result["reply_was_fallback"] = True
            zendesk.add_tag(ticket_id, "ai_bot_fallback_reply")
        except Exception:
            log.exception(
                f"[{ticket_id}] EN fallback reply also failed to post"
            )
            zendesk.add_tag(ticket_id, "needs_manual_review")
        zendesk.add_internal_note(
            ticket_id,
            f"🤖 Bot cancelled the subscription successfully, but the "
            f"localised reply failed validation (reason: {reason}, "
            f"language: {language}). The customer was sent the English "
            f"master cancellation confirmation as a safe fallback. "
            f"Please follow up with a localised reply in {language} if "
            f"appropriate.",
        )
        zendesk.set_open(ticket_id)
        result.update({
            "status": "success_en_fallback",
            "action": "cancelled_en_fallback_reply",
            "validation_fail_reason": reason,
            "cancel_outcome": "success",
            "reason": (
                f"Cancellation succeeded; localised reply failed validation "
                f"({reason}). Sent EN master template as fallback."
            ),
        })
        log_result(result)
        return result

    cancel_tag = {
        "TRIAL_CANCELLATION": "trial_cancellation",
        "SUB_CANCELLATION": "sub_cancellation",
    }.get(intent, "cancelled")

    # By the time we reach this block the WC cancellation has ALREADY happened
    # (cancel_result["cancelled"] is True). Track it explicitly so a downstream
    # Zendesk write failure can no longer silently degrade the bot's recorded
    # outcome to "error" / "1 reply, status failed" — Slack and BQ should still
    # see the cancel as a success, with a separate flag for the Zendesk-side
    # failure so ops knows to follow up on the ticket close.
    result["cancel_outcome"] = "success"
    result["subscription_id"] = cancel_result.get("subscription_id")
    result["plan"] = cancel_result.get("plan")
    result["cancel_source"] = (
        cancel_result.get("source") or result.get("cancel_source")
    )

    _confidence = result.get("confidence") or 0
    _reasoning = result.get("reasoning") or "—"
    _sub_id = cancel_result.get("subscription_id", "—")
    _plan = cancel_result.get("plan") or "—"
    _source = cancel_result.get("source") or result.get("cancel_source") or "—"
    audit_note = (
        f"🤖 Bot auto-cancelled this ticket.\n"
        f"Intent: {intent} (confidence: {_confidence:.0%})\n"
        f"Language: {language}\n"
        f"Source: {_source}\n"
        f"Subscription: #{_sub_id} ({_plan})\n"
        f"Reasoning: {_reasoning}"
    )

    zendesk_step = "add_tag:bot_handled"
    try:
        zendesk.add_tag(ticket_id, "bot_handled")  # first — blocks re-entry from webhook re-fires
        zendesk_step = "post_reply"
        zendesk.post_reply(ticket_id, reply_text)
        result["reply_posted"] = True
        zendesk_step = "add_tag:cancel_tag"
        zendesk.add_tag(ticket_id, cancel_tag)
        zendesk.add_tag(ticket_id, "ai_bot_success")
        zendesk_step = "set_topic"
        _set_topic_for_intent(ticket_id, intent)
        # Audit note BEFORE solve so it shows up on the closed ticket.
        zendesk_step = "add_internal_note"
        zendesk.add_internal_note(ticket_id, audit_note)
        zendesk_step = "solve_ticket"
        zendesk.solve_ticket(ticket_id)
    except TicketNotWritableError as e:
        # Ticket got merged/closed by an agent in the middle of our writes.
        # The cancel has already happened in WC — preserve that fact so the
        # outer handler reports "merged after cancel" instead of erasing the
        # cancellation from the audit trail.
        log.info(
            f"[{ticket_id}] Cancel succeeded but ticket was merged/closed "
            f"during Zendesk write at step `{zendesk_step}` "
            f"({e.method} {e.url} → 422). Re-raising for outer skip handler."
        )
        result["zendesk_outcome"] = "merged_mid_flight"
        result["zendesk_failed_at"] = zendesk_step
        # Attach cancel context to the exception so the outer handler can
        # render "cancelled, then merged" rather than a bare skip.
        e.cancel_outcome = "success"
        e.zendesk_step = zendesk_step
        e.subscription_id = cancel_result.get("subscription_id")
        raise
    except Exception as e:
        # Zendesk write failed, but the cancellation IS already done in
        # WooCommerce. Don't report this ticket as a generic "error" — that
        # would lose the fact that we cancelled, and the customer would see
        # an inconsistent picture (sub gone in WC, ticket says bot failed).
        # Mark a partial-success status so support can finish the Zendesk
        # side manually without re-cancelling.
        log.exception(
            f"[{ticket_id}] WC cancel succeeded but Zendesk write failed at "
            f"step `{zendesk_step}`: {e}"
        )
        try:
            zendesk.add_internal_note(
                ticket_id,
                f"🤖 Bot cancelled the subscription in WooCommerce (sub "
                f"#{_sub_id}, {_plan}), but the Zendesk write failed at "
                f"step `{zendesk_step}`: {str(e)[:200]}.\n\n"
                f"Please verify the subscription is cancelled and close the "
                f"ticket manually. Do NOT re-trigger the bot — the cancel "
                f"already went through.",
            )
        except Exception:
            log.exception(f"[{ticket_id}] Could not even post fallback note")
        try:
            zendesk.add_tag(ticket_id, "bot_handled")
            zendesk.add_tag(ticket_id, "needs_manual_review")
        except Exception:
            pass
        result.update({
            "status": "success_zendesk_failed",
            "action": "cancelled_zendesk_write_failed",
            "reply_text": reply_text,
            "zendesk_outcome": "write_failed",
            "zendesk_failed_at": zendesk_step,
            "zendesk_error": str(e)[:300],
        })
        log_result(result)
        return result

    result.update({
        "status": "success",
        "action": "cancelled_and_replied",
        "reply_text": reply_text,
        "zendesk_outcome": "posted_and_closed",
    })
    log.info(f"[{ticket_id}] ✅ Done")
    log_result(result)
    return result


def _cancel_by_email(email: str, ticket_id: str) -> dict:
    """
    WooCommerce-only cancellation by email.

    Stripe is NOT used here — it is only used as a fallback in the main
    flow when WC returns `not_found` / `no_active_sub`.

    Returns one of:
    - cancel result dict             — WC found and cancelled (or dry_run / already_cancelled)
    - status="found_no_active_sub"   — customer found in WC but no active subscription
                                       → Slack alert / manual review
    - status="wc_lookup_error"       — WC lookup failed (auth/timeout/api error).
                                       Bot CANNOT tell whether the sub exists.
                                       Escalate to Slack immediately; no customer reply.
    - status="not_found_anywhere"    — WC confirmed "no such email anywhere".
                                       Fall through to Stripe fallback in caller.
    """
    # Pass the renewal threshold so WC skips the PUT entirely for
    # subscriptions with too many orders. Previously this was gated AFTER
    # the cancel, leaving customers with their sub cancelled in WC, no
    # reply sent, and the ticket tagged "ai_bot_failed".
    woo_result = woo.cancel_subscription(
        email, max_auto_cancel_orders=MAX_BOT_ORDERS
    )
    woo_status = woo_result.get("status", "")

    # Successful WC outcome — return immediately
    _TERMINAL_MISS = {
        "not_found", "no_active_sub",
        "auth_error", "timeout_error", "api_error", "transient_error",
        "timeout",  # legacy
        "error",    # legacy
    }
    if woo_status not in _TERMINAL_MISS:
        return {**woo_result, "source": "woocommerce"}

    # Customer exists in WC but has no active subscription → manual review
    if woo_status == "no_active_sub":
        log.info(
            f"[{ticket_id}] WooCommerce: customer found but no active sub "
            "→ escalate to agent"
        )
        return {
            "status": "found_no_active_sub",
            "email": email,
            "cancelled": False,
            "source": "woocommerce",
            "found_in": "WooCommerce",
        }

    # Typed WC errors (auth/timeout/api) — the bot does NOT know whether the
    # customer has a subscription. Do NOT reply to the customer. Escalate
    # immediately so a human can take over.
    #
    # If WC DID locate the subscription before the PUT failed (auth_error on
    # write but read was fine), woo.cancel_subscription already populated
    # subscription_type, subscription_id, order_count. Preserve them so the
    # caller can use WC data (not the text classifier) to label the ticket
    # correctly (e.g. renewal with 5 orders should NOT be TRIAL_CANCELLATION).
    if woo_status in ("auth_error", "timeout_error", "api_error", "transient_error"):
        log.error(
            f"[{ticket_id}] WooCommerce {woo_status}: "
            f"{woo_result.get('error_detail', '')[:200]} — escalating to Slack, "
            "no customer reply"
        )
        return {
            "status": "wc_lookup_error",
            "email": email,
            "cancelled": False,
            "source": "woocommerce",
            "error_kind": woo_status,
            "error_detail": woo_result.get("error_detail", ""),
            "error_step": woo_result.get("error_step", ""),
            "subscription_type": woo_result.get("subscription_type"),
            "subscription_id": woo_result.get("subscription_id"),
            "order_count": woo_result.get("order_count"),
            "plan": woo_result.get("plan"),
        }

    # Legacy timeout/error statuses — treat same as typed errors, but
    # preserve whatever detail the WC client attached to the result
    # (real HTTP status, response body) so operators can see the actual
    # failure in Slack instead of a bare "legacy status: error".
    if woo_status in ("timeout", "error"):
        real_detail = woo_result.get("error_detail") or woo_result.get("error") or ""
        real_step = woo_result.get("error_step") or "put_cancel"
        log.error(
            f"[{ticket_id}] WooCommerce legacy {woo_status} status "
            f"(step={real_step}): {str(real_detail)[:300]} — escalating as "
            "wc_lookup_error, no customer reply"
        )
        return {
            "status": "wc_lookup_error",
            "email": email,
            "cancelled": False,
            "source": "woocommerce",
            "error_kind": "api_error",
            "error_detail": (
                str(real_detail) if real_detail
                else f"legacy status: {woo_status} (no further detail from WC client)"
            ),
            "error_step": real_step,
            "subscription_type": woo_result.get("subscription_type"),
            "subscription_id": woo_result.get("subscription_id"),
            "order_count": woo_result.get("order_count"),
            "plan": woo_result.get("plan"),
        }

    # woo_status == "not_found" — WC said "no such email anywhere" cleanly
    log.info(
        f"[{ticket_id}] WooCommerce: not_found (no errors) — "
        "falling through to Stripe fallback"
    )
    return {
        "status": "not_found_anywhere",
        "email": email,
        "cancelled": False,
        "source": "none",
    }


# ── Refund detection ───────────────────────────────────────────────────── #

# Keywords that indicate the customer wants a refund/repayment in addition
# to cancellation. If these appear alongside a cancellation intent, we
# escalate to a human instead of auto-cancelling — the refund decision
# requires manual review.
_REFUND_KEYWORDS = [
    # Japanese — explicit refund words
    "返済", "返金", "払い戻し", "返還", "弁償",
    # Japanese — cooling-off / legal right of withdrawal (= refund request)
    "クーリングオフ", # cooling-off period (legal refund right, common in JP)
    "クーリング・オフ", # variant with middle dot
    "cooling off", # English variant sometimes written by JP customers
    # Japanese — implicit refund requests (asking about money already paid)
    "お金を返して", # return my money
    "お金を返していただ", # please return my money (polite)
    "お金返して", # colloquial variant without を particle
    "料金を返して", # return the fee
    "代金を返して", # return the price/payment
    "先日の請求を", # about the recent billing (implying dispute)
    # Japanese — "return/restore money" using 戻す (alternate verb for giving back)
    "お金を戻して", # return my money (戻す form)
    "代金を戻して", # return the payment
    "料金を戻して", # return the fee
    "円を返して", # return the N-yen amount (e.g. 1990円を返して)
    "円を戻して", # return the N-yen amount (戻す form)
    # Japanese — asking if refund is possible
    "返してもらえ", # "can I get it back" / "could you return it"
    "戻してもらえ", # variant using 戻す
    "返ってきますか", # "will it come back?" (asking if refund possible)
    "戻ってきますか", # variant: "will it be returned?"
    "お金が戻", # "money comes back / money is returned"
    "お金は戻", # variant
    "更新料", # renewal fee (asking about renewal charge = refund context)
    # Japanese — billing discrepancy / charge error (needs human investigation)
    "意味不明な金額", # "strange/unknown amount" — billing dispute signal
    "報告金額", # "report amount" — fee discrepancy (IQ Booster specific)
    # Japanese — payment cancellation / reversal = wanting a past payment undone (refund intent)
    # These are distinct from "subscription cancellation" — customer is asking to reverse a charge
    "支払いをキャンセル", # cancel the payment (refund this specific payment)
    "支払いを取り消", # reverse/cancel the payment
    "支払いのキャンセル", # payment cancellation (noun form)
    "課金を取り消", # cancel/reverse the charge
    "課金のキャンセル", # charge cancellation
    "決済をキャンセル", # cancel the transaction/settlement
    "決済を取り消", # reverse the transaction
    "引き落としを取り消", # reverse the bank deduction
    "請求を取り消", # reverse/cancel the billing
    "請求のキャンセル", # billing cancellation (noun form)
    "添付の支払い", # "the attached payment" — specific payment reference = refund intent
    # Japanese — unauthorized / unexpected charge patterns
    "身に覚えの", # "I don't recognise this charge" (身に覚えのない引き落とし)
    "身に覚えがない", # variant
    "勝手に引き落とし", # "deducted without consent"
    "勝手にサブスク登録", # "subscription registered without consent" — 106350 pattern
    "勝手にサブスクリプション登録", # full-katakana variant
    "勝手に登録", # generic "registered without consent"
    "勝手にサブスク", # "subscription [done] without consent"
    "知らない間に登録", # "registered without my knowing"
    "知らないうちに登録", # variant
    "登録した覚えがない", # "don't recall registering"
    "登録した覚えはありません", # polite variant
    # "next month surprise charge" pattern (106911): customer is alarmed
    # about an upcoming withdrawal they didn't expect AND asks "why? when?"
    # — that combination is a refund-style dispute, not just a cancel.
    "来月引き落とし", # "next month's withdrawal"
    "引き落とさないでください", # "please don't withdraw"
    "引き落とさないで下さい", # variant kanji
    "なぜこうなった", # "why did this happen?"
    "いつこうなった", # "when did this happen?"
    "なぜ、いつ", # combined "why, when?" with charge complaint
    "不正請求", # "fraudulent/unauthorized charge"
    "不法請求", # "illegal billing/charge" — variant seen in real tickets
    "詐欺", # fraud / scam
    "不正利用", # unauthorized / fraudulent use
    "無断で引き落とし", # "deducted without consent"
    # Japanese — explicit "did not consent / did not agree" phrases.
    # Real tickets: "フルレポート分1,990円は承諾しておりません" — customer
    # accepted the small charge but refuses the larger one. Even when paired
    # with a cancel verb, this is a charge dispute → human must handle.
    "承諾しておりません",   # did not consent (polite keigo)
    "承諾していません",     # did not consent
    "承諾していない",       # did not consent (plain)
    "承諾した覚えがない",   # don't recall consenting
    "承諾した覚えはありません", # polite variant
    "同意しておりません",   # did not agree (polite keigo)
    "同意していません",     # did not agree
    "同意していない",       # did not agree (plain)
    "同意した覚えがない",   # don't recall agreeing
    "同意した覚えはありません", # polite variant
    "許可しておりません",   # did not authorize (polite)
    "許可していません",     # did not authorize
    "許可していない",       # did not authorize (plain)
    "知らない間に", # "without my knowledge" (charged)
    "登録した覚えがない", # "I don't recall signing up"
    "利用した覚えがない", # "I don't recall using it"
    "利用をした覚えがない", # particle variant
    "利用の覚えがない", # another variant
    "月額利用をした覚えがない", # "I don't recall using the monthly service"
    "心当たりがない", # "have no recollection of this" — strong implicit refund signal
    "心当たりがございません", # polite variant
    "心当たりがありません", # another polite variant
    "身に覚えがありません", # polite variant of 身に覚えがない
    # NOTE: removed generic "覚えがありません" / "覚えがございません" — too broad,
    # matches innocent "入会した覚えがありません" (don't recall signing up = trial cancel).
    # Specific variants like 身に覚えがありません, 利用した覚えがない are kept.
    "気づかなかった", # "I didn't notice" (the charge)
    # NOTE: removed generic "知りませんでした" / "知らなかった" — too broad,
    # customer may say "I didn't know how to cancel" which is not a refund signal.
    "把握していなかった", # "I wasn't aware"
    "引き落とされている", # "is being deducted" — customer noticing unexpected ongoing charge
    "引き落とされていた", # past tense variant
    "勝手に課金", # "charged without consent"
    "勝手に請求", # "billed without consent"
    # Japanese — unauthorized/auto-charge patterns (complement _STRONG_REFUND_SIGNALS)
    "購読契約していないのに", # "despite not having a subscription"
    "契約していないのに",     # "despite not subscribing"
    "登録していないのに",     # "despite not registering"
    "自動で料金が",           # "fee was charged automatically"
    "自動で引かれ",           # "automatically deducted"
    "自動で課金",             # "automatically charged"
    "2回も引かれ",            # "charged twice"
    "何度も引かれ",           # "charged multiple times"
    "二重に引かれ",           # "double-charged"
    "二重請求",               # "double billing"
    "二重課金",               # "double charging"
    # English
    "refund", "repayment", "reimbursement", "money back", "chargeback",
    "charge back", "get my money", "pay me back",
    "cancel payment", # "cancel payment" = wanting a payment reversed
    "cancel charge", # variant
    "unauthorized charge", "unknown charge", "unexpected charge",
    "charged without", # "charged without my consent/knowledge"
    "didn't authorize", # "I didn't authorize this charge"
    "did not authorize",
    "fraud", "fraudulent charge", "fraudulent payment",
    "illegal charge", "illegal billing",
    "without my consent", "without my permission", "without my knowledge",
    "i never signed up", "never agreed to",
    "didn't sign up", "did not sign up",
    "don't recognize this", "do not recognize this",
    "unrecognized charge", "unrecognised charge",
    "unrecognized subscription", "unrecognised subscription",
    "unrecognized payment", "unrecognised payment",
    "didn't know i was", "did not know i was", # "I didn't know I was being charged"
    "didn't realize i was", "did not realize i was",
    "wasn't aware", "was not aware",
    "had no idea", "have no idea", # "I had no idea I was being charged"
    "never intended to", "never wanted",
    # Korean — explicit refund
    "환불",
    "승인취소", # "approval cancellation" = payment reversal (Korean payment term)
    "승인 취소", # spaced variant
    # NOTE: "결제취소" / "결제 취소" removed — too ambiguous, causes false positives
    # on legitimate cancellation tickets (e.g. "결제 취소 관련" = about payment cancellation)
    "모르게 결제", # "payment made without my knowledge"
    "무단 결제", # "unauthorized payment"
    "무단결제", # no-space variant
    # Korean — payment dispute / wrong charge
    "결제시도",       # "payment attempt" (왜 결제시도된거죠? = why was a payment attempted?)
    "잘못된 결제",    # "wrong payment"
    "결제가 잘못",    # "payment is wrong"
    "결제를 한 적",   # "I never made this payment"
    "결제한 기억",    # "I don't recall making this payment"
    "결제한 적이 없", # "I never made this payment" (formal)
    "결제된 거",      # "something was charged"
    "결제가 된",      # "a payment was made" (disputing)
    "왜 결제",        # "why was I charged" — strong dispute signal
    # Korean — "I never subscribed / didn't sign up / don't recognize this charge"
    # These phrases signal the customer disputes the charge entirely, not just cancels.
    # Example: "전 구독한게 없고 구매한것도 없는데" = "I have no subscription and made no purchase"
    "구독한게 없",    # "I have no subscription" (no-space variant)
    "구독한 게 없",   # space variant
    "구독한적 없",    # "never subscribed" (no-space)
    "구독한 적 없",   # "never subscribed" (spaced)
    "구독하지 않았",  # "did not subscribe"
    "구매한것도 없",  # "made no purchase either" (no-space)
    "구매한 것도 없", # space variant
    "결제한 적 없",   # "never made this payment"
    "결제한적 없",    # no-space variant
    "가입한 적 없",   # "never signed up / registered"
    "가입한적 없",    # no-space variant
    "가입하지 않았",  # "did not sign up"
    "신청한 적 없",   # "never applied / registered for this"
    "신청한적 없",    # no-space variant
    # German — refund + fraud / unauthorized signals
    "rückerstattung", "rückzahlung", "erstattet",
    "widerruf", # legal right of withdrawal (= refund, very common in DE/AT/CH)
    "widerrufen", # to withdraw/revoke
    "widerrufsrecht", # right of withdrawal
    "widerrufsfrist", # withdrawal period
    # Vietnamese — refund / unauthorized charge
    "hoàn tiền",                # refund
    "hoàn lại",                 # return / refund
    "hoàn lại tiền",            # refund money
    "trả lại tiền",             # give back the money
    "huỷ thanh toán",           # cancel the payment (= reverse the charge)
    "hủy thanh toán",           # variant spelling
    "huỷ giao dịch",            # cancel the transaction
    "hủy giao dịch",            # variant spelling
    "bị trừ thêm",              # was charged extra
    "bị trừ tiền",              # money was deducted
    "trừ tiền tự động",         # money deducted automatically
    "tự ý trừ",                 # deducted unilaterally / without permission
    "không đăng ký",            # didn't register / sign up
    "không đăng kí",            # variant spelling
    "chưa đăng ký",             # haven't registered
    "chưa đăng kí",             # variant spelling
    "tôi không đăng ký",        # I didn't sign up
    "tôi không đăng kí",        # variant spelling
    "không đồng ý",             # didn't agree / consent
    "lừa đảo",                  # fraud / scam
    # German
    "geld zurück", # money back
    "geld zurückfordern", # demand money back
    "betrug", # fraud ("Achtung Betrug" = attention fraud)
    "betrügerisch", # fraudulent
    "nicht autorisiert", # not authorized
    "nicht genehmigt", # not approved
    "unberechtigte abbuchung", # unauthorized debit
    "unberechtigte zahlung", # unauthorized payment
    "unberechtigt abgebucht", # debited without authorization
    "ohne mein wissen", # without my knowledge
    "ohne meine zustimmung", # without my consent
    "unbekannte abbuchung", # unknown debit — "I don't know this charge"
    "unbekannte zahlung", # unknown payment
    "unbekannte transaktion", # unknown transaction
    "unbekannter abbuchung", # genitive variant
    "unerwartete abbuchung", # unexpected debit
    "unerwartete zahlung", # unexpected payment
    "unerwartete belastung", # unexpected charge
    "nicht bestellt", # didn't order
    "nicht gewollt", # didn't want
    "nicht angemeldet", # didn't sign up
    "nicht registriert", # didn't register
    "keine kenntnis", # had no knowledge (of the charge)
    "nichts davon gewusst", # knew nothing about it
    "weiß nichts davon", # know nothing about it
    "kenne dieses abonnement nicht", # don't know this subscription
    "kenne diese abbuchung nicht", # don't know this charge
    "falsche abbuchung", # wrong/erroneous debit
    "fehlerhafte abbuchung", # erroneous debit
    "versehentlich abgebucht", # accidentally charged
    # French
    "remboursement", "rembourser",
    # Spanish / Portuguese
    "reembolso",
    "devolución",                # refund / return
    "devolver",                  # to refund / return
    "devolverme",                # refund me
    "devuelvan",                 # refund (formal imperative)
    "devuelvanme",               # refund me (formal imperative)
    "cobro sin razón",           # charged without reason
    "cobro sin razon",           # without accent variant
    "cargo no autorizado",       # unauthorized charge
    "cargo sin autorización",    # charge without authorization
    "cargo sin autorizacion",    # without accent variant
    "sin autorización",          # without authorization
    "sin autorizacion",          # without accent variant
    "sin mi autorización",       # without my authorization
    "sin mi autorizacion",       # without accent variant
    "sin autorizar",             # without authorizing
    "cobro indebido",            # improper / undue charge
    "cobro injustificado",       # unjustified charge
    "me cobraron sin",           # they charged me without …
    "me han cobrado sin",        # they have charged me without …
    "no autoricé",               # I did not authorize
    "no autorice",               # without accent variant
    "no he autorizado",          # I have not authorized
    # Russian
    "возврат",
    # Italian
    "rimborso",
    # Dutch
    "geld terug", # money back
    "terugbetaling", # refund/repayment
    "terugbetalen", # to refund
    "terugvordering", # reimbursement/reclaim
    "ongeautoriseerd", # unauthorized
    "ongeautoriseerde betaling", # unauthorized payment
    "niet geautoriseerd", # not authorized
    "onbekende afschrijving", # unknown debit
    "niet besteld", # didn't order
    # Norwegian
    "tilbakebetaling", # refund/repayment
    "refusjon", # refund
    "penger tilbake", # money back
    "uautorisert", # unauthorized
    "ukjent trekk", # unknown deduction
    "ikke bestilt", # didn't order
    "ikke autorisert", # not authorized
    "feilbelastet", # incorrectly charged
    "belastet feil", # charged incorrectly
    # Swedish
    "återbetalning", # refund
    "obehörig", # unauthorized
    "feldebiterad", # incorrectly debited
    # Danish
    "tilbagebetaling", # refund
    "uautoriseret", # unauthorized
]


def _contains_refund_request(text: str) -> bool:
    """
    Return True if *text* contains refund/repayment/unauthorized-charge keywords.

    Should be called with subject + body concatenated so that a refund signal
    in either field is caught (e.g. subject='Cancel payment', body='I want
    to cancel' should still be flagged).
    """
    text_lower = text.lower()
    return any(kw in text_lower for kw in _REFUND_KEYWORDS)


# ── Strong refund signals ────────────────────────────────────────────── #
# These indicate the PRIMARY intent is a refund/dispute, NOT cancellation.
# When present, they override cancel signals (exception to Rule 1a).
#
# Triggers:
# - Explicit "refund please / 返金してください" (= direct demand, not just mention)
# - Fraud accusation (詐欺, fraud, betrug, etc.)
# - Specific amount + refund verb (5490円返金, refund $49, etc.)
# - Unauthorized/unknown charge patterns (勝手に, without my consent, etc.)
# - Legal withdrawal terms (クーリングオフ, Widerruf, etc.)
#
# Rationale: when a customer says "cancel" AND "please refund 5490 yen" +
# "this is fraud" — the real intent is charge dispute / refund, and cancel
# is just a secondary wish.  These tickets MUST go to a human for refund
# review; auto-cancelling would miss the refund part.
_STRONG_REFUND_SIGNALS = [
    # ── Bare explicit refund words (unambiguous = always strong) ──
    "返金",                 # refund (JP) — any mention = strong refund intent
    "払い戻し",             # repayment (JP)
    "refund",               # refund (EN)
    "환불",                 # refund (KR)
    "rückerstattung",       # refund (DE)
    "rückzahlung",          # repayment (DE)
    "erstattet",            # refunded (DE)
    "remboursement",        # refund (FR)
    "rembourser",           # to refund (FR)
    "reembolso",            # refund (ES/PT)
    "rimborso",             # refund (IT)
    "возврат",              # refund (RU)
    "terugbetaling",        # refund (NL)
    "terugbetalen",         # to refund (NL)
    "tilbakebetaling",      # refund (NO)
    "återbetalning",        # refund (SE)
    "tilbagebetaling",      # refund (DA)
    # ── Japanese — explicit refund requests ──
    "返金してください",     # please refund (direct request)
    "返金して",             # refund me (imperative/request)
    "返金を希望",           # want a refund
    "返金お願い",           # refund please
    "返金を求め",           # demand a refund
    "返金していただ",       # please refund (polite keigo)
    "返金をお願い",         # polite variant
    "払い戻しをお願い",     # repayment please
    "払い戻しを希望",       # want repayment
    "払い戻してください",   # please repay
    "お金を返して",         # return my money
    "お金返して",           # return my money (colloquial)
    "詐欺",                 # fraud / scam — always strong signal
    "不正請求",             # fraudulent charge
    "不正利用",             # fraudulent use
    "クーリングオフ",       # cooling-off (legal right of withdrawal)
    "クーリング・オフ",     # variant with middle dot
    "勝手に引き落とし",     # deducted without consent
    "勝手に課金",           # charged without consent
    "勝手に請求",           # billed without consent
    "無断で引き落とし",     # deducted without consent
    "身に覚えの",           # I don't recognize this (charge)
    "身に覚えがない",       # variant
    "身に覚えがありません", # polite variant
    # ── Japanese — explicit "did not consent / agree" phrases ──
    "承諾しておりません",   # did not consent (polite keigo)
    "承諾していません",     # did not consent
    "承諾していない",       # did not consent (plain)
    "同意しておりません",   # did not agree (polite keigo)
    "同意していません",     # did not agree
    "同意していない",       # did not agree (plain)
    "許可しておりません",   # did not authorize (polite)
    "許可していません",     # did not authorize
    # ── Japanese — unauthorized/auto-charge without consent ──
    "購読契約していないのに",  # "despite not having a subscription" + charged
    "契約していないのに",      # "despite not subscribing" (shorter)
    "登録していないのに",      # "despite not registering"
    "自動で料金が",            # "fee was charged automatically"
    "自動で引かれ",            # "automatically deducted"
    "自動で課金",              # "automatically charged"
    "2回も引かれ",             # "charged twice" (specific complaint)
    "何度も引かれ",            # "charged multiple times"
    "二重に引かれ",            # "double-charged"
    "二重請求",                # "double billing"
    "二重課金",                # "double charging"
    # ── Japanese — unauthorized signup ("registered without my consent") ──
    "勝手にサブスク登録",      # "subscription registered without consent"
    "勝手にサブスクリプション登録", # full-katakana variant
    "勝手に登録",              # "registered without consent" (general)
    "勝手にサブスク",          # "subscription [done] without consent"
    "知らない間に登録",        # "registered without my knowing"
    "知らないうちに登録",      # variant
    "登録した覚えがない",      # "don't recall registering"
    "登録した覚えはありません", # polite variant
    "サブスクになってる",      # "[I notice] I'm in a subscription" (surprise)
    # ── Japanese — surprise about an upcoming/recurring charge ──
    # "Why and when did this happen?" pattern alongside a charge complaint
    # is a classic refund-dispute signal: customer didn't expect the charge
    # and is asking how it came to be — they want their money back, not
    # just to cancel going forward.
    "なぜこうなった",          # "why did this happen"
    "いつこうなった",          # "when did this happen"
    "なぜ、いつ",              # combined "why, when" (charge complaint)
    "来月引き落とし",          # "next month's withdrawal" (with complaint)
    "引き落とさないでください", # "please don't withdraw" (= refund-style)
    "引き落とさないで下さい",  # variant kanji
    # ── English ──
    "fraud",
    "fraudulent",
    "money back",
    "get my money",
    "pay me back",
    "unauthorized charge",
    "unknown charge",
    "unexpected charge",
    "without my consent",
    "without my permission",
    "without my knowledge",
    "didn't authorize",
    "did not authorize",
    "i never signed up",
    "never agreed to",
    "please refund",
    "refund please",
    "i want a refund",
    "i want my refund",
    "i want to refund",
    "i want refund",
    "i want my money",
    "want a refund",
    "demand a refund",
    "full refund",
    # ── Korean ──
    "환불해주세요",         # please refund
    "환불 해주세요",        # spaced variant
    "환불해 주세요",        # variant
    "환불을 원합니다",      # I want a refund
    "환불 요청",            # refund request
    "환불요청",             # no-space variant
    "사기",                 # fraud
    "무단 결제",            # unauthorized payment
    "무단결제",             # no-space
    # ── German ──
    "betrug",               # fraud
    "betrügerisch",         # fraudulent
    "widerruf",             # legal withdrawal (= refund)
    "widerrufen",           # to withdraw
    "geld zurück",          # money back
    "nicht autorisiert",    # not authorized
    "unberechtigte abbuchung", # unauthorized debit
    "ohne mein wissen",     # without my knowledge
    "ohne meine zustimmung", # without my consent
    # ── Dutch ──
    "geld terug",           # money back
    "ongeautoriseerd",      # unauthorized
    "ongeautoriseerde betaling", # unauthorized payment
    # ── French ──
    "remboursez",           # refund (imperative)
    "fraude",               # fraud
    # ── Spanish ──
    "reembolso",            # refund
    "devolución",           # refund / return (ES)
    "devuelvan",            # refund / return (formal imperative)
    "devuélvanme",          # refund me (formal imperative)
    "devuelvanme",          # without-accent variant
    "cargo no autorizado",  # unauthorized charge
    "cobro no autorizado",  # unauthorized charge (LATAM)
    "cobro sin razón",      # charged without reason
    "cobro sin razon",      # without-accent variant
    "cobro indebido",       # improper / undue charge
    "cobro injustificado",  # unjustified charge
    "sin autorización",     # without authorization
    "sin autorizacion",     # without-accent variant
    "sin mi autorización",  # without my authorization
    "sin mi autorizacion",  # without-accent variant
    "no autoricé",          # I did not authorize
    "no autorice",          # without-accent variant
    "me cobraron sin",      # they charged me without …
    "me han cobrado sin",   # they have charged me without …
    "estafa",               # scam (Spanish)
    "fraude",               # fraud
    # ── Norwegian/Swedish/Danish ──
    "penger tilbake",       # money back (NO)
    "uautorisert",          # unauthorized (NO)
    "uautoriseret",         # unauthorized (DA)
    "återbetalning",        # refund (SE)
    "tilbakebetaling",      # refund (NO)
    "tilbagebetaling",      # refund (DA)
    # ── Vietnamese ──
    "hoàn tiền",            # refund
    "hoàn lại tiền",        # refund money
    "trả lại tiền",         # give back the money
    "huỷ thanh toán",       # cancel the payment (= reverse the charge)
    "hủy thanh toán",       # variant spelling
    "huỷ giao dịch",        # cancel the transaction
    "hủy giao dịch",        # variant spelling
    "bị trừ thêm",          # was charged extra (surprise)
    "tự ý trừ",             # deducted without consent
    "tôi không đăng ký",    # I didn't sign up
    "tôi không đăng kí",    # variant spelling
    "không đăng ký bất kì", # didn't sign up for anything
    "không đăng kí bất kì", # variant
    "lừa đảo",              # fraud / scam
]


def _contains_strong_refund_signal(text: str) -> bool:
    """
    Return True if *text* contains a STRONG refund signal that should
    override cancel signals (exception to Rule 1a "cancel always wins").

    Strong signals = explicit refund demand, fraud accusation, unauthorized
    charge, legal withdrawal, or "money back" phrasing.

    Also detects amount+refund patterns like "5490円返金" or "refund $49".
    """
    text_lower = text.lower()
    if any(kw in text_lower for kw in _STRONG_REFUND_SIGNALS):
        return True

    # Pattern: [amount] + refund verb in same vicinity
    # e.g. "5490円返金", "refund 49.99", "$5,490 refund"
    import re
    # Japanese: number + 円 + 返金/返して/戻して
    if re.search(r"\d+円\s*(?:返金|返して|戻して|払い戻)", text_lower):
        return True
    # English: "refund" near a currency amount
    if re.search(r"refund.{0,20}[\$€£¥]\s*[\d,]+", text_lower):
        return True
    if re.search(r"[\$€£¥]\s*[\d,]+.{0,20}refund", text_lower):
        return True

    return False


# ── Explanation question detection ─────────────────────────────────────── #
#
# Phrases customers use when they don't understand a charge and want someone
# to explain it ("what is this?", "что это?", "これなに?"). When such a
# question appears inside a TRIAL_CANCELLATION / SUB_CANCELLATION ticket,
# the bot must NOT just auto-cancel and reply with a generic confirmation —
# a human needs to explain the charge(s). The customer is asking about money
# they don't recognise, and an auto-reply "your trial was cancelled" leaves
# the real question unanswered.
#
# This is narrower than refund detection: the customer is not (yet) asking
# for money back, just asking "what is this?". But the bot has no way to
# answer that — only a human can identify the specific charge.
_EXPLANATION_QUESTION_KEYWORDS = [
    # Japanese — "what is this [charge/payment]?"
    "これなに",          # what is this (casual)
    "これ何",            # what is this
    "これはなに",        # what is this (polite)
    "これは何",          # what is this
    "これって何",        # what is this (colloquial)
    "これってなに",      # casual variant
    "なんの料金",        # what fee
    "何の料金",          # what fee
    "なんの請求",        # what billing
    "何の請求",          # what billing
    "なんの引き落とし",  # what deduction
    "何の引き落とし",    # what deduction
    "なんの支払い",      # what payment
    "何の支払い",        # what payment
    "なんのお金",        # what money
    "何のお金",          # what money
    "なんの課金",        # what charge
    "何の課金",          # what charge
    # Ukrainian — "what is this?"
    "що це таке",        # what is this (full phrase)
    "що за",             # what kind of / what's with
    # Russian — "what is this?"
    "что это такое",     # what is this (full phrase)
    "что это за",        # what kind of (this)
    "что за ",           # what kind of (note trailing space to avoid false hits)
    # English
    "what is this charge",
    "what is this payment",
    "what is this fee",
    "what is this for",
    "what's this charge",
    "what's this payment",
    "what's this for",
    "what are these charges",
    "what are these payments",
    "why am i being charged",
    "why was i charged",
    "why did you charge",
    "why did i get charged",
    # Korean
    "이게 뭐",           # what is this
    "이게뭐",            # no-space
    "이건 뭐",           # what is this (variant)
    "이건뭐",            # no-space
    "이 결제는 뭐",      # what is this payment
    "이 요금은 뭐",      # what is this fee
    "이 돈은 뭐",        # what is this money
    "이게 무슨",         # what kind of... is this
    "무슨 돈",           # what money
    "무슨 결제",         # what payment/charge
    "무슨 요금",         # what fee
    "왜 돈이",           # why is money [being taken]
    # German
    "was ist das für",          # what is this for
    "was ist diese abbuchung",  # what is this debit
    "was ist diese zahlung",    # what is this payment
    "was soll diese abbuchung", # what is this debit meant to be
    "was ist das für eine",     # what kind of [charge] is this
    "wofür ist diese",          # what is this for
    "wofür wurde",              # what was [I charged] for
    "wofür bezahle ich",        # what am I paying for
    "warum wurde ich",          # why was I [charged]
    "warum habe ich bezahlt",   # why did I pay
    "warum wurde mir",          # why was [money taken] from me
    # Dutch
    "wat is dit voor",          # what is this for
    "wat is deze afschrijving", # what is this debit
    "wat is deze betaling",     # what is this payment
    "waar is deze betaling voor", # what is this payment for
    "waarvoor is deze",         # what is this for
    "waarvoor betaal ik",       # what am I paying for
    "waarom ben ik",            # why am I
    "waarom is er geld",        # why was money [taken]
    # French
    "qu'est-ce que c'est",      # what is it
    "c'est quoi ce",            # what is this (charge)
    "c'est quoi ça",            # what is that
    "c'est pour quoi",          # what is this for
    "pourquoi on m'a",          # why was I
    "pourquoi j'ai été",        # why was I [charged]
    "pourquoi ai-je été",       # polite variant
    # Spanish
    "qué es este cargo",        # what is this charge
    "qué es este pago",         # what is this payment
    "qué es este cobro",        # what is this charge (LATAM)
    "qué es esto",              # what is this
    "qué es eso",               # what is that
    "por qué me cobraron",      # why was I charged
    "por qué me cobran",        # why am I being charged
    "por qué me han cobrado",   # why have I been charged (ES)
    # Italian
    "cos'è questo addebito",    # what is this charge
    "cos'è questo pagamento",   # what is this payment
    "cos'è questo",             # what is this
    "che cos'è questo",         # polite variant
    "perché mi avete addebitato", # why did you charge me
    "perché sono stato addebitato", # why was I charged
    # Portuguese
    "o que é este",             # what is this
    "o que é isso",             # what is this
    "o que é essa cobrança",    # what is this charge
    "por que fui cobrado",      # why was I charged (BR)
    "porque fui cobrado",       # variant
    "por que me cobraram",      # why did they charge me
    # Chinese (ZH) — both simplified and traditional
    "这是什么",                  # what is this (simplified)
    "這是什麼",                  # what is this (traditional)
    "这是什么费用",              # what is this fee
    "這是什麼費用",              # what is this fee (traditional)
    "什么扣款",                  # what is this deduction
    "什麼扣款",                  # traditional
    "为什么扣我",                # why was I charged
    "為什麼扣我",                # traditional
    "为什么收我",                # why charge me
    "為什麼收我",                # traditional
    "这笔是什么",                # what is this (amount)
    "這筆是什麼",                # traditional
    # Indonesian (ID)
    "ini apa",                   # what is this
    "apa ini",                   # what is this
    "ini tagihan apa",           # what bill is this
    "pembayaran apa ini",        # what payment is this
    "kenapa saya dibayar",       # why was I charged (colloquial)
    "kenapa saya ditagih",       # why was I billed
    "mengapa saya ditagih",      # why was I billed (formal)
    "biaya apa ini",             # what fee is this
    # Vietnamese (VI)
    "đây là gì",                 # what is this
    "cái này là gì",             # what is this (colloquial)
    "khoản này là gì",           # what amount is this
    "phí gì",                    # what fee
    "tại sao tôi bị",            # why was I [charged]
    "vì sao tôi bị",             # why am I [being charged]
    # Thai (TH)
    "นี่คืออะไร",                # what is this
    "อันนี้คืออะไร",             # what is this (colloquial)
    "ค่าอะไร",                   # what fee
    "ทำไมฉันถูก",                # why was I [charged]
    "ทำไมฉันโดน",                # why did I get [charged]
    "ทำไมถึงเก็บเงิน",           # why was money collected
]


def _contains_explanation_question(text: str) -> bool:
    """
    Return True if *text* contains a phrase asking "what is this charge?"
    or "why was I charged?" — a request to explain a payment the customer
    doesn't recognise.

    Even when a ticket carries a cancel intent, the presence of such a
    question means auto-cancelling is not enough: a human must explain
    the unidentified charge before closing the ticket.
    """
    text_lower = text.lower()
    return any(kw in text_lower for kw in _EXPLANATION_QUESTION_KEYWORDS)


# ── "No results received" detection ───────────────────────────────────── #
#
# Customers who say they haven't received their IQ test results / full
# report — even when paired with "please cancel" — need a human to look
# at the account. The bot's generic "your trial was cancelled" reply
# ignores the missing-delivery complaint, and it may turn into a refund
# dispute later. Policy: any "haven't received the result(s)" phrasing
# on a cancellation ticket → escalate to human.
_NO_RESULTS_RECEIVED_KEYWORDS = [
    # ── Japanese ──
    "結果を受け取っておらず",   # have not received the results (polite)
    "結果を受け取っていない",   # have not received the results
    "結果を受け取れていない",   # cannot/have not received the results
    "結果を受け取れない",       # cannot receive the results
    "結果を受け取ってない",     # colloquial
    "結果をまだ受け取っていない", # still haven't received
    "まだ結果を受け取って",      # still... receive results (partial, catches both negations)
    "結果が届いていない",       # results haven't arrived
    "結果が届いておりません",   # polite
    "結果がまだ届いていない",   # still haven't arrived
    "結果が届かない",           # results don't arrive
    "結果をまだもらっていない", # still haven't got the results
    "結果をもらっていない",     # haven't got the results
    "結果を見ていない",         # haven't seen the results
    "結果を見られない",         # cannot see the results
    "結果が見られない",         # cannot see the results (variant)
    "結果が見れない",           # cannot see (colloquial)
    "結果を確認できていない",   # haven't been able to confirm the results
    "結果を確認できない",       # cannot confirm the results
    "結果が確認できない",       # cannot confirm the results (variant)
    "レポートが届いていない",   # report hasn't arrived
    "レポートを受け取っていない", # haven't received the report
    "レポートをまだ受け取って", # still... receive report (partial)
    "フルレポートが届かない",   # full report doesn't arrive
    "フルレポートを受け取っていない", # haven't received full report
    # ── English ──
    "haven't received the results",
    "haven't received the result",
    "haven't received my results",
    "haven't received results",
    "have not received the results",
    "have not received my results",
    "have not received results",
    "didn't receive the results",
    "didn't receive results",
    "didn't receive my results",
    "did not receive the results",
    "did not receive results",
    "didn't get the results",
    "didn't get my results",
    "did not get the results",
    "did not get my results",
    "never received the results",
    "never received my results",
    "never got the results",
    "no results yet",
    "no results received",
    "results not received",
    "results haven't arrived",
    "results have not arrived",
    "results are not received",
    "haven't received the report",
    "haven't received my report",
    "didn't receive the report",
    "did not receive the report",
    "never received the report",
    "report not received",
    "full report not received",
    "haven't got my report",
    # ── Ukrainian ──
    "не отримав результат",     # haven't received result (masc)
    "не отримала результат",    # haven't received result (fem)
    "не отримав результати",    # plural
    "не отримала результати",   # plural (fem)
    "не отримав(ла) результат", # gendered form used in the real ticket
    "результат не отрима",      # result not received (partial)
    "результати не отрима",     # plural
    "результатів не отрима",    # genitive plural
    "не отримав звіт",          # haven't received report
    "не отримала звіт",         # haven't received report (fem)
    "не отримав(ла) звіт",      # gendered
    "звіту не отрима",          # report not received (partial)
    # ── Russian ──
    "не получил результат",     # haven't received result (masc)
    "не получила результат",    # haven't received result (fem)
    "результат не получил",     # result not received
    "результат не получен",     # result not received (passive)
    "результаты не получил",    # haven't received results
    "результаты не получен",    # results not received (passive)
    "результатов не получил",   # genitive
    "не получил отчет",         # haven't received report
    "не получила отчет",        # fem variant
    "отчет не получен",         # report not received
    # ── Korean ──
    "결과를 받지 못",            # didn't receive the results
    "결과를 받지 않",            # didn't receive (variant)
    "결과가 오지 않",            # results haven't come
    "결과가 안 왔",              # results didn't come
    "결과를 못 받",              # couldn't receive the results
    "결과를 아직 못",            # still haven't [received] the results
    "아직 결과를",               # still... results (partial)
    "리포트를 받지 못",          # didn't receive report
    "리포트가 오지 않",          # report hasn't arrived
    # ── German ──
    "keine ergebnisse erhalten",     # have not received any results
    "ergebnisse nicht erhalten",     # results not received
    "ergebnis nicht erhalten",       # result not received
    "noch keine ergebnisse",         # still no results
    "habe die ergebnisse nicht",     # I don't have the results
    "bericht nicht erhalten",        # report not received
    "noch keinen bericht",           # still no report
    # ── French ──
    "n'ai pas reçu les résultats",   # haven't received the results
    "pas reçu les résultats",        # not received the results
    "pas encore reçu les résultats", # not yet received the results
    "n'ai pas reçu le rapport",      # haven't received the report
    "pas reçu le rapport",           # not received the report
    # ── Spanish ──
    "no he recibido los resultados", # haven't received the results
    "no recibí los resultados",      # didn't receive the results
    "no recibí mis resultados",      # didn't receive my results
    "no he recibido el resultado",   # haven't received the result
    "no he recibido el informe",     # haven't received the report
    "no recibí el informe",          # didn't receive the report
    # ── Italian ──
    "non ho ricevuto i risultati",   # haven't received the results
    "non ho ricevuto il risultato",  # haven't received the result
    "non ho ricevuto il report",     # haven't received the report
    "non ho ricevuto il rapporto",   # alt report word
    # ── Portuguese ──
    "não recebi os resultados",      # didn't receive the results
    "não recebi o resultado",        # didn't receive the result
    "não recebi o relatório",        # didn't receive the report
    "ainda não recebi os resultados", # still haven't received the results
    # ── Dutch ──
    "heb de resultaten niet ontvangen", # haven't received the results
    "resultaten niet ontvangen",        # results not received
    "nog geen resultaten",              # no results yet
    "rapport niet ontvangen",           # report not received
    # ── Chinese (simplified + traditional) ──
    "还没收到结果",                    # haven't received the results yet
    "還沒收到結果",                    # traditional
    "没有收到结果",                    # didn't receive the results
    "沒有收到結果",                    # traditional
    "还未收到结果",                    # haven't received yet
    "還未收到結果",                    # traditional
    "没收到报告",                      # didn't receive report
    "沒收到報告",                      # traditional
    # ── Indonesian ──
    "belum menerima hasil",          # haven't received the result
    "tidak menerima hasil",          # didn't receive the result
    "belum dapat hasil",             # haven't got the result
    "belum terima hasil",            # haven't received (colloquial)
    "belum menerima laporan",        # haven't received report
    # ── Vietnamese ──
    "chưa nhận được kết quả",        # haven't received the result
    "không nhận được kết quả",       # didn't receive the result
    "chưa nhận được báo cáo",        # haven't received report
    # ── Thai ──
    "ยังไม่ได้รับผล",                # haven't received result yet
    "ไม่ได้รับผล",                   # didn't receive result
    "ยังไม่ได้รับรายงาน",            # haven't received report yet
]


def _contains_no_results_received_complaint(text: str) -> bool:
    """
    Return True if *text* says the customer hasn't received their IQ test
    results / full report.

    Even when paired with a cancel verb, this is a delivery / product
    complaint the bot can't resolve — someone has to look at the account
    and see why the result was never delivered (or whether it actually was).
    """
    text_lower = text.lower()
    return any(kw in text_lower for kw in _NO_RESULTS_RECEIVED_KEYWORDS)
