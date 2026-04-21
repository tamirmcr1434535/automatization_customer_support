"""
Zendesk Cancellation Bot — Google Cloud Function (Gen 2)
=========================================================
Cancellation flow:
 1. Check by Zendesk email → WooCommerce cancel
    └─ Found → cancel in WC → done ✅
 2. WC: not found / timeout / error
    → extract any emails mentioned in ticket body → try each one in WC
    └─ Found by alt email → cancel in WC → done ✅
 3. Still not found in WC → Stripe fallback by email
    └─ Stripe found and cancelled → done ✅
 4. Still not found → ask for last 4 card digits (tag: awaiting_card_digits)
    └─ 7 days no reply → Zendesk Automation closes ticket
    └─ Customer replied with last4:
       ├─ Stripe: find email by last4 → WC: cancel with that email → done ✅
       └─ Not found → ask for correct digits (tag: awaiting_card_digits_retry)
          └─ 2 days no reply → Zendesk Automation closes ticket
          └─ Customer replied:
             ├─ Stripe: find email by last4 → WC: cancel with that email → done ✅
             └─ Not found → close ticket

NOTE: WooCommerce is the primary cancellation target. Stripe is used as:
      (a) email-based fallback when WC lookup fails (step 3)
      (b) email-lookup tool via card last4 digits (step 4)

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
from zendesk_client import ZendeskClient
from woocommerce_client import WooCommerceClient
from stripe_client import StripeClient
from slack_client import SlackClient
from reply_generator import (
    generate_reply,
    generate_ask_card_digits_reply,
    generate_ask_card_digits_retry_reply,
    generate_not_found_reply,
    generate_timeout_reply,
    validate_reply,
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
    Wrapper around bq_logger.log_result.
    In SHADOW_MODE, skip logging inside _process — the webhook handler
    will call _bq_log_result once with fully enriched data.
    """
    if SHADOW_MODE:
        return  # shadow logging happens in webhook handler after enrichment
    _bq_log_result(result)

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger("bot")

SHADOW_MODE        = os.getenv("SHADOW_MODE", "false").lower() == "true"
DRY_RUN            = os.getenv("DRY_RUN", "true").lower() == "true"
TEST_MODE          = os.getenv("TEST_MODE", "true").lower() == "true"
TEST_TAG           = "automation_test"

# SHADOW_MODE: process ALL tickets, skip ALL writes, send Slack report per ticket.
# Overrides: DRY_RUN=true (no writes), TEST_MODE=false (all tickets), Slack stays live.
if SHADOW_MODE:
    DRY_RUN   = True
    TEST_MODE = False
    logging.info("🔍 SHADOW_MODE enabled — processing all tickets, no writes, Slack reports ON")
# How many days to wait for card/payment info before auto-closing the ticket.
# Controlled via env var so it can be changed without a code deploy.
AWAITING_CARD_DAYS = int(os.getenv("AWAITING_CARD_DAYS", "7"))

HANDLED_INTENTS = {
    "TRIAL_CANCELLATION",
    "SUB_CANCELLATION",
}

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
    Called for non-terminal states (e.g., awaiting_card_digits) where
    the ticket needs to be re-processed when the customer replies.
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
# Separate live Slack client for shadow reports only (used in webhook handler)
_shadow_slack = SlackClient(
    bot_token=os.getenv("SLACK_BOT_TOKEN", ""),
    target_email=os.getenv("SLACK_TARGET_EMAIL", ""),
    dry_run=not SHADOW_MODE,  # Live only when SHADOW_MODE is on
) if SHADOW_MODE else None

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
    except Exception as e:
        log.exception(f"[{ticket_id}] Unhandled error: {e}")
        result = {"ticket_id": ticket_id, "status": "error", "error": str(e)}
        try:
            slack.notify_error(
                ticket_id=ticket_id,
                error_msg=str(e),
                zendesk_subdomain=ZENDESK_SUBDOMAIN,
            )
        except Exception:
            log.exception(f"[{ticket_id}] Failed to send Slack error alert")

    # ── Clear dedup lock for non-terminal states ─────────────────────── #
    # States that expect the customer to reply later need the dedup lock
    # removed so the next webhook can re-process.
    # Terminal states (success, bot_handled, manual_review, error) keep
    # the lock as an extra anti-spam layer.
    _NON_TERMINAL_STATUSES = {
        "awaiting_card_digits",             # legacy — old tickets still in pipeline
        "awaiting_card_digits_retry",       # legacy
        "skipped_pending_awaiting_reply",
        "waiting_for_customer_reply",
        "skipped_no_test_tag",              # TEST_MODE skip — not a real processing
        "skipped_agent_already_replied",    # agent handling — might change
        # NOTE: "error" intentionally NOT here — clearing dedup on error
        # causes infinite retry loops (especially in shadow mode).
    }
    if result.get("status") in _NON_TERMINAL_STATUSES:
        _dedup_clear(ticket_id)

    # ── SHADOW_MODE: enrich → log → Slack ────────────────────────────── #
    # Every ticket gets full classification + detailed BQ log so we can
    # review accuracy by querying: WHERE shadow_mode = TRUE
    # NOTE: NO Zendesk tag writes here — each add_tag triggers a new
    # webhook, which caused the infinite duplication loop.
    if SHADOW_MODE and result.get("status") not in (
        "skipped_already_handled",
        "skipped_merged",
    ):
        # 1. Enrich: classify tickets that hit early exits (before classifier)
        _shadow_enrich_result(ticket_id, result)

        # 2. Add shadow-specific fields for BQ
        shadow_tag = _shadow_tag_for_status(result.get("status", ""))
        result["shadow_mode"] = True
        result["shadow_decision"] = shadow_tag.replace("shadow_", "")

        # 3. Log enriched result to BQ (the authoritative shadow entry)
        _bq_log_result(result)

        # 4. Send ONE Slack report (via dedicated shadow Slack client)
        try:
            _shadow_slack.notify_shadow_result(
                ticket_id=ticket_id,
                result=result,
                zendesk_subdomain=ZENDESK_SUBDOMAIN,
            )
        except Exception:
            log.exception(f"[{ticket_id}] Failed to send shadow Slack report")

        # NOTE: no zendesk.add_tag here — tags trigger new webhooks!

    return json.dumps(result), 200, {"Content-Type": "application/json"}


# ── Shadow mode helpers ───────────────────────────────────────────────── #

_SHADOW_STATUS_TO_TAG = {
    "success":                      "shadow_would_cancel",
    "manual_review_required":       "shadow_would_escalate",
    "escalated_low_confidence":     "shadow_would_escalate",
    "escalated_delete_account":     "shadow_would_escalate",
    "skipped_refund_request":       "shadow_would_skip_refund",
    "escalated_unknown":            "shadow_would_escalate",     # FIX-C: new status
    "skipped_not_handled":          "shadow_would_skip",
    "skipped_followup":             "shadow_would_skip",
    "skipped_closed":               "shadow_would_skip",
    "awaiting_card_digits":         "shadow_would_ask_card",     # legacy
    "awaiting_card_digits_retry":   "shadow_would_ask_card",     # legacy
    "escalated_not_found":          "shadow_would_escalate",
    "skipped_agent_already_replied":"shadow_agent_handling",
    "skipped_spam_detected":        "shadow_spam",
    "not_found_closed":             "shadow_would_escalate",
    "closed_no_response":           "shadow_would_timeout",
    "error":                        "shadow_error",
}

def _shadow_tag_for_status(status: str) -> str:
    """Map a processing status to a shadow decision tag for daily comparison."""
    return _SHADOW_STATUS_TO_TAG.get(status, "shadow_other")


def _shadow_enrich_result(ticket_id: str, result: dict) -> None:
    """
    SHADOW_MODE: if _process exited early (before classification), fetch the
    ticket again and classify it so the BQ log has full data for every ticket.
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
        slack_sent = slack.notify_manual_review(
            ticket_id=ticket_id,
            email=email,
            intent="FOLLOWUP",
            zendesk_subdomain=ZENDESK_SUBDOMAIN,
        )
        result["slack_sent"] = slack_sent
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
        slack_sent = slack.notify_refund_skip(
            ticket_id=ticket_id,
            email=email,
            intent="REFUND_REQUEST",
            zendesk_subdomain=ZENDESK_SUBDOMAIN,
        )
        result["slack_sent"] = slack_sent
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
        slack_sent = slack.notify_spam_detected(
            ticket_id=ticket_id,
            email=email,
            reply_count=bot_reply_count,
            zendesk_subdomain=ZENDESK_SUBDOMAIN,
        )
        result["slack_sent"] = slack_sent
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
            "konto löschen", "supprimer mon compte",
            "видалити акаунт", "удалить аккаунт",
            "account verwijderen",
        ]
        _REFUND_KEYWORD_FALLBACK = [
            "refund", "返金", "払い戻し", "クーリングオフ", "お金を返して", "geld zurück",
            "rückerstattung", "widerruf", "remboursement", "환불", "reembolso", "возврат",
            "rimborso", "money back", "chargeback",
        ]
        has_cancel = any(kw in full_text_lower for kw in _CANCEL_SIGNALS)
        has_delete = any(kw in full_text_lower for kw in _DELETE_ACCOUNT_KEYWORD_FALLBACK)
        has_refund = any(kw in full_text_lower for kw in _REFUND_KEYWORD_FALLBACK)

        if has_cancel:
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
        elif has_delete and not has_refund:
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
        elif has_refund:
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
        zendesk.add_tag(ticket_id, "bot_handled")
        slack_sent = slack.notify_refund_skip(
            ticket_id=ticket_id,
            email=email,
            intent=intent,
            zendesk_subdomain=ZENDESK_SUBDOMAIN,
        )
        result["slack_sent"] = slack_sent
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
        zendesk.add_tag(ticket_id, "bot_handled")  # block parallel webhook
        slack_sent = slack.notify_refund_skip(
            ticket_id=ticket_id, email=email,
            intent=intent, zendesk_subdomain=ZENDESK_SUBDOMAIN,
        )
        result["slack_sent"] = slack_sent
        log_result(result)
        return result

    # REFUND_REQUEST / SUB_RENEWAL_REFUND — always human.
    # Any refund intent = money question = human decides. No exceptions.
    if intent in ("REFUND_REQUEST", "SUB_RENEWAL_REFUND"):
        log.info(
            f"[{ticket_id}] {intent} — refund intent, always escalate to human"
        )
        result["status"] = "skipped_refund_request"
        zendesk.add_tag(ticket_id, "bot_handled")
        slack_sent = slack.notify_refund_skip(
            ticket_id=ticket_id, email=email,
            intent=intent, zendesk_subdomain=ZENDESK_SUBDOMAIN,
        )
        result["slack_sent"] = slack_sent
        log_result(result)
        return result

    # ── LEGACY CARD DIGITS CLEANUP ──────────────────────────────────── #
    # Card digits flow has been removed. If old tickets still have these
    # tags from before the change, skip them silently (don't re-process).
    # The timeout handler is kept so Zendesk Automation can still close
    # stale tickets that were already in the card-digits pipeline.
    if "card_digits_timeout" in tags:
        return _handle_card_digits_timeout(ticket_id, name, language, result)

    if "awaiting_card_digits_retry" in tags or "awaiting_card_digits" in tags:
        if ticket_status == "pending":
            log.info(
                f"[{ticket_id}] Legacy card-digits tag present, ticket pending "
                "— skipping (waiting for customer or timeout automation)"
            )
            result["status"] = "skipped_pending_awaiting_reply"
            return result
        # Customer replied to an old card-digits ask → try to process it
        # using the legacy handler (honour promises already made to customers)
        tag_is_retry = "awaiting_card_digits_retry" in tags
        return _handle_card_digits(
            ticket_id, email, name, language, result, is_retry=tag_is_retry
        )

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
        slack_sent = slack.notify_manual_review(
            ticket_id=ticket_id,
            email=email,
            intent="DELETE_ACCOUNT",
            zendesk_subdomain=ZENDESK_SUBDOMAIN,
        )
        result.update({
            "status": "escalated_delete_account",
            "action": "escalated_to_agent_delete_account",
            "slack_sent": slack_sent,
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
        slack.notify_manual_review(
            ticket_id=ticket_id,
            email=email,
            reason=(
                f"UNKNOWN intent after all safety nets — "
                f"confidence={confidence:.0%}, "
                f"reasoning: {classification.get('reasoning', 'N/A')}"
            ),
        )
        result["status"] = "escalated_unknown"
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
        slack_sent = slack.notify_manual_review(
            ticket_id=ticket_id,
            email=email,
            intent=intent,
            zendesk_subdomain=ZENDESK_SUBDOMAIN,
        )
        result["status"] = "escalated_low_confidence"
        result["slack_sent"] = slack_sent
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
            f"🤖 Bot: customer email ({email}) found in {found_in} but has NO active subscription. "
            "Subscription may already be cancelled, or registered under a different email. "
            "Please review and handle manually.",
        )
        zendesk.set_open(ticket_id)
        slack_sent = slack.notify_manual_review(
            ticket_id=ticket_id,
            email=email,
            intent=intent,
            zendesk_subdomain=ZENDESK_SUBDOMAIN,
        )
        result.update({
            "status": "manual_review_required",
            "action": "slack_alerted_no_active_sub",
            "slack_sent": slack_sent,
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
            f"🤖 Bot: customer email ({email}) not found in WooCommerce or Stripe. "
            "Could not locate subscription. Please find and cancel manually.",
        )
        zendesk.set_open(ticket_id)
        slack_sent = slack.notify_manual_review(
            ticket_id=ticket_id,
            email=email,
            intent=intent,
            zendesk_subdomain=ZENDESK_SUBDOMAIN,
        )
        result.update({
            "status": "escalated_not_found",
            "action": "slack_alerted_not_found",
            "slack_sent": slack_sent,
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

def _handle_card_digits(
    ticket_id: str,
    email: str,
    name: str,
    language: str,
    result: dict,
    is_retry: bool,
) -> dict:
    """
    Process customer's reply that should contain last 4 card digits.
    is_retry=False → first attempt (came from awaiting_card_digits)
    is_retry=True → second attempt (came from awaiting_card_digits_retry)
    """
    step = "retry" if is_retry else "first"
    log.info(f"[{ticket_id}] Card lookup ({step}) — reading last customer comment")

    last_comment = zendesk.get_last_customer_comment(ticket_id)
    if not last_comment:
        log.warning(f"[{ticket_id}] No customer comment found yet — waiting")
        result["status"] = "waiting_for_customer_reply"
        return result

    # Extract 4 consecutive digits
    match = re.search(r'\b(\d{4})\b', last_comment)
    if not match:
        log.info(f"[{ticket_id}] No 4-digit sequence in reply")
        # Treat as if digits were wrong — ask again or close
        return _digits_not_found(ticket_id, name, language, result, is_retry)

    last4 = match.group(1)
    log.info(f"[{ticket_id}] Extracted last4={last4} — looking up email in Stripe")

    # Step 1: Use Stripe only to find the customer's email by card last4.
    # Stripe is NOT used for cancellation — WooCommerce is always the cancel target.
    email_from_stripe = stripe_cli.find_email_by_last4(last4)
    if email_from_stripe:
        email_from_stripe = _normalize_email(email_from_stripe)

    if email_from_stripe:
        log.info(
            f"[{ticket_id}] Stripe found email {email_from_stripe!r} for card last4={last4} "
            "— cancelling in WooCommerce"
        )

        # Step 2: Cancel in WooCommerce using the email we found via Stripe
        woo_result = woo.cancel_subscription(email_from_stripe)
        woo_status = woo_result.get("status", "")

        if woo_status not in ("not_found", "no_active_sub", "timeout", "error"):
            # ✅ WC found and cancelled (or dry_run / already_cancelled)
            cancel_result = {**woo_result, "source": "stripe_last4_woocommerce"}
            result["cancel_source"] = "stripe_last4_woocommerce"
            result["email"] = email_from_stripe  # update to the email that worked

            # Clean up card-lookup tags
            zendesk.remove_tag(ticket_id, "awaiting_card_digits")
            zendesk.remove_tag(ticket_id, "awaiting_card_digits_retry")

            # Resolve trial vs sub from actual WC data
            final_intent = _resolve_intent(result.get("intent", "SUB_CANCELLATION"), cancel_result)
            result["intent"] = final_intent

            log.info(
                f"[{ticket_id}] ✅ Found by card last4={last4} via Stripe → "
                f"WC cancelled — intent={final_intent}"
            )
            zendesk.add_internal_note(
                ticket_id,
                f"🤖 Bot: primary email not found. "
                f"Located subscription in WooCommerce via Stripe card lookup (last4={last4}). "
                f"Email: {email_from_stripe}",
            )
            return _finish_cancellation(
                ticket_id, name, language, final_intent, cancel_result, result
            )

        # Stripe found the email but WC still has no subscription for it → digits not found path
        log.info(
            f"[{ticket_id}] Stripe found email {email_from_stripe!r} but WC returned "
            f"{woo_status!r} — treating as not found"
        )

    # ❌ Email not found in Stripe, or WC had nothing for that email
    return _digits_not_found(ticket_id, name, language, result, is_retry)


def _digits_not_found(
    ticket_id: str,
    name: str,
    language: str,
    result: dict,
    is_retry: bool,
) -> dict:
    """Called when Stripe search by last4 found nothing."""

    if not is_retry:
        # First failure: ask for correct digits one more time (2-day window)
        log.info(f"[{ticket_id}] Digits not found (first attempt) → asking for correct ones")

        reply_text = generate_ask_card_digits_retry_reply(
            language=language, customer_name=name
        )

        # FIX-A: Validate reply before sending to customer
        is_valid, reason = validate_reply(reply_text, language)
        if not is_valid:
            log.error(f"[{ticket_id}] digits_retry reply failed validation ({reason})")
            zendesk.add_tag(ticket_id, "bot_handled")
            zendesk.add_tag(ticket_id, "needs_manual_review")
            zendesk.add_internal_note(
                ticket_id,
                f"🤖 Bot: digits retry reply failed validation ({reason}). "
                f"Please follow up with the customer manually.",
            )
            zendesk.set_open(ticket_id)
            slack.notify_manual_review(
                ticket_id=ticket_id, email=result.get("email", "unknown"),
                reason=f"digits_retry reply validation failed: {reason}",
            )
            result["status"] = "manual_review_required"
            result["validation_fail_reason"] = reason
            log_result(result)
            return result

        # Tags BEFORE reply — same race-condition fix as in the initial ask:
        # swap state first so any duplicate webhook call routes to the retry handler.
        zendesk.remove_tag(ticket_id, "awaiting_card_digits")
        zendesk.add_tag(ticket_id, "awaiting_card_digits_retry")
        # Keep ticket Pending so trigger fires only on customer reply
        zendesk.post_reply_and_set_pending(ticket_id, reply_text)
        zendesk.add_internal_note(
            ticket_id,
            "🤖 Bot: card digits not found in Stripe. "
            "Asked customer for correct digits. Waiting up to 2 days.",
        )

        # Slack alert: first digit attempt failed, asked again
        slack_sent = slack.notify_card_digits_asked(
            ticket_id=ticket_id, email=email,
            zendesk_subdomain=ZENDESK_SUBDOMAIN, is_retry=True,
        )

        result.update({
            "status": "awaiting_card_digits_retry",
            "action": "asked_for_correct_digits",
            "reply_text": reply_text,
            "slack_sent": slack_sent,
        })
        log_result(result)
        return result # ticket set to Pending — awaiting customer reply

    else:
        # Second failure: close ticket
        log.info(f"[{ticket_id}] Digits not found (retry) → closing ticket")

        reply_text = generate_not_found_reply(language=language, customer_name=name)

        # FIX-A: Validate reply before sending to customer
        is_valid, reason = validate_reply(reply_text, language)
        if not is_valid:
            log.error(f"[{ticket_id}] not_found reply failed validation ({reason})")
            zendesk.add_tag(ticket_id, "bot_handled")
            zendesk.add_tag(ticket_id, "needs_manual_review")
            zendesk.add_internal_note(
                ticket_id,
                f"🤖 Bot: not_found reply failed validation ({reason}). "
                f"Please close ticket manually.",
            )
            zendesk.set_open(ticket_id)
            slack.notify_manual_review(
                ticket_id=ticket_id, email=result.get("email", "unknown"),
                reason=f"not_found reply validation failed: {reason}",
            )
            result["status"] = "manual_review_required"
            result["validation_fail_reason"] = reason
            log_result(result)
            return result

        zendesk.post_reply(ticket_id, reply_text)
        zendesk.remove_tag(ticket_id, "awaiting_card_digits_retry")
        zendesk.add_tag(ticket_id, "not_found_closed")
        zendesk.add_tag(ticket_id, "bot_handled")
        zendesk.add_tag(ticket_id, "ai_bot_failed")
        zendesk.solve_ticket(ticket_id)

        # Slack alert so the team knows this customer slipped through
        slack.notify_not_found(
            ticket_id=ticket_id,
            email=result.get("email", "unknown"),
            zendesk_subdomain=ZENDESK_SUBDOMAIN,
        )

        result.update({
            "status": "not_found_closed",
            "action": "closed_not_relevant",
            "reply_text": reply_text,
        })
        log.info(f"[{ticket_id}] ❌ Closed as not relevant after 2 failed attempts")
        log_result(result)
        return result


# ── Card digits timeout ───────────────────────────────────────────────── #

def _handle_card_digits_timeout(
    ticket_id: str,
    name: str,
    language: str,
    result: dict,
) -> dict:
    """
    Called by the Zendesk Automation after AWAITING_CARD_DAYS days of no reply.
    The automation adds tag 'card_digits_timeout', which triggers this webhook call.
    Sends a sorry-we-didn't-hear-from-you message and closes the ticket.
    """
    log.info(
        f"[{ticket_id}] No reply in {AWAITING_CARD_DAYS}d (card_digits_timeout) → closing"
    )

    reply_text = generate_timeout_reply(language=language, customer_name=name)
    zendesk.post_reply(ticket_id, reply_text)

    # Clean up all awaiting tags
    zendesk.remove_tag(ticket_id, "awaiting_card_digits")
    zendesk.remove_tag(ticket_id, "awaiting_card_digits_retry")
    zendesk.remove_tag(ticket_id, "card_digits_timeout")
    zendesk.add_tag(ticket_id, "closed_no_response")
    zendesk.add_tag(ticket_id, "bot_handled")
    zendesk.add_tag(ticket_id, "ai_bot_failed")
    zendesk.solve_ticket(ticket_id)

    # Slack alert: customer never replied with card digits
    slack_sent = slack.notify_card_digits_timeout(
        ticket_id=ticket_id,
        email=result.get("email", "unknown"),
        days=AWAITING_CARD_DAYS,
        zendesk_subdomain=ZENDESK_SUBDOMAIN,
    )

    result.update({
        "status": "closed_no_response",
        "action": f"timeout_closed_{AWAITING_CARD_DAYS}d",
        "reply_text": reply_text,
        "slack_sent": slack_sent,
    })
    log.info(f"[{ticket_id}] ⏰ Closed — no response within {AWAITING_CARD_DAYS} days")
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
        if alt_status not in ("not_found_anywhere", "found_no_active_sub", "error"):
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
        slack_sent = slack.notify_manual_review(
            ticket_id=ticket_id,
            email=email,
            intent=intent,
            zendesk_subdomain=ZENDESK_SUBDOMAIN,
        )
        result.update({
            "status": "manual_review_required",
            "action": "skipped_renewal_too_many_orders",
            "order_count": order_count,
            "slack_sent": slack_sent,
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
        log.error(
            f"[{ticket_id}] Reply failed validation ({reason}) — escalating to human"
        )
        zendesk.add_tag(ticket_id, "bot_handled")
        zendesk.add_tag(ticket_id, "needs_manual_review")
        zendesk.add_tag(ticket_id, "ai_bot_failed")
        zendesk.add_internal_note(
            ticket_id,
            f"🤖 Bot: cancellation succeeded but generated reply failed validation.\n"
            f"Reason: {reason}\n"
            f"Language: {language}\n\n"
            f"Please reply to the customer manually to confirm cancellation.",
        )
        zendesk.set_open(ticket_id)
        slack.notify_manual_review(
            ticket_id=ticket_id,
            email=result.get("email", "unknown"),
            reason=f"Reply validation failed: {reason}",
        )
        result["status"] = "manual_review_required"
        result["validation_fail_reason"] = reason
        log_result(result)
        return result

    cancel_tag = {
        "TRIAL_CANCELLATION": "trial_cancellation",
        "SUB_CANCELLATION": "subscription_cancelled",
    }.get(intent, "cancelled")

    zendesk.add_tag(ticket_id, "bot_handled") # first — blocks any re-entry from webhook re-fires
    zendesk.post_reply(ticket_id, reply_text)
    zendesk.add_tag(ticket_id, cancel_tag)
    zendesk.add_tag(ticket_id, "ai_bot_success")
    zendesk.solve_ticket(ticket_id)

    result.update({
        "status": "success",
        "action": "cancelled_and_replied",
        "reply_text": reply_text,
    })
    log.info(f"[{ticket_id}] ✅ Done")
    log_result(result)
    return result


def _cancel_by_email(email: str, ticket_id: str) -> dict:
    """
    WooCommerce-only cancellation by email.

    Stripe is NOT used here — it is only used in the card-digits flow
    (_handle_card_digits) to find an email by last4, then we come back to WC.

    Returns one of:
    - cancel result dict          — WC found and cancelled (or dry_run / already_cancelled)
    - status="found_no_active_sub"— customer found in WC but no active subscription
                                    → Slack alert / manual review
    - status="not_found_anywhere" — not found or timeout / error in WC
                                    → ask for last 4 card digits
    """
    woo_result = woo.cancel_subscription(email)
    woo_status = woo_result.get("status", "")

    # Successful WC outcome — return immediately
    if woo_status not in ("not_found", "no_active_sub", "timeout", "error"):
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

    # not_found / timeout / error — ask customer for last 4 card digits
    if woo_status == "timeout":
        log.warning(
            f"[{ticket_id}] WooCommerce timed out — asking customer for last 4 card digits"
        )
    else:
        log.info(
            f"[{ticket_id}] WooCommerce: {woo_status} — asking customer for last 4 card digits"
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
    "不正請求", # "fraudulent/unauthorized charge"
    "不法請求", # "illegal billing/charge" — variant seen in real tickets
    "詐欺", # fraud / scam
    "不正利用", # unauthorized / fraudulent use
    "無断で引き落とし", # "deducted without consent"
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
    "cargo no autorizado",  # unauthorized charge
    "fraude",               # fraud
    # ── Norwegian/Swedish/Danish ──
    "penger tilbake",       # money back (NO)
    "uautorisert",          # unauthorized (NO)
    "uautoriseret",         # unauthorized (DA)
    "återbetalning",        # refund (SE)
    "tilbakebetaling",      # refund (NO)
    "tilbagebetaling",      # refund (DA)
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
