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
)
from bq_logger import log_result as _bq_log_result


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
# If these are present alongside refund/fraud keywords, the CANCEL intent wins
# (Rule 1a: "cancel always wins"). Only override to REFUND_REQUEST when there
# are ZERO cancel signals.
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

# ── Shadow deduplication (Firestore-backed distributed lock) ─────────── #
# Zendesk fires 5-15 webhooks per ticket (creation, agent reply, tag
# change, status change, etc.). Each webhook is a separate HTTP request.
#
# In-memory dedup only works within a single Cloud Run instance.
# Cloud Functions Gen 2 can spin up MULTIPLE instances → each has its
# own empty Python dict → duplicates leak through.
#
# Fix: Firestore atomic create() as distributed mutex.
#   - create() fails with ALREADY_EXISTS if another instance already claimed
#   - Two-layer approach: in-memory fast path + Firestore distributed lock
#   - Firestore TTL policy auto-deletes old entries (set in GCP Console)
#
# One-time Firestore TTL setup (run once in GCP Console or gcloud):
#   gcloud firestore fields ttls update expire_at \
#     --collection-group=shadow_dedup --enable-ttl

import threading as _threading, time as _time
from datetime import datetime, timedelta, timezone

# Layer 1: in-memory cache (fast path, avoids Firestore call for same-instance dupes)
_shadow_seen: dict[str, float] = {}   # ticket_id → timestamp
_shadow_lock = _threading.Lock()
_SHADOW_TTL  = 7200   # 2 hours

# Layer 2: Firestore (distributed across all Cloud Run instances)
_firestore_db = None

def _get_firestore_db():
    """Lazy-init Firestore client (reused across requests in same instance)."""
    global _firestore_db
    if _firestore_db is None:
        from google.cloud import firestore as _fs
        _firestore_db = _fs.Client()
    return _firestore_db


def _shadow_dedup(ticket_id: str) -> bool:
    """
    Return True if this ticket was already processed (duplicate).
    Two-layer dedup:
      1. In-memory dict — instant, handles same-instance dupes (no network call)
      2. Firestore create() — atomic distributed lock across all instances
    """
    now = _time.time()
    tid = str(ticket_id)

    # ── Layer 1: in-memory (fast path) ──
    with _shadow_lock:
        if len(_shadow_seen) > 500:
            stale = [k for k, v in _shadow_seen.items() if now - v > _SHADOW_TTL]
            for k in stale:
                del _shadow_seen[k]
        if tid in _shadow_seen:
            return True  # duplicate (same instance hit)

    # ── Layer 2: Firestore (distributed lock) ──
    try:
        db = _get_firestore_db()
        doc_ref = db.collection("shadow_dedup").document(tid)
        # Atomic create — raises AlreadyExists if another instance claimed first
        doc_ref.create({
            "ticket_id": tid,
            "created_at": datetime.now(timezone.utc),
            "expire_at": datetime.now(timezone.utc) + timedelta(hours=2),
        })
        # Success — we are the first instance to claim this ticket
        with _shadow_lock:
            _shadow_seen[tid] = now
        log.info(f"[{tid}] Shadow dedup: claimed in Firestore (first)")
        return False
    except Exception as e:
        err_str = str(e)
        if "already exists" in err_str.lower() or "ALREADY_EXISTS" in err_str:
            # Another instance already claimed — this is a duplicate
            with _shadow_lock:
                _shadow_seen[tid] = now  # cache for future fast-path
            return True
        # Firestore error (network, permissions, etc.) — fail open with in-memory only
        log.warning(f"[{tid}] Firestore dedup error, falling back to in-memory: {e}")
        with _shadow_lock:
            if tid in _shadow_seen:
                return True
            _shadow_seen[tid] = now
            return False


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

    # ── SHADOW_MODE: in-memory deduplication ─────────────────────────── #
    # Zendesk fires 5-15 webhooks per ticket (creation, agent reply, tag
    # change, status change — EACH triggers a new webhook).
    # Zendesk tags do NOT work as a lock:
    #   - add_tag itself triggers ANOTHER webhook (infinite loop)
    #   - get_ticket_tags has eventual consistency (stale reads)
    #   - Multiple Cloud Function instances read tags concurrently
    #
    # Fix: in-memory Python set. Atomic check+claim under a thread lock.
    # Works within a single Cloud Run instance (handles 95%+ of dupes).
    # No Zendesk API calls = no new webhooks triggered.
    if SHADOW_MODE and _shadow_dedup(ticket_id):
        log.info(f"[{ticket_id}] Shadow: duplicate (in-memory) — skip")
        return json.dumps({"ticket_id": ticket_id, "status": "skipped_shadow_duplicate"}), 200, {"Content-Type": "application/json"}

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
    "skipped_not_handled":          "shadow_would_skip",
    "skipped_followup":             "shadow_would_skip",
    "skipped_closed":               "shadow_would_skip",
    "awaiting_card_digits":         "shadow_would_ask_card",
    "awaiting_card_digits_retry":   "shadow_would_ask_card",
    "skipped_agent_already_replied":"shadow_agent_handling",
    "skipped_spam_detected":        "shadow_spam",
    "not_found_closed":             "shadow_would_escalate",
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
        # so always fetch first customer comment for classification.
        _is_msg = subject.lower().startswith("conversation with")
        if _is_msg or len(body.strip()) < 30:
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
    email      = requester.get("email", "")
    name       = requester.get("name", "")
    result["email"] = email

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
    # For Messaging tickets (subject "Conversation with ..."), ALWAYS fetch the first
    # customer comment — that's where the actual request is (form data, typed message).
    # For other tickets, fallback if description is very short (< 30 chars).
    _is_messaging = subject.lower().startswith("conversation with")
    if _is_messaging or len(body.strip()) < 30:
        first_comment = zendesk.get_first_customer_comment(ticket_id)
        if first_comment:
            if _is_messaging:
                log.info(
                    f"[{ticket_id}] Messaging ticket — using first customer comment "
                    f"for classification (description is agent greeting)"
                )
            else:
                log.info(f"[{ticket_id}] Empty description — using first customer comment for classification")
            body = first_comment

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
    if intent in HANDLED_INTENTS:
        try:
            all_comments = zendesk.get_all_customer_comments_text(ticket_id)
            if all_comments:
                _all_text_for_refund += " " + all_comments
        except Exception:
            log.warning(f"[{ticket_id}] Failed to fetch comments for refund check")
    _has_refund_kw = intent in HANDLED_INTENTS and _contains_refund_request(_all_text_for_refund)
    _has_cancel_kw = _contains_cancel_signal(_all_text_for_refund) if _has_refund_kw else False

    if _has_refund_kw and _has_cancel_kw:
        # Both cancel AND refund/fraud signals present — CANCEL WINS (Rule 1a).
        # Examples: "cancel my subscription + I'll report as fraud"
        #           "解約したい + 返金してほしい"
        # The bot cancels first; refund is handled by humans later.
        log.info(
            f"[{ticket_id}] {intent}: refund keywords detected BUT cancel signals "
            "also present → cancel wins (Rule 1a), NOT overriding to REFUND_REQUEST"
        )
        # Continue to cancellation flow — don't override

    elif _has_refund_kw and not _has_cancel_kw:
        # ONLY refund/fraud signals, ZERO cancel signals → pure refund request.
        # Human must handle this (bot cannot auto-cancel).
        log.info(
            f"[{ticket_id}] {intent} but refund keywords detected (no cancel signals) "
            "→ overriding to REFUND_REQUEST (human must handle refund)"
        )
        intent = "REFUND_REQUEST"
        result["intent"] = intent
        result["status"] = "skipped_refund_request"
        zendesk.add_tag(ticket_id, "bot_handled")  # block parallel webhook
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

    # REFUND_REQUEST / SUB_RENEWAL_REFUND — check if there's also a cancel intent.
    # Many customers say "cancel and refund" but the priority is to cancel the
    # subscription immediately so they stop being charged. The bot cancels,
    # and adds an internal note about the refund request for the human team.
    if intent in ("REFUND_REQUEST", "SUB_RENEWAL_REFUND"):
        full_text = subject + " " + body
        has_cancel = _contains_cancel_signal(full_text)

        if has_cancel:
            # Override intent: customer wants cancellation + refund → cancel first
            log.info(
                f"[{ticket_id}] {intent} but cancel signal found in text — "
                "overriding to TRIAL_CANCELLATION (cancel first, refund later)"
            )
            intent = "TRIAL_CANCELLATION"
            result["intent"] = intent
            result["refund_also_requested"] = True
        else:
            # Pure refund request with no cancellation intent → human handles
            log.info(f"[{ticket_id}] {intent} — pure refund, no cancel signal → skipping")
            result["status"] = "skipped_refund_request"
            zendesk.add_tag(ticket_id, "bot_handled")  # block parallel webhook
            slack_sent = slack.notify_refund_skip(
                ticket_id=ticket_id, email=email,
                intent=intent, zendesk_subdomain=ZENDESK_SUBDOMAIN,
            )
            result["slack_sent"] = slack_sent
            log_result(result)
            return result

    # ── CARD DIGITS FLOWS ─────────────────────────────────────────────── #
    # Timeout: Zendesk Automation added tag after AWAITING_CARD_DAYS days of no reply
    if "card_digits_timeout" in tags:
        return _handle_card_digits_timeout(ticket_id, name, language, result)

    # FIX: Anti-spam / webhook-loop guard for card-digits flows.
    #
    # Root cause of the spam bug:
    #   When the bot adds tag "awaiting_card_digits" or calls post_reply_and_set_pending,
    #   Zendesk fires the webhook again. The new call sees "awaiting_card_digits" in tags
    #   and routes to _handle_card_digits. But the ticket is still "pending" — no new
    #   customer reply has arrived. get_last_customer_comment returns the ORIGINAL message
    #   which has no 4-digit code, so _digits_not_found is called → sends ANOTHER reply.
    #   This cascades: each reply triggers a new webhook → 4 identical messages.
    #
    # Fix: only enter card-digits handlers if ticket_status == "open".
    #   "open"    = customer just replied → process it
    #   "pending" = we asked, waiting for customer → skip (no action needed)
    #
    # When the customer replies, Zendesk auto-moves the ticket from pending → open,
    # which fires the webhook with status="open" and we process their reply correctly.

    # Second attempt: customer replied after we asked again (retry)
    if "awaiting_card_digits_retry" in tags:
        if ticket_status == "pending":
            log.info(
                f"[{ticket_id}] awaiting_card_digits_retry but ticket is pending "
                "— no new customer reply yet, skipping (anti-spam)"
            )
            result["status"] = "skipped_pending_awaiting_reply"
            return result
        return _handle_card_digits(
            ticket_id, email, name, language, result, is_retry=True
        )

    # First attempt: customer replied with digits after first ask
    if "awaiting_card_digits" in tags:
        if ticket_status == "pending":
            log.info(
                f"[{ticket_id}] awaiting_card_digits but ticket is pending "
                "— no new customer reply yet, skipping (anti-spam)"
            )
            result["status"] = "skipped_pending_awaiting_reply"
            return result
        return _handle_card_digits(
            ticket_id, email, name, language, result, is_retry=False
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

    # 4. Skip unhandled intents
    if intent not in HANDLED_INTENTS:
        log.info(f"[{ticket_id}] Skip — not a cancellation ({intent})")
        result["status"] = "skipped_not_handled"
        log_result(result)
        return result

    # 5. Low confidence → escalate (unless cancel keyword confirms the intent)
    # For short Messaging form submissions like "cancel" or "解約", the classifier
    # often returns low confidence due to lack of context. But the keyword itself
    # is unambiguous — if it matches a cancel signal, trust the intent.
    _keyword_confirms_intent = (
        intent in HANDLED_INTENTS
        and _contains_cancel_signal(subject + " " + body)
    )
    if _keyword_confirms_intent and confidence < 0.65:
        log.info(
            f"[{ticket_id}] Low confidence {confidence:.0%} BUT cancel keyword "
            f"confirms {intent} → proceeding (keyword override)"
        )
        # Don't escalate — keyword match confirms the classifier's intent

    elif confidence < 0.65:
        log.info(f"[{ticket_id}] Low confidence {confidence:.0%} → escalate to agent")

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
            f"🤖 Bot: detected {intent} but confidence {confidence:.0%} is too low to act automatically. "
            "Please review and handle manually.",
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
        log.info(f"[{ticket_id}] No alt email worked → trying Stripe by email")
        stripe_result = stripe_cli.cancel_subscription(email)
        stripe_status = stripe_result.get("status", "")

        if stripe_status not in ("not_found", "no_active_sub", "error"):
            log.info(
                f"[{ticket_id}] ✅ Stripe fallback: cancelled {stripe_result.get('subscription_type')} "
                f"sub {stripe_result.get('subscription_id')} for {email}"
            )
            cancel_result = {**stripe_result, "source": "stripe"}
            result["cancel_source"] = "stripe"
            final_intent = _resolve_intent(intent, cancel_result)
            result["intent"] = final_intent
            zendesk.add_internal_note(
                ticket_id,
                f"🤖 Bot: found in WooCommerce but no active sub. "
                f"Cancelled in Stripe directly "
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
        log.info(f"[{ticket_id}] WC not found → trying Stripe by email as fallback")
        stripe_result = stripe_cli.cancel_subscription(email)
        stripe_status = stripe_result.get("status", "")

        if stripe_status not in ("not_found", "no_active_sub", "error"):
            # ✅ Stripe found and cancelled the subscription
            log.info(
                f"[{ticket_id}] ✅ Stripe fallback: cancelled {stripe_result.get('subscription_type')} "
                f"sub {stripe_result.get('subscription_id')} for {email}"
            )
            cancel_result = {**stripe_result, "source": "stripe"}
            result["cancel_source"] = "stripe"
            final_intent = _resolve_intent(intent, cancel_result)
            result["intent"] = final_intent
            zendesk.add_internal_note(
                ticket_id,
                f"🤖 Bot: not found in WooCommerce by email ({email}). "
                f"Found and cancelled in Stripe directly "
                f"(sub={stripe_result.get('subscription_id')}).",
            )
            return _finish_cancellation(
                ticket_id, name, language, final_intent, cancel_result, result
            )

        if stripe_status == "no_active_sub":
            log.info(f"[{ticket_id}] Stripe: customer found but no active sub")

        # Still not found → ask for last 4 card digits (step 1)
        log.info(f"[{ticket_id}] Not found by email → asking for last 4 card digits")

        # FIX: Race condition guard — re-fetch current tags just before sending the reply.
        # Multiple concurrent webhook calls (Zendesk can fire several in quick succession)
        # may all reach this point before any of them adds the tag.
        # Re-fetching tags gives us a chance to detect if a parallel call already acted.
        current_tags = zendesk.get_ticket_tags(ticket_id)
        if (
            "awaiting_card_digits" in current_tags
            or "awaiting_card_digits_retry" in current_tags
            or "bot_handled" in current_tags
        ):
            log.info(
                f"[{ticket_id}] Race condition: tag already set by a concurrent webhook call "
                "— skipping duplicate reply"
            )
            result["status"] = "skipped_race_condition"
            return result

        reply_text = generate_ask_card_digits_reply(language=language, customer_name=name)
        # Tag BEFORE reply: Zendesk can fire the webhook twice in quick succession.
        # If the tag is set first, any concurrent/duplicate call will be routed to
        # _handle_card_digits instead of re-entering this path and sending a second message.
        zendesk.add_tag(ticket_id, "awaiting_card_digits")
        # Set ticket to Pending so Zendesk trigger fires only on customer reply,
        # not on bot/agent updates. Pending → Open transition triggers the webhook.
        zendesk.post_reply_and_set_pending(ticket_id, reply_text)
        zendesk.add_internal_note(
            ticket_id,
            f"🤖 Bot: customer not found by email ({email}). "
            "Asked for last 4 card digits. Waiting up to 7 days.",
        )

        result.update({
            "status": "awaiting_card_digits",
            "action": "asked_for_card_digits",
            "reply_text": reply_text,
        })
        log_result(result)
        return result # ticket set to Pending — awaiting customer reply

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

        result.update({
            "status": "awaiting_card_digits_retry",
            "action": "asked_for_correct_digits",
            "reply_text": reply_text,
        })
        log_result(result)
        return result # ticket set to Pending — awaiting customer reply

    else:
        # Second failure: close ticket
        log.info(f"[{ticket_id}] Digits not found (retry) → closing ticket")

        reply_text = generate_not_found_reply(language=language, customer_name=name)
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

    result.update({
        "status": "closed_no_response",
        "action": f"timeout_closed_{AWAITING_CARD_DAYS}d",
        "reply_text": reply_text,
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
            return None

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
