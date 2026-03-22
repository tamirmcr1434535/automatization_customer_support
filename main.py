"""
Zendesk Cancellation Bot — Google Cloud Function (Gen 2)
=========================================================
Entry point: zendesk_webhook(request)

Handles: Trial Cancel, Sub Cancel, Sub Renewal Cancel
Languages: EN / JP / KR
"""

import os
import json
import logging
import functions_framework
from classifier import classify_ticket
from zendesk_client import ZendeskClient
from stripe_client import StripeClient
from reply_generator import generate_reply
from bq_logger import log_result, ensure_log_table

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger("bot")

# ── CONFIG (з environment variables) ──────────────────────────────────────
DRY_RUN    = os.getenv("DRY_RUN", "true").lower() == "true"
TEST_MODE  = os.getenv("TEST_MODE", "true").lower() == "true"
TEST_TAG   = "automation_test"

HANDLED_INTENTS = {
    "TRIAL_CANCELLATION",
    "SUB_CANCELLATION",
    "SUB_RENEWAL_CANCELLATION",
}

# Ініціалізуємо клієнтів один раз (між викликами функції — кешується)
zendesk = ZendeskClient(
    subdomain  = os.getenv("ZENDESK_SUBDOMAIN"),
    email      = os.getenv("ZENDESK_EMAIL"),
    api_token  = os.getenv("ZENDESK_API_TOKEN"),
    dry_run    = DRY_RUN,
)
stripe_cli = StripeClient(
    api_key  = os.getenv("STRIPE_SECRET_KEY"),
    dry_run  = DRY_RUN,
)


# ── ENTRY POINT ────────────────────────────────────────────────────────────
@functions_framework.http
def zendesk_webhook(request):
    """
    Cloud Function entry point.
    Zendesk шле POST сюди при кожному новому тікеті.
    """

    # Health check
    if request.method == "GET":
        return json.dumps({
            "status": "ok",
            "dry_run": DRY_RUN,
            "test_mode": TEST_MODE,
            "handles": list(HANDLED_INTENTS),
        }), 200, {"Content-Type": "application/json"}

    if request.method != "POST":
        return "Method not allowed", 405

    # Parse payload
    try:
        payload = request.get_json(silent=True) or {}
    except Exception:
        return "Invalid JSON", 400

    ticket_id = str(payload.get("ticket_id") or payload.get("id", ""))
    if not ticket_id:
        return "No ticket_id", 400

    log.info(f"[{ticket_id}] Webhook received")

    # Відповідаємо Zendesk одразу (200), обробляємо синхронно
    # (Cloud Functions Gen 2 підтримує до 60 хв timeout)
    try:
        result = _process(ticket_id)
    except Exception as e:
        log.exception(f"[{ticket_id}] Unhandled error: {e}")
        result = {"ticket_id": ticket_id, "status": "error", "error": str(e)}

    return json.dumps(result), 200, {"Content-Type": "application/json"}


# ── MAIN LOGIC ─────────────────────────────────────────────────────────────
def _process(ticket_id: str) -> dict:
    result = {
        "ticket_id": ticket_id,
        "status": "skipped",
        "intent": None,
        "language": None,
        "action": None,
        "dry_run": DRY_RUN,
    }

    # 1. Fetch ticket
    ticket = zendesk.get_ticket(ticket_id)
    if not ticket:
        log.warning(f"[{ticket_id}] Not found in Zendesk")
        result["status"] = "not_found"
        return result

    subject   = ticket.get("subject", "")
    body      = ticket.get("description", "")
    tags      = ticket.get("tags", [])
    requester = ticket.get("requester", {})
    email     = requester.get("email", "")
    name      = requester.get("name", "")

    log.info(f"[{ticket_id}] Subject: {subject[:60]} | Email: {email}")

    # 2. Test mode — тільки тікети з тегом automation_test
    if TEST_MODE and TEST_TAG not in tags:
        log.info(f"[{ticket_id}] Skip — test mode, missing tag '{TEST_TAG}'")
        result["status"] = "skipped_no_test_tag"
        log_result(result)
        return result

    # 3. Classify
    classification = classify_ticket(subject, body)
    intent     = classification["intent"]
    language   = classification["language"]
    confidence = classification["confidence"]

    result["intent"]   = intent
    result["language"] = language
    log.info(f"[{ticket_id}] Intent: {intent} ({confidence:.0%}) | Lang: {language}")

    # 4. Не наш інтент — пропускаємо
    if intent not in HANDLED_INTENTS:
        log.info(f"[{ticket_id}] Skip — not a cancellation ({intent})")
        result["status"] = "skipped_not_handled"
        log_result(result)
        return result

    # 5. Низька впевненість → людина
    if confidence < 0.75:
        log.info(f"[{ticket_id}] Low confidence {confidence:.0%} → escalate")
        zendesk.add_tag(ticket_id, "bot_low_confidence")
        zendesk.add_internal_note(
            ticket_id,
            f"🤖 Bot: detected {intent} but confidence {confidence:.0%} — needs human review"
        )
        result["status"] = "escalated_low_confidence"
        log_result(result)
        return result

    # 6. Cancel in Stripe
    cancel_result = stripe_cli.cancel_subscription(email)
    log.info(f"[{ticket_id}] Stripe: {cancel_result['status']}")

    # 7. Generate reply
    reply_text = generate_reply(
        intent=intent,
        language=language,
        customer_name=name,
        stripe_result=cancel_result,
    )

    # 8. Zendesk: reply + tag + solve
    cancel_tag = {
        "TRIAL_CANCELLATION":       "trial_cancellation",
        "SUB_CANCELLATION":         "subscription_cancelled",
        "SUB_RENEWAL_CANCELLATION": "renewal_cancellation",
    }.get(intent, "cancelled")

    zendesk.post_reply(ticket_id, reply_text)
    zendesk.add_tag(ticket_id, cancel_tag)
    zendesk.add_tag(ticket_id, "bot_handled")
    zendesk.solve_ticket(ticket_id)

    result["status"] = "success"
    result["action"] = "cancelled_and_replied"
    log.info(f"[{ticket_id}] ✅ Done")

    log_result(result)
    return result
