"""
Zendesk Cancellation Bot — Google Cloud Function (Gen 2)
=========================================================
Entry point: zendesk_webhook(request)

Cancellation flow:
  1. Receive Zendesk webhook with ticket_id
  2. Fetch ticket from Zendesk
  3. Classify intent with Claude Haiku
  4. Try WooCommerce first → cancel trial or subscription
  5. If not found in WooCommerce → fall back to Stripe
  6. If not found anywhere → Slack alert + tag ticket for manual review
  7. Generate multilingual reply with Claude Sonnet
  8. Reply + tag + solve ticket in Zendesk
  9. Log result to BigQuery

Handles: Trial Cancel, Sub Cancel, Sub Renewal Cancel
Languages: EN / JP / KR
"""

import os
import json
import logging
import functions_framework

from classifier import classify_ticket
from zendesk_client import ZendeskClient
from woocommerce_client import WooCommerceClient
from stripe_client import StripeClient
from slack_client import SlackClient
from reply_generator import generate_reply
from bq_logger import log_result, ensure_log_table

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger("bot")

DRY_RUN   = os.getenv("DRY_RUN", "true").lower() == "true"
TEST_MODE = os.getenv("TEST_MODE", "true").lower() == "true"
TEST_TAG  = "automation_test"

HANDLED_INTENTS = {
    "TRIAL_CANCELLATION",
    "SUB_CANCELLATION",
    "SUB_RENEWAL_CANCELLATION",
}

ZENDESK_SUBDOMAIN = os.getenv("ZENDESK_SUBDOMAIN", "")

# ── Clients ────────────────────────────────────────────────────────────────── #

zendesk = ZendeskClient(
    subdomain=ZENDESK_SUBDOMAIN,
    email=os.getenv("ZENDESK_EMAIL"),
    api_token=os.getenv("ZENDESK_API_TOKEN"),
    dry_run=DRY_RUN,
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
    webhook_url=os.getenv("SLACK_WEBHOOK_URL", ""),
    dry_run=DRY_RUN,
)


# ── HTTP handler ───────────────────────────────────────────────────────────── #

@functions_framework.http
def zendesk_webhook(request):
    if request.method == "GET":
        return json.dumps({
            "status": "ok",
            "dry_run": DRY_RUN,
            "test_mode": TEST_MODE,
            "handles": list(HANDLED_INTENTS),
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

    try:
        result = _process(ticket_id)
    except Exception as e:
        log.exception(f"[{ticket_id}] Unhandled error: {e}")
        result = {"ticket_id": ticket_id, "status": "error", "error": str(e)}

    return json.dumps(result), 200, {"Content-Type": "application/json"}


# ── Core logic ─────────────────────────────────────────────────────────────── #

def _process(ticket_id: str) -> dict:
    result = {
        "ticket_id": ticket_id,
        "status": "skipped",
        "intent": None,
        "language": None,
        "action": None,
        "dry_run": DRY_RUN,
        "cancel_source": None,
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

    # 2. TEST_MODE gate
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

    result.update({
        "intent":          intent,
        "language":        language,
        "confidence":      confidence,
        "chargeback_risk": classification.get("chargeback_risk", ""),
        "reasoning":       classification.get("reasoning", ""),
    })
    log.info(f"[{ticket_id}] Intent: {intent} ({confidence:.0%}) | Lang: {language}")

    # 4. Skip unhandled intents
    if intent not in HANDLED_INTENTS:
        log.info(f"[{ticket_id}] Skip — not a cancellation ({intent})")
        result["status"] = "skipped_not_handled"
        log_result(result)
        return result

    # 5. Low confidence → escalate
    if confidence < 0.75:
        log.info(f"[{ticket_id}] Low confidence {confidence:.0%} → escalate")
        zendesk.add_tag(ticket_id, "bot_low_confidence")
        zendesk.add_internal_note(
            ticket_id,
            f"🤖 Bot: detected {intent} but confidence {confidence:.0%} — needs human review.",
        )
        result["status"] = "escalated_low_confidence"
        log_result(result)
        return result

    # 6. Cancel — WooCommerce first, Stripe as fallback
    cancel_result = _cancel(email, ticket_id)
    cancel_status = cancel_result.get("status", "")
    result["cancel_source"] = cancel_result.get("source", "unknown")

    log.info(
        f"[{ticket_id}] Cancel result: {cancel_status} "
        f"via {result['cancel_source']} | type={cancel_result.get('subscription_type')}"
    )

    # 7. Customer not found anywhere → Slack alert, leave ticket open
    if cancel_status == "not_found_anywhere":
        log.info(f"[{ticket_id}] Not found in WooCommerce or Stripe → Slack alert")

        zendesk.add_tag(ticket_id, "needs_manual_review")
        zendesk.add_internal_note(
            ticket_id,
            (
                "🤖 Bot: could not find this customer in WooCommerce or Stripe "
                f"(email: {email}). Manual review required — "
                "please verify by last 4 digits of card or alternative email."
            ),
        )

        slack.notify_manual_review(
            ticket_id=ticket_id,
            email=email,
            intent=intent,
            zendesk_subdomain=ZENDESK_SUBDOMAIN,
        )

        result["status"] = "manual_review_required"
        result["action"] = "slack_alerted"
        log_result(result)
        return result   # ← ticket stays OPEN, no reply sent

    # 8. Generate reply
    reply_text = generate_reply(
        intent=intent,
        language=language,
        customer_name=name,
        cancel_result=cancel_result,
    )

    # 9. Zendesk actions
    cancel_tag = {
        "TRIAL_CANCELLATION":       "trial_cancellation",
        "SUB_CANCELLATION":         "subscription_cancelled",
        "SUB_RENEWAL_CANCELLATION": "renewal_cancellation",
    }.get(intent, "cancelled")

    zendesk.post_reply(ticket_id, reply_text)
    zendesk.add_tag(ticket_id, cancel_tag)
    zendesk.add_tag(ticket_id, "bot_handled")
    zendesk.solve_ticket(ticket_id)

    result.update({
        "status":     "success",
        "action":     "cancelled_and_replied",
        "reply_text": reply_text,
    })
    log.info(f"[{ticket_id}] ✅ Done")

    log_result(result)
    return result


# ── Cancellation helpers ───────────────────────────────────────────────────── #

def _cancel(email: str, ticket_id: str) -> dict:
    """
    Try WooCommerce → Stripe → if both fail, return not_found_anywhere.
    """
    # WooCommerce
    woo_result = woo.cancel_subscription(email)
    woo_status = woo_result.get("status", "")

    if woo_status not in ("not_found", "no_active_sub", "error"):
        return {**woo_result, "source": "woocommerce"}

    log.info(f"[{ticket_id}] WooCommerce: {woo_status} → trying Stripe")

    # Stripe fallback
    stripe_result = stripe_cli.cancel_subscription(email)
    stripe_status = stripe_result.get("status", "")

    if stripe_status not in ("not_found", "no_active_sub", "error"):
        return {
            **stripe_result,
            "source": "stripe",
            "subscription_type": "trial" if stripe_status == "trialing" else "subscription",
        }

    log.info(f"[{ticket_id}] Stripe: {stripe_status} → customer not found anywhere")

    # Neither found
    return {
        "status": "not_found_anywhere",
        "email": email,
        "cancelled": False,
        "source": "none",
    }
