"""
Zendesk Cancellation Bot — Google Cloud Function (Gen 2)
=========================================================
Cancellation flow:
 1. Check by Zendesk email (WooCommerce → Stripe)
 2. Found → cancel → done
 3. Not found → extract any emails mentioned in ticket body → try each one
    └─ Found by alt email → cancel → done
 4. Still not found → ask for last 4 card digits (tag: awaiting_card_digits)
    └─ 7 days no reply → Zendesk Automation closes ticket
    └─ Customer replied:
       ├─ Found → cancel → done
       └─ Not found → ask for correct digits (tag: awaiting_card_digits_retry)
          └─ 2 days no reply → Zendesk Automation closes ticket
          └─ Customer replied:
             ├─ Found → cancel → done
             └─ Not found → close ticket
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
from bq_logger import log_result

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger("bot")

DRY_RUN            = os.getenv("DRY_RUN", "true").lower() == "true"
TEST_MODE          = os.getenv("TEST_MODE", "true").lower() == "true"
TEST_TAG           = "automation_test"
# How many days to wait for card/payment info before auto-closing the ticket.
# Controlled via env var so it can be changed without a code deploy.
AWAITING_CARD_DAYS = int(os.getenv("AWAITING_CARD_DAYS", "7"))

HANDLED_INTENTS = {
    "TRIAL_CANCELLATION",
    "SUB_CANCELLATION",
    "SUB_RENEWAL_CANCELLATION",
}

ZENDESK_SUBDOMAIN = os.getenv("ZENDESK_SUBDOMAIN", "")

# ── Clients ──────────────────────────────────────────────────────────── #

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
    bot_token=os.getenv("SLACK_BOT_TOKEN", ""),
    target_email=os.getenv("SLACK_TARGET_EMAIL", ""),
    dry_run=DRY_RUN,
)


# ── HTTP handler ──────────────────────────────────────────────────────── #

@functions_framework.http
def zendesk_webhook(request):
    if request.method == "GET":
        return json.dumps({
            "status": "ok", "dry_run": DRY_RUN,
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

    try:
        result = _process(ticket_id)
    except Exception as e:
        log.exception(f"[{ticket_id}] Unhandled error: {e}")
        result = {"ticket_id": ticket_id, "status": "error", "error": str(e)}

    return json.dumps(result), 200, {"Content-Type": "application/json"}


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

    subject   = ticket.get("subject", "")
    body      = ticket.get("description", "")
    tags      = ticket.get("tags", [])
    requester = ticket.get("requester", {})
    email     = requester.get("email", "")
    name      = requester.get("name", "")
    result["email"] = email

    log.info(f"[{ticket_id}] Subject: {subject[:60]} | Email: {email}")

    # 2. Idempotency check — skip if bot already handled this ticket.
    #    Prevents duplicate replies from double Zendesk webhook triggers.
    #    (Zendesk sometimes fires the same trigger twice in quick succession.)
    if "bot_handled" in tags:
        log.info(f"[{ticket_id}] Already handled by bot (tag: bot_handled) — skipping duplicate webhook")
        result["status"] = "skipped_already_handled"
        return result

    # 2b. Skip merged-away tickets (Zendesk adds 'merge' tag to the donor ticket
    #     when it is merged into another ticket). The donor has no actionable content —
    #     all conversation was moved to the target ticket which will be processed separately.
    if "merge" in tags:
        log.info(f"[{ticket_id}] Skipping — ticket was merged into another (tag: merge)")
        result["status"] = "skipped_merged"
        return result

    # 2c. Skip if a human agent already replied publicly.
    #     If the last public comment is from our team, they are already handling it —
    #     no need for the bot to classify, escalate, or interfere.
    if zendesk.last_public_comment_is_from_agent(ticket_id):
        log.info(
            f"[{ticket_id}] Last public comment is from an agent — "
            "skipping (human already replied)"
        )
        result["status"] = "skipped_agent_already_replied"
        return result

    # 3. TEST_MODE gate
    if TEST_MODE and TEST_TAG not in tags:
        log.info(f"[{ticket_id}] Skip — test mode, missing tag '{TEST_TAG}'")
        result["status"] = "skipped_no_test_tag"
        log_result(result)
        return result

    # 2b. Messaging/chat tickets have empty description — fetch first real customer comment
    if len(body.strip()) < 30:
        first_comment = zendesk.get_first_customer_comment(ticket_id)
        if first_comment:
            log.info(f"[{ticket_id}] Empty description — using first customer comment for classification")
            body = first_comment

    # 3. Classify (needed for language even in card-lookup flows)
    classification = classify_ticket(subject, body)
    intent     = classification["intent"]
    language   = classification["language"]
    confidence = classification["confidence"]

    result.update({
        "intent": intent,
        "language": language,
        "confidence": confidence,
        "chargeback_risk": classification.get("chargeback_risk", ""),
        "reasoning": classification.get("reasoning", ""),
    })
    log.info(f"[{ticket_id}] Intent: {intent} ({confidence:.0%}) | Lang: {language}")

    # ── REFUND / PAYMENT DISPUTE intents → skip (human must handle) ──────── #
    # These are payment disputes / refund requests — NOT simple cancellations.
    # The bot must not touch these tickets; route to human review.
    # All of these return skipped_refund_request so eval ground truth matches.
    _REFUND_INTENTS = {
        "CHARGEBACK_THREAT",   # customer threatening / filing chargeback
        "REFUND_REQUEST",      # customer explicitly asking for a refund
        "PAYPAL_DISPUTE",      # PayPal dispute opened
        "SUB_RENEWAL_REFUND",  # refund on renewal charge
    }
    if intent in _REFUND_INTENTS:
        log.info(f"[{ticket_id}] {intent} — skipping (payment dispute / refund, not handled by bot)")
        result["status"] = "skipped_refund_request"
        log_result(result)
        return result

    # ── REFUND + CANCEL COMBINED → skip silently ──────────────────────── #
    # If the customer asks for both cancellation AND a refund/repayment,
    # we don't touch the ticket — a human will handle it.
    # Check subject + body together: the refund signal may appear in either field.
    if intent in HANDLED_INTENTS and _contains_refund_request(subject + " " + body):
        log.info(
            f"[{ticket_id}] Refund request detected alongside {intent} — "
            "skipping (not handled by bot)"
        )
        result["status"] = "skipped_refund_request"
        log_result(result)
        return result

    # ── CARD DIGITS FLOWS ─────────────────────────────────────────────── #
    # Timeout: Zendesk Automation added tag after AWAITING_CARD_DAYS days of no reply
    if "card_digits_timeout" in tags:
        return _handle_card_digits_timeout(ticket_id, name, language, result)

    # Second attempt: customer replied after we asked again (retry)
    if "awaiting_card_digits_retry" in tags:
        return _handle_card_digits(
            ticket_id, email, name, language, result, is_retry=True
        )

    # First attempt: customer replied with digits after first ask
    if "awaiting_card_digits" in tags:
        return _handle_card_digits(
            ticket_id, email, name, language, result, is_retry=False
        )

    # ── NORMAL CANCELLATION FLOW ──────────────────────────────────────── #

    # 4. Skip unhandled intents
    if intent not in HANDLED_INTENTS:
        log.info(f"[{ticket_id}] Skip — not a cancellation ({intent})")
        result["status"] = "skipped_not_handled"
        log_result(result)
        return result

    # 5. Low confidence → escalate
    if confidence < 0.65:
        log.info(f"[{ticket_id}] Low confidence {confidence:.0%} → escalate to agent")
        zendesk.add_tag(ticket_id, "bot_handled")        # first — blocks webhook re-fires
        zendesk.add_tag(ticket_id, "bot_low_confidence")
        zendesk.add_tag(ticket_id, "ai_bot_failed")
        zendesk.add_internal_note(
            ticket_id,
            f"🤖 Bot: detected {intent} but confidence {confidence:.0%} is too low to act automatically. "
            "Please review and handle manually.",
        )
        zendesk.set_open(ticket_id)
        slack.notify_manual_review(
            ticket_id=ticket_id,
            email=email,
            intent=intent,
            zendesk_subdomain=ZENDESK_SUBDOMAIN,
        )
        result["status"] = "escalated_low_confidence"
        log_result(result)
        return result

    # 6. Cancel by email (WooCommerce → Stripe)
    cancel_result = _cancel_by_email(email, ticket_id)
    cancel_status = cancel_result.get("status", "")
    result["cancel_source"] = cancel_result.get("source", "unknown")

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
        search_text = f"{body}\n{all_comments}"
        alt_found = _try_alt_emails(ticket_id, email, search_text, intent, name, language, result)
        if alt_found:
            return alt_found

        # No working alt email → Slack alert for manual review
        log.info(f"[{ticket_id}] No alt email worked → Slack alert + escalate to agent")
        zendesk.add_tag(ticket_id, "bot_handled")        # first — blocks webhook re-fires
        zendesk.add_tag(ticket_id, "needs_manual_review")
        zendesk.add_tag(ticket_id, "ai_bot_failed")
        zendesk.add_internal_note(
            ticket_id,
            f"🤖 Bot: customer email ({email}) found in {found_in} but has NO active subscription. "
            "Subscription may already be cancelled, or registered under a different email. "
            "Please review and handle manually.",
        )
        zendesk.set_open(ticket_id)
        slack.notify_manual_review(
            ticket_id=ticket_id,
            email=email,
            intent=intent,
            zendesk_subdomain=ZENDESK_SUBDOMAIN,
        )
        result.update({
            "status": "manual_review_required",
            "action": "slack_alerted_no_active_sub",
        })
        log_result(result)
        return result

    # 7b. Email not found → try alternative emails from ALL comments (not just description)
    if cancel_status == "not_found_anywhere":
        # Fetch all customer comments to cover emails mentioned in follow-up replies,
        # not only the initial ticket description.
        all_comments = zendesk.get_all_customer_comments_text(ticket_id)
        search_text = f"{body}\n{all_comments}"
        alt_found = _try_alt_emails(ticket_id, email, search_text, intent, name, language, result)
        if alt_found:
            return alt_found

        # Still not found → ask for last 4 card digits (step 1)
        log.info(f"[{ticket_id}] Not found by email → asking for last 4 card digits")

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
        return result  # ticket set to Pending — awaiting customer reply

    # 8. Override intent from actual subscription data (trial vs active sub)
    #    Text classifier gives a hint, but the source of truth is WooCommerce/Stripe.
    if cancel_status == "already_cancelled":
        log.info(
            f"[{ticket_id}] Subscription already cancelled in WC — "
            f"type={cancel_result.get('subscription_type')} → confirming to customer"
        )
    intent = _resolve_intent(intent, cancel_result)
    result["intent"] = intent
    log.info(f"[{ticket_id}] Final intent after data lookup: {intent}")

    # 9. Found → generate reply, tag, solve
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
    is_retry=True  → second attempt (came from awaiting_card_digits_retry)
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
    log.info(f"[{ticket_id}] Extracted last4={last4} — searching Stripe")

    stripe_result = stripe_cli.find_and_cancel_by_last4(last4)

    if stripe_result.get("found"):
        # ✅ Found by card — cancel and close
        cancel_result = {**stripe_result, "source": "stripe_by_card"}
        result["cancel_source"] = "stripe_by_card"

        # Clean up card-lookup tags
        zendesk.remove_tag(ticket_id, "awaiting_card_digits")
        zendesk.remove_tag(ticket_id, "awaiting_card_digits_retry")

        # Determine trial vs sub from actual Stripe data
        final_intent = _resolve_intent(result.get("intent", "SUB_CANCELLATION"), cancel_result)
        result["intent"] = final_intent

        log.info(f"[{ticket_id}] ✅ Found by card last4={last4} — intent={final_intent} — cancelling")
        return _finish_cancellation(
            ticket_id, name, language, final_intent, cancel_result, result
        )

    # ❌ Digits not found in Stripe
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
        return result  # ticket set to Pending — awaiting customer reply

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


def _extract_emails(text: str, exclude: str = "") -> list[str]:
    """
    Extract email addresses from ticket text.
    Handles both ASCII @ and full-width ＠ (common in Japanese tickets).
    Returns unique emails excluding the one already tried (Zendesk email).
    """
    # Normalise full-width ＠ → @
    normalized = text.replace("＠", "@")
    found = re.findall(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}', normalized)
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
    - subscription_type == "trial"        → TRIAL_CANCELLATION
    - subscription_type == "subscription" → SUB_CANCELLATION
    - anything else                       → keep text_intent as fallback
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
    """Generate reply, tag, and solve the ticket after a successful cancellation."""
    reply_text = generate_reply(
        intent=intent,
        language=language,
        customer_name=name,
        cancel_result=cancel_result,
    )

    cancel_tag = {
        "TRIAL_CANCELLATION":       "trial_cancellation",
        "SUB_CANCELLATION":         "subscription_cancelled",
        "SUB_RENEWAL_CANCELLATION": "renewal_cancellation",
    }.get(intent, "cancelled")

    zendesk.add_tag(ticket_id, "bot_handled")   # first — blocks any re-entry from webhook re-fires
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
    WooCommerce → Stripe by email.

    Returns one of:
      - cancel result dict           — found and cancelled
      - status="not_found_anywhere"  — email unknown in BOTH systems → ask card digits
      - status="found_no_active_sub" — email found somewhere but NO active subscription
                                       (already cancelled or in different system) → Slack alert
    """
    woo_result = woo.cancel_subscription(email)
    woo_status = woo_result.get("status", "")

    if woo_status not in ("not_found", "no_active_sub", "error", "timeout"):
        return {**woo_result, "source": "woocommerce"}

    if woo_status == "timeout":
        # WC subscription lookup timed out — server overloaded.
        # Try Stripe directly; do NOT count as "customer found" because we
        # couldn't confirm subscription state. If Stripe also misses them,
        # ask for card digits rather than triggering a Slack alert (we don't
        # actually know their subscription is gone, just that WC was slow).
        log.warning(f"[{ticket_id}] WooCommerce timed out → trying Stripe directly")
        woo_customer_found = False
    else:
        woo_customer_found = woo_status == "no_active_sub"  # customer exists but no active sub
    log.info(f"[{ticket_id}] WooCommerce: {woo_status} → trying Stripe")

    stripe_result = stripe_cli.cancel_subscription(email)
    stripe_status = stripe_result.get("status", "")

    if stripe_status not in ("not_found", "no_active_sub", "error"):
        return {
            **stripe_result,
            "source": "stripe",
            "subscription_type": "trial" if stripe_status == "trialing" else "subscription",
        }

    stripe_customer_found = stripe_status == "no_active_sub"  # customer exists in Stripe

    # Customer found in at least one system but no active subscription
    if woo_customer_found or stripe_customer_found:
        found_in = "WooCommerce" if woo_customer_found else "Stripe"
        log.info(
            f"[{ticket_id}] Customer found in {found_in} but no active sub → "
            "might already be cancelled or sub in different system"
        )
        return {
            "status": "found_no_active_sub",
            "email": email,
            "cancelled": False,
            "source": found_in.lower(),
            "found_in": found_in,
        }

    # Email completely unknown in both systems → ask for card digits
    log.info(f"[{ticket_id}] Email not found anywhere → ask for card digits")
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
    # Japanese — payment cancellation / reversal = wanting a past payment undone (refund intent)
    # These are distinct from "subscription cancellation" — customer is asking to reverse a charge
    "支払いをキャンセル",    # cancel the payment (refund this specific payment)
    "支払いを取り消",         # reverse/cancel the payment
    "支払いのキャンセル",     # payment cancellation (noun form)
    "課金を取り消",           # cancel/reverse the charge
    "課金のキャンセル",       # charge cancellation
    "決済をキャンセル",       # cancel the transaction/settlement
    "決済を取り消",           # reverse the transaction
    "引き落としを取り消",     # reverse the bank deduction
    "請求を取り消",           # reverse/cancel the billing
    "請求のキャンセル",       # billing cancellation (noun form)
    "添付の支払い",           # "the attached payment" — specific payment reference = refund intent
    # Japanese — unauthorized / unexpected charge patterns
    "身に覚えの",        # "I don't recognise this charge" (身に覚えのない引き落とし)
    "身に覚えがない",    # variant
    "勝手に引き落とし",  # "deducted without consent"
    "不正請求",          # "fraudulent/unauthorized charge"
    "不法請求",          # "illegal billing/charge" — variant seen in real tickets
    "詐欺",             # fraud / scam
    "不正利用",         # unauthorized / fraudulent use
    "無断で引き落とし",  # "deducted without consent"
    "知らない間に",      # "without my knowledge" (charged)
    "登録した覚えがない", # "I don't recall signing up"
    "利用した覚えがない",   # "I don't recall using it"
    "利用をした覚えがない",  # particle variant
    "利用の覚えがない",     # another variant
    "月額利用をした覚えがない",  # "I don't recall using the monthly service"
    "心当たりがない",    # "have no recollection of this" — strong implicit refund signal
    "心当たりがございません",  # polite variant
    "心当たりがありません",    # another polite variant
    "身に覚えがありません",    # polite variant of 身に覚えがない
    "覚えがありません",  # polite "I don't recall"
    "覚えがございません", # formal polite variant
    "気づかなかった",    # "I didn't notice" (the charge)
    "知りませんでした",  # "I didn't know about it"
    "知らなかった",      # "I didn't know"
    "把握していなかった", # "I wasn't aware"
    "引き落とされている", # "is being deducted" — customer noticing unexpected ongoing charge
    "引き落とされていた", # past tense variant
    "勝手に課金",        # "charged without consent"
    "勝手に請求",        # "billed without consent"
    # English
    "refund", "repayment", "reimbursement", "money back", "chargeback",
    "charge back", "get my money", "pay me back",
    "cancel payment",    # "cancel payment" = wanting a payment reversed
    "cancel charge",     # variant
    "unauthorized charge", "unknown charge", "unexpected charge",
    "charged without",   # "charged without my consent/knowledge"
    "didn't authorize",  # "I didn't authorize this charge"
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
    "didn't know i was", "did not know i was",   # "I didn't know I was being charged"
    "didn't realize i was", "did not realize i was",
    "wasn't aware", "was not aware",
    "had no idea", "have no idea",               # "I had no idea I was being charged"
    "never intended to", "never wanted",
    # Korean
    "환불",
    "승인취소",    # "approval cancellation" = payment reversal (Korean payment term)
    "승인 취소",   # spaced variant
    "결제취소",    # "payment cancellation" (can mean refund)
    "결제 취소",   # spaced variant
    "모르게 결제", # "payment made without my knowledge"
    "무단 결제",   # "unauthorized payment"
    "무단결제",    # no-space variant
    # German — refund + fraud / unauthorized signals
    "rückerstattung", "rückzahlung", "erstattet",
    "betrug",                   # fraud ("Achtung Betrug" = attention fraud)
    "betrügerisch",             # fraudulent
    "nicht autorisiert",        # not authorized
    "nicht genehmigt",          # not approved
    "unberechtigte abbuchung",  # unauthorized debit
    "unberechtigte zahlung",    # unauthorized payment
    "unberechtigt abgebucht",   # debited without authorization
    "ohne mein wissen",         # without my knowledge
    "ohne meine zustimmung",    # without my consent
    "unbekannte abbuchung",     # unknown debit — "I don't know this charge"
    "unbekannte zahlung",       # unknown payment
    "unbekannte transaktion",   # unknown transaction
    "unbekannter abbuchung",    # genitive variant
    "unerwartete abbuchung",    # unexpected debit
    "unerwartete zahlung",      # unexpected payment
    "unerwartete belastung",    # unexpected charge
    "nicht bestellt",           # didn't order
    "nicht gewollt",            # didn't want
    "nicht angemeldet",         # didn't sign up
    "nicht registriert",        # didn't register
    "keine kenntnis",           # had no knowledge (of the charge)
    "nichts davon gewusst",     # knew nothing about it
    "weiß nichts davon",        # know nothing about it
    "kenne dieses abonnement nicht",  # don't know this subscription
    "kenne diese abbuchung nicht",    # don't know this charge
    "falsche abbuchung",        # wrong/erroneous debit
    "fehlerhafte abbuchung",    # erroneous debit
    "versehentlich abgebucht",  # accidentally charged
    # French
    "remboursement", "rembourser",
    # Spanish / Portuguese
    "reembolso",
    # Russian
    "возврат",
    # Italian
    "rimborso",
    # Norwegian
    "tilbakebetaling",   # refund/repayment
    "refusjon",          # refund
    "penger tilbake",    # money back
    "uautorisert",       # unauthorized
    "ukjent trekk",      # unknown deduction
    "ikke bestilt",      # didn't order
    "ikke autorisert",   # not authorized
    "feilbelastet",      # incorrectly charged
    "belastet feil",     # charged incorrectly
    # Swedish
    "återbetalning",     # refund
    "obehörig",          # unauthorized
    "feldebiterad",      # incorrectly debited
    # Danish
    "tilbagebetaling",   # refund
    "uautoriseret",      # unauthorized
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
