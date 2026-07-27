"""
Reply Generator
===============
Produces customer-facing replies by translating exact master templates into
the customer's language via Claude.

Master templates (English) define the exact content and tone.
Claude translates them faithfully — it does NOT generate free-form replies.

NOTE: No sign-off / signature is appended — the Zendesk agent profile
already has a footer configured, so adding one here would duplicate it.

FIX-A: Added try/except to _translate() — if Claude API fails, returns EN
       master template instead of crashing. Added validate_reply() to catch
       hallucinated or garbage responses before they reach the customer.
       Added alert callback so main.py can wire Slack notifications on API failure.
"""

import os
import logging
from anthropic import Anthropic

log = logging.getLogger("reply_generator")

_client    = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
BRAND_NAME = os.getenv("BRAND_NAME", "IQ Booster")
AGENT_NAME = os.getenv("AGENT_NAME", "Mia")

# ── FIX-A: Alert callback (set by main.py to send Slack alerts on API failure) ── #
_alert_callback = None


def set_alert_callback(callback):
    """
    Register a callback function that will be called when Claude API fails.
    Signature: callback(error_msg: str) -> None
    Called from main.py to wire up Slack notifications.
    """
    global _alert_callback
    _alert_callback = callback


def _notify_api_failure(error_msg: str):
    """Internal: call the alert callback if registered."""
    log.error(f"Reply generator API failure: {error_msg}")
    if _alert_callback:
        try:
            _alert_callback(f"Reply Generator: {error_msg}")
        except Exception as e:
            log.error(f"Failed to send API failure alert: {e}")


# ── Master templates (source of truth) ────────────────────────────────── #
# These are the EXACT texts approved for each scenario.
# Claude translates them into the customer's language — never rewrites them.

def _master_trial_cancel() -> str:
    return (
        "Hello,\n\n"
        f"Thank you for your email. We confirm that your {BRAND_NAME} 7-day free trial has been "
        "successfully cancelled. No further charges will be applied to your account.\n\n"
        "If you have any other questions or need further assistance, please don't hesitate "
        "to contact us."
    )

def _master_sub_cancel() -> str:
    return (
        "Hello,\n\n"
        f"Thank you for your email. We're sorry to hear you'd like to cancel your {BRAND_NAME} "
        "subscription. As requested, your subscription has been canceled and no additional "
        "payments will occur. You will, however, continue to have access to the service "
        "until the end of the period you have already paid for. After that, the plan will "
        "end and access will be discontinued. If you have any further concerns or need "
        "additional assistance, please don't hesitate to contact us. We're happy to help!"
    )

# ── Translation system prompt ──────────────────────────────────────────── #

_TRANSLATE_SYSTEM = f"""You are a professional translator for {BRAND_NAME} customer support.

Your ONLY task is to translate the given English message into the target language.

Rules:
- Translate faithfully and completely — do NOT paraphrase, shorten, or add content
- Preserve all factual content, but DO restructure long single paragraphs into
  2–4 shorter paragraphs at natural topic boundaries, separated by a blank line.
  Goal: replies should not be a wall of text on screen.
- GREETING: if the source starts with "Hello," (or a similar opener), translate
  it into the target language's natural greeting and keep it on its own line
  as the first paragraph — do NOT merge it into the next paragraph. Examples:
    • JP: こんにちは。
    • KR: 안녕하세요.
    • DE: Hallo,
    • FR: Bonjour,
    • ES: Hola,
    • VI: Xin chào,
    • TH: สวัสดีค่ะ
  The customer should see the greeting as a clear first line before the body.
- Replace "{BRAND_NAME}" with "{BRAND_NAME}" as-is (brand name stays unchanged)
- Use the appropriate formal register for the target language:
  • JP: polite keigo (〜でございます, いただきありがとうございます).
       FORMATTING (Japanese reading conventions):
       - Use TWO different newline patterns. CRITICAL — do not confuse them:
         · BETWEEN paragraphs → exactly TWO newlines (\\n\\n) — one blank line
         · WITHIN a paragraph → exactly ONE newline (\\n) — NO blank line.
           Lines inside a paragraph must be directly adjacent, with nothing
           between them. NEVER put \\n\\n between lines that belong to the
           same paragraph.
       - Keep "こんにちは。" as its own first paragraph (followed by \\n\\n)
         — do not absorb it into the お問い合わせいただき opener.
       - Split the message into 3–4 short paragraphs total (greeting counts
         as one of them).
       - Within each paragraph, insert single-newline soft wraps so that
         each visible line is roughly 40–50 characters long, with 50 as
         the hard upper limit (never exceed 50 chars per line). Break at
         NATURAL points: after particles (は / が / を / に / で / と / の),
         after punctuation (、 。), or between grammatical phrases.
         Never break a word/compound in the middle.
       - Aim for the upper end (45–50 chars). Do NOT produce short
         choppy fragments under 30 chars unless a sentence naturally
         ends there — full lines read better than over-broken text.
  • KR: formal 존댓말 (〜드립니다)
  • DE: formal Sie-form
  • FR: formal vous-form
  • VI: polite Vietnamese (Kính gửi Quý khách, xin cảm ơn Quý khách)
  • TH: formal Thai (ใช้คำว่า ท่าน, ขอบพระคุณ)
  • Other: match a formal customer support tone
- Do NOT add any sign-off, signature, or "Best regards" — the email footer handles that
- Output ONLY the translated text, nothing else"""


def _translate(text: str, language: str) -> str:
    """
    Translate *text* into *language* using the strict translation prompt.

    FIX-A: wrapped in try/except — if Claude API fails (empty key, auth error,
    network issue, overload), returns the original EN text as fallback and
    sends an alert via the registered callback.
    """
    if language.upper() == "EN":
        return text  # already English — no translation needed

    try:
        r = _client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=600,
            messages=[{
                "role": "user",
                "content": (
                    f"{_TRANSLATE_SYSTEM}\n\n"
                    f"Target language: {language}\n\n"
                    f"Translate this message:\n\n{text}"
                ),
            }],
        )
        translated = r.content[0].text.strip()

        # FIX-A: Validate the translation before returning
        is_valid, reason = validate_reply(translated, language)
        if not is_valid:
            log.warning(
                f"Translation validation failed ({reason}) — using EN fallback"
            )
            _notify_api_failure(
                f"Translation to {language} failed validation: {reason}. "
                f"Sending EN fallback to customer."
            )
            return text  # return EN master template as safe fallback

        return translated

    except Exception as e:
        log.error(f"Translation to {language} failed: {e} — using EN fallback")
        _notify_api_failure(
            f"Claude API error during translation to {language}: {e}. "
            f"Sending EN master template to customer instead."
        )
        return text  # return EN master template — better than crashing


# ── FIX-A: Reply validation ──────────────────────────────────────────── #
# Validates that a generated/translated reply is safe to send to a customer.
# Catches hallucinations, system prompt leakage, garbage output.

_HALLUCINATION_MARKERS = [
    # System prompt leakage
    "you are a professional translator",
    "your only task is to translate",
    "target language:",
    "translate this message:",
    "anthropic",
    "as an ai",
    "as a language model",
    "i'm an ai",
    "i am an ai",
    # JSON/code leakage
    '"intent"',
    '"confidence"',
    '"language"',
    "```",
    # Internal bot markers
    "bot_handled",
    "dry_run",
    "shadow_mode",
    # Inappropriate content
    "i cannot",
    "i can't help",
    "i'm sorry, but i cannot",
]


def validate_reply(reply_text: str, language: str = "EN") -> tuple[bool, str]:
    """
    Validate that a reply is safe to send to a customer.

    Returns (is_valid, reason).
    - is_valid=True: reply is OK to send
    - is_valid=False: reply should NOT be sent, use EN fallback or escalate

    Checks:
    1. Minimum length (at least 20 chars — anything shorter is likely garbage)
    2. No hallucination/prompt leakage markers
    3. No empty or whitespace-only response
    4. No excessively long response (runaway generation)
    5. Not raw JSON
    """
    if not reply_text or not reply_text.strip():
        return False, "empty_response"

    stripped = reply_text.strip()

    if len(stripped) < 20:
        return False, f"too_short ({len(stripped)} chars)"

    # Check for hallucination markers (case-insensitive)
    lower = stripped.lower()
    for marker in _HALLUCINATION_MARKERS:
        if marker in lower:
            return False, f"hallucination_marker: {marker}"

    # Check that the response doesn't look like raw JSON
    if stripped.startswith("{") and stripped.endswith("}"):
        return False, "looks_like_json"

    # Master templates are ~100-400 chars; translations shouldn't exceed ~5x
    if len(stripped) > 3000:
        return False, f"too_long ({len(stripped)} chars)"

    return True, "ok"


# ── Public API ─────────────────────────────────────────────────────────── #

def generate_reply(intent: str, language: str, customer_name: str, cancel_result: dict) -> str:
    """Cancellation confirmation reply — exact master template, translated."""
    sub_type = cancel_result.get("subscription_type")
    if sub_type == "trial" or intent == "TRIAL_CANCELLATION":
        master = _master_trial_cancel()
    else:
        master = _master_sub_cancel()
    return _translate(master, language)


def english_fallback_reply(intent: str, cancel_result: dict) -> str:
    """English master cancellation template — no Claude, no translation.

    Use this when the localised reply failed validation but the WC cancel
    already happened, so the customer would otherwise get no notification.
    Better to send the English confirmation than to silently leave them
    wondering whether their cancellation went through.
    """
    sub_type = cancel_result.get("subscription_type")
    if sub_type == "trial" or intent == "TRIAL_CANCELLATION":
        return _master_trial_cancel()
    return _master_sub_cancel()


# ── AN-192 refund reply templates (would-be / shadow) ──────────────────── #
# English master templates for the refund decisions we auto-answer, reconstructed
# faithfully from real agent replies (audit 2026-07-27). Same translate-not-generate
# pipeline as cancellations. These are DRAFTS pending Anna's canonical wording.
#
# SAFETY: generating a draft here NEVER sends anything. The caller (main.py) logs
# the draft to BigQuery for shadow comparison and only posts to the customer when
# REFUNDS_ENABLED=true (and, for an APPROVED refund, only when the money movement
# actually executed — which is a no-op stub today).

# Reason codes we produce a customer draft for. Everything else → human, no draft.
REFUND_AUTOREPLY_CODES = {
    "WOULD_BE_REFUNDED",
    "REPORT_NOT_REFUNDABLE_PER_TOS",
    "OUTSIDE_REFUND_WINDOW",
}


def _fmt_amount(amount, currency: str) -> str:
    a = str(amount) if amount is not None else ""
    c = (currency or "").strip()
    return f"{a} {c}".strip() if a else (c or "the charge")


def _master_refund_approved(d: dict) -> str:
    amt = _fmt_amount(d.get("refund_amount"), d.get("currency"))
    brand = d.get("brand", BRAND_NAME)
    pdate = d.get("purchase_date")
    order_line = f"Order date: {pdate}\n\n" if pdate else ""
    return (
        "Hello,\n\n"
        "Thank you for reaching out, and we appreciate your patience while we "
        "looked into this.\n\n"
        f"{order_line}"
        "Refund status:\n"
        f"- Your {brand} subscription has been cancelled — no further charges will be made.\n"
        f"- Your refund of {amt} has been approved and processed to your original payment "
        "method. Depending on your bank or card provider, it may take up to 10 business "
        "days to appear.\n\n"
        "If you have any further questions, please don't hesitate to contact us."
    )


def _master_refund_report_not_refundable(d: dict) -> str:
    brand = d.get("brand", BRAND_NAME)
    price = d.get("report_price")  # display-ready (already carries its symbol)
    price_ref = f"the {price} charge" if price else "this charge"
    return (
        "Hello,\n\n"
        "Thank you for reaching out, and we're sorry for any confusion.\n\n"
        f"Having re-checked your order, {price_ref} is for the IQ Test Report — an optional "
        "one-time purchase shown on a separate screen after the test, and charged only when "
        "it is manually confirmed. Our records show the purchase was confirmed and the report "
        "was delivered immediately.\n\n"
        "As this is digital content delivered instantly, it is not eligible for a refund under "
        "our Terms of Use and Refund Policy.\n\n"
        f"Your {brand} free-trial subscription has already been cancelled, so no further "
        "charges will be made.\n\n"
        "If you have any other questions, we're happy to help."
    )


def _master_refund_outside_window(d: dict) -> str:
    brand = d.get("brand", BRAND_NAME)
    window = d.get("window_days")
    window_ref = f"the {window}-day refund window" if window else "our refund window"
    renewal = d.get("renewal_price")  # display-ready (already carries its symbol)
    renew_line = (
        f"After the trial, the {brand} plan automatically renewed at {renewal}, as shown on "
        "the checkout page.\n\n"
        if renewal else ""
    )
    return (
        "Hello,\n\n"
        "Thank you for reaching out, and we understand there may have been some confusion.\n\n"
        f"{renew_line}"
        "Please note we do not charge any currency-exchange or transaction fees — any "
        "difference on your statement would come from your bank or card provider.\n\n"
        "Subscription & refund status:\n"
        "- Your subscription has been cancelled — no further charges will be made.\n"
        f"- Refund for past charges: the service was already provided and the charge is beyond "
        f"{window_ref}, so it is not eligible for a refund.\n\n"
        "If you have any questions, please don't hesitate to contact us."
    )


_REFUND_MASTERS = {
    "WOULD_BE_REFUNDED": _master_refund_approved,
    "REPORT_NOT_REFUNDABLE_PER_TOS": _master_refund_report_not_refundable,
    "OUTSIDE_REFUND_WINDOW": _master_refund_outside_window,
}


def refund_master_reply(reason_code: str, data: dict) -> str | None:
    """English master refund reply for `reason_code`, or None if we don't auto-answer it."""
    builder = _REFUND_MASTERS.get(reason_code)
    return builder(data or {}) if builder else None


def generate_refund_reply(reason_code: str, language: str, data: dict) -> str | None:
    """Localised refund draft for `reason_code`, or None if not an auto-reply code.
    Pure text generation — sends nothing. FAIL-SOFT: on translation failure the
    English master is returned (same policy as cancellations)."""
    master = refund_master_reply(reason_code, data)
    if master is None:
        return None
    return _translate(master, language)


