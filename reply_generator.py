"""
Reply Generator
===============
Produces customer-facing replies by translating exact master templates into
the customer's language via Claude.

Master templates (English) define the exact content and tone.
Claude translates them faithfully — it does NOT generate free-form replies.

NOTE: No sign-off / signature is appended — the Zendesk agent profile
already has a footer configured, so adding one here would duplicate it.
"""

import os
from anthropic import Anthropic

_client    = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
BRAND_NAME = os.getenv("BRAND_NAME", "IQ Booster")
AGENT_NAME = os.getenv("AGENT_NAME", "Mia")


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

def _master_ask_digits() -> str:
    return (
        "Hello,\n\n"
        "Thank you for your email. Currently, we cannot locate your account using the email "
        "you are contacting us from. To assist us in resolving this matter promptly, could "
        "you kindly provide the following details:\n"
        "• The email address registered with your account (in case you used another email)\n"
        "• If you used a Credit Card, the last 4 digits of the card\n"
        "• If you used PayPal, the PayPal username associated with the payment\n"
        "• The date and time of the transaction\n"
        "• A screenshot of the receipt from your bank app or bank statement (if available)\n\n"
        "Once we have this information, we will check the issue further and get back to you "
        "as quickly as possible. Looking forward to your cooperation and reply."
    )

def _master_ask_digits_retry() -> str:
    return (
        "Hello,\n\n"
        "Thank you for your email. Unfortunately, we were unable to locate an account matching "
        "the card digits you provided. Could you kindly double-check and provide:\n"
        "• The correct last 4 digits of the card used when signing up\n"
        "• Or your registered email address if different from this ticket's email\n"
        "• Or a screenshot of the receipt showing the charge\n\n"
        "Please note that if we don't hear back within 2 days, this ticket will be "
        "automatically closed."
    )

def _master_not_found() -> str:
    return (
        "Hello,\n\n"
        "Thank you for your patience. Despite searching thoroughly using your email address "
        "and the payment details you provided, we were unable to locate an active account "
        "in our system.\n\n"
        "If you believe you used a different email address or payment method when signing up, "
        "please don't hesitate to contact us again with that information and we will be happy "
        "to help. This ticket is now being closed."
    )

def _master_timeout() -> str:
    return (
        "Hello,\n\n"
        "We haven't heard back from you with the information we requested to locate your "
        "account. As a result, this ticket is being closed automatically.\n\n"
        "If you still need assistance, please don't hesitate to open a new ticket or reply "
        "here — we'll be happy to help."
    )


# ── Translation system prompt ──────────────────────────────────────────── #

_TRANSLATE_SYSTEM = f"""You are a professional translator for {BRAND_NAME} customer support.

Your ONLY task is to translate the given English message into the target language.

Rules:
- Translate faithfully and completely — do NOT paraphrase, shorten, or add content
- Preserve the exact structure, bullet points, and paragraph breaks
- Replace "{BRAND_NAME}" with "{BRAND_NAME}" as-is (brand name stays unchanged)
- Use the appropriate formal register for the target language:
  • JP: polite keigo (〜でございます, いただきありがとうございます)
  • KR: formal 존댓말 (〜드립니다)
  • DE: formal Sie-form
  • FR: formal vous-form
  • Other: match a formal customer support tone
- Do NOT add any sign-off, signature, or "Best regards" — the email footer handles that
- Output ONLY the translated text, nothing else"""


def _translate(text: str, language: str) -> str:
    """Translate *text* into *language* using the strict translation prompt."""
    if language.upper() == "EN":
        return text  # already English — no translation needed

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
    return r.content[0].text.strip()


# ── Public API ─────────────────────────────────────────────────────────── #

def generate_reply(intent: str, language: str, customer_name: str, cancel_result: dict) -> str:
    """Cancellation confirmation reply — exact master template, translated."""
    sub_type = cancel_result.get("subscription_type")
    if sub_type == "trial" or intent == "TRIAL_CANCELLATION":
        master = _master_trial_cancel()
    else:
        master = _master_sub_cancel()
    return _translate(master, language)


def generate_ask_card_digits_reply(language: str, customer_name: str) -> str:
    """First request: account not found — ask for payment details."""
    return _translate(_master_ask_digits(), language)


def generate_ask_card_digits_retry_reply(language: str, customer_name: str) -> str:
    """Second request: provided digits didn't match — ask again."""
    return _translate(_master_ask_digits_retry(), language)


def generate_not_found_reply(language: str, customer_name: str) -> str:
    """Final close: not found after all attempts."""
    return _translate(_master_not_found(), language)


def generate_timeout_reply(language: str, customer_name: str) -> str:
    """Timeout close: customer didn't reply within AWAITING_CARD_DAYS days."""
    return _translate(_master_timeout(), language)
