"""
Reply Generator
===============
Uses Claude Sonnet to produce multilingual customer-facing replies.

Accepts a unified `cancel_result` dict that can come from either
WooCommerce or Stripe, so the prompt logic is source-agnostic.
"""

import os
from anthropic import Anthropic

_client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

SYSTEM = """You are a professional customer support agent for an IQ test subscription service.

Language rules:
- Japanese (JP): polite keigo — 〜でございます, いただきありがとうございます, 誠に申し訳ございません
- Korean (KR): formal 존댓말 — 〜드립니다, 〜하시겠습니까, 진심으로 사과드립니다
- English (EN): warm, professional, concise

Structure (3-5 sentences):
1. Thank customer for contacting
2. Confirm action taken (cancelled / will not renew)
3. Reassure: no further charges
4. Offer further help

Output ONLY the reply text, ready to send. No subject line, no sign-off template."""

# Context strings per intent
INTENT_CONTEXT = {
    "TRIAL_CANCELLATION": "The free trial has been cancelled. No charge will occur.",
    "SUB_CANCELLATION": (
        "The subscription has been cancelled. "
        "Access continues until the end of the current billing period, then no further charges."
    ),
    "SUB_RENEWAL_CANCELLATION": (
        "Auto-renewal has been disabled. "
        "Current access continues until the period end, then the subscription ends automatically."
    ),
}

# Override context when WooCommerce tells us exactly what happened
SUB_TYPE_CONTEXT = {
    "trial": "The free trial has been cancelled. No charge will occur.",
    "subscription": (
        "The subscription has been cancelled. "
        "Access continues until the end of the current billing period, then no further charges."
    ),
}


def generate_reply(
    intent: str,
    language: str,
    customer_name: str,
    cancel_result: dict,
) -> str:
    """
    Generate a ready-to-send customer reply.

    Parameters
    ----------
    intent        : classified intent string (TRIAL_CANCELLATION, etc.)
    language      : EN | JP | KR | OTHER
    customer_name : customer's display name from Zendesk
    cancel_result : unified dict from WooCommerceClient or StripeClient,
                    including optional 'subscription_type' key
    """
    # Prefer the factual subscription_type from the cancellation result
    # (WooCommerce tells us exactly whether it was a trial or a subscription)
    sub_type = cancel_result.get("subscription_type")
    if sub_type and sub_type in SUB_TYPE_CONTEXT:
        situation = SUB_TYPE_CONTEXT[sub_type]
    else:
        situation = INTENT_CONTEXT.get(intent, "The subscription has been cancelled.")

    # Normalise status label for the prompt
    cancel_status = cancel_result.get("status", "unknown")
    source = cancel_result.get("source", "")

    prompt = f"""Language: {language}
Customer name: {customer_name or 'the customer'}
Situation: {situation}
Cancellation status: {cancel_status}{f' (via {source})' if source else ''}

Write the customer reply:"""

    r = _client.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=400,
        messages=[{"role": "user", "content": f"{SYSTEM}\n\n{prompt}"}],
    )
    return r.content[0].text.strip()
