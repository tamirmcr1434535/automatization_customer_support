"""
Reply Generator
===============
Uses Claude Sonnet to produce multilingual customer-facing replies.
"""

import os
from anthropic import Anthropic

_client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

SYSTEM = """You are a professional customer support agent for an IQ test subscription service.

Language rules:
- Japanese (JP): polite keigo — 〜でございます, いただきありがとうございます, 誠に申し訳ございません
- Korean (KR): formal 존댓말 — 〜드립니다, 〜하시겠습니까, 진심으로 사과드립니다
- English (EN): warm, professional, concise

Output ONLY the reply text, ready to send. No subject line, no sign-off template."""

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

SUB_TYPE_CONTEXT = {
    "trial": "The free trial has been cancelled. No charge will occur.",
    "subscription": (
        "The subscription has been cancelled. "
        "Access continues until the end of the current billing period, then no further charges."
    ),
}


def generate_reply(intent: str, language: str, customer_name: str, cancel_result: dict) -> str:
    """Generate a cancellation confirmation reply."""
    sub_type = cancel_result.get("subscription_type")
    situation = (
        SUB_TYPE_CONTEXT[sub_type]
        if sub_type in SUB_TYPE_CONTEXT
        else INTENT_CONTEXT.get(intent, "The subscription has been cancelled.")
    )
    cancel_status = cancel_result.get("status", "unknown")
    source = cancel_result.get("source", "")

    prompt = f"""Language: {language}
Customer name: {customer_name or 'the customer'}
Situation: {situation}
Cancellation status: {cancel_status}{f' (via {source})' if source else ''}

Structure (3-5 sentences):
1. Thank customer for contacting
2. Confirm action taken (cancelled / will not renew)
3. Reassure: no further charges
4. Offer further help

Write the customer reply:"""

    r = _client.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=400,
        messages=[{"role": "user", "content": f"{SYSTEM}\n\n{prompt}"}],
    )
    return r.content[0].text.strip()


def generate_ask_card_digits_reply(language: str, customer_name: str) -> str:
    """
    First request: ask for last 4 card digits when email not found.
    Inform that the ticket will close in 7 days if no reply.
    """
    prompt = f"""Language: {language}
Customer name: {customer_name or 'the customer'}

Situation: We searched our system but could not find any subscription or trial 
linked to the customer's email address.

Write a reply that:
1. Apologises briefly that we couldn't locate the account by email
2. Asks them to reply with the last 4 digits of the payment card used for the subscription
3. Mentions that if we don't hear back within 7 days, the ticket will be automatically closed
4. Keeps it short, warm, and professional (3-4 sentences)

Write the customer reply:"""

    r = _client.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=300,
        messages=[{"role": "user", "content": f"{SYSTEM}\n\n{prompt}"}],
    )
    return r.content[0].text.strip()


def generate_ask_card_digits_retry_reply(language: str, customer_name: str) -> str:
    """
    Second request: digits were provided but not found in the system.
    Ask for correct digits. Inform 2-day window.
    """
    prompt = f"""Language: {language}
Customer name: {customer_name or 'the customer'}

Situation: The customer provided the last 4 digits of their card, but we were 
unable to find a matching subscription in our system using those digits.
This may mean the card digits provided are incorrect, or the subscription 
was registered under a different card.

Write a reply that:
1. Apologises that the digits provided didn't match any account
2. Asks them to double-check and provide the correct last 4 digits of the card 
   used when signing up
3. Mentions that if we don't hear back within 2 days, the ticket will be closed
4. Keeps it empathetic and concise (3-4 sentences)

Write the customer reply:"""

    r = _client.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=300,
        messages=[{"role": "user", "content": f"{SYSTEM}\n\n{prompt}"}],
    )
    return r.content[0].text.strip()


def generate_not_found_reply(language: str, customer_name: str) -> str:
    """
    Final close: not found after both attempts. Close ticket as not relevant.
    """
    prompt = f"""Language: {language}
Customer name: {customer_name or 'the customer'}

Situation: After searching by email address and two attempts with card last 4 digits, 
we could not find any active subscription or trial in our system for this customer.
We are closing this ticket as we have exhausted our lookup options.

Write a reply that:
1. Apologises for the inconvenience
2. Explains that despite thorough searching by email and card digits, no active account was found
3. Suggests they may have used a different email or payment method, and invites them 
   to contact us again with that information
4. Informs that this ticket is now being closed
5. Empathetic, professional, 3-5 sentences

Write the customer reply:"""

    r = _client.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=350,
        messages=[{"role": "user", "content": f"{SYSTEM}\n\n{prompt}"}],
    )
    return r.content[0].text.strip()
