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

Output ONLY the reply text, ready to send. No subject line, no sign-off template.

---
REFERENCE EXAMPLES (follow this tone, structure and length exactly):

[JP | TRIAL_CANCELLATION]
こんにちは。
お問い合わせいただきありがとうございます。
お客様のIQ Boosterの7日間無料トライアルは正常にキャンセルされたことを確認いたしました。今後、お客様のアカウントに料金が請求されることはございません。
ご不明な点やその他サポートが必要な場合は、どうぞお気軽にご連絡ください。いつでもお手伝いさせていただきます。

[JP | SUB_CANCELLATION]
こんにちは。
お問い合わせいただきありがとうございます。
お客様のサブスクリプションは正常にキャンセルされたことを確認いたします。現在の請求期間終了まではサービスをご利用いただけますが、それ以降は追加料金は発生いたしません。
ご不明な点がございましたら、いつでもお気軽にご連絡ください。

[KR | TRIAL_CANCELLATION]
안녕하세요.
문의해 주셔서 감사합니다.
고객님의 7일 무료 체험이 성공적으로 취소되었음을 확인해 드립니다. 앞으로 고객님의 계정에 추가 요금이 청구되지 않을 것입니다.
궁금하신 점이나 추가 도움이 필요하신 경우 언제든지 연락해 주세요.

[KR | SUB_CANCELLATION]
안녕하세요.
문의해 주셔서 감사합니다.
고객님의 구독이 성공적으로 취소되었습니다. 현재 결제 기간이 끝날 때까지 서비스를 이용하실 수 있으며, 이후에는 추가 요금이 발생하지 않습니다.
다른 문의 사항이 있으시면 언제든지 연락해 주세요.

[EN | TRIAL_CANCELLATION]
Hi,
Thank you for reaching out to us.
I have successfully cancelled your 7-day free trial. You will not be charged going forward.
If you have any other questions, please don't hesitate to contact us.

[EN | SUB_CANCELLATION]
Hi,
Thank you for contacting us.
Your subscription has been successfully cancelled. You will continue to have access until the end of your current billing period, after which no further charges will occur.
Please feel free to reach out if you need any further assistance.
---"""

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
Intent: {intent}
Customer name: {customer_name or 'the customer'}
Situation: {situation}
Cancellation status: {cancel_status}{f' (via {source})' if source else ''}

Follow the reference example for [{language} | {intent}] from the REFERENCE EXAMPLES above.
Use the same tone, structure, and length. Replace placeholders with the actual customer name if provided.

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
