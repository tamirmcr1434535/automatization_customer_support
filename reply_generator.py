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

Output ONLY the reply text, ready to send."""

CONTEXT = {
    "TRIAL_CANCELLATION":       "Trial has been cancelled. No charge will occur.",
    "SUB_CANCELLATION":         "Subscription cancelled. Access continues until end of billing period, then no further charges.",
    "SUB_RENEWAL_CANCELLATION": "Auto-renewal disabled. Current access continues until period end, then subscription ends automatically.",
}


def generate_reply(intent: str, language: str, customer_name: str, stripe_result: dict) -> str:
    context = CONTEXT.get(intent, "Subscription has been cancelled.")

    prompt = f"""Language: {language}
Customer name: {customer_name or 'the customer'}
Situation: {context}
Stripe status: {stripe_result.get('status')}

Write the customer reply:"""

    r = _client.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=400,
        messages=[{"role": "user", "content": f"{SYSTEM}\n\n{prompt}"}]
    )
    return r.content[0].text.strip()
