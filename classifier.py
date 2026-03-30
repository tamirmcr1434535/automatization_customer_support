import os, json
from anthropic import Anthropic

_client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

PROMPT = """You are a classifier for a support ticket system of an IQ test subscription service.

Classify into ONE intent:
- TRIAL_CANCELLATION     — wants to cancel a free trial (explicitly or implicitly).
                           ALSO USE when: customer says they signed up for something by mistake
                           and it appears to be a trial period, or they want to stop before being charged.
- SUB_CANCELLATION       — wants to cancel a paid subscription.
                           ALSO USE when: customer says they did not intend to sign up for a subscription,
                           discovered an unexpected charge or enrollment, and wants it cancelled.
                           If the customer says "I want to cancel" about any subscription or plan — use this.
- SUB_RENEWAL_CANCELLATION — wants to stop auto-renewal before next billing date
- REFUND_REQUEST         — wants a refund for a charge that already happened
- SUB_RENEWAL_REFUND     — was charged for a renewal and wants that specific charge refunded
- CHARGEBACK_THREAT      — explicitly threatens a chargeback, dispute, or PayPal claim
- PAYPAL_DISPUTE         — PayPal or bank dispute already opened
- TECHNICAL_ISSUE        — STRICTLY technical access problems: cannot log in, did not receive
                           login credentials, wrong email used, account access error.
                           Do NOT use for billing questions or unwanted subscriptions.
- GENERAL_QUESTION       — general question about the service or account, no action needed
- UNSUBSCRIBE_EMAIL      — only wants to be removed from mailing/marketing list
- DUPLICATE              — repeat of an existing ticket
- UNKNOWN                — genuinely unclear intent

IMPORTANT RULES:
1. If the customer says any form of "cancel", "キャンセル", "취소", "解約" — always pick a cancellation intent.
2. "I only wanted to pay for X but got signed up for Y, please cancel" → SUB_CANCELLATION.
3. "I don't want brain training / subscription" + cancel request → SUB_CANCELLATION or TRIAL_CANCELLATION.
4. TECHNICAL_ISSUE is ONLY for login/access problems, never for billing or enrollment disputes.
5. When in doubt between TRIAL_CANCELLATION and SUB_CANCELLATION, pick TRIAL_CANCELLATION if
   the customer mentions a trial, free period, or hasn't been charged yet.

Return ONLY raw valid JSON. No markdown, no ```json, no extra text.
{
  "intent": "...",
  "confidence": 0.0,
  "language": "EN|JP|KR|OTHER",
  "chargeback_risk": false,
  "reasoning": "one line"
}"""


def classify_ticket(subject: str, body: str) -> dict:
    response = _client.messages.create(
        model="claude-haiku-4-5",
        max_tokens=200,
        messages=[{
            "role": "user",
            "content": f"{PROMPT}\n\nSubject: {subject}\n\nBody:\n{body[:1500]}"
        }]
    )

    raw_text = response.content[0].text

    start_idx = raw_text.find('{')
    end_idx   = raw_text.rfind('}')

    if start_idx != -1 and end_idx != -1:
        clean_json_str = raw_text[start_idx:end_idx + 1]
        try:
            return json.loads(clean_json_str)
        except json.JSONDecodeError as e:
            print(f"JSON Decode Error on cleaned string: {clean_json_str}")
            raise e
    else:
        print(f"Claude returned invalid response without JSON brackets: {raw_text}")
        raise ValueError("No JSON found in Claude's response")
