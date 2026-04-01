import os, json
from anthropic import Anthropic

_client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

PROMPT = """You are a classifier for a support ticket system of an IQ test + brain training subscription service.

Your job is to detect the CUSTOMER'S PURPOSE — not the subscription type.
NOTE: the distinction between TRIAL_CANCELLATION and SUB_CANCELLATION is resolved later by
checking the actual subscription data in WooCommerce and Stripe. Your task is to correctly
identify the overall intent category.

Classify into ONE intent:
- TRIAL_CANCELLATION     — wants to cancel / stop a subscription they signed up for without
                           realising it, or didn't intend to, or wants future charges stopped.
                           USE THIS when the customer says "cancel", "解約", "退会", "キャンセル",
                           "취소" etc. — even if they also mention seeing past charges.
                           The PRIMARY goal must be stopping future charges / cancellation.
- SUB_CANCELLATION       — clearly wants to cancel an active paid subscription they knowingly have.
                           Use this when customer explicitly mentions ongoing monthly charges
                           and wants them stopped.
                           NOTE: if uncertain between TRIAL and SUB → always pick TRIAL_CANCELLATION.
- SUB_RENEWAL_CANCELLATION — wants to stop auto-renewal before next billing date (no refund request)
- REFUND_REQUEST         — ONLY if cancellation is NOT mentioned and customer asks ONLY for money
                           back for a specific past charge. Must NOT contain cancel/解約/退会.
- SUB_RENEWAL_REFUND     — ONLY if customer was charged for a renewal AND asks ONLY to refund
                           THAT SPECIFIC charge, with NO request to cancel the subscription.
                           DO NOT use if the message contains any form of "cancel" / "解約" / "退会".
- CHARGEBACK_THREAT      — explicitly threatens a chargeback, dispute, or PayPal claim
- PAYPAL_DISPUTE         — PayPal or bank dispute already opened
- TECHNICAL_ISSUE        — STRICTLY: cannot log in, did not receive credentials, wrong email,
                           account access error. NEVER use for billing or cancellation requests.
- GENERAL_QUESTION       — general question about the service or account, no action needed
- UNSUBSCRIBE_EMAIL      — only wants to be removed from mailing/marketing list
- DUPLICATE              — repeat of an existing ticket
- UNKNOWN                — genuinely unclear intent

IMPORTANT RULES:
1. Any form of "cancel", "キャンセル", "취소", "解約", "解除", "退会", "解除", "メンバーシップの解約"
   → ALWAYS a cancellation intent (TRIAL_CANCELLATION or SUB_CANCELLATION). NEVER REFUND_REQUEST
   or SUB_RENEWAL_REFUND if ANY cancellation word is present.
2. "I noticed recurring/unexpected charges + please cancel" → TRIAL_CANCELLATION.
   Mentioning past charges does NOT make it a refund intent if the customer asks to cancel.
3. "I only wanted the IQ test / 知能テスト but got a subscription" → TRIAL_CANCELLATION.
4. "I signed up by mistake / didn't know I'd be charged" → TRIAL_CANCELLATION.
5. TECHNICAL_ISSUE is ONLY for login/access problems — never for billing or cancellation requests.
6. Default for any ambiguous cancellation → TRIAL_CANCELLATION.
7. SUB_RENEWAL_REFUND requires ALL THREE: (a) specific renewal charge already happened,
   (b) explicit refund request, (c) NO cancellation word anywhere in the message.

Return ONLY raw valid JSON. No markdown, no ```json, no extra text.
{
  "intent": "...",
  "confidence": 0.0,
  "language": "<ISO 639-1 code of the language the customer wrote in: EN, JP, KR, DE, FR, ZH, ES, PT, IT, RU, etc.>",
  "chargeback_risk": false,
  "reasoning": "one line"
}

Language detection rules:
- EN  = English
- JP  = Japanese (ひらがな / カタカナ / 漢字)
- KR  = Korean (한글)
- DE  = German (Hallo, bitte, Kündigung, danke)
- FR  = French (bonjour, annuler, abonnement)
- ZH  = Chinese (Simplified or Traditional)
- ES  = Spanish
- PT  = Portuguese
- RU  = Russian (Кириллица)
- IT  = Italian
- Use the primary language of the customer's message body.
- If the message contains multiple languages, pick the dominant one."""


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
