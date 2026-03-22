import os, json
from anthropic import Anthropic

_client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

# Трохи посилив PROMPT, щоб Клод не використовував markdown-обгортки
PROMPT = """You are a classifier for a support ticket system of an IQ test subscription service.

Classify into ONE intent:
- TRIAL_CANCELLATION         — cancel free trial
- SUB_CANCELLATION           — cancel paid subscription
- SUB_RENEWAL_CANCELLATION   — cancel before next auto-renewal
- REFUND_REQUEST             — wants refund for a charge
- SUB_RENEWAL_REFUND         — charged for renewal, wants refund
- CHARGEBACK_THREAT          — threatens dispute/chargeback/PayPal claim
- PAYPAL_DISPUTE             — PayPal or bank dispute already opened
- TECHNICAL_ISSUE            — login not received, charged after cancel, wrong email
- GENERAL_QUESTION           — question about service/account
- UNSUBSCRIBE_EMAIL          — only wants off mailing list
- DUPLICATE                  — repeat ticket
- UNKNOWN                    — unclear

Return ONLY raw and valid JSON. Do not wrap it in markdown blocks, do not use ```json, and do not include any conversational text.
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
    
    # Знаходимо першу { і останню }
    start_idx = raw_text.find('{')
    end_idx = raw_text.rfind('}')
    
    # Якщо дужки знайдено, вирізаємо тільки JSON-частину
    if start_idx != -1 and end_idx != -1:
        clean_json_str = raw_text[start_idx:end_idx+1]
        try:
            return json.loads(clean_json_str)
        except json.JSONDecodeError as e:
            print(f"JSON Decode Error on cleaned string: {clean_json_str}")
            raise e
    else:
        # Логуємо, якщо Клод повернув щось зовсім незрозуміле без дужок
        print(f"Claude returned invalid response without JSON brackets: {raw_text}")
        raise ValueError("No JSON found in Claude's response")
