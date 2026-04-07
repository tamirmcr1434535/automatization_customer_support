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
- GENERAL_QUESTION       — STRICTLY for questions about test results, IQ score interpretation,
                           how the test works, or account features — with ABSOLUTELY ZERO mention
                           of billing, charges, subscription, or payments. If the customer
                           mentions ANY charge, billing, or subscription → NEVER GENERAL_QUESTION.
- UNSUBSCRIBE_EMAIL      — only wants to be removed from mailing/marketing list
- DUPLICATE              — repeat of an existing ticket
- UNKNOWN                — genuinely unclear intent

IMPORTANT RULES:
0. FRAUD / ILLEGAL BILLING OVERRIDE — evaluate this BEFORE rules 1-9:
   If the customer's PRIMARY complaint is about unauthorized charges, fraud, or billing without
   their consent — AND they are NOT explicitly asking to cancel a subscription going forward —
   → REFUND_REQUEST (not TRIAL_CANCELLATION).
   Signals that trigger this override:
     DE: "Betrug", "betrügerisch", "nicht autorisiert", "nicht genehmigt", "ohne mein Wissen",
         "ohne meine Zustimmung", "unberechtigte Abbuchung"
     JP: "不法請求", "不正請求", "詐欺", "不正利用"
     EN: "fraud", "fraudulent", "illegal charge", "illegal billing",
         "charged without my consent/permission/knowledge",
         "I never signed up", "I never authorized", "I didn't know about this charge"
   Exception (Rule 0 does NOT apply — use TRIAL_CANCELLATION instead):
   - Message contains ANY word from Rule 1 cancel list (cancel, 解約したい, opzeggen,
     account verwijderen, uitschrijven, beëindigen, etc.)
   - EN account deletion: "delete my account", "remove my account", "close my account",
     "deactivate my account" → ALWAYS TRIAL_CANCELLATION (account deletion = cancel request,
     even if the customer also mentions not knowing about the subscription).
   - Dutch (NL) messages with "account verwijderen" or "opzeggen" → ALWAYS TRIAL_CANCELLATION
     even if the customer also mentions not knowing about the subscription.
   - German (DE) messages with "nichts bestellt", "kein Abonnement", "nicht abonniert"
     WITHOUT explicit refund words (Rückerstattung, Geld zurück, erstattet, zurückzahlen)
     → TRIAL_CANCELLATION (customer wants to cancel the unwanted subscription, NOT get a refund).
   Pure fraud complaint with ZERO cancel words AND ZERO account-deletion phrases
   → REFUND_REQUEST.
1. Any form of "cancel", "キャンセル", "취소", "解約", "解除", "退会", "解除", "メンバーシップの解約",
   "退会したい", "解約したい", "止めたい", "やめたい", "kansellere", "avbryte", "avslutte",
   "annuleren", "avboka", "annullere",
   "batalkan", "hentikan langganan", "berhenti berlangganan" (ID: Indonesian)
   "opzeggen", "beëindigen", "stopzetten", "abonnement annuleren",
   "account verwijderen", "opzegging", "uitschrijven" (NL: Dutch)
   EN account deletion: "delete my account", "delete account", "remove my account",
   "remove account", "close my account", "close account", "deactivate my account"
   → ALWAYS a cancellation intent (TRIAL_CANCELLATION or SUB_CANCELLATION). NEVER REFUND_REQUEST
   or SUB_RENEWAL_REFUND if ANY cancellation word is present — UNLESS Rule 0 fraud override applies.
2. "I noticed recurring/unexpected charges + please cancel" → TRIAL_CANCELLATION.
   Mentioning past charges does NOT make it a refund intent if the customer asks to cancel.
3. "I only wanted the IQ test / 知能テスト but got a subscription" → TRIAL_CANCELLATION.
4. "I signed up by mistake / didn't know I'd be charged" → TRIAL_CANCELLATION.
5. TECHNICAL_ISSUE is ONLY for login/access problems — never for billing or cancellation requests.
6. Default for any ambiguous cancellation → TRIAL_CANCELLATION.
7. SUB_RENEWAL_REFUND requires ALL THREE: (a) specific renewal charge already happened,
   (b) explicit refund request, (c) NO cancellation word anywhere in the message.
8. If the ticket subject is "Conversation with [name]", this is a Zendesk LIVE CHAT transcript.
   The customer's messages are embedded in the conversation body.
   → ALWAYS classify as TRIAL_CANCELLATION by default — customers reach this chat through
     the cancellation/support flow.
   → Scan the entire transcript for refund signals (returns, fraud, money-back requests).
   → Only override to REFUND_REQUEST if the body contains ONLY refund/fraud signals with
     absolutely ZERO cancel intent.
   → NEVER return GENERAL_QUESTION, UNKNOWN, or TECHNICAL_ISSUE for chat transcripts.
9. BILLING CONTACT RULE — any message where the customer mentions a charge, billing, subscription,
   payment, or monthly deduction → TRIAL_CANCELLATION by default.
   GENERAL_QUESTION is FORBIDDEN if billing is mentioned. Concrete examples:
     JP: "請求について" (about billing) → TRIAL_CANCELLATION
     JP: "料金について" (about the fee) → TRIAL_CANCELLATION
     JP: "課金について" (about the charge) → TRIAL_CANCELLATION
     JP: "引き落としについて" (about the deduction) → TRIAL_CANCELLATION
     EN: "about my subscription" → TRIAL_CANCELLATION
   EXCEPTION 1: if the complaint is pure fraud/unauthorized (see Rule 0) with ZERO cancel words
   → REFUND_REQUEST.
   EXCEPTION 2: if the message contains billing/charge mention AND ALSO explicit payment-reversal
   or refund intent — i.e. the customer wants a SPECIFIC PAST PAYMENT cancelled/reversed/refunded
   (not just subscription cancelled going forward) — → REFUND_REQUEST takes priority.
   Key signals for Exception 2:
     JP: "支払いをキャンセル" (cancel the payment), "支払いを取り消" (reverse the payment),
         "課金を取り消" (reverse the charge), "決済をキャンセル" (cancel the transaction),
         "返金", "払い戻し", "お金を返して" (return the money)
     EN: "cancel this payment", "reverse this charge", "refund", "money back"
     DE: "Rückerstattung", "Geld zurück", "erstattet"
   DISTINCTION: "please cancel my subscription" → TRIAL_CANCELLATION (future charges).
                "please cancel/reverse this payment" → REFUND_REQUEST (past charge reversal).

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
- ID  = Indonesian (Bahasa Indonesia: "saya", "langganan", "tagihan", "batalkan", "hentikan")
- NL  = Dutch (Nederlands: "abonnement", "opzeggen", "beëindigen", "annuleren", "verwijderen")
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
