import os, json
from anthropic import Anthropic

_client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

PROMPT = """You classify support tickets for an IQ test + brain training subscription service.

STEP 1 — READ ONLY THE CUSTOMER'S OWN WORDS.
Ignore quoted agent replies, signatures ("Best regards", "IQ Booster Support Team"),
and previous ticket history. Focus ONLY on what the customer wrote.

STEP 2 — CHECK FOR THESE SIGNALS (in order of priority):

A) REFUND signals (返金, 払い戻し, refund, money back, geld terug, Rückerstattung,
   terugbetaling, 환불, クーリングオフ, Widerruf, 料金返金):
   → If refund signal is present → REFUND_REQUEST.
   → If refund + fraud/unauthorized (詐欺, 不正請求, fraud, Betrug) → REFUND_REQUEST.
   → Special: "詐欺です。返金してください" = REFUND_REQUEST (even without cancel word).

A2) UNAUTHORIZED CHARGE signals — customer says they were charged without consent,
   didn't sign up, don't recognize the charge, or charges happened automatically
   without their knowledge. These are REFUND signals, NOT chargeback threats:
   → "購読契約していないのに料金が引かれている" = REFUND_REQUEST (NOT CHARGEBACK_THREAT).
   → "I was charged without my consent" = REFUND_REQUEST (NOT CHARGEBACK_THREAT).
   → "I didn't sign up for this" + billing complaint = REFUND_REQUEST.
   → "自動で料金が引かれている" (auto-charged) = REFUND_REQUEST.
   → "覚えがない" (don't recall) + charge = REFUND_REQUEST.
   → If customer mentions RENEWAL charges specifically (自動更新, auto-renewal,
     2回目の請求, second charge, renewal charge) + refund → SUB_RENEWAL_REFUND.
   CRITICAL: unauthorized/unknown charge complaints are REFUND_REQUEST by default.
   Only use CHARGEBACK_THREAT if customer uses explicit threat words (see Step 3).

B) CANCEL signals (解約, キャンセル, 退会, cancel, 취소, kündigen, opzeggen,
   annuleren, beëindigen, stopzetten, uitschrijven, cancelar, 止めたい, やめたい,
   サブスクリプション削除, サブスク解約, 구독 해지, 정기 결제 취소):
   → If cancel signal is present AND NO refund signal → TRIAL_CANCELLATION.
   → If cancel signal + weak refund mention ("お金が戻る?", "refund possible?") → TRIAL_CANCELLATION.
   → If cancel signal + STRONG refund (explicit 返金してください + fraud/amount) → REFUND_REQUEST.
   → "サブスクリプション削除確認依頼" = cancel request → TRIAL_CANCELLATION (NOT EXPLANATION).
   → SUB_CANCELLATION: use ONLY when customer explicitly states they have a PAID
     subscription (not a trial) that they knowingly maintain and wants to cancel it.
     If there is ANY doubt → TRIAL_CANCELLATION. Err strongly toward TRIAL_CANCELLATION.
   → German "Kündigung" / "kündigen": if ONLY cancel words with NO billing complaint,
     NO unauthorized charge signals, NO refund words → TRIAL_CANCELLATION.
     But if "Kündigung" + complaint about charges / unauthorized / Abbuchung →
     check for refund signals first (Rückerstattung, Widerruf, geld zurück, etc.)
     → REFUND_REQUEST if refund signals found.

C) DELETE ACCOUNT signals (アカウント削除, delete my account, 계정 삭제,
   Konto löschen, account verwijderen, видалити акаунт, remove my account):
   → If ONLY account deletion with NO billing/subscription context → DELETE_ALL_DATA.
   → If delete account + billing/charges mentioned → TRIAL_CANCELLATION.
   → If delete account + refund request → REFUND_REQUEST.

D) BILLING COMPLAINT (mentions charges, payments, withdrawals, 출금, 결제, 과금,
   引き落とし, 請求, billing, Abbuchung) WITHOUT cancel/refund/delete words:
   → Default to TRIAL_CANCELLATION (customer contacting about billing = wants to stop charges).
   → NEVER use EXPLANATION for billing complaints with complaint tone or multiple charges.
   → Korean: 계좌 출금 관련, 결제, 과금, 인출 in complaint context → TRIAL_CANCELLATION.
   → BUT if customer expresses strong dissatisfaction about being charged (anger, surprise,
     "why was I charged", "I didn't agree to this") → REFUND_REQUEST, not TRIAL_CANCELLATION.

E) NONE of the above signals:
   → SPAM: gibberish, bot-generated, "Contact Form" with no real question.
   → TECHNICAL_ISSUE: login/access problems only, zero billing.
   → GENERAL_QUESTION: IQ score, test results questions, zero billing.
   → EXPLANATION: ONLY for pure neutral questions about a charge with zero complaint tone,
     zero refund words, zero cancel words, AND the customer is genuinely just asking
     "what is this charge?" with no anger/frustration. This is VERY RARE.
   → UNKNOWN: genuinely unclear.

STEP 3 — SPECIAL CASES:

- "Conversation with [name]" subjects = live chat transcripts. Read the BODY for the
  actual customer request. Apply the same signal detection. If body mentions account
  deletion / data removal → DELETE_ALL_DATA. Default → TRIAL_CANCELLATION.

- Cancellation VERIFICATION (past tense questions: "解約できていますか", "was my
  cancellation successful", "취소가 되었나요") → EXPLANATION. But "how to cancel" /
  "解約の方法" → TRIAL_CANCELLATION.

- SUB_RENEWAL_CANCELLATION: use ONLY when ALL of these are true:
  (1) customer explicitly mentions "auto-renewal" / "自動更新" / "자동 갱신"
  (2) there is NO cancel word (解約, cancel, kündigen, etc.) anywhere in the message
  (3) there is NO refund word anywhere in the message
  (4) the customer is asking ONLY to stop future renewals, not to cancel the subscription itself
  If ANY cancel or refund word is present → use TRIAL_CANCELLATION, SUB_CANCELLATION,
  or REFUND_REQUEST instead. When in doubt → TRIAL_CANCELLATION.

- SUB_RENEWAL_REFUND: customer charged for renewal + asks for refund of THAT charge
  only, with NO cancel word → SUB_RENEWAL_REFUND.
  Also: customer says they were charged multiple times / auto-charged without consent
  and wants money back for those specific charges → SUB_RENEWAL_REFUND.

- CHARGEBACK_THREAT: ONLY when customer uses EXPLICIT threat language:
  "I will file a chargeback", "I will dispute this with my bank",
  "I will contact my credit card company", "チャージバック", "紛争",
  "I will report this", "legal action", "lawyer", "弁護士", "消費者センター".
  IMPORTANT: simply complaining about unauthorized charges, saying "I didn't sign up",
  or expressing anger about being charged is NOT a chargeback threat — that is
  REFUND_REQUEST. The customer must explicitly state they WILL take action.

- PAYPAL_DISPUTE: PayPal or bank dispute already opened.
- UNSUBSCRIBE_EMAIL: only wants off mailing list.
- DUPLICATE: repeat of an existing ticket.

CRITICAL RULES:
1. NEVER use EXPLANATION if ANY cancel, refund, or fraud signal is present.
2. NEVER use GENERAL_QUESTION if billing/charges/subscription is mentioned.
3. When in doubt between TRIAL and SUB → TRIAL_CANCELLATION. Always.
4. When in doubt between EXPLANATION and anything else → pick the other one.
   EXPLANATION is a last resort for truly neutral billing inquiries.
5. "削除" in subscription/account context = cancel/delete request, NOT explanation.
6. Dutch (NL) "Contact Form" tickets: look for refund signals (geld terug,
   terugbetaling, betaling, onterecht) or cancel signals (opzeggen, annuleren).
   If complaint about charges → REFUND_REQUEST. If cancel → TRIAL_CANCELLATION.
7. NEVER use CHARGEBACK_THREAT for unauthorized charge complaints. Unauthorized
   charge = REFUND_REQUEST. Only CHARGEBACK_THREAT for explicit dispute/legal threats.
8. NEVER use SUB_RENEWAL_CANCELLATION if any cancel word is present.
   Cancel word + auto-renewal mention → TRIAL_CANCELLATION or SUB_CANCELLATION.
9. Default to TRIAL_CANCELLATION over SUB_CANCELLATION unless the customer clearly
   describes having an active paid subscription they knowingly maintain.

Return ONLY raw valid JSON:
{
  "intent": "...",
  "confidence": 0.0,
  "language": "<ISO 639-1: EN, JP, KR, DE, FR, ZH, ES, PT, IT, RU, ID, NL, UK>",
  "chargeback_risk": false,
  "reasoning": "one line"
}

Language codes: EN=English, JP=Japanese, KR=Korean, DE=German, FR=French,
ZH=Chinese, ES=Spanish, PT=Portuguese, RU=Russian, IT=Italian,
ID=Indonesian, NL=Dutch, UK=Ukrainian."""


_FALLBACK = {
    "intent": "UNKNOWN",
    "confidence": 0.0,
    "language": "EN",
    "chargeback_risk": False,
    "reasoning": "classifier error — fallback to UNKNOWN",
}


def classify_ticket(subject: str, body: str) -> dict:
    import logging, time
    log = logging.getLogger("classifier")

    # Retry up to 3 times on 529 overloaded; immediate fail on other errors.
    last_err = None
    for attempt in range(3):
        try:
            response = _client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=200,
                messages=[{
                    "role": "user",
                    "content": f"{PROMPT}\n\nSubject: {subject}\n\nBody:\n{body[:1500]}"
                }]
            )
            last_err = None
            break  # success
        except Exception as e:
            last_err = e
            # 529 = Anthropic overloaded — worth retrying after a short pause
            status = getattr(e, "status_code", None)
            if status == 529 and attempt < 2:
                wait = 3 * (attempt + 1)  # 3s, 6s
                log.warning(
                    f"classify_ticket: Anthropic overloaded (529), "
                    f"retry {attempt + 1}/2 in {wait}s…"
                )
                time.sleep(wait)
                continue
            # Any other error (auth, network, etc.) — fail immediately
            log.error(f"classify_ticket API error: {e}")
            return {**_FALLBACK, "reasoning": f"API error: {e}"}

    if last_err is not None:
        log.error(f"classify_ticket: all retries exhausted — {last_err}")
        return {**_FALLBACK, "reasoning": f"overloaded after retries: {last_err}"}

    raw_text = response.content[0].text

    start_idx = raw_text.find('{')
    end_idx   = raw_text.rfind('}')

    result = None
    if start_idx != -1 and end_idx != -1:
        clean_json_str = raw_text[start_idx:end_idx + 1]
        try:
            result = json.loads(clean_json_str)
        except json.JSONDecodeError as e:
            print(f"JSON Decode Error on cleaned string: {clean_json_str}")
    else:
        print(f"Claude returned invalid response without JSON brackets: {raw_text}")

    if result is None:
        return {**_FALLBACK, "reasoning": "parse error — classifier fallback"}

    return result
