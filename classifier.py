import os, json
from anthropic import Anthropic

_client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

# ── FIX-A: Alert callback (set by main.py to send Slack alerts on API failure) ── #
_alert_callback = None

def set_alert_callback(callback):
    """
    Register a callback function that will be called when Claude API fails.
    Signature: callback(error_msg: str) -> None
    Called from main.py to wire up Slack notifications.
    """
    global _alert_callback
    _alert_callback = callback


def _notify_api_failure(error_msg: str):
    """Internal: call the alert callback if registered."""
    import logging
    logging.getLogger("classifier").error(f"Classifier API failure: {error_msg}")
    if _alert_callback:
        try:
            _alert_callback(f"Classifier API failure: {error_msg}")
        except Exception as e:
            logging.getLogger("classifier").error(f"Failed to send API failure alert: {e}")


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
  "language": "<ISO 639-1: EN, JP, KR, DE, FR, ZH, ES, PT, IT, RU, ID, NL, UK, VI, TH>",
  "chargeback_risk": false,
  "reasoning": "one line"
}

Language codes: EN=English, JP=Japanese, KR=Korean, DE=German, FR=French,
ZH=Chinese, ES=Spanish, PT=Portuguese, RU=Russian, IT=Italian,
ID=Indonesian, NL=Dutch, UK=Ukrainian, VI=Vietnamese, TH=Thai.

If the customer text is in a language not listed above, pick the code for the
CLOSEST supported language the customer is writing in (fall back to EN only as
a last resort). NEVER return EN for text written in Vietnamese, Thai, or any
other listed language — the bot uses this code to translate its reply back to
the customer, so wrong codes mean wrong-language replies."""

_FALLBACK = {
    "intent": "UNKNOWN",
    "confidence": 0.0,
    "language": "EN",
    "chargeback_risk": False,
    "reasoning": "classifier error — fallback to UNKNOWN",
}


def _parse_claude_json(raw_text: str, log) -> dict | None:
    """Extract and parse the JSON object from Claude's response.

    Returns the parsed dict on success, or None if the response contains
    no parseable JSON (e.g. truncated, preamble only, malformed).
    Always logs the raw text at debug / the offending substring at error
    so operators can diagnose what Claude actually returned.
    """
    start_idx = raw_text.find('{')
    end_idx   = raw_text.rfind('}')

    if start_idx == -1 or end_idx == -1 or end_idx <= start_idx:
        log.error(
            f"classify_ticket: Claude response has no JSON brackets — "
            f"raw={raw_text[:500]!r}"
        )
        return None

    clean_json_str = raw_text[start_idx:end_idx + 1]
    try:
        return json.loads(clean_json_str)
    except json.JSONDecodeError as e:
        # Truncated or malformed JSON — most common cause is max_tokens
        # being too small, which cuts the JSON mid-field. Log the raw
        # text so we can tell apart truncation from genuine bad output.
        log.error(
            f"classify_ticket: JSON decode error ({e}) — "
            f"cleaned={clean_json_str[:500]!r}, raw_len={len(raw_text)}"
        )
        return None


def classify_ticket(subject: str, body: str) -> dict:
    import logging, time
    log = logging.getLogger("classifier")

    # ── FIX-A: Check if API key is configured ──────────────────────────── #
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key or api_key.strip() == "":
        error_msg = (
            "ANTHROPIC_API_KEY is empty or not set — classifier cannot work. "
            "All tickets will get UNKNOWN intent and fall through to safety net / escalation."
        )
        log.critical(error_msg)
        _notify_api_failure(error_msg)
        return {**_FALLBACK, "reasoning": "API key not configured — classifier disabled"}

    user_content = f"{PROMPT}\n\nSubject: {subject}\n\nBody:\n{body[:1500]}"

    # Up to 3 API-level attempts (retries 529 overloaded).
    # On parse success: return immediately.
    # On parse FAILURE: retry once with a larger max_tokens budget, in case
    #   the first attempt was truncated. 200 tokens was the historical
    #   value — far too small for JP/KR reasoning strings — and caused
    #   "parse error — classifier fallback" on otherwise obvious tickets
    #   (e.g. "解約をすぐしたいのですが、解約方法を教えて下さい").
    _PARSE_BUDGETS = [500, 900]  # first try 500; if parse fails, retry with 900

    last_err = None
    for parse_attempt, max_tokens in enumerate(_PARSE_BUDGETS):
        # Inner API retry loop (handles 529 overloaded)
        response = None
        for api_attempt in range(3):
            try:
                response = _client.messages.create(
                    model="claude-sonnet-4-6",
                    max_tokens=max_tokens,
                    messages=[{"role": "user", "content": user_content}],
                )
                last_err = None
                break
            except Exception as e:
                last_err = e
                status = getattr(e, "status_code", None)
                if status == 529 and api_attempt < 2:
                    wait = 3 * (api_attempt + 1)
                    log.warning(
                        f"classify_ticket: Anthropic overloaded (529), "
                        f"retry {api_attempt + 1}/2 in {wait}s…"
                    )
                    time.sleep(wait)
                    continue
                error_msg = f"Claude API error (status={status}): {e}"
                log.error(f"classify_ticket API error: {e}")
                _notify_api_failure(error_msg)
                return {**_FALLBACK, "reasoning": f"API error: {e}"}

        if response is None:
            # all API retries exhausted on this parse attempt
            error_msg = f"Anthropic overloaded after all retries: {last_err}"
            log.error(f"classify_ticket: all API retries exhausted — {last_err}")
            _notify_api_failure(error_msg)
            return {**_FALLBACK, "reasoning": f"overloaded after retries: {last_err}"}

        raw_text = response.content[0].text
        stop_reason = getattr(response, "stop_reason", "unknown")

        # If Claude hit max_tokens the JSON is almost certainly truncated;
        # skip the parse attempt and go straight to retry with larger budget.
        if stop_reason == "max_tokens" and parse_attempt < len(_PARSE_BUDGETS) - 1:
            log.warning(
                f"classify_ticket: Claude hit max_tokens={max_tokens} "
                f"(stop_reason={stop_reason}) — retrying with larger budget. "
                f"raw_len={len(raw_text)}"
            )
            continue

        result = _parse_claude_json(raw_text, log)
        if result is not None:
            return result

        # Parse failed — if we have another budget to try, retry with it.
        if parse_attempt < len(_PARSE_BUDGETS) - 1:
            log.warning(
                f"classify_ticket: parse failed at max_tokens={max_tokens}, "
                f"stop_reason={stop_reason}, retrying with bigger budget"
            )
            continue

    # All parse attempts failed → fallback to UNKNOWN. Safety net in
    # main.py still has a shot at rescuing the ticket via keyword match.
    return {**_FALLBACK, "reasoning": "parse error — classifier fallback"}
