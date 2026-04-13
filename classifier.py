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
- DELETE_ACCOUNT         — customer wants their ACCOUNT DELETED (data removal, GDPR, privacy).
                           USE THIS when the customer says "delete my account", "アカウント削除",
                           "アカウントの削除", "アカウントを削除", "계정 삭제", "계정 삭제 요청",
                           "Konto löschen", "supprimer mon compte", "eliminar mi cuenta",
                           "видалити акаунт", "удалить аккаунт", "delete account",
                           "remove my account", "close my account", "deactivate my account",
                           "アカウントを消して", "アカウントを消去", "退会してデータを削除",
                           "account verwijderen" (NL)
                           AND the message does NOT mention subscription cancellation, billing,
                           charges, or payments as the primary concern.
                           Key distinction: if the customer says ONLY "delete my account" or
                           "remove my data" WITHOUT any billing/subscription/charge context
                           → DELETE_ACCOUNT.
                           If the customer says "delete my account" AND also mentions
                           cancelling subscription, stopping charges, billing, or payments
                           → TRIAL_CANCELLATION (cancel intent takes priority).
                           If the Zendesk topic/subject contains "Delete account" and the body
                           is a short deletion request with no billing context → DELETE_ACCOUNT.
- SUB_RENEWAL_CANCELLATION — wants to stop auto-renewal before next billing date (no refund request)
- REFUND_REQUEST         — Use when:
                           (a) customer asks ONLY for money back (no cancel request), OR
                           (b) customer asks BOTH to cancel AND to refund/reverse a past charge.
                           If the customer wants BOTH cancellation AND money back → REFUND_REQUEST
                           (human must handle the refund assessment; bot cannot auto-cancel these).
                           Key refund signals: 返金, 払い戻し, クーリングオフ, Widerruf, refund,
                           お金を返して, 先日の請求を返して, Rückerstattung, 料金返金,
                           geld terug (NL: money back), terugbetaling (NL: repayment).
                           IMPORTANT: if the message contains 返金 (refund) WITHOUT any cancel word
                           (解約, キャンセル, 退会, 取り消し) → ALWAYS REFUND_REQUEST, never TRIAL_CANCELLATION.
                           Examples:
                             "IQテストレポートの料金返金" → REFUND_REQUEST (返金 only, no cancel)
                             "料金返金をお願いします" → REFUND_REQUEST
                             "返金希望" → REFUND_REQUEST
                             "テスト費用の返金をお願いしたい" → REFUND_REQUEST
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
- EXPLANATION            — customer is asking about a charge, requesting clarification, or
                           verifying a previous cancellation. NOT requesting new action.
- UNSUBSCRIBE_EMAIL      — only wants to be removed from mailing/marketing list
- SPAM                   — message is spam, automated form submission with no real customer
                           request, or nonsensical content. Signals: "16 Persons", "Contact Form"
                           with no actual support question, gibberish text, SEO spam, bot-generated
                           content with no human intent.
- DUPLICATE              — repeat of an existing ticket
- UNKNOWN                — genuinely unclear intent

IMPORTANT RULES:

-2. QUOTED TEXT / AGENT REPLY RULE — evaluate BEFORE all other rules:
   Support tickets often contain QUOTED REPLIES from support agents (IQ Booster Support Team,
   Daniel, Mia, Rina, Anastasiia, Iryna, etc.) and previous ticket history.
   YOU MUST ONLY classify based on the CUSTOMER'S OWN LATEST MESSAGE — ignore:
   - Text after "Best regards," or "Best," from support agents
   - Text inside quoted blocks ("引用元メッセージ", "From:", "-----", "> ")
   - Previous agent responses explaining billing or confirming cancellation
   - Signature blocks with "IQ Booster Support Team"
   If the customer's OWN words contain cancel/refund keywords → use those.
   If ONLY the quoted agent text contains cancel/refund keywords → DO NOT use them.
   Example: customer says "詐欺のような誘導!" (fraud-like!) and the quoted agent reply
   contains "キャンセルされました" (was cancelled) → the customer is NOT requesting cancel.
   Focus on the customer's complaint = REFUND_REQUEST (fraud).

-1.5. CANCELLATION VERIFICATION RULE — evaluate BEFORE cancel rules:
   If the customer is ASKING WHETHER a previous cancellation was successful, or CONFIRMING
   that their subscription was already cancelled — this is NOT a new cancel request.
   → EXPLANATION (not TRIAL_CANCELLATION, not SUB_CANCELLATION).
   Signals (any language):
     KR: "취소가 올바르게 되었나요", "취소가 됐나요", "취소 확인", "취소가 잘 되었나요",
         "취소가 되었는지", "취소 처리 되었나요", "취소가 완료되었나요"
     JP: "解約できていますか", "解約されましたか", "キャンセルされていますか",
         "キャンセルは完了していますか", "解約手続きは完了しましたか",
         "ちゃんと解約されていますか", "解約の確認"
     EN: "was my cancellation successful", "did my cancellation go through",
         "is my subscription cancelled", "can you confirm cancellation",
         "has my subscription been cancelled"
     DE: "wurde meine Kündigung bearbeitet", "ist mein Abo gekündigt"
     NL: "is mijn abonnement opgezegd", "is de opzegging verwerkt"
   Key pattern: cancel word + PAST TENSE + QUESTION form = verification, not new request.
   The customer already cancelled and wants confirmation → EXPLANATION.

-1. EXPLANATION / BILLING INQUIRY RULE — evaluate FIRST, before ALL other rules:
   If the customer is ASKING about a charge (what is this? why was I billed? what is this payment?)
   and the message has a QUESTION TONE, with NO explicit refund demand and NO cancel request:
   → EXPLANATION (not REFUND_REQUEST, not TRIAL_CANCELLATION).
   The customer wants to UNDERSTAND the charge, not necessarily get money back or cancel.
   Signals (any language):
     JP: "これはなんの支払いですか", "何の請求ですか", "何の料金ですか",
         "この引き落としは何ですか", "について教えてください", "確認したい"
     EN: "what is this charge", "why was I charged", "what is this payment",
         "can you explain this charge", "what am I being charged for"
     DE: "was ist diese Abbuchung", "wofür wurde ich belastet"
     KR: "이게 뭔 결제인가요", "왜 결제된 건가요"
   IMPORTANT: even if "勝手に" (without consent) or similar emotional words appear,
   if the PRIMARY message is a QUESTION asking for explanation → EXPLANATION.
   Example: "勝手に支払いされていますが、これはなんの支払いですか？" → EXPLANATION
            (asking WHAT is this charge, not demanding refund)
   EXCEPTION: if also contains explicit refund demand (返金してください, refund, money back)
   or cancel request (解約, cancel) → use those rules instead.

-0.5 AUTO-REPLY / OOO / SYSTEM MESSAGE RULE:
   If the message is an automated reply, out-of-office, vacation notice, or system notification
   with NO actual customer request → EMAIL_UPDATES/NOTIFICATIONS.
   Signals: "Abwesenheit", "out of office", "auto-reply", "automatic reply",
   "自動返信", "不在", "Urlaub", "vacation", "I am currently out".

0. FRAUD / ILLEGAL BILLING OVERRIDE — evaluate this BEFORE rules 1-9:
   If the customer's PRIMARY complaint is about unauthorized charges, fraud, or billing without
   their consent — AND they are NOT explicitly asking to cancel a subscription going forward —
   → REFUND_REQUEST (not TRIAL_CANCELLATION).
   Signals that trigger this override:
     DE: "Betrug", "betrügerisch", "nicht autorisiert", "nicht genehmigt", "ohne mein Wissen",
         "ohne meine Zustimmung", "unberechtigte Abbuchung"
     JP: "不法請求", "不正請求", "詐欺", "不正利用"
     KR: "구독한게 없", "구독한 게 없", "구독한적 없", "구독한 적 없", "구독하지 않았",
         "구매한것도 없", "구매한 것도 없", "결제한 적 없", "결제한적 없",
         "가입한 적 없", "가입한적 없", "가입하지 않았", "신청한 적 없",
         "모르게 결제", "무단 결제", "무단결제", "결제시도", "잘못된 결제",
         "결제가 잘못", "결제를 한 적", "결제한 기억", "결제한 적이 없"
     EN: "fraud", "fraudulent", "illegal charge", "illegal billing",
         "charged without my consent/permission/knowledge",
         "I never signed up", "I never authorized", "I didn't know about this charge"
   Exception (Rule 0 does NOT apply — use TRIAL_CANCELLATION instead):
   - Message contains cancel words from Rule 1 list (cancel, 解約したい, opzeggen,
     uitschrijven, beëindigen, etc.)
     HOWEVER this exception has SUB-EXCEPTIONS — the following ALWAYS stay REFUND_REQUEST
     even if "취소" is present:
   - KR: if "취소" appears WITH any Korean fraud signal from the list above
     (구독한게 없, 구매한것도 없, 결제시도, 모르게 결제, etc.)
     → REFUND_REQUEST. The Korean word "취소" in a fraud/dispute context means
     "reverse/undo the charge", NOT "cancel my subscription".
     Examples:
       "전 구독한게 없고 구매한 것도 없는데 취소해주세요" → REFUND_REQUEST (not TRIAL_CANCELLATION!)
       "왜 결제시도된거죠? 취소해주세요" → REFUND_REQUEST
       "모르게 결제된 것 같은데 취소 부탁합니다" → REFUND_REQUEST
       BUT: "구독 취소하고 싶습니다" (no fraud signal) → TRIAL_CANCELLATION
   - German (DE) messages with "nichts bestellt", "kein Abonnement", "nicht abonniert"
     WITHOUT explicit refund words (Rückerstattung, Geld zurück, erstattet, zurückzahlen)
     → TRIAL_CANCELLATION (customer wants to cancel the unwanted subscription, NOT get a refund).
   - JP: "支払いには応じていない", "支払いを拒否", "この請求に応じていない", "支払いを認めない"
     (refusing/rejecting a charge) WITHOUT explicit 返金/払い戻し/お金を返して
     → TRIAL_CANCELLATION (customer rejecting an unwanted charge, not requesting a refund).
   IMPORTANT — fraud/chargeback THREAT is NOT a refund request:
   If the customer says "cancel my subscription" AND adds a THREAT like
   "I will report as fraud / I will dispute / I will file a chargeback IF you don't cancel"
   → this is TRIAL_CANCELLATION (cancel is the primary request, fraud is a conditional threat).
   Examples:
     "Cancel my subscription. If billing continues, I will report as unauthorized billing." → TRIAL_CANCELLATION
     "Please cancel immediately or I will file a chargeback" → TRIAL_CANCELLATION
     "解約してください。続けたらクレジットカード会社に連絡します" → TRIAL_CANCELLATION
   Only classify as REFUND_REQUEST when the fraud/unauthorized claim is the PRIMARY complaint
   with ZERO cancel words:
     "I was charged without my consent. I never signed up." → REFUND_REQUEST
     "This is fraud. I want my money back." → REFUND_REQUEST

   Pure fraud complaint with ZERO cancel words AND ZERO account-deletion phrases
   → REFUND_REQUEST.

0b. DELETE_ACCOUNT RULE — evaluate BEFORE cancel rules:
   If the customer's request is PURELY about deleting their account or removing their data,
   with NO mention of subscription, billing, charges, or cancellation → DELETE_ACCOUNT.
   Signals: "delete my account", "アカウント削除", "アカウントの削除", "アカウントを削除して",
   "계정 삭제", "Konto löschen", "supprimer mon compte", "видалити акаунт",
   "удалить аккаунт", "remove my account", "close my account", "deactivate my account",
   "account verwijderen", "アカウントを消して", "アカウントを消去".
   CRITICAL: if "delete account" + subscription/billing/charge context → TRIAL_CANCELLATION.
   CRITICAL: if "delete account" + refund/money back → REFUND_REQUEST.
   ONLY use DELETE_ACCOUNT when the SOLE request is account/data removal.

1a. CANCEL vs REFUND priority (check this FIRST, before any other rule):
   If the customer message contains cancellation signal + WEAK refund mention
   → TRIAL_CANCELLATION (cancel wins, bot cancels, refund handled by humans later).
   Examples (ALL are TRIAL_CANCELLATION):
     JP: "解約したい + 返金してほしい" → TRIAL_CANCELLATION
     JP: "解約 + お金が戻ってきますか" → TRIAL_CANCELLATION
     DE: "kündigen + Rückerstattung" → TRIAL_CANCELLATION
     EN: "cancel + refund" → TRIAL_CANCELLATION

   EXCEPTION — STRONG REFUND overrides cancel:
   If the customer message contains cancel signal + STRONG refund signal → REFUND_REQUEST.
   Strong refund signals = explicit refund demand with specific amount, fraud accusation,
   unauthorized charge complaint, or "money back" / 返金してください phrasing.
   Examples (ALL are REFUND_REQUEST despite cancel words):
     JP: "解約希望 + 5490円返金してください + 詐欺です" → REFUND_REQUEST
     JP: "解約 + 身に覚えのない請求 + 返金してください" → REFUND_REQUEST
     JP: "解約 + 勝手に引き落とし + お金を返して" → REFUND_REQUEST
     EN: "cancel + this is fraud + I want my money back" → REFUND_REQUEST
     EN: "cancel + unauthorized charge + full refund" → REFUND_REQUEST
     DE: "kündigen + Betrug + Geld zurück" → REFUND_REQUEST
   Rationale: when a customer complains about fraud/unauthorized charges AND demands
   a specific refund amount, the PRIMARY intent is charge dispute/refund.  Cancel is
   secondary. These tickets MUST go to a human for refund review.

   REFUND_REQUEST without any cancel signals → always REFUND_REQUEST (no change).
1b. Any form of "cancel", "キャンセル", "취소", "解約", "解除", "退会", "解除", "メンバーシップの解約",
   "退会したい", "解約したい", "止めたい", "やめたい", "kansellere", "avbryte", "avslutte",
   "annuleren", "avboka", "annullere",
   "batalkan", "hentikan langganan", "berhenti berlangganan" (ID: Indonesian)
   "opzeggen", "op zeggen", "beëindigen", "stopzetten", "abonnement annuleren",
   "opzegging", "uitschrijven", "abroment" (common typo for abonnement) (NL: Dutch)
   → ALWAYS a cancellation intent (TRIAL_CANCELLATION or SUB_CANCELLATION). NEVER REFUND_REQUEST
   or SUB_RENEWAL_REFUND if ANY cancellation word is present — UNLESS Rule 0 fraud override applies.
   NOTE: "delete my account" / "remove my account" etc. are NO LONGER in this list.
   They are handled by Rule 0b (DELETE_ACCOUNT) unless billing context is present.
2. "I noticed recurring/unexpected charges + please cancel" → TRIAL_CANCELLATION.
   Mentioning past charges does NOT make it a refund intent if the customer asks to cancel.
3. "I only wanted the IQ test / 知能テスト but got a subscription" → TRIAL_CANCELLATION.
4. "I signed up by mistake / didn't know I'd be charged" → TRIAL_CANCELLATION.
5. TECHNICAL_ISSUE is ONLY for login/access problems — never for billing or cancellation requests.
6. Default for any ambiguous cancellation → TRIAL_CANCELLATION.
7. SUB_RENEWAL_REFUND requires ALL THREE: (a) specific renewal charge already happened,
   (b) explicit refund request, (c) NO cancellation word anywhere in the message.
8. If the ticket subject is "Conversation with [name]", this is a Zendesk LIVE CHAT / Messaging transcript.
   The customer's actual request is in the conversation body (often a form submission).
   Classification rules for chat/messaging transcripts:
   → If body contains ANY cancel signal (解約, キャンセル, 退会, cancel, 취소, etc.) → TRIAL_CANCELLATION.
   → If body contains refund signals (返金, refund, 払い戻し, 환불, etc.) WITH ZERO cancel words → REFUND_REQUEST.
   → If body contains BOTH cancel + WEAK refund → TRIAL_CANCELLATION (cancel wins per Rule 1a).
   → If body contains BOTH cancel + STRONG refund (fraud, explicit amount+refund, 返金してください) → REFUND_REQUEST.
   → If body contains fraud signals (不正請求, unauthorized, etc.) WITH ZERO cancel words → REFUND_REQUEST.
   → If body is a form submission like "料金返金" or "返金希望" with no cancel words → REFUND_REQUEST.
   → If body has no clear signal at all → default to TRIAL_CANCELLATION (cancellation flow origin).
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
   EXCEPTION 2: if the message mentions ONLY a specific past payment reversal with ZERO
   subscription/trial cancel words → REFUND_REQUEST. But if ANY cancel word is present
   alongside the refund request → TRIAL_CANCELLATION (Rule 1a always wins).
   DISTINCTION: "please cancel my subscription + refund" → TRIAL_CANCELLATION (cancel wins).
                "please reverse this specific payment" (no cancel word) → REFUND_REQUEST.
10. DELETE_ACCOUNT examples:
   JP: "アカウントの削除をお願いします" → DELETE_ACCOUNT
   JP: "アカウントを削除してください" → DELETE_ACCOUNT
   JP: "アカウント削除の依頼" → DELETE_ACCOUNT
   JP: "私のアカウントの削除の要請" → DELETE_ACCOUNT
   EN: "Please delete my account" → DELETE_ACCOUNT
   EN: "I want to remove my account" → DELETE_ACCOUNT
   UK: "Прошу видалити мій акаунт" → DELETE_ACCOUNT
   RU: "Удалите мой аккаунт" → DELETE_ACCOUNT
   DE: "Bitte löschen Sie mein Konto" → DELETE_ACCOUNT
   NL: "Account verwijderen alstublieft" → DELETE_ACCOUNT
   KR: "계정 삭제 부탁드립니다" → DELETE_ACCOUNT
   BUT: "Delete my account, I was charged 1990 yen" → TRIAL_CANCELLATION (billing context!)
   BUT: "アカウント削除して、返金もお願いします" → REFUND_REQUEST (refund context!)
   BUT: "解約してアカウントも削除して" → TRIAL_CANCELLATION (cancel keyword present!)

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
- NL  = Dutch (Nederlands: "abonnement", "abroment" (common typo), "opzeggen", "op zeggen",
         "beëindigen", "annuleren", "verwijderen", "geld terug", "goedemiddag", "vriendelijke groet")
- UK  = Ukrainian (Українська: "акаунт", "видалити", "підписка")
- Use the primary language of the customer's message body.
- If the message contains multiple languages, pick the dominant one."""


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

    if start_idx != -1 and end_idx != -1:
        clean_json_str = raw_text[start_idx:end_idx + 1]
        try:
            return json.loads(clean_json_str)
        except json.JSONDecodeError as e:
            print(f"JSON Decode Error on cleaned string: {clean_json_str}")
            # Fall through to fallback
    else:
        print(f"Claude returned invalid response without JSON brackets: {raw_text}")
        # Fall through to fallback

    # Fallback: could not parse valid JSON — treat as UNKNOWN so bot skips safely
    return {**_FALLBACK, "reasoning": "parse error — classifier fallback"}
