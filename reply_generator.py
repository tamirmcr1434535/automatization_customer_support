"""
Reply Generator
===============
Uses Claude Sonnet to produce multilingual customer-facing replies.

Language is detected by the classifier (ISO 639-1 code) and all replies
are generated in the customer's own language.

Sign-off format (always appended):
    Best regards,
    {AGENT_NAME}
    {BRAND_NAME} Support Team
    ※[Disclaimer translated to the customer's language]
"""

import os
from anthropic import Anthropic

_client    = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
BRAND_NAME = os.getenv("BRAND_NAME", "IQ Booster")
AGENT_NAME = os.getenv("AGENT_NAME", "Mia")

# ── Disclaimer translations ───────────────────────────────────────────── #
# Appended to every reply after "Best regards, {AGENT_NAME} / {BRAND_NAME} Support Team"
_DISCLAIMERS = {
    "EN": "※Our support is officially provided in English. When contacting us in other languages, we may use a translation tool to assist you. Please let us know if anything needs clarification.",
    "DE": "※Unser Support wird offiziell auf Englisch angeboten. Bei Anfragen in anderen Sprachen kann ein Übersetzungstool zum Einsatz kommen. Bitte teilen Sie uns mit, wenn etwas unklar ist.",
    "JP": "※当サポートの公式対応言語は英語です。英語以外の言語でのお問い合わせには、翻訳ツールを使用して対応させていただく場合がございます。あらかじめご了承ください。",
    "KR": "※저희 지원은 공식적으로 영어로 제공됩니다. 다른 언어로 문의하시는 경우 번역 도구를 사용할 수 있습니다. 불명확한 부분이 있으면 알려주세요.",
    "FR": "※Notre support est officiellement fourni en anglais. Lorsque vous nous contactez dans d'autres langues, nous pouvons utiliser un outil de traduction. N'hésitez pas à nous le faire savoir si quelque chose n'est pas clair.",
    "ZH": "※我们的支持服务官方语言为英语。当您以其他语言联系我们时，我们可能会使用翻译工具为您提供帮助。如有任何不清楚之处，请告知我们。",
    "ES": "※Nuestro soporte se ofrece oficialmente en inglés. Cuando nos contacta en otros idiomas, podemos utilizar una herramienta de traducción. Háganos saber si algo no está claro.",
    "PT": "※O nosso suporte é oficialmente fornecido em inglês. Quando nos contacta noutras línguas, podemos usar uma ferramenta de tradução. Informe-nos se algo não estiver claro.",
    "IT": "※Il nostro supporto è ufficialmente fornito in inglese. Quando ci contatta in altre lingue, potremmo utilizzare uno strumento di traduzione. Ci faccia sapere se qualcosa non è chiaro.",
    "RU": "※Наша поддержка официально осуществляется на английском языке. При обращении на других языках мы можем использовать инструмент перевода. Сообщите нам, если что-то непонятно.",
}

def _disclaimer(language: str) -> str:
    return _DISCLAIMERS.get(language.upper(), _DISCLAIMERS["EN"])

def _signoff(language: str) -> str:
    return f"Best regards,\n{AGENT_NAME}\n{BRAND_NAME} Support Team\n\n{_disclaimer(language)}"


# ── System prompt ─────────────────────────────────────────────────────── #

SYSTEM = f"""You are {AGENT_NAME}, a professional customer support agent for {BRAND_NAME}.

CRITICAL LANGUAGE RULE: You MUST write the reply body in the EXACT same language the customer used.
- Customer wrote in Japanese → reply body in Japanese
- Customer wrote in German → reply body in German
- Customer wrote in Korean → reply body in Korean
- Customer wrote in English → reply body in English
- Any other language → reply body in that same language
The sign-off block (Best regards / {AGENT_NAME} / {BRAND_NAME} Support Team / disclaimer)
is provided separately and must be appended as-is — do NOT translate or alter it.

Tone by language:
- JP: polite keigo — 〜でございます, いただきありがとうございます
- KR: formal 존댓말 — 〜드립니다, 감사합니다
- DE: formal Sie-form — Sehr geehrte/r, vielen Dank für Ihre Anfrage
- FR: formal vous-form — Nous vous confirmons, Veuillez nous contacter
- EN: warm, professional, concise
- Other: match the formality of the customer's own message

Output ONLY the reply body text followed by the sign-off block. No subject line.

---
REFERENCE TEMPLATES (exact structure and length to follow):

[JP | TRIAL_CANCELLATION]
こんにちは、[Name]様。

お問い合わせいただきありがとうございます。{BRAND_NAME}の7日間無料トライアルが正常にキャンセルされたことをご確認いたします。今後、お客様のアカウントに料金が請求されることはございません。

ご不明な点やその他サポートが必要な場合は、どうぞお気軽にご連絡ください。いつでもお手伝いさせていただきます。

[JP | SUB_CANCELLATION]
こんにちは。

ご連絡ありがとうございます。

{BRAND_NAME}のサブスクリプション解約のご依頼を承りました。ご依頼どおり、サブスクリプションは解約済みです。今後の追加請求は発生しません。ただし、すでにお支払いいただいている期間の終了までは引き続きご利用いただけます。その後はプランが終了し、アクセスが停止されます。

ご不明点やサポートが必要な場合は、どうぞ遠慮なくお知らせください。

[KR | TRIAL_CANCELLATION]
안녕하세요, [Name]님.

문의해 주셔서 감사합니다.
고객님의 {BRAND_NAME} 7일 무료 체험이 성공적으로 취소되었음을 확인해 드립니다. 앞으로 고객님의 계정에 추가 요금이 청구되지 않을 것입니다.

궁금하신 점이나 추가 도움이 필요하신 경우 언제든지 연락해 주세요.

[KR | SUB_CANCELLATION]
안녕하세요.

문의해 주셔서 감사합니다.
고객님의 {BRAND_NAME} 구독이 성공적으로 취소되었습니다. 현재 결제 기간이 끝날 때까지 서비스를 이용하실 수 있으며, 이후에는 추가 요금이 발생하지 않습니다.

다른 문의 사항이 있으시면 언제든지 연락해 주세요.

[EN | TRIAL_CANCELLATION]
Hi [Name],

Thank you for reaching out to us.
We confirm that your 7-day free trial for {BRAND_NAME} has been successfully cancelled. No further charges will be applied to your account.

If you have any other questions or need further assistance, please don't hesitate to contact us.

[EN | SUB_CANCELLATION]
Hi [Name],

Thank you for contacting us.
Your {BRAND_NAME} subscription has been successfully cancelled as requested. No further charges will apply. You will continue to have access until the end of your current billing period, after which your plan will expire and access will be discontinued.

If you have any questions or need support, please don't hesitate to let us know.

[DE | TRIAL_CANCELLATION]
Hallo, [Name]!

Vielen Dank für Ihre Anfrage.
Wir bestätigen, dass Ihre 7-tägige kostenlose Testphase von {BRAND_NAME} erfolgreich gekündigt wurde. Es werden keine weiteren Gebühren anfallen.

Sollten Sie weitere Fragen haben oder Unterstützung benötigen, zögern Sie bitte nicht, uns zu kontaktieren. Wir helfen Ihnen jederzeit gerne weiter.

[DE | SUB_CANCELLATION]
Hallo, [Name]!

Vielen Dank für Ihre Nachricht.
Wir bestätigen, dass Ihr {BRAND_NAME}-Abonnement erfolgreich gekündigt wurde. Es werden keine weiteren Gebühren erhoben. Sie haben jedoch bis zum Ende des aktuellen Abrechnungszeitraums weiterhin Zugang zum Dienst, danach endet Ihr Plan automatisch.

Bei weiteren Fragen stehen wir Ihnen jederzeit gerne zur Verfügung.
---"""


INTENT_CONTEXT = {{
    "TRIAL_CANCELLATION": "The free trial has been cancelled. No charge will occur.",
    "SUB_CANCELLATION": (
        "The subscription has been cancelled. "
        "Access continues until the end of the current billing period, then no further charges."
    ),
    "SUB_RENEWAL_CANCELLATION": (
        "Auto-renewal has been disabled. "
        "Current access continues until the period end, then the subscription ends automatically."
    ),
}}

SUB_TYPE_CONTEXT = {{
    "trial": "The free trial has been cancelled. No charge will occur.",
    "subscription": (
        "The subscription has been cancelled. "
        "Access continues until the end of the current billing period, then no further charges."
    ),
}}


def generate_reply(intent: str, language: str, customer_name: str, cancel_result: dict) -> str:
    """Generate a cancellation confirmation reply in the customer's language."""
    sub_type  = cancel_result.get("subscription_type")
    situation = (
        SUB_TYPE_CONTEXT[sub_type]
        if sub_type in SUB_TYPE_CONTEXT
        else INTENT_CONTEXT.get(intent, "The subscription has been cancelled.")
    )
    cancel_status = cancel_result.get("status", "unknown")
    source        = cancel_result.get("source", "")
    signoff       = _signoff(language)

    prompt = f"""Language: {language}
Intent: {intent}
Customer name: {customer_name or 'the customer'}
Situation: {situation}
Cancellation status: {cancel_status}{f' (via {{source}})' if source else ''}

Follow the reference template for [{{language}} | {{intent}}] from the REFERENCE TEMPLATES above.
Use the same tone, structure, and length.
Replace [Name] with the actual customer name if provided.

After the reply body, append the following sign-off block EXACTLY as written:
{signoff}

Write the full reply (body + sign-off):"""

    r = _client.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=500,
        messages=[{{"role": "user", "content": f"{{SYSTEM}}\n\n{{prompt}}"}}],
    )
    return r.content[0].text.strip()


def generate_ask_card_digits_reply(language: str, customer_name: str) -> str:
    """
    First request: email not found — ask for registered email + payment proof.
    Matches the template shown in screenshots:
      - Registered email address
      - Last 4 card digits (credit card)
      - PayPal account (if PayPal)
      - Bank receipt screenshot (if bank transfer)
    """
    signoff = _signoff(language)

    prompt = f"""Language: {language}
Customer name: {customer_name or 'the customer'}

Situation: We searched our system but could not find any subscription or trial linked
to the email address in this support ticket.

Write a reply that:
1. Briefly apologises that we couldn't locate the account using the email from the ticket
2. Asks the customer to provide ANY of the following to help us locate their account:
   • Their registered email address (if different from the one used to send this ticket)
   • If paid by credit/debit card: the last 4 digits of the card
   • If paid via PayPal: the PayPal account email used
   • A screenshot of the receipt from their banking app (showing charge amount and merchant name)
3. States that once we receive this information we will process their request as soon as possible
4. Warm, empathetic, professional tone (4–6 sentences)

After the reply body, append the following sign-off block EXACTLY as written:
{signoff}

Write the full reply (body + sign-off):"""

    r = _client.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=400,
        messages=[{{"role": "user", "content": f"{{SYSTEM}}\n\n{{prompt}}"}}],
    )
    return r.content[0].text.strip()


def generate_ask_card_digits_retry_reply(language: str, customer_name: str) -> str:
    """
    Second request: provided digits/info didn't match. Ask again. 2-day window.
    """
    signoff = _signoff(language)

    prompt = f"""Language: {language}
Customer name: {customer_name or 'the customer'}

Situation: The customer provided their last 4 card digits but we could not find a matching
subscription in Stripe. The digits may be incorrect or the subscription is under a different card.

Write a reply that:
1. Apologises that the card digits provided did not match any account in our system
2. Asks the customer to double-check and provide:
   • The correct last 4 digits of the card used when signing up
   • Or their registered email address if different from the ticket email
   • Or a screenshot of the receipt showing the charge
3. Informs that if we don't hear back within 2 days the ticket will be automatically closed
4. Empathetic and concise (3–5 sentences)

After the reply body, append the following sign-off block EXACTLY as written:
{signoff}

Write the full reply (body + sign-off):"""

    r = _client.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=400,
        messages=[{{"role": "user", "content": f"{{SYSTEM}}\n\n{{prompt}}"}}],
    )
    return r.content[0].text.strip()


def generate_not_found_reply(language: str, customer_name: str) -> str:
    """
    Final close: not found after all attempts. Close ticket.
    """
    signoff = _signoff(language)

    prompt = f"""Language: {language}
Customer name: {customer_name or 'the customer'}

Situation: After searching by email address and two rounds of card digit / payment info lookup,
we could not find any active subscription or trial for this customer.
We are closing the ticket as we have exhausted our lookup options.

Write a reply that:
1. Apologises for the inconvenience
2. Explains that despite thorough searching we could not find an active account
3. Suggests they may have used a different email or payment method and invites them
   to contact us again with that information if they wish to pursue this further
4. Informs that this ticket is now being closed
5. Empathetic, professional, 3–5 sentences

After the reply body, append the following sign-off block EXACTLY as written:
{signoff}

Write the full reply (body + sign-off):"""

    r = _client.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=450,
        messages=[{{"role": "user", "content": f"{{SYSTEM}}\n\n{{prompt}}"}}],
    )
    return r.content[0].text.strip()
