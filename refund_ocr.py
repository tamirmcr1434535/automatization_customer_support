"""
Refund screenshot OCR (AN-192) — read a payment amount out of an attached image.

Many refund complaints (especially JP/billing) attach a payment SCREENSHOT that
shows the charged amount + currency + card last-4 + transaction id, while the
email TEXT states no amount at all. The would-be engine then correctly returns
"amount not stated" but misses a real, matchable amount sitting in the image.

This module sends the image(s) to the SAME Claude model the classifier already
uses (vision-capable Sonnet) and extracts a compact JSON object. It is PURE-ish
(one read-only model call), never moves money, and is fully FAIL-CLOSED: any
error, timeout, or unparseable response returns None so the caller falls back to
the text-only path unchanged.
"""

from __future__ import annotations

import base64
import json
import os
import re

from anthropic import Anthropic

_client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

# Same model family as the classifier — we are already on Sonnet; do not diverge.
_MODEL = os.getenv("REFUND_OCR_MODEL", "claude-sonnet-4-6")

# Single env toggle; on by default. Set REFUND_OCR_ENABLED=false to disable.
def is_enabled() -> bool:
    return os.getenv("REFUND_OCR_ENABLED", "true").lower() == "true"


_ALLOWED_MEDIA = {"image/png", "image/jpeg", "image/gif", "image/webp"}

_PROMPT = (
    "This image is a screenshot a customer attached to a REFUND request (often a "
    "bank/card statement, a payment-app receipt, or an order confirmation). Extract "
    "the single payment the customer is most likely asking to be refunded — the "
    "charged/paid TOTAL, not a balance, date, phone number, or order number.\n\n"
    "Return ONLY a compact JSON object, no prose, with these keys:\n"
    '  "amount":    the numeric amount as written (e.g. "5,490" or "9.99"), or null\n'
    '  "currency":  the currency SYMBOL or code exactly as shown (e.g. "¥", "JPY", '
    '"$", "€"), or null\n'
    '  "card_last4": last 4 digits of the card if visible, or null\n'
    '  "txn_id":    the transaction / order id if visible, or null\n'
    "If several amounts appear and you cannot tell which is the charge, set amount "
    "to null. Never guess."
)


def _media_type(content_type: str) -> str | None:
    ct = (content_type or "").split(";")[0].strip().lower()
    if ct == "image/jpg":
        ct = "image/jpeg"
    return ct if ct in _ALLOWED_MEDIA else None


def _parse_json(text: str) -> dict | None:
    """Extract the first JSON object from the model text. Fail-closed to None."""
    if not text:
        return None
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if not m:
        return None
    try:
        obj = json.loads(m.group(0))
    except (ValueError, TypeError):
        return None
    return obj if isinstance(obj, dict) else None


def _clean(v):
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def extract_amount_from_images(images, max_images: int = 3) -> dict | None:
    """images: list of (raw_bytes, content_type). Returns a dict with keys
    amount/currency/card_last4/txn_id (any may be None) if an amount was read,
    else None. FAIL-CLOSED — never raises."""
    try:
        if not is_enabled() or not images:
            return None
        content: list = []
        used = 0
        for raw, content_type in images:
            if used >= max_images:
                break
            media = _media_type(content_type)
            if not media or not raw:
                continue
            content.append({
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": media,
                    "data": base64.standard_b64encode(raw).decode("ascii"),
                },
            })
            used += 1
        if not content:
            return None
        content.append({"type": "text", "text": _PROMPT})

        resp = _client.messages.create(
            model=_MODEL,
            max_tokens=300,
            messages=[{"role": "user", "content": content}],
        )
        text = "".join(b.text for b in resp.content if getattr(b, "type", None) == "text")
        obj = _parse_json(text)
        if not obj:
            return None
        amount = _clean(obj.get("amount"))
        if not amount:
            return None
        return {
            "amount": amount,
            "currency": _clean(obj.get("currency")),
            "card_last4": _clean(obj.get("card_last4")),
            "txn_id": _clean(obj.get("txn_id")),
        }
    except Exception:  # noqa: BLE001 — fail-closed on ANYTHING (API/parse/network)
        return None
