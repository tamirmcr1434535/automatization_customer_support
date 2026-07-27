"""
Refund flow disambiguation (AN-192) — pick the DISPUTED charge with the LLM.

When the pure engine can't tell which charge/flow the customer means it returns
`AMBIGUOUS_FLOW` and defers to a human. But a human resolves most of these easily
by reading context ("refund everything except the first payment", "I accept the
199 test but not the recurring charge"). This module does the same: it hands the
ticket text + the customer's charges to the SAME Sonnet the classifier uses and
asks which charge the customer is disputing.

It returns only a `charge_id` (or None to abstain) — it NEVER decides YES/NO. The
pure engine then routes to that charge's flow and applies the normal window / type
rules, so the country-window guard still gates the outcome (no false YES) and an
abstain leaves the ticket AMBIGUOUS -> human. Fully FAIL-CLOSED: any error,
timeout, or unparseable/invalid response returns None.
"""

from __future__ import annotations

import json
import os
import re

from anthropic import Anthropic

_client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
_MODEL = os.getenv("REFUND_DISAMBIG_MODEL", "claude-sonnet-4-6")


def is_enabled() -> bool:
    return os.getenv("REFUND_DISAMBIG_ENABLED", "true").lower() == "true"


_SYSTEM = (
    "You route customer refund tickets. You are given the ticket text and the list "
    "of charges on the customer's account. Decide which SINGLE charge the customer "
    "is asking to be refunded / disputing. Customers often list the charges they "
    "ACCEPT (e.g. a small one-time test fee) and separately dispute the recurring "
    "subscription - pick the DISPUTED one, not the accepted one. "
    'Reply ONLY with a compact JSON object: {"charge_id": "<id>"} using an id from '
    'the list, or {"charge_id": null} if you cannot tell with confidence. Never '
    "guess; abstain (null) when the ticket is a general question, asks only for an "
    "explanation, or genuinely doesn't identify a charge."
)


def _charges_lines(charges) -> str:
    out = []
    for c in charges:
        cid = c.get("charge_id") or c.get("id")
        if not cid:
            continue
        out.append(
            f"- id={cid} amount={c.get('amount')} {c.get('currency') or ''} "
            f"type={c.get('type')} date={str(c.get('date') or '')[:10]} "
            f"refundable={c.get('refundable')}"
        )
    return "\n".join(out)


def _parse_charge_id(text: str, valid_ids: set) -> str | None:
    if not text:
        return None
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if not m:
        return None
    try:
        obj = json.loads(m.group(0))
    except (ValueError, TypeError):
        return None
    if not isinstance(obj, dict):
        return None
    cid = obj.get("charge_id")
    if cid is None:
        return None
    cid = str(cid).strip()
    # Only accept an id that actually exists in the customer's charge list.
    return cid if cid in valid_ids else None


def pick_target_charge_id(ticket_text: str, charges) -> str | None:
    """Return the charge_id the customer is disputing, or None to abstain.
    FAIL-CLOSED — never raises."""
    try:
        if not is_enabled() or not ticket_text or not charges:
            return None
        valid_ids = {str(c.get("charge_id") or c.get("id")) for c in charges
                     if (c.get("charge_id") or c.get("id"))}
        if len(valid_ids) < 2:
            return None  # nothing to disambiguate
        lines = _charges_lines(charges)
        if not lines:
            return None
        user = f"Ticket:\n{ticket_text}\n\nCharges:\n{lines}"
        resp = _client.messages.create(
            model=_MODEL,
            max_tokens=120,
            system=_SYSTEM,
            messages=[{"role": "user", "content": user}],
        )
        out = "".join(b.text for b in resp.content if getattr(b, "type", None) == "text")
        return _parse_charge_id(out, valid_ids)
    except Exception:  # noqa: BLE001 - fail-closed on ANYTHING
        return None
