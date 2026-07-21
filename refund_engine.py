"""
Refund decision engine — PURE, would-be-only (AN-192)
======================================================
Given Nexus-lookup data + classifier output, computes a *would-be* refund
decision: **would_be_refunded YES / NO + reason_code**. It does NOT move
money and has NO I/O — it is a pure function over plain data, so it is
trivially testable and can never call a payment API.

Scope of THIS iteration (see plan glowing-churning-steele.md):
  - Data source is ONLY Nexus `search_subscription` + the classifier.
    No Stripe verification, no two-source check, no PayPal/dispute check.
  - The result is a *draft signal for learning*, NOT a permission to pay.
    Nexus cannot see disputes / PayPal / non-Nexus payments (Slack #2/#3/#4),
    so a YES here does NOT mean "safe to refund". Dispute/PayPal checks are a
    hard prerequisite before any real execution (future iteration).

Fail-closed: any unexpected error → would_be_refunded = False (NO).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Optional

ENGINE_VERSION = "wb-nexus-v1"

# Intents that represent a refund request (the only ones in scope).
REFUND_INTENTS = ("REFUND_REQUEST", "SUB_RENEWAL_REFUND")

# ── Reason codes ─────────────────────────────────────────────────────────── #
# Stable strings — logged to BQ and shown in Slack. Each NO/blocked code marks
# the LEVEL of check at which the decision stopped (the "рівні перевірок").
RC_WOULD_BE_REFUNDED = "WOULD_BE_REFUNDED"   # YES (Nexus-only)
RC_UNABLE_TO_EVAL    = "UNABLE_TO_EVAL"      # Nexus not available — cannot even evaluate
RC_OUT_OF_SCOPE      = "OUT_OF_SCOPE"        # not a refund intent
RC_LOW_CONFIDENCE    = "LOW_CONFIDENCE"      # classifier below threshold
RC_NOT_FOUND_IN_NEXUS = "NOT_FOUND_IN_NEXUS" # no subscription/order found
RC_CHARGE_AMBIGUOUS  = "CHARGE_AMBIGUOUS"    # multi-site/multi-email/cross-sell
RC_AMOUNT_UNAVAILABLE = "AMOUNT_UNAVAILABLE" # no verifiable amount from Nexus
RC_EVAL_ERROR        = "EVAL_ERROR"          # fail-closed on unexpected error

# Candidate keys under which a numeric charge amount MIGHT appear in Nexus data.
# Nexus `search_subscription` currently returns subscription *state* (not a
# charge amount), so this is defensive: if a future/undocumented field carries
# an amount we pick it up; otherwise the engine reports AMOUNT_UNAVAILABLE.
_AMOUNT_KEYS = (
    "amount", "total", "price", "renewal_amount", "subscription_total",
    "last_charge_amount", "charge_amount", "order_total",
)
_CURRENCY_KEYS = ("currency", "currency_code", "curr")


@dataclass(frozen=True)
class RefundConfig:
    """Immutable snapshot of config, passed in by the caller (no env reads here)."""
    min_confidence: float = 0.90
    refunds_enabled: bool = False   # informational only this iteration (no live path)
    engine_version: str = ENGINE_VERSION


@dataclass
class RefundContext:
    """Plain input data for a single refund ticket. No I/O objects."""
    intent: str
    confidence: float
    language: str = "EN"
    nexus_available: bool = False        # was a Nexus client configured & queried?
    nexus_data: Optional[dict] = None    # raw search_subscription `data` dict, or None
    customer_stated_amount: Optional[str] = None  # parsed from ticket — AUDIT ONLY


@dataclass
class RefundDecision:
    would_be_refunded: bool
    reason_code: str
    human_message: str
    guard_trail: list = field(default_factory=list)
    computed_amount: Optional[str] = None     # str(Decimal) or None — verified amount
    currency: Optional[str] = None
    amount_is_split: bool = False             # A/B "150+150" was summed
    amount_source: str = "unavailable"        # "nexus" / "unavailable"
    customer_stated_amount: Optional[str] = None  # audit only, NEVER used for payout
    source: Optional[str] = None              # Nexus `source` (brand/site), for analytics
    engine_version: str = ENGINE_VERSION


# ── Pure helpers ─────────────────────────────────────────────────────────── #

def parse_amount(raw) -> tuple[Optional[Decimal], bool]:
    """Parse an amount that may be an A/B split like "150+150".

    Returns (summed_amount, is_split). SUMS split components (Slack #1 — admin
    shows A/B price as "150+150"; we never quote the split, we sum it).
    Returns (None, False) if nothing parseable.
    """
    if raw is None:
        return None, False
    if isinstance(raw, (int, float, Decimal)):
        try:
            return Decimal(str(raw)), False
        except (InvalidOperation, ValueError):
            return None, False

    s = str(raw)
    # Grab all numeric tokens (handles "¥150+¥150", "150 + 150", "1,500").
    tokens = re.findall(r"\d[\d,]*(?:\.\d+)?", s)
    if not tokens:
        return None, False
    total = Decimal("0")
    ok = False
    for t in tokens:
        try:
            total += Decimal(t.replace(",", ""))
            ok = True
        except (InvalidOperation, ValueError):
            continue
    if not ok:
        return None, False
    is_split = len(tokens) > 1
    return total, is_split


def _extract_amount(nexus_data: Optional[dict]) -> tuple[Optional[Decimal], bool, Optional[str]]:
    """Best-effort amount + currency extraction from Nexus data.

    Nexus `search_subscription` does not currently expose a charge amount, so
    this usually returns (None, False, None) → AMOUNT_UNAVAILABLE. Kept
    defensive so an undocumented/future amount field is picked up automatically.
    """
    if not isinstance(nexus_data, dict):
        return None, False, None
    amount, is_split = None, False
    for k in _AMOUNT_KEYS:
        if k in nexus_data and nexus_data[k] not in (None, "", 0, "0"):
            amount, is_split = parse_amount(nexus_data[k])
            if amount is not None:
                break
    currency = None
    for k in _CURRENCY_KEYS:
        val = nexus_data.get(k)
        if val:
            currency = str(val).upper()
            break
    return amount, is_split, currency


def _is_charge_ambiguous(nexus_data: dict) -> bool:
    """More than one distinct chargeable subscription/order in Nexus → ambiguous
    (multi-site / multi-email-per-card / legacy cross-sell — Slack #8/#9/#10).

    `renewal_subscriptions` is a COUNT of renewals on the SAME sub (not separate
    charges), so it does NOT make a case ambiguous. We look for evidence of
    multiple distinct subscriptions when Nexus exposes a list.
    """
    for list_key in ("subscriptions", "matches", "all_subscriptions"):
        val = nexus_data.get(list_key)
        if isinstance(val, list) and len(val) > 1:
            return True
    return bool(nexus_data.get("ambiguous"))


def _decision(would: bool, code: str, msg: str, trail: list, **kw) -> RefundDecision:
    trail = trail + [code]
    return RefundDecision(
        would_be_refunded=would, reason_code=code, human_message=msg,
        guard_trail=trail, **kw,
    )


# ── The pipeline ─────────────────────────────────────────────────────────── #

def decide(ctx: RefundContext, cfg: RefundConfig) -> RefundDecision:
    """Compute the would-be refund decision. PURE. Never raises (fail-closed).

    Ordered, fail-closed guards. Returns would_be_refunded=True ONLY if every
    guard passes; otherwise False with the reason_code of the level it stopped at.
    """
    trail: list = []
    try:
        # Data we can annotate regardless of verdict.
        nexus = ctx.nexus_data if isinstance(ctx.nexus_data, dict) else {}
        source = nexus.get("source") or None
        amount, is_split, currency = _extract_amount(nexus)
        common = dict(
            computed_amount=(str(amount) if amount is not None else None),
            currency=currency,
            amount_is_split=is_split,
            amount_source=("nexus" if amount is not None else "unavailable"),
            customer_stated_amount=ctx.customer_stated_amount,
            source=source,
            engine_version=cfg.engine_version,
        )

        # 0. Nexus available? (else we cannot evaluate at all — distinct from NO)
        if not ctx.nexus_available:
            trail.append("nexus_available")
            return _decision(False, RC_UNABLE_TO_EVAL,
                             "Nexus lookup unavailable — cannot evaluate refund.",
                             trail, **common)

        # 1. Scope — must be a refund intent.
        trail.append("scope")
        if ctx.intent not in REFUND_INTENTS:
            return _decision(False, RC_OUT_OF_SCOPE,
                             f"Intent {ctx.intent} is not a refund request.",
                             trail, **common)

        # 2. Confidence.
        trail.append("confidence")
        try:
            conf = float(ctx.confidence)
        except (TypeError, ValueError):
            conf = 0.0
        if conf < cfg.min_confidence:
            return _decision(False, RC_LOW_CONFIDENCE,
                             f"Classifier confidence {conf:.2f} < {cfg.min_confidence:.2f}.",
                             trail, **common)

        # 3. Found in Nexus?
        trail.append("found_in_nexus")
        if not nexus.get("subscription_id"):
            return _decision(False, RC_NOT_FOUND_IN_NEXUS,
                             "No subscription/order found in Nexus for this customer.",
                             trail, **common)

        # 4. Unambiguous charge?
        trail.append("unambiguous")
        if _is_charge_ambiguous(nexus):
            return _decision(False, RC_CHARGE_AMBIGUOUS,
                             "Multiple candidate charges — needs human (multi-site / "
                             "multi-email / cross-sell).",
                             trail, **common)

        # 5. Verifiable amount?
        trail.append("amount")
        if amount is None:
            return _decision(False, RC_AMOUNT_UNAVAILABLE,
                             "No verifiable charge amount from Nexus (customer-stated "
                             "amount is never trusted).",
                             trail, **common)

        # 6. All Nexus-only checks pass → would-be refund YES (draft, not a payout).
        trail.append("would_be")
        return _decision(True, RC_WOULD_BE_REFUNDED,
                         f"Would be refunded (Nexus-only, disputes NOT checked): "
                         f"{amount}{(' ' + currency) if currency else ''}"
                         f"{' [A/B summed]' if is_split else ''}.",
                         trail, **common)

    except Exception as e:  # noqa: BLE001 — fail-closed on ANYTHING
        return RefundDecision(
            would_be_refunded=False, reason_code=RC_EVAL_ERROR,
            human_message=f"Refund eval error (fail-closed to NO): {e}",
            guard_trail=trail + [RC_EVAL_ERROR], engine_version=cfg.engine_version,
        )
