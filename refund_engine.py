"""
Refund decision engine — PURE, would-be-only (AN-192 / IQTEST-1431)
====================================================================
Given the Nexus `charges[]` list + the customer's ticket text, computes a
*would-be* refund decision: **would_be_refunded YES / NO + reason_code**, and
which charge would be the candidate. It does NOT move money and has NO I/O — it
is a pure function over plain data, so it is trivially testable and can never
call a payment API.

Validation logic (matches the two-API design agreed 2026-07-21):
  - Read the amount(s) the customer states in the ticket.
  - Match them against the real `charges[]` from Nexus (amount + currency).
  - Exactly one *refundable* charge matches one stated amount → candidate (YES).
  - No match / already-refunded / several matches / several stated amounts →
    NO with a specific reason → human decides.

The result is a *draft signal for learning*, NOT permission to pay. Execution
(refund by charge_id) is a separate, gated API. Nexus cannot see disputes, so a
YES here still requires the refund-API dispute guard before any real refund.

Fail-closed: any unexpected error → would_be_refunded = False (NO).
"""

from __future__ import annotations

import re
import unicodedata
from collections import Counter
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Optional

ENGINE_VERSION = "wb-charges-v4"   # v4: multi-amount resolve (repetition + refund-proximity)

# Multilingual refund keywords — used for proximity resolution of multiple amounts.
_REFUND_KW = re.compile(
    r"refund|money\s*back|reimburs|返金|払い戻し|返して|환불|erstatt|rückerstatt|"
    r"rembours|reembols|reembolso|devoluci|devolver|estorno|rimbors|terugbetal|"
    r"hoàn\s*tiền|hoàn\s*lại|возврат|верн|поверн|iade|pengembalian|kembalikan",
    re.IGNORECASE,
)

REFUND_INTENTS = ("REFUND_REQUEST", "SUB_RENEWAL_REFUND")

# ── Reason codes (level at which the decision stopped) ───────────────────── #
RC_WOULD_BE_REFUNDED = "WOULD_BE_REFUNDED"    # YES — one clean refundable match
RC_UNABLE_TO_EVAL    = "UNABLE_TO_EVAL"       # Nexus not available
RC_OUT_OF_SCOPE      = "OUT_OF_SCOPE"         # not a refund intent
RC_LOW_CONFIDENCE    = "LOW_CONFIDENCE"       # classifier below threshold
RC_NOT_FOUND_IN_NEXUS = "NOT_FOUND_IN_NEXUS"  # no charges for this customer
RC_NOTHING_REFUNDABLE = "NOTHING_REFUNDABLE"  # charges exist but none refundable
RC_AMOUNT_NOT_STATED = "AMOUNT_NOT_STATED"    # customer gave no amount → can't validate
RC_AMOUNT_MISMATCH   = "AMOUNT_MISMATCH"      # stated amount matches no charge (FX/fees?) → human
RC_ALREADY_REFUNDED  = "ALREADY_REFUNDED"     # stated amount matches an already-refunded charge
RC_CHARGE_AMBIGUOUS  = "CHARGE_AMBIGUOUS"     # several refundable charges match one amount
RC_MULTIPLE_AMOUNTS_STATED = "MULTIPLE_AMOUNTS_STATED"  # customer cited several amounts
RC_EVAL_ERROR        = "EVAL_ERROR"           # fail-closed on unexpected error

# Currency indicators — used to extract amounts the customer *states* (anchored
# to a currency so we don't pick up dates like "7/21" or "24 hours").
_CUR = (
    r"(?:¥|￥|\$|€|£|₩|₺|₹|₴|₪|₽|zł|Kč|円|圆|元|원|"
    r"USD|JPY|EUR|GBP|KRW|IDR|THB|VND|PHP|MXN|BRL|TRY|TL|INR|PLN|UAH|ILS|CHF|RUB|"
    r"dollars?|yen|won|lira|евро|долл?|грн)"
)
_NUM = r"\d[\d.,]*\d|\d"   # must start and end with a digit (no trailing separator)
_STATED_RE = re.compile(
    rf"(?:{_CUR}\s*({_NUM}))|(?:({_NUM})\s*{_CUR})",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class RefundConfig:
    min_confidence: float = 0.90
    refunds_enabled: bool = False   # informational only — no live path exists
    engine_version: str = ENGINE_VERSION


@dataclass
class RefundContext:
    """Plain input data for a single refund ticket. No I/O objects."""
    intent: str
    confidence: float
    language: str = "EN"
    nexus_available: bool = False
    nexus_data: Optional[dict] = None   # includes "charges", "subscription_id", "source"
    ticket_text: str = ""               # subject + body (+ comments) for amount parsing


@dataclass
class RefundDecision:
    would_be_refunded: bool
    reason_code: str
    human_message: str
    guard_trail: list = field(default_factory=list)
    computed_amount: Optional[str] = None       # candidate charge amount (verified)
    currency: Optional[str] = None
    customer_stated_amount: Optional[str] = None  # first stated amount (audit only)
    customer_stated_amounts: Optional[str] = None  # ALL stated amounts, comma-joined (audit)
    candidate_charge_id: Optional[str] = None
    charge_type: Optional[str] = None           # first_sale / cross_sale / subscription
    source: Optional[str] = None                # Nexus `source` (brand/site)
    engine_version: str = ENGINE_VERSION


# ── Pure helpers ─────────────────────────────────────────────────────────── #

def _to_decimal(raw) -> Optional[Decimal]:
    """Locale-aware numeric parse. Handles both US ("1,234.56", "9.99") and
    European ("1.234,56", "9,99") notation so a comma used as a DECIMAL separator
    is not mistaken for a thousands separator (bug: "9,99" € must be 9.99, not 999)."""
    if raw is None:
        return None
    # NFKC folds full-width (zenkaku) digits/punctuation to ASCII ("１，９９０"→"1,990"),
    # so a full-width comma no longer truncates the number (was "１，９９０円" → 990).
    s = unicodedata.normalize("NFKC", str(raw)).strip().replace(" ", "").replace(" ", "")
    if not s:
        return None
    has_dot, has_comma = "." in s, "," in s
    if has_dot and has_comma:
        # The separator that appears LAST is the decimal one.
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")   # EU: 1.234,56 → 1234.56
        else:
            s = s.replace(",", "")                       # US: 1,234.56 → 1234.56
    elif has_comma:
        parts = s.split(",")
        # single comma + exactly 2 trailing digits → decimal (9,99 → 9.99);
        # otherwise treat comma(s) as thousands separators (1,990 → 1990).
        if len(parts) == 2 and len(parts[1]) == 2:
            s = s.replace(",", ".")
        else:
            s = s.replace(",", "")
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


def _amounts_detailed(text: str):
    """All stated amounts WITH their positions (repeats kept), on NFKC-normalised
    text → list of (Decimal, position). Position is used for refund-proximity."""
    if not text:
        return []
    text = unicodedata.normalize("NFKC", text)  # full-width → ASCII before matching
    out = []
    for m in _STATED_RE.finditer(text):
        tok = m.group(1) or m.group(2)
        d = _to_decimal(tok)
        if d is not None and d > 0:
            out.append((d, m.start()))
    return out


def parse_stated_amounts(text: str) -> list[Decimal]:
    """Distinct amounts the customer states, in order seen (currency-anchored)."""
    out, seen = [], set()
    for d, _ in _amounts_detailed(text):
        if d not in seen:
            seen.add(d)
            out.append(d)
    return out


def _amount_nearest_refund(text: str, candidates: set, detailed):
    """Among `candidates`, the amount whose occurrence is closest to a refund
    keyword. Returns that amount, or None if there's no refund keyword or a tie."""
    norm = unicodedata.normalize("NFKC", text or "")
    kw_pos = [m.start() for m in _REFUND_KW.finditer(norm)]
    if not kw_pos:
        return None
    best_amt, best_dist = None, None
    ties = False
    for amt, pos in detailed:
        if amt not in candidates:
            continue
        dist = min(abs(pos - k) for k in kw_pos)
        if best_dist is None or dist < best_dist:
            best_amt, best_dist, ties = amt, dist, False
        elif dist == best_dist and amt != best_amt:
            ties = True
    return None if ties else best_amt


def _charge_amount(charge: dict) -> Optional[Decimal]:
    return _to_decimal(charge.get("amount"))


def _is_refundable(charge: dict) -> bool:
    # Trust the explicit flag; fall back to status if the flag is absent.
    if "refundable" in charge:
        return charge.get("refundable") is True
    return str(charge.get("status", "")).lower() == "success"


# Date like "2026年7月21日", "2026/7/21", "2026-07-21", "7/21", "7月21日", "21.07.2026".
_DATE_RE = re.compile(
    r"(?:(\d{4})[年/\-.])?(\d{1,2})[月/\-.](\d{1,2})日?"
)


def parse_stated_dates(text: str):
    """Extract candidate dates the customer states → list of (year|None, month, day).
    Handles JP M/D order and EU D/M order by emitting both valid interpretations,
    so matching against the real charge date resolves the order. Full-width safe."""
    if not text:
        return []
    text = unicodedata.normalize("NFKC", text)
    out = []
    for m in _DATE_RE.finditer(text):
        y = int(m.group(1)) if m.group(1) else None
        a, b = int(m.group(2)), int(m.group(3))
        if 1 <= a <= 12 and 1 <= b <= 31:      # (month=a, day=b) — JP / ISO order
            out.append((y, a, b))
        if 1 <= b <= 12 and 1 <= a <= 31 and a != b:  # (month=b, day=a) — EU order
            out.append((y, b, a))
    return out


def _charge_ymd(charge: dict):
    """(year, month, day) from a charge's ISO date, or None."""
    m = re.search(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})", str(charge.get("date") or ""))
    return (int(m.group(1)), int(m.group(2)), int(m.group(3))) if m else None


def _charge_matches_date(charge: dict, dates) -> bool:
    ymd = _charge_ymd(charge)
    if not ymd:
        return False
    cy, cm, cd = ymd
    for (y, mo, d) in dates:
        if mo == cm and d == cd and (y is None or y == cy):
            return True
    return False


def _decision(would: bool, code: str, msg: str, trail: list, **kw) -> RefundDecision:
    return RefundDecision(
        would_be_refunded=would, reason_code=code, human_message=msg,
        guard_trail=trail + [code], **kw,
    )


# ── The pipeline ─────────────────────────────────────────────────────────── #

def decide(ctx: RefundContext, cfg: RefundConfig) -> RefundDecision:
    """Compute the would-be refund decision. PURE. Never raises (fail-closed)."""
    trail: list = []
    try:
        nexus = ctx.nexus_data if isinstance(ctx.nexus_data, dict) else {}
        source = nexus.get("source") or None
        charges = nexus.get("charges") if isinstance(nexus.get("charges"), list) else []
        stated = parse_stated_amounts(ctx.ticket_text)
        common = dict(
            source=source,
            customer_stated_amount=(str(stated[0]) if stated else None),
            customer_stated_amounts=(",".join(str(a) for a in stated) if stated else None),
            engine_version=cfg.engine_version,
        )

        # 0. Nexus available?
        trail.append("nexus_available")
        if not ctx.nexus_available:
            return _decision(False, RC_UNABLE_TO_EVAL,
                             "Nexus lookup unavailable — cannot evaluate refund.",
                             trail, **common)

        # 1. Scope.
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

        # 3. Charges present?
        trail.append("charges_present")
        if not charges:
            return _decision(False, RC_NOT_FOUND_IN_NEXUS,
                             "No charges found in Nexus for this customer.",
                             trail, **common)

        # 4. Anything refundable?
        trail.append("refundable_exists")
        refundable = [c for c in charges if _is_refundable(c)]
        if not refundable:
            return _decision(False, RC_NOTHING_REFUNDABLE,
                             "Charges exist but none are refundable (already refunded / not eligible).",
                             trail, **common)

        # 5. Customer stated an amount?
        trail.append("amount_stated")
        if not stated:
            return _decision(False, RC_AMOUNT_NOT_STATED,
                             "Customer did not state a verifiable amount — cannot pick a charge.",
                             trail, **common)

        # 6. Match stated amount(s) against charges → resolve to a single candidate.
        trail.append("match")

        def _refundable_for(a):
            return [ch for ch in refundable if _charge_amount(ch) == a]

        def _amounts_csv():
            return ", ".join(str(a) for a in stated)

        c = None
        if len(stated) == 1:
            amt1 = stated[0]
            matched_all = [ch for ch in charges if _charge_amount(ch) == amt1]
            matched_refundable = _refundable_for(amt1)
            if not matched_all:
                return _decision(False, RC_AMOUNT_MISMATCH,
                                 "Stated amount matches no charge (may be bank/FX fees) — human decides.",
                                 trail, **common)
            if not matched_refundable:
                return _decision(False, RC_ALREADY_REFUNDED,
                                 "Stated amount matches a charge already refunded / not refundable.",
                                 trail, **common)
            if len(matched_refundable) == 1:
                c = matched_refundable[0]
            else:
                # Same-amount charges → disambiguate by a stated DATE ("7/21付けで5490円").
                dates = parse_stated_dates(ctx.ticket_text)
                hits = [ch for ch in matched_refundable if _charge_matches_date(ch, dates)]
                if len(hits) == 1:
                    c = hits[0]
                    trail.append("date_disambiguated")
                else:
                    return _decision(
                        False, RC_CHARGE_AMBIGUOUS,
                        f"{len(matched_refundable)} refundable charges match the amount"
                        + ("; stated date not unique" if dates else "; no date to disambiguate")
                        + " — human decides.",
                        trail, **common)
        else:
            # Multiple stated amounts. Resolve ONLY if repetition AND refund-proximity
            # agree on the same amount — otherwise hand to a human (conservative).
            detailed = _amounts_detailed(ctx.ticket_text)
            cand = [a for a in stated if len(_refundable_for(a)) == 1]  # each maps to 1 refundable charge
            if not cand:
                return _decision(False, RC_MULTIPLE_AMOUNTS_STATED,
                                 f"Customer cited {len(stated)} amounts ({_amounts_csv()}); none maps to a "
                                 "single refundable charge — human decides.",
                                 trail, **common)
            if len(cand) == 1:
                c = _refundable_for(cand[0])[0]
                trail.append("multi_single_candidate")
            else:
                freq = Counter(a for a, _ in detailed)
                mx = max(freq[a] for a in cand)
                top = [a for a in cand if freq[a] == mx]
                a_win = top[0] if len(top) == 1 else None                    # A: repetition
                b_win = _amount_nearest_refund(ctx.ticket_text, set(cand), detailed)  # B: proximity
                if a_win is not None and a_win == b_win:
                    c = _refundable_for(a_win)[0]
                    trail.append("multi_resolved_repetition_proximity")
                else:
                    return _decision(False, RC_MULTIPLE_AMOUNTS_STATED,
                                     f"Customer cited {len(stated)} amounts ({_amounts_csv()}); repetition "
                                     "and refund-proximity disagree — human decides.",
                                     trail, **common)

        # 7. Single refundable candidate (direct / date / multi-resolved).
        amt = _charge_amount(c)
        cur = c.get("currency")
        trail.append("would_be")
        return _decision(
            True, RC_WOULD_BE_REFUNDED,
            f"Would be refunded (Nexus-only, disputes NOT checked): "
            f"{amt}{(' ' + str(cur)) if cur else ''} "
            f"charge {c.get('charge_id')} ({c.get('type')}).",
            trail,
            computed_amount=(str(amt) if amt is not None else None),
            currency=(str(cur) if cur else None),
            candidate_charge_id=c.get("charge_id"),
            charge_type=c.get("type"),
            **common,
        )

    except Exception as e:  # noqa: BLE001 — fail-closed on ANYTHING
        return RefundDecision(
            would_be_refunded=False, reason_code=RC_EVAL_ERROR,
            human_message=f"Refund eval error (fail-closed to NO): {e}",
            guard_trail=trail + [RC_EVAL_ERROR], engine_version=cfg.engine_version,
        )
