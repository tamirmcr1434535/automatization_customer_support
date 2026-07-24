"""
Refund decision engine — PURE, would-be-only (AN-192, Flow #1: Subscription fee)
================================================================================
Implements the Figma "IQ Booster Subscription — Refund Flow" decision tree for the
**subscription-fee** refund (flow #1), as a *would-be* signal only. It computes
`would_be_refunded YES/NO + reason_code`; it has NO I/O and NEVER moves money.

Flow #1 rule (Figma + Anna's business rules):
  refund request → find account & charges (Nexus) → found? →
    dispute (Stripe/PayPal)? → yes: do NOT refund (wait for dispute) →
    no dispute → is the **LAST subscription payment** within the **country refund
    window**? → yes: refund that last payment (earlier months NOT refunded) → no: decline.

Key points:
  - Target = the **latest refundable `subscription` renewal** charge (rule-selected,
    NOT chosen by the amount the customer typed). Earlier renewals are never refunded.
  - One-time charges (`first_sale`=IQ Test fee, `cross_sale`=IQ Test Report) are out of
    scope for flow #1 (handled by other flows / a human).
  - Window is measured from the charge date to the TICKET date (`as_of_date`).
  - Customer-stated amount is INFORMATIONAL (logged, never used to pick/gate).
  - Disputes are invisible to Nexus → this is a would-be draft, NOT a payout permit; a
    real refund still requires the dispute guard (separate, gated iteration).

Fail-closed: any unexpected error → would_be_refunded = False (NO).
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Optional

ENGINE_VERSION = "wb-flow12-v9"  # v9: type-keyword routing + one-time-collapse cut AMBIGUOUS_FLOW (Anna)

REFUND_INTENTS = ("REFUND_REQUEST", "SUB_RENEWAL_REFUND")

# ── Reason codes ─────────────────────────────────────────────────────────── #
RC_WOULD_BE_REFUNDED  = "WOULD_BE_REFUNDED"    # YES — latest sub renewal within window
RC_UNABLE_TO_EVAL     = "UNABLE_TO_EVAL"       # Nexus not available
RC_OUT_OF_SCOPE       = "OUT_OF_SCOPE"         # not a refund intent
RC_LOW_CONFIDENCE     = "LOW_CONFIDENCE"       # classifier below threshold
RC_NOT_FOUND_IN_NEXUS = "NOT_FOUND_IN_NEXUS"   # no charges for this customer
RC_NOTHING_REFUNDABLE = "NOTHING_REFUNDABLE"   # charges exist but none refundable
RC_ONE_TIME_OUT_OF_SCOPE = "ONE_TIME_OUT_OF_SCOPE"  # IQ Test fee (first_sale) — flow #3 pending
RC_OUTSIDE_REFUND_WINDOW = "OUTSIDE_REFUND_WINDOW"  # last payment older than the country window
RC_WINDOW_UNKNOWN     = "WINDOW_UNKNOWN"       # missing charge/ticket date → can't compute window
RC_REPORT_NOT_REFUNDABLE = "REPORT_NOT_REFUNDABLE_PER_TOS"  # flow #2 — IQ Test Report is not refunded
RC_AMBIGUOUS_FLOW     = "AMBIGUOUS_FLOW"       # can't tell which charge/flow the customer means → human
RC_EVAL_ERROR         = "EVAL_ERROR"           # fail-closed on unexpected error

# ── Country → refund window (days). Only entries differing from the 14-day
# default really matter; everything else falls through to DEFAULT_WINDOW. ──── #
DEFAULT_WINDOW = 14
_COUNTRY_WINDOW = {
    "JAPAN": 8, "JP": 8,
    "KOREA": 7, "SOUTH KOREA": 7, "KR": 7,
    "VIETNAM": 7, "VN": 7,
    "HONG KONG": 7, "HK": 7,
    "INDONESIA": 7, "ID": 7,
    "TAIWAN": 7, "TW": 7,
    "SAUDI ARABIA": 7, "SA": 7,
    "TURKEY": 14, "TR": 14,
    "BRAZIL": 10, "BR": 10, "MEXICO": 10, "MX": 10, "ARGENTINA": 10, "AR": 10,
    "CHILE": 10, "CL": 10, "COLOMBIA": 10, "CO": 10, "PERU": 10, "PE": 10,
    "URUGUAY": 10, "UY": 10, "ECUADOR": 10, "EC": 10, "GUATEMALA": 10, "GT": 10,
    "USA": 14, "US": 14, "UNITED STATES": 14, "CANADA": 14, "CA": 14,
    "UK": 14, "UNITED KINGDOM": 14, "GB": 14, "AUSTRALIA": 14, "AU": 14,
    "NEW ZEALAND": 14, "NZ": 14,
}
# Charge-currency proxy for country when billing country is unknown (these brands
# charge in local currency, so currency ≈ country). Already present in Nexus charges,
# so this needs no extra call. Only short-window currencies matter; else → default 14.
_CURRENCY_WINDOW = {
    "JPY": 8,
    "KRW": 7, "VND": 7, "IDR": 7, "TWD": 7, "HKD": 7, "SAR": 7,
    "TRY": 14,
    "BRL": 10, "MXN": 10, "ARS": 10, "CLP": 10, "COP": 10, "PEN": 10, "UYU": 10, "GTQ": 10,
}
# Language proxy (last resort before default): reliably-mappable short-window markets.
_LANG_WINDOW = {"JP": 8, "KR": 7, "VI": 7, "ID": 7}


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
    country: str = ""                    # billing country if known (else language proxy)
    as_of_date: Optional[str] = None     # ISO date to measure the window from (ticket created)
    nexus_available: bool = False
    nexus_data: Optional[dict] = None     # includes "charges", "subscription_id", "source"
    ticket_text: str = ""                 # subject + body — for informational amount logging


@dataclass
class RefundDecision:
    would_be_refunded: bool
    reason_code: str
    human_message: str
    guard_trail: list = field(default_factory=list)
    computed_amount: Optional[str] = None       # candidate (latest sub) charge amount
    currency: Optional[str] = None
    customer_stated_amount: Optional[str] = None  # informational (audit), never used to pick
    customer_stated_amounts: Optional[str] = None
    candidate_charge_id: Optional[str] = None
    charge_type: Optional[str] = None
    candidate_charges: Optional[str] = None     # relevant charges "id:amount:date;…"
    refund_flow: Optional[str] = None           # flow1_subscription / flow2_report / flow3_pending
    source: Optional[str] = None
    engine_version: str = ENGINE_VERSION


# ── Pure helpers ─────────────────────────────────────────────────────────── #

def _to_decimal(raw) -> Optional[Decimal]:
    """Locale-aware numeric parse (US "1,234.56"/"9.99" and EU "1.234,56"/"9,99"),
    NFKC-folding full-width digits/punctuation first."""
    if raw is None:
        return None
    s = unicodedata.normalize("NFKC", str(raw)).strip().replace(" ", "").replace(" ", "")
    if not s:
        return None
    has_dot, has_comma = "." in s, "," in s
    if has_dot and has_comma:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif has_comma:
        parts = s.split(",")
        s = s.replace(",", ".") if (len(parts) == 2 and len(parts[1]) == 2) else s.replace(",", "")
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


_CUR = (
    r"(?:¥|￥|\$|€|£|₩|₺|₹|₴|₪|₽|zł|Kč|円|圆|元|원|"
    r"USD|JPY|EUR|GBP|KRW|IDR|THB|VND|PHP|MXN|BRL|TRY|TL|INR|PLN|UAH|ILS|CHF|RUB|"
    r"dollars?|yen|won|lira|евро|долл?|грн)"
)
_NUM = r"\d[\d.,]*\d|\d"
_STATED_RE = re.compile(rf"(?:{_CUR}\s*({_NUM}))|(?:({_NUM})\s*{_CUR})", re.IGNORECASE)


def parse_stated_amounts(text: str) -> list[Decimal]:
    """Distinct amounts the customer states (currency-anchored). INFORMATIONAL only."""
    if not text:
        return []
    text = unicodedata.normalize("NFKC", text)
    out, seen = [], set()
    for m in _STATED_RE.finditer(text):
        d = _to_decimal(m.group(1) or m.group(2))
        if d is not None and d > 0 and d not in seen:
            seen.add(d)
            out.append(d)
    return out


def _dedup_charges(charges: list) -> list:
    """Drop duplicate charge objects sharing the same charge_id (Nexus can return the
    same charge twice)."""
    out, seen = [], set()
    for ch in charges:
        cid = ch.get("charge_id")
        if cid and cid in seen:
            continue
        if cid:
            seen.add(cid)
        out.append(ch)
    return out


def _charge_amount(charge: dict) -> Optional[Decimal]:
    return _to_decimal(charge.get("amount"))


def _is_refundable(charge: dict) -> bool:
    if "refundable" in charge:
        return charge.get("refundable") is True
    return str(charge.get("status", "")).lower() == "success"


def _is_subscription(charge: dict) -> bool:
    return str(charge.get("type", "")).lower() == "subscription"


def _charge_date(charge: dict) -> Optional[date]:
    return _parse_iso_date(charge.get("date"))


def _parse_iso_date(s) -> Optional[date]:
    m = re.search(r"(\d{4})-(\d{1,2})-(\d{1,2})", str(s or ""))
    if not m:
        return None
    try:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


# ── Date-based routing (Anna's rule) ─────────────────────────────────────── #
# Customers rarely name WHICH charge they want back; humans disambiguate by the
# DATE they mention ("charged on 24.07" / "the payment that same day"). We parse
# a date from the ticket, match it to a charge, and route to that charge's flow.

# Date like "2026年7月21日", "2026/7/21", "2026-07-21", "7/21", "7月21日", "21.07.2026".
_DATE_RE = re.compile(r"(?:(\d{4})[年/\-.])?(\d{1,2})[月/\-.](\d{1,2})日?")

# "same day / today" markers across the bot's main languages → the ticket date.
_SAME_DAY_MARKERS = (
    "same day", "same-day", "that day", "today",          # EN
    "selben tag", "gleichen tag", "heute",                # DE
    "même jour", "meme jour", "aujourd'hui", "aujourdhui",  # FR
    "mismo día", "mismo dia", "hoy",                       # ES
    "mesmo dia", "hoje",                                   # PT
    "aynı gün", "ayni gun", "bugün", "bugun",              # TR
    "同じ日", "本日", "今日",                               # JA
    "той самий день", "того ж дня", "сьогодні",            # UK
    "тот же день", "того же дня", "сегодня",               # RU
)


def parse_stated_dates(text: str):
    """Candidate dates the customer states → list of (year|None, month, day).
    Emits BOTH JP/ISO (M/D) and EU (D/M) interpretations so matching against the
    real charge date resolves the order. Full-width safe (NFKC)."""
    if not text:
        return []
    text = unicodedata.normalize("NFKC", text)
    out = []
    for m in _DATE_RE.finditer(text):
        y = int(m.group(1)) if m.group(1) else None
        a, b = int(m.group(2)), int(m.group(3))
        if 1 <= a <= 12 and 1 <= b <= 31:               # (month=a, day=b) — JP / ISO order
            out.append((y, a, b))
        if 1 <= b <= 12 and 1 <= a <= 31 and a != b:    # (month=b, day=a) — EU order
            out.append((y, b, a))
    return out


def _mentions_same_day(text: str) -> bool:
    if not text:
        return False
    t = unicodedata.normalize("NFKC", text).lower()
    return any(mk in t for mk in _SAME_DAY_MARKERS)


def _charge_ymd(charge: dict):
    m = re.search(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})", str(charge.get("date") or ""))
    return (int(m.group(1)), int(m.group(2)), int(m.group(3))) if m else None


def _charge_matches_date(charge: dict, dates) -> bool:
    ymd = _charge_ymd(charge)
    if not ymd:
        return False
    cy, cm, cd = ymd
    return any(mo == cm and d == cd and (y is None or y == cy) for (y, mo, d) in dates)


def _type_group(charge: dict) -> Optional[str]:
    """Charge type → the routing group label ('subscription'/'report'/'first_sale')."""
    t = str(charge.get("type", "")).lower()
    return {"subscription": "subscription", "cross_sale": "report",
            "first_sale": "first_sale"}.get(t)


def _route_by_date(ctx: "RefundContext", refundable: list) -> Optional[str]:
    """Anna's rule: when the amount can't tell which flow, match a stated (or
    'same-day') date to a charge. Returns the target group iff every date-matched
    refundable charge is the SAME type (one flow); else None (stays ambiguous)."""
    dates = list(parse_stated_dates(ctx.ticket_text))
    if _mentions_same_day(ctx.ticket_text):
        adate = _parse_iso_date(ctx.as_of_date)
        if adate:
            dates.append((adate.year, adate.month, adate.day))
    if not dates:
        return None
    matched = [c for c in refundable if _charge_matches_date(c, dates)]
    types = {g for g in (_type_group(c) for c in matched) if g}
    return next(iter(types)) if len(types) == 1 else None


# ── Type-keyword routing (Anna's rule, words) ────────────────────────────── #
# Customers frequently NAME the charge type in words ("I didn't sign up for the
# subscription", "サブスク", "recurring charge", "my report"). When the amount
# and date can't disambiguate, the word they used can. Only "subscription" and
# "report" are matched — a bare "test" is far too noisy (the product IS an IQ
# test), so first_sale is never keyword-routed.
_TYPE_KEYWORDS = {
    "subscription": (
        "subscription", "subscrib", "recurring", "recurrent", "monthly", "renewal",
        "renew", "membership", "sign up", "signed up", "signup", "signing up",
        "abonnement", "abonelik", "abonament", "suscrip", "assinatura", "mensual",
        "mensal", "maandelijks", "terugkerend", "abbonamento",
        "подписк", "підписк", "サブスク", "定期", "月額", "毎月", "구독", "정기", "langganan",
    ),
    "report": (
        "full report", "results report", "the report", "my report", "test report",
        "レポート", "診断結果", "検査結果", "informe", "relatório", "rapport", "bericht",
        "отчёт", "отчет", "звіт", "보고서",
    ),
}


def _route_by_type_keyword(ctx: "RefundContext", groups) -> Optional[str]:
    """Route to the group whose type-word the customer used, iff exactly ONE
    group's keywords hit AND that group has refundable charges present. If both
    'subscription' and 'report' words appear → not resolvable here → None."""
    text = unicodedata.normalize("NFKC", ctx.ticket_text or "").lower()
    if not text:
        return None
    present = {t for t, chs in groups if chs}
    hit = [t for t, kws in _TYPE_KEYWORDS.items()
           if t in present and any(k in text for k in kws)]
    return hit[0] if len(hit) == 1 else None


def window_for(country: str, currency: str, language: str) -> tuple[int, str]:
    """Refund window (days) + source. Priority: explicit billing country → charge
    currency proxy → language proxy → default 14."""
    c = (country or "").strip().upper()
    if c in _COUNTRY_WINDOW:
        return _COUNTRY_WINDOW[c], "country"
    cur = (currency or "").strip().upper()
    if cur in _CURRENCY_WINDOW:
        return _CURRENCY_WINDOW[cur], "currency"
    lang = (language or "").strip().upper()
    if lang in _LANG_WINDOW:
        return _LANG_WINDOW[lang], "language"
    return DEFAULT_WINDOW, "default"


def _charges_summary(charge_list: list) -> str:
    parts = []
    for ch in charge_list:
        d = str(ch.get("date") or "")[:10]
        parts.append(f"{ch.get('charge_id')}:{ch.get('amount')}:{d}")
    return ";".join(parts)


def _decision(would: bool, code: str, msg: str, trail: list, **kw) -> RefundDecision:
    return RefundDecision(
        would_be_refunded=would, reason_code=code, human_message=msg,
        guard_trail=trail + [code], **kw,
    )


# ── The pipeline (Figma flow #1) ─────────────────────────────────────────── #

def decide(ctx: RefundContext, cfg: RefundConfig) -> RefundDecision:
    """Compute the would-be refund decision for the subscription-fee flow. PURE;
    never raises (fail-closed to NO)."""
    trail: list = []
    try:
        nexus = ctx.nexus_data if isinstance(ctx.nexus_data, dict) else {}
        charges = _dedup_charges(nexus.get("charges") if isinstance(nexus.get("charges"), list) else [])
        stated = parse_stated_amounts(ctx.ticket_text)
        common = dict(
            source=nexus.get("source") or None,
            customer_stated_amount=(str(stated[0]) if stated else None),
            customer_stated_amounts=(",".join(str(a) for a in stated) if stated else None),
            engine_version=cfg.engine_version,
        )

        # 0. Nexus available?
        trail.append("nexus_available")
        if not ctx.nexus_available:
            return _decision(False, RC_UNABLE_TO_EVAL,
                             "Nexus lookup unavailable — cannot evaluate refund.", trail, **common)

        # 1. Scope — refund intent.
        trail.append("scope")
        if ctx.intent not in REFUND_INTENTS:
            return _decision(False, RC_OUT_OF_SCOPE,
                             f"Intent {ctx.intent} is not a refund request.", trail, **common)

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

        # 3. Found in Nexus (charges present)?
        trail.append("found")
        if not charges:
            return _decision(False, RC_NOT_FOUND_IN_NEXUS,
                             "No charges found in Nexus for this customer.", trail, **common)

        # 4. Refundable charges + route to the right flow (A: by stated amount → C: else human).
        trail.append("refundable")
        refundable = [c for c in charges if _is_refundable(c)]
        if not refundable:
            return _decision(False, RC_NOTHING_REFUNDABLE,
                             "Charges exist but none are refundable (already refunded).",
                             trail, candidate_charges=_charges_summary(charges), **common)
        subs    = [c for c in refundable if _is_subscription(c)]
        reports = [c for c in refundable if str(c.get("type", "")).lower() == "cross_sale"]
        firsts  = [c for c in refundable if str(c.get("type", "")).lower() == "first_sale"]

        stated_set = set(parse_stated_amounts(ctx.ticket_text))
        _groups = (("subscription", subs), ("report", reports), ("first_sale", firsts))
        present = [t for t, chs in _groups if chs]
        by_amount = [t for t, chs in _groups if chs and any(_charge_amount(c) in stated_set for c in chs)]
        target_type = None
        if len(by_amount) == 1:                       # A: stated amount picks one flow
            target_type = by_amount[0]
        elif len(by_amount) == 0 and len(present) == 1:  # B: only one refundable type present
            target_type = present[0]

        if target_type is None:                       # C: date ("same day" / "on <date>")
            dated = _route_by_date(ctx, refundable)
            if dated is not None:
                target_type = dated
                trail.append("date_routed")

        if target_type is None:                       # D: explicit charge-type word in the text
            typed = _route_by_type_keyword(ctx, _groups)
            if typed is not None:
                target_type = typed
                trail.append("type_routed")

        if target_type is None and not subs:          # E: no subscription → only one-time charges
            # first_sale (IQ Test fee) and cross_sale (Report) are never refundable, so
            # even without an amount/date/word the money outcome is a definite NO — decline
            # via the report flow (per-ToS NO) if a report is present, else the one-time flow.
            target_type = "report" if reports else "first_sale"
            trail.append("one_time_collapsed")

        if target_type is None:                       # F: still ambiguous → human decides
            return _decision(False, RC_AMBIGUOUS_FLOW,
                             "Multiple refundable charge types, no amount/date/type-word "
                             "to disambiguate — human decides.",
                             trail, candidate_charges=_charges_summary(refundable), **common)
        trail.append(f"route:{target_type}")

        # ── Flow #2 — IQ Test Report (cross_sale): NON-refundable per Terms of Use ──
        if target_type == "report":
            rep = max(reports, key=lambda c: (_charge_date(c) or date.min))
            note = " (also cancel active subscription)" if any(_is_subscription(c) for c in charges) else ""
            return _decision(False, RC_REPORT_NOT_REFUNDABLE,
                             f"IQ Test Report fee is non-refundable per Terms of Use{note}.",
                             trail, refund_flow="flow2_report",
                             candidate_charge_id=rep.get("charge_id"), charge_type="cross_sale",
                             candidate_charges=_charges_summary(reports), **common)

        # ── Flow #3 — IQ Test fee (first_sale): not designed yet → human ──
        if target_type == "first_sale":
            return _decision(False, RC_ONE_TIME_OUT_OF_SCOPE,
                             "IQ Test fee (first_sale) — refund flow not implemented yet; human handles.",
                             trail, refund_flow="flow3_pending",
                             candidate_charges=_charges_summary(firsts), **common)

        # ── Flow #1 — Subscription fee: latest renewal + country window ──
        trail.append("latest_subscription")
        target = max(subs, key=lambda c: (_charge_date(c) or date.min))
        cdate, adate = _charge_date(target), _parse_iso_date(ctx.as_of_date)
        if cdate is None or adate is None:
            return _decision(False, RC_WINDOW_UNKNOWN,
                             "Cannot determine charge/ticket date to check the refund window.",
                             trail, refund_flow="flow1_subscription",
                             candidate_charge_id=target.get("charge_id"),
                             candidate_charges=_charges_summary(subs), **common)
        amt, cur = _charge_amount(target), target.get("currency")
        window, wsrc = window_for(ctx.country, cur, ctx.language)
        days = (adate - cdate).days
        trail.append(f"window[{wsrc}={window}d,age={days}d]")
        base = dict(
            refund_flow="flow1_subscription",
            computed_amount=(str(amt) if amt is not None else None),
            currency=(str(cur) if cur else None),
            candidate_charge_id=target.get("charge_id"),
            charge_type=target.get("type"),
        )
        if days > window:
            return _decision(False, RC_OUTSIDE_REFUND_WINDOW,
                             f"Last subscription payment is {days}d old > {window}d window "
                             f"({wsrc}) — previous months are non-refundable.",
                             trail, candidate_charges=_charges_summary(subs), **base, **common)

        trail.append("would_be")
        return _decision(
            True, RC_WOULD_BE_REFUNDED,
            f"Would be refunded (Nexus-only, disputes NOT checked): latest subscription "
            f"{amt}{(' ' + str(cur)) if cur else ''} charge {target.get('charge_id')} "
            f"({days}d ago, within {window}d {wsrc} window).",
            trail, **base, **common,
        )

    except Exception as e:  # noqa: BLE001 — fail-closed on ANYTHING
        return RefundDecision(
            would_be_refunded=False, reason_code=RC_EVAL_ERROR,
            human_message=f"Refund eval error (fail-closed to NO): {e}",
            guard_trail=trail + [RC_EVAL_ERROR], engine_version=cfg.engine_version,
        )
