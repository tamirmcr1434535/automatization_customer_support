"""The amount a customer types almost never equals the amount we charged.

`by_amount` (refund_engine route A) and the STATED_AMOUNT_MISMATCH guard both
compared `_charge_amount(c) in stated_set` — exact Decimal equality — and the
regex that finds the number threw the currency anchor away. Two consequences,
both measured over 2026-08-15..09-03:

  • 241 tickets were escalated as STATED_AMOUNT_MISMATCH. Replaying the real
    logged amounts against the real logged charges, 120 of them match on
    tolerance and major/minor scale alone; of the 71 the simulation still could
    not match, 53 were the customer quoting the charge in a DIFFERENT currency
    ("$11" against a ¥5,490 renewal) — a unit mismatch read as evidence that
    they meant an account we never found.

  • Worse, and less visible: 130 of the 421 soft-routed tickets — the ones
    Guard 2b then suppresses — had already named an amount. They fell out of
    route A on the same exact-equality check, then fell through date and type
    routing into the heuristic/LLM route, and were suppressed for being
    heuristically routed. The customer had told us which charge they meant.

The founding case of the mismatch guard (#175712) must keep firing, so the
tolerance is deliberately narrow: ±5% covers a foreign-card FX markup (3-8%)
plus rounding and a transposed digit, and the scale set is x100/÷100 only.
x1000 starts matching the real prices on one account against each other.
"""
from decimal import Decimal

import main
import refund_engine as re_


def _ctx(charges, text, as_of="2026-08-20", country="Japan"):
    return re_.RefundContext(
        intent="REFUND_REQUEST", confidence=1.0, language="EN", country=country,
        as_of_date=as_of, nexus_available=True,
        nexus_data={"charges": charges}, nexus_lookup_failed=False,
        ticket_text=text,
    )


def _charge(amount, cur, ctype, d, cid="ch_1"):
    return {"charge_id": cid, "amount": amount, "currency": cur,
            "date": d, "type": ctype, "status": "success"}


# ── Route A: the amount the customer typed now picks the charge ──────────── #

def _routes_hard_to_subscription(charges, text, country="Japan"):
    d = re_.decide(_ctx(charges, text, country=country), main.REFUND_CONFIG)
    trail = d.guard_trail or []
    assert "route:subscription" in trail, trail
    # Hard-routed by amount — not by a later, weaker signal that Guard 2b then
    # suppresses. This is the whole point of the change.
    for weak in ("date_routed", "type_routed", "dispute_target_subscription",
                 "llm_disambiguated", "one_time_collapsed"):
        assert weak not in trail, f"{weak} in {trail}"
    return d


def test_rounded_up_euro_routes_to_the_subscription():
    charges = [_charge("29.99", "EUR", "renewal", "2026-08-19", "ch_sub"),
               _charge("9.99", "EUR", "cross_sale", "2026-08-12", "ch_rep")]
    _routes_hard_to_subscription(charges, "I want the 30 euro payment back", "Germany")


def test_german_decimal_comma_typo_routes_to_the_subscription():
    charges = [_charge("29.99", "EUR", "renewal", "2026-08-19", "ch_sub"),
               _charge("1.90", "EUR", "first_sale", "2026-08-12", "ch_fee")]
    _routes_hard_to_subscription(charges, "Es wurden 29,90 EUR abgebucht", "Germany")


def test_transposed_won_digits_route_to_the_subscription():
    charges = [_charge("39990", "KRW", "renewal", "2026-08-18", "ch_sub"),
               _charge("19990", "KRW", "cross_sale", "2026-08-11", "ch_rep")]
    _routes_hard_to_subscription(charges, "39,900원이 결제되었습니다", "Korea")


def test_bank_fx_markup_routes_to_the_subscription():
    # Customer reads 5,900 off a statement that added an FX fee to a 5,490 charge.
    charges = [_charge("5490", "JPY", "renewal", "2026-08-18", "ch_sub"),
               _charge("1990", "JPY", "cross_sale", "2026-08-11", "ch_rep")]
    _routes_hard_to_subscription(charges, "5,900円が引き落とされました")


def test_minor_unit_confusion_routes_to_the_subscription():
    # "299.990" for a 299990 charge — a thousands separator read as a decimal
    # point. Covered by the ÷100 scale from the other direction: 2999.90 typed
    # against a 299990 charge is the same confusion.
    charges = [_charge("2999", "USD", "renewal", "2026-08-18", "ch_sub"),
               _charge("99", "USD", "cross_sale", "2026-08-11", "ch_rep")]
    _routes_hard_to_subscription(charges, "charged $29.99 for the subscription",
                                 country="United States")


# ── The mismatch guard: narrower, not weaker ─────────────────────────────── #

def test_amount_in_another_currency_is_not_evidence_of_a_wrong_account():
    # "$11" against ¥5,490 is the same money in another unit. Before, this was
    # STATED_AMOUNT_MISMATCH and a human had to look at it.
    charges = [_charge("5490", "JPY", "renewal", "2026-08-18", "ch_sub"),
               _charge("1990", "JPY", "cross_sale", "2026-08-11", "ch_rep")]
    d = re_.decide(_ctx(charges, "my card shows $11 taken for a subscription"),
                   main.REFUND_CONFIG)
    assert d.reason_code != re_.RC_STATED_AMOUNT_MISMATCH


def test_genuine_same_currency_mismatch_still_escalates():
    # Named 1990 JPY; the account holds 5490 and 199 JPY. Same currency, no
    # tolerance or scale reaches it → still a human's problem.
    charges = [_charge("5490", "JPY", "renewal", "2026-08-18", "ch_sub"),
               _charge("199", "JPY", "first_sale", "2026-08-11", "ch_fee")]
    d = re_.decide(_ctx(charges, "please refund the 1990 yen charge"), main.REFUND_CONFIG)
    assert d.reason_code == re_.RC_STATED_AMOUNT_MISMATCH


def test_tolerance_stops_well_short_of_the_next_real_price():
    ch5490 = _charge("5490", "JPY", "renewal", "2026-08-18")
    assert re_._amount_matches(Decimal("5490"), None, ch5490)
    assert re_._amount_matches(Decimal("5700"), None, ch5490)       # +3.8%, rounding
    assert re_._amount_matches(Decimal("5900"), None, ch5490)       # +7.5%, FX markup
    assert re_._amount_matches(Decimal("5940"), None, ch5490)       # +8.2%, FX markup
    assert not re_._amount_matches(Decimal("6100"), None, ch5490)   # +11.1% — out
    assert not re_._amount_matches(Decimal("1990"), None, ch5490)   # a different price
    assert not re_._amount_matches(Decimal("199"), None, ch5490)    # #175712

    # The band is nowhere near the distance between two real prices on one
    # account: across the 240 logged accounts holding 2+ distinct refundable
    # amounts, the closest pair is 45% apart at the 1st percentile.
    assert not re_._amount_matches(Decimal("19990"), None,
                                   _charge("39990", "KRW", "renewal", "2026-08-18"))


def test_scale_variants_are_x100_only_never_x1000():
    ch = _charge("1990", "JPY", "cross_sale", "2026-08-11")
    assert re_._amount_matches(Decimal("19.90"), None, ch)          # x100
    assert not re_._amount_matches(Decimal("1.99"), None, ch)       # x1000 — refused
    assert not re_._amount_matches(Decimal("199"), None, ch)        # the real other price


def test_an_amount_reaching_two_groups_yields_no_route_not_a_wrong_one():
    # Route A demands exactly one group hit. This is what actually makes a wide
    # tolerance safe: on the one logged account whose two prices sit 0.7% apart,
    # the amount matches both groups and the engine falls through to the later
    # routes instead of confidently answering about one of them.
    charges = [_charge("5490", "JPY", "renewal", "2026-08-18", "ch_sub"),
               _charge("5530", "JPY", "cross_sale", "2026-08-11", "ch_rep")]
    d = re_.decide(_ctx(charges, "please refund the 5,490 yen"), main.REFUND_CONFIG)
    trail = d.guard_trail or []
    assert "route:subscription" not in trail or "type_routed" in trail, trail
    assert d.reason_code != re_.RC_STATED_AMOUNT_MISMATCH


# ── Parser contract ─────────────────────────────────────────────────────── #

def test_parser_keeps_the_currency_it_matched():
    pairs = re_.parse_stated_amounts_with_currency("charged $30.99 and 5490円")
    assert pairs[0][0] == Decimal("30.99") and "USD" in pairs[0][1]
    assert pairs[1][0] == Decimal("5490") and pairs[1][1] == frozenset({"JPY"})


def test_ambiguous_dollar_sign_is_still_not_a_yen():
    usd = re_._currency_codes("$")
    assert "USD" in usd and "JPY" not in usd
    assert not re_._currencies_comparable(usd, frozenset({"JPY"}))
    assert re_._currencies_comparable(usd, frozenset({"USD"}))


def test_unknown_currency_stays_comparable_with_everything():
    # An unrecognised or missing anchor must never become a silent way to skip
    # the mismatch check.
    assert re_._currencies_comparable(None, frozenset({"JPY"}))
    assert re_._currencies_comparable(frozenset({"JPY"}), None)
    assert re_._charge_currency_codes({"amount": "1", "currency": ""}) is None


def test_legacy_parser_contract_unchanged():
    # main.py's OCR gate and the log fields still call this and expect Decimals.
    assert re_.parse_stated_amounts("charged $30.99") == [Decimal("30.99")]
    assert re_.parse_stated_amounts("5490円と199円") == [Decimal("5490"), Decimal("199")]
    assert re_.parse_stated_amounts("respond within 24 hours") == []
    # Same number under two anchors is one amount for the legacy list, two pairs
    # for the currency-aware one.
    assert re_.parse_stated_amounts("$30 or 30 euro") == [Decimal("30")]
    assert len(re_.parse_stated_amounts_with_currency("$30 or 30 euro")) == 2
