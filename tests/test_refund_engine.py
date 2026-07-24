"""
Smoke tests for refund_engine (AN-192) — PURE, no mocks needed.

Exercises the would-be refund decision engine directly: it matches the amount
the customer states in the ticket against the real Nexus `charges[]`, and never
moves money (it has no I/O).
"""

from decimal import Decimal
from unittest.mock import patch

import refund_engine as re_
from refund_engine import RefundConfig, RefundContext, decide


CFG = RefundConfig(min_confidence=0.90)


def _charges():
    return [
        {"charge_id": "ch_A", "amount": 30.99, "currency": "USD",
         "type": "subscription", "status": "success", "refundable": True},
        {"charge_id": "ch_B", "amount": 9.99, "currency": "USD",
         "type": "cross_sale", "status": "success", "refundable": True},
        {"charge_id": "ch_C", "amount": 6.99, "currency": "USD",
         "type": "first_sale", "status": "refunded", "refundable": False},
    ]


def _ctx(**kw):
    base = dict(
        intent="REFUND_REQUEST", confidence=0.95, language="EN",
        nexus_available=True,
        nexus_data={"subscription_id": "1", "source": "stripe", "charges": _charges()},
        ticket_text="I was charged $30.99, please refund",
    )
    base.update(kw)
    return RefundContext(**base)


def test_unable_to_eval_when_nexus_off():
    d = decide(_ctx(nexus_available=False, nexus_data=None), CFG)
    assert d.would_be_refunded is False and d.reason_code == re_.RC_UNABLE_TO_EVAL


def test_out_of_scope():
    d = decide(_ctx(intent="TRIAL_CANCELLATION"), CFG)
    assert d.would_be_refunded is False and d.reason_code == re_.RC_OUT_OF_SCOPE


def test_low_confidence():
    d = decide(_ctx(confidence=0.80), CFG)
    assert d.would_be_refunded is False and d.reason_code == re_.RC_LOW_CONFIDENCE


def test_not_found_in_nexus_no_charges():
    d = decide(_ctx(nexus_data={"subscription_id": "1", "charges": []}), CFG)
    assert d.would_be_refunded is False and d.reason_code == re_.RC_NOT_FOUND_IN_NEXUS


def test_nothing_refundable():
    charges = [{"charge_id": "ch_C", "amount": 6.99, "currency": "USD",
                "type": "first_sale", "status": "refunded", "refundable": False}]
    d = decide(_ctx(nexus_data={"subscription_id": "1", "charges": charges},
                    ticket_text="charged $6.99"), CFG)
    assert d.would_be_refunded is False and d.reason_code == re_.RC_NOTHING_REFUNDABLE


def test_amount_not_stated():
    d = decide(_ctx(ticket_text="please refund my payment"), CFG)
    assert d.would_be_refunded is False and d.reason_code == re_.RC_AMOUNT_NOT_STATED


def test_would_be_refunded_yes_single_clean_match():
    d = decide(_ctx(ticket_text="I was charged $30.99"), CFG)
    assert d.would_be_refunded is True
    assert d.reason_code == re_.RC_WOULD_BE_REFUNDED
    assert d.candidate_charge_id == "ch_A"
    assert d.computed_amount == "30.99"
    assert d.charge_type == "subscription"
    assert d.customer_stated_amount == "30.99"


def test_amount_mismatch_goes_to_human():
    d = decide(_ctx(ticket_text="I was charged $500.00"), CFG)
    assert d.would_be_refunded is False and d.reason_code == re_.RC_AMOUNT_MISMATCH


def test_already_refunded_charge_match():
    d = decide(_ctx(ticket_text="refund my $6.99 charge"), CFG)
    assert d.would_be_refunded is False and d.reason_code == re_.RC_ALREADY_REFUNDED


def test_multiple_stated_amounts():
    d = decide(_ctx(ticket_text="charged $30.99 and $9.99, why?"), CFG)
    assert d.would_be_refunded is False
    assert d.reason_code == re_.RC_MULTIPLE_AMOUNTS_STATED
    assert d.customer_stated_amounts == "30.99,9.99"  # all stated amounts logged


def test_european_decimal_comma_matches():
    # "9,99" (EU decimal comma) must parse to 9.99, not 999 → matches ch_B.
    charges = [{"charge_id": "ch_B", "amount": 9.99, "currency": "EUR",
                "type": "cross_sale", "status": "success", "refundable": True}]
    d = decide(_ctx(nexus_data={"subscription_id": "1", "charges": charges},
                    ticket_text="ich wurde 9,99€ berechnet, bitte erstatten"), CFG)
    assert d.would_be_refunded is True
    assert d.computed_amount == "9.99" and d.candidate_charge_id == "ch_B"


def test_turkish_lira_amount_parsed():
    charges = [{"charge_id": "ch_T", "amount": 199, "currency": "TRY",
                "type": "first_sale", "status": "success", "refundable": True}]
    d = decide(_ctx(nexus_data={"subscription_id": "1", "charges": charges},
                    ticket_text="199 TL ödeme iade"), CFG)
    assert d.would_be_refunded is True and d.candidate_charge_id == "ch_T"


def test_multiple_refundable_charges_same_amount_ambiguous():
    charges = [
        {"charge_id": "ch_X", "amount": 9.99, "currency": "USD",
         "type": "cross_sale", "status": "success", "refundable": True},
        {"charge_id": "ch_Y", "amount": 9.99, "currency": "USD",
         "type": "cross_sale", "status": "success", "refundable": True},
    ]
    d = decide(_ctx(nexus_data={"subscription_id": "1", "charges": charges},
                    ticket_text="refund $9.99"), CFG)
    assert d.would_be_refunded is False and d.reason_code == re_.RC_CHARGE_AMBIGUOUS


def _two_same_amount():
    return [
        {"charge_id": "ch_JUL", "amount": 5490, "currency": "JPY", "type": "subscription",
         "status": "success", "refundable": True, "date": "2026-07-21T09:00:00Z"},
        {"charge_id": "ch_AUG", "amount": 5490, "currency": "JPY", "type": "subscription",
         "status": "success", "refundable": True, "date": "2026-08-21T09:00:00Z"},
    ]


def test_date_disambiguates_same_amount_charges():
    d = decide(_ctx(nexus_data={"subscription_id": "1", "charges": _two_same_amount()},
                    ticket_text="7/21付けで5490円請求、返金してください"), CFG)
    assert d.would_be_refunded is True
    assert d.candidate_charge_id == "ch_JUL"       # July charge picked by date
    assert "date_disambiguated" in d.guard_trail


def test_same_amount_no_date_stays_ambiguous():
    d = decide(_ctx(nexus_data={"subscription_id": "1", "charges": _two_same_amount()},
                    ticket_text="5490円を返金してください"), CFG)
    assert d.would_be_refunded is False and d.reason_code == re_.RC_CHARGE_AMBIGUOUS


def test_same_amount_wrong_date_stays_ambiguous():
    d = decide(_ctx(nexus_data={"subscription_id": "1", "charges": _two_same_amount()},
                    ticket_text="9/15付けで5490円"), CFG)  # no charge on 9/15
    assert d.would_be_refunded is False and d.reason_code == re_.RC_CHARGE_AMBIGUOUS


def test_parse_stated_dates():
    assert (None, 7, 21) in re_.parse_stated_dates("7/21付けで5490円")
    assert (None, 7, 21) in re_.parse_stated_dates("7月21日に請求")
    assert (2026, 7, 21) in re_.parse_stated_dates("2026年7月21日")
    assert (None, 7, 21) in re_.parse_stated_dates("７／２１")          # full-width
    assert (None, 7, 21) in re_.parse_stated_dates("charged on 21.07.2026")  # EU D/M order (month+day)


def test_guard_exception_fails_closed():
    with patch.object(re_, "_is_refundable", side_effect=RuntimeError("boom")):
        d = decide(_ctx(), CFG)
    assert d.would_be_refunded is False and d.reason_code == re_.RC_EVAL_ERROR


# ── parse_stated_amounts ──────────────────────────────────────────────────── #

def test_parse_amount_currency_anchored():
    assert re_.parse_stated_amounts("I was charged $30.99") == [Decimal("30.99")]
    assert re_.parse_stated_amounts("5490円 請求") == [Decimal("5490")]
    # Japanese multi-charge letter: dates must NOT be picked up as amounts.
    txt = "7/21付けで5490円請求、7/14付けで199円と1,990円の2口"
    assert re_.parse_stated_amounts(txt) == [Decimal("5490"), Decimal("199"), Decimal("1990")]
    # No currency anchor → nothing (avoid dates / "24 hours").
    assert re_.parse_stated_amounts("respond within 24 hours, ticket 165573") == []


def test_fullwidth_japanese_digits_and_comma():
    # Zenkaku digits + full-width comma must normalise to ASCII (was truncated to 990).
    assert re_.parse_stated_amounts("１，９９０円") == [Decimal("1990")]
    assert re_.parse_stated_amounts("５４９０円請求") == [Decimal("5490")]
    assert re_.parse_stated_amounts("金額は１２，３４５円です") == [Decimal("12345")]


def test_number_parsing_locales():
    # comma decimal (EU) vs comma thousands vs dot decimal
    assert re_._to_decimal("9,99") == Decimal("9.99")     # EU decimal
    assert re_._to_decimal("9.99") == Decimal("9.99")     # US decimal
    assert re_._to_decimal("1,990") == Decimal("1990")    # comma thousands
    assert re_._to_decimal("1.234,56") == Decimal("1234.56")  # EU full
    assert re_._to_decimal("1,234.56") == Decimal("1234.56")  # US full
    assert re_._to_decimal("5490") == Decimal("5490")
