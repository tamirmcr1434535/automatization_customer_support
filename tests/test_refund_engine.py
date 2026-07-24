"""
Smoke tests for refund_engine (AN-192, Flow #1: subscription-fee refund) — PURE.

Exercises the would-be decision: target = the LATEST refundable subscription renewal,
refundable only if within the country's refund window (measured to the ticket date).
The engine has no I/O and never moves money.
"""

from decimal import Decimal
from unittest.mock import patch

import refund_engine as re_
from refund_engine import RefundConfig, RefundContext, decide


CFG = RefundConfig(min_confidence=0.90)


def _sub(cid, amount, dt, refundable=True, status="success"):
    return {"charge_id": cid, "amount": amount, "currency": "JPY", "type": "subscription",
            "status": status, "refundable": refundable, "date": dt}


def _ctx(**kw):
    base = dict(
        intent="REFUND_REQUEST", confidence=0.95, language="EN", country="",
        as_of_date="2026-07-20T00:00:00Z", nexus_available=True,
        nexus_data={"subscription_id": "1", "source": "stripe",
                    "charges": [_sub("ch_sub", 5490, "2026-07-18T09:00:00Z")]},
        ticket_text="please refund",
    )
    base.update(kw)
    return RefundContext(**base)


def _data(charges):
    return {"subscription_id": "1", "source": "stripe", "charges": charges}


def test_unable_to_eval_when_nexus_off():
    d = decide(_ctx(nexus_available=False, nexus_data=None), CFG)
    assert d.would_be_refunded is False and d.reason_code == re_.RC_UNABLE_TO_EVAL


def test_out_of_scope():
    d = decide(_ctx(intent="TRIAL_CANCELLATION"), CFG)
    assert d.reason_code == re_.RC_OUT_OF_SCOPE


def test_low_confidence():
    d = decide(_ctx(confidence=0.80), CFG)
    assert d.reason_code == re_.RC_LOW_CONFIDENCE


def test_not_found_no_charges():
    d = decide(_ctx(nexus_data=_data([])), CFG)
    assert d.reason_code == re_.RC_NOT_FOUND_IN_NEXUS


def test_nothing_refundable():
    d = decide(_ctx(nexus_data=_data([_sub("ch1", 5490, "2026-07-18", refundable=False, status="refunded")])), CFG)
    assert d.reason_code == re_.RC_NOTHING_REFUNDABLE


def test_one_time_only_out_of_scope():
    charges = [{"charge_id": "ch_f", "amount": 199, "currency": "JPY", "type": "first_sale",
                "status": "success", "refundable": True, "date": "2026-07-18"},
               {"charge_id": "ch_c", "amount": 990, "currency": "JPY", "type": "cross_sale",
                "status": "success", "refundable": True, "date": "2026-07-18"}]
    d = decide(_ctx(nexus_data=_data(charges)), CFG)
    assert d.would_be_refunded is False and d.reason_code == re_.RC_ONE_TIME_OUT_OF_SCOPE


def test_would_be_refunded_within_window():
    # sub charged 2 days before ticket; EN default window 14 → within → YES.
    d = decide(_ctx(), CFG)
    assert d.would_be_refunded is True
    assert d.reason_code == re_.RC_WOULD_BE_REFUNDED
    assert d.candidate_charge_id == "ch_sub" and d.computed_amount == "5490"


def test_outside_refund_window():
    d = decide(_ctx(nexus_data=_data([_sub("ch_old", 5490, "2026-06-20")])), CFG)  # 30 days
    assert d.would_be_refunded is False and d.reason_code == re_.RC_OUTSIDE_REFUND_WINDOW


def test_picks_latest_subscription():
    charges = [_sub("ch_old", 5490, "2026-07-01"), _sub("ch_new", 5490, "2026-07-18")]
    d = decide(_ctx(nexus_data=_data(charges)), CFG)
    assert d.would_be_refunded is True and d.candidate_charge_id == "ch_new"


def test_japan_window_8_days():
    # language JP → 8-day window.
    within = decide(_ctx(language="JP", nexus_data=_data([_sub("ch", 5490, "2026-07-15")])), CFG)  # 5d
    outside = decide(_ctx(language="JP", nexus_data=_data([_sub("ch", 5490, "2026-07-08")])), CFG)  # 12d
    assert within.would_be_refunded is True
    assert outside.reason_code == re_.RC_OUTSIDE_REFUND_WINDOW


def test_country_overrides_language():
    # Korea window 7; sub 10d old → outside even though language default would be 14.
    d = decide(_ctx(country="Korea", language="EN",
                    nexus_data=_data([_sub("ch", 5490, "2026-07-10")])), CFG)  # 10d
    assert d.reason_code == re_.RC_OUTSIDE_REFUND_WINDOW


def test_window_unknown_when_dates_missing():
    d = decide(_ctx(nexus_data=_data([_sub("ch", 5490, "")])), CFG)
    assert d.reason_code == re_.RC_WINDOW_UNKNOWN


def test_stated_amount_is_informational_only():
    # Customer names 9,99€ (EU) but target is still the latest subscription by rule.
    d = decide(_ctx(ticket_text="I was charged 9,99€, refund"), CFG)
    assert d.would_be_refunded is True and d.candidate_charge_id == "ch_sub"
    assert d.customer_stated_amount == "9.99"  # logged, not used to pick


def test_guard_exception_fails_closed():
    with patch.object(re_, "_is_subscription", side_effect=RuntimeError("boom")):
        d = decide(_ctx(), CFG)
    assert d.would_be_refunded is False and d.reason_code == re_.RC_EVAL_ERROR


# ── Pure helper unit tests ────────────────────────────────────────────────── #

def test_window_for():
    assert re_.window_for("JP", "EN") == (8, "country")
    assert re_.window_for("Turkey", "EN") == (14, "country")
    assert re_.window_for("", "JP") == (8, "language")
    assert re_.window_for("", "EN") == (14, "default")


def test_number_parsing_locales():
    assert re_._to_decimal("9,99") == Decimal("9.99")      # EU decimal
    assert re_._to_decimal("1,990") == Decimal("1990")     # comma thousands
    assert re_._to_decimal("1.234,56") == Decimal("1234.56")
    assert re_._to_decimal("１，９９０") == Decimal("1990")   # full-width + comma


def test_parse_stated_amounts_currency_anchored():
    assert re_.parse_stated_amounts("charged $30.99") == [Decimal("30.99")]
    assert re_.parse_stated_amounts("5490円と199円") == [Decimal("5490"), Decimal("199")]
    assert re_.parse_stated_amounts("respond within 24 hours") == []  # no currency anchor
