"""
Smoke tests for refund_engine (AN-192) — PURE, no mocks needed.

These exercise the would-be refund decision engine directly. They assert the
engine never moves money (it can't — it has no I/O) and that each guard level
yields the expected would_be_refunded + reason_code.
"""

from unittest.mock import patch

import refund_engine as re_
from refund_engine import RefundConfig, RefundContext, decide


CFG = RefundConfig(min_confidence=0.90)


def _nexus(**kw):
    d = {"subscription_id": 12345, "source": "iqbooster", "amount": "150"}
    d.update(kw)
    return d


def _ctx(**kw):
    base = dict(
        intent="REFUND_REQUEST", confidence=0.95, language="EN",
        nexus_available=True, nexus_data=_nexus(),
    )
    base.update(kw)
    return RefundContext(**base)


def test_unable_to_eval_when_nexus_off():
    d = decide(_ctx(nexus_available=False, nexus_data=None), CFG)
    assert d.would_be_refunded is False
    assert d.reason_code == re_.RC_UNABLE_TO_EVAL


def test_out_of_scope_non_refund_intent():
    d = decide(_ctx(intent="TRIAL_CANCELLATION"), CFG)
    assert d.would_be_refunded is False
    assert d.reason_code == re_.RC_OUT_OF_SCOPE


def test_low_confidence():
    d = decide(_ctx(confidence=0.80), CFG)
    assert d.would_be_refunded is False
    assert d.reason_code == re_.RC_LOW_CONFIDENCE


def test_not_found_in_nexus():
    d = decide(_ctx(nexus_data={"source": "iqbooster"}), CFG)  # no subscription_id
    assert d.would_be_refunded is False
    assert d.reason_code == re_.RC_NOT_FOUND_IN_NEXUS


def test_charge_ambiguous_multiple_subs():
    data = _nexus(subscriptions=[{"id": 1}, {"id": 2}])
    d = decide(_ctx(nexus_data=data), CFG)
    assert d.would_be_refunded is False
    assert d.reason_code == re_.RC_CHARGE_AMBIGUOUS


def test_amount_unavailable():
    # Nexus data without any amount key (the realistic default today).
    data = {"subscription_id": 12345, "source": "iqbooster"}
    d = decide(_ctx(nexus_data=data), CFG)
    assert d.would_be_refunded is False
    assert d.reason_code == re_.RC_AMOUNT_UNAVAILABLE
    assert d.amount_source == "unavailable"


def test_full_pass_would_be_refunded_yes():
    d = decide(_ctx(nexus_data=_nexus(amount="150", currency="JPY")), CFG)
    assert d.would_be_refunded is True
    assert d.reason_code == re_.RC_WOULD_BE_REFUNDED
    assert d.computed_amount == "150"
    assert d.currency == "JPY"
    assert d.source == "iqbooster"
    assert d.amount_source == "nexus"


def test_ab_split_amount_summed():
    d = decide(_ctx(nexus_data=_nexus(amount="150+150")), CFG)
    assert d.would_be_refunded is True
    assert d.computed_amount == "300"
    assert d.amount_is_split is True


def test_customer_stated_amount_never_used_for_payout():
    # Customer says 200 (e.g. Korea bank fees); verified Nexus amount is 150.
    d = decide(_ctx(nexus_data=_nexus(amount="150"),
                    customer_stated_amount="200"), CFG)
    assert d.computed_amount == "150"                 # payout basis = verified
    assert d.customer_stated_amount == "200"          # recorded for audit only


def test_guard_exception_fails_closed_to_no():
    with patch.object(re_, "_is_charge_ambiguous", side_effect=RuntimeError("boom")):
        d = decide(_ctx(), CFG)
    assert d.would_be_refunded is False
    assert d.reason_code == re_.RC_EVAL_ERROR


def test_parse_amount_helper():
    assert re_.parse_amount("150+150") == (__import__("decimal").Decimal("300"), True)
    assert re_.parse_amount("1,500") == (__import__("decimal").Decimal("1500"), False)
    assert re_.parse_amount(None) == (None, False)
    assert re_.parse_amount("free") == (None, False)
