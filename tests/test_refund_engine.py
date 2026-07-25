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


def _report(cid="ch_rep", amount=990, dt="2026-07-18"):
    return {"charge_id": cid, "amount": amount, "currency": "JPY", "type": "cross_sale",
            "status": "success", "refundable": True, "date": dt}


def _first(cid="ch_first", amount=199, dt="2026-07-18"):
    return {"charge_id": cid, "amount": amount, "currency": "JPY", "type": "first_sale",
            "status": "success", "refundable": True, "date": dt}


def test_flow2_report_not_refundable():
    d = decide(_ctx(nexus_data=_data([_report()])), CFG)
    assert d.would_be_refunded is False
    assert d.reason_code == re_.RC_REPORT_NOT_REFUNDABLE
    assert d.refund_flow == "flow2_report"


def test_first_sale_flow_pending():
    d = decide(_ctx(nexus_data=_data([_first()])), CFG)
    assert d.reason_code == re_.RC_ONE_TIME_OUT_OF_SCOPE and d.refund_flow == "flow3_pending"


def test_ambiguous_flow_when_multiple_types_no_amount():
    d = decide(_ctx(nexus_data=_data([_sub("ch_sub", 5490, "2026-07-18"), _report()]),
                    ticket_text="please refund"), CFG)
    assert d.reason_code == re_.RC_AMBIGUOUS_FLOW


def test_route_by_amount_to_report():
    # sub 5490 + report 990; customer names 990円 → routes to report flow (non-refundable).
    d = decide(_ctx(nexus_data=_data([_sub("ch_sub", 5490, "2026-07-18"), _report(amount=990)]),
                    ticket_text="refund my 990円 charge"), CFG)
    assert d.reason_code == re_.RC_REPORT_NOT_REFUNDABLE and d.refund_flow == "flow2_report"


def test_route_by_amount_to_subscription():
    d = decide(_ctx(nexus_data=_data([_sub("ch_sub", 5490, "2026-07-18"), _report(amount=990)]),
                    ticket_text="refund my 5490円 charge"), CFG)
    assert d.would_be_refunded is True and d.refund_flow == "flow1_subscription"


def test_would_be_refunded_within_window():
    # sub charged 2 days before ticket; EN default window 14 → within → YES.
    d = decide(_ctx(), CFG)
    assert d.would_be_refunded is True
    assert d.reason_code == re_.RC_WOULD_BE_REFUNDED
    assert d.candidate_charge_id == "ch_sub" and d.computed_amount == "5490"
    assert d.refund_flow == "flow1_subscription"


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


def test_currency_proxy_sets_window():
    # JPY charge + EN language + no country → currency proxy → 8-day window.
    within = decide(_ctx(language="EN", country="",
                         nexus_data=_data([_sub("ch", 5490, "2026-07-15")])), CFG)  # 5d ≤ 8
    outside = decide(_ctx(language="EN", country="",
                          nexus_data=_data([_sub("ch", 5490, "2026-07-08")])), CFG)  # 12d > 8
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

def test_window_for_priority():
    # country → currency → language → default
    assert re_.window_for("JP", "USD", "EN") == (8, "country")     # country wins
    assert re_.window_for("", "JPY", "EN") == (8, "currency")      # currency proxy
    assert re_.window_for("", "BRL", "EN") == (10, "currency")     # LATAM currency
    assert re_.window_for("", "USD", "JP") == (8, "language")      # USD not mapped → language
    assert re_.window_for("", "USD", "EN") == (14, "default")      # all fall through


def test_number_parsing_locales():
    assert re_._to_decimal("9,99") == Decimal("9.99")      # EU decimal
    assert re_._to_decimal("1,990") == Decimal("1990")     # comma thousands
    assert re_._to_decimal("1.234,56") == Decimal("1234.56")
    assert re_._to_decimal("１，９９０") == Decimal("1990")   # full-width + comma


def test_parse_stated_amounts_currency_anchored():
    assert re_.parse_stated_amounts("charged $30.99") == [Decimal("30.99")]
    assert re_.parse_stated_amounts("5490円と199円") == [Decimal("5490"), Decimal("199")]
    assert re_.parse_stated_amounts("respond within 24 hours") == []  # no currency anchor


# ── Date-based routing (Anna's rule) ─────────────────────────────────────── #

def test_parse_stated_dates_both_orders():
    # "17.07" is unambiguously month 7 day 17 (17 can't be a month); "5/6" emits both.
    assert (None, 7, 17) in re_.parse_stated_dates("на 17.07")
    both = re_.parse_stated_dates("5/6")
    assert (None, 5, 6) in both and (None, 6, 5) in both
    assert re_.parse_stated_dates("2026年7月24日") == [(2026, 7, 24)]


def test_date_routes_to_subscription_same_day_166905():
    # Ticket 166905: 3 refundable charges, NO amount stated. Customer says the
    # payment was "the same day" → the ticket-date charge (the subscription).
    charges = [_sub("ch_sub", 5490, "2026-07-24"),
               _report(cid="ch_rep", amount=1990, dt="2026-07-17"),
               _first(cid="ch_first", amount=199, dt="2026-07-17")]
    d = decide(_ctx(nexus_data=_data(charges), as_of_date="2026-07-24T10:00:00Z",
                    ticket_text="I want a refund for the payment I made the same day"), CFG)
    assert d.would_be_refunded is True and d.refund_flow == "flow1_subscription"
    assert "date_routed" in d.guard_trail


def test_date_routes_by_explicit_date_to_report():
    # Explicit date uniquely matches the report charge → routes to flow #2 (non-refundable).
    charges = [_sub("ch_sub", 5490, "2026-07-24"),
               _report(cid="ch_rep", amount=1990, dt="2026-07-17"),
               _first(cid="ch_first", amount=199, dt="2026-07-15")]
    d = decide(_ctx(nexus_data=_data(charges), as_of_date="2026-07-25T10:00:00Z",
                    ticket_text="refund the charge from 17.07 please"), CFG)
    assert d.reason_code == re_.RC_REPORT_NOT_REFUNDABLE and d.refund_flow == "flow2_report"
    assert "date_routed" in d.guard_trail


def test_date_matches_two_types_stays_ambiguous():
    # Same-day date matches BOTH a subscription and a report on that date → can't
    # tell which flow → human decides (no date_routed). A subscription is present
    # so the one-time-collapse must NOT fire.
    charges = [_sub("ch_sub", 5490, "2026-07-17"),
               _report(cid="ch_rep", amount=1990, dt="2026-07-17")]
    d = decide(_ctx(nexus_data=_data(charges), as_of_date="2026-07-17T10:00:00Z",
                    ticket_text="refund the payment from today"), CFG)
    assert d.reason_code == re_.RC_AMBIGUOUS_FLOW and "date_routed" not in d.guard_trail


def test_no_date_multiple_types_stays_ambiguous():
    charges = [_sub("ch_sub", 5490, "2026-07-24"), _report()]
    d = decide(_ctx(nexus_data=_data(charges), ticket_text="please refund my money"), CFG)
    assert d.reason_code == re_.RC_AMBIGUOUS_FLOW and "date_routed" not in d.guard_trail


# ── Type-keyword routing + one-time-collapse (Anna's rule, v9) ───────────── #

def test_type_keyword_routes_to_subscription_166931():
    # JP: no amount, no date; customer names the subscription ("サブスク"). sub + IQ-fee.
    charges = [_sub("ch_sub", 5490, "2026-07-24"), _first(cid="ch_first", amount=199, dt="2026-07-17")]
    d = decide(_ctx(language="JP", nexus_data=_data(charges), as_of_date="2026-07-24T10:00:00Z",
                    ticket_text="サブスクは登録した認識はありません。返金お願い致します"), CFG)
    assert d.would_be_refunded is True and d.refund_flow == "flow1_subscription"
    assert "type_routed" in d.guard_trail


def test_type_keyword_routes_to_subscription_en():
    charges = [_sub("ch_sub", 4990, "2026-07-18"), _first(cid="ch_first", amount=199, dt="2026-07-10")]
    d = decide(_ctx(nexus_data=_data(charges),
                    ticket_text="I have no recollection of signing up for this recurring subscription"), CFG)
    assert d.would_be_refunded is True and "type_routed" in d.guard_trail


def test_type_keyword_routes_to_report():
    charges = [_sub("ch_sub", 5490, "2026-07-18"), _report(cid="ch_rep", amount=1990, dt="2026-07-17")]
    d = decide(_ctx(nexus_data=_data(charges), ticket_text="please refund my report, I don't need it"), CFG)
    assert d.reason_code == re_.RC_REPORT_NOT_REFUNDABLE and "type_routed" in d.guard_trail


def test_type_keyword_conflict_stays_ambiguous():
    # Customer names BOTH a subscription and a report → can't resolve by word → human.
    charges = [_sub("ch_sub", 5490, "2026-07-18"), _report(cid="ch_rep", amount=1990, dt="2026-07-17")]
    d = decide(_ctx(nexus_data=_data(charges),
                    ticket_text="refund the subscription and the report"), CFG)
    assert d.reason_code == re_.RC_AMBIGUOUS_FLOW and "type_routed" not in d.guard_trail


def test_one_time_collapse_no_subscription_166906():
    # DE: no subscription at all; only one-time charges (report + IQ fee), no amount/date/word.
    # One-time is never refundable → definite NO (not ambiguous).
    charges = [_report(cid="ch_rep", amount=1.90, dt="2026-07-24"),
               _first(cid="ch_first", amount=9.99, dt="2026-07-24")]
    d = decide(_ctx(language="DE", nexus_data=_data(charges), as_of_date="2026-07-24T10:00:00Z",
                    ticket_text="Ich möchte eine vollständige Rückerstattung, versehentliche Zahlung"), CFG)
    assert d.would_be_refunded is False
    assert d.reason_code == re_.RC_REPORT_NOT_REFUNDABLE and "one_time_collapsed" in d.guard_trail


def test_first_sale_only_multi_is_not_refundable():
    # Two first_sale charges = ONE type (single-type path B, not the collapse) → NO.
    charges = [_first(cid="ch_a", amount=199, dt="2026-07-20"),
               _first(cid="ch_b", amount=299, dt="2026-07-10")]
    d = decide(_ctx(nexus_data=_data(charges), ticket_text="please refund"), CFG)
    assert d.would_be_refunded is False and d.refund_flow == "flow3_pending"


def test_subscription_plus_one_time_no_signal_stays_ambiguous():
    # subscription present (could be YES) + one-time, but no amount/date/word AND no
    # unauthorized-recurring signal → genuinely can't tell → human (safety valve preserved).
    charges = [_sub("ch_sub", 5490, "2026-07-18"), _first(cid="ch_first", amount=199, dt="2026-07-10")]
    d = decide(_ctx(nexus_data=_data(charges), ticket_text="please refund my money"), CFG)
    assert d.reason_code == re_.RC_AMBIGUOUS_FLOW
    assert "one_time_collapsed" not in d.guard_trail and "type_routed" not in d.guard_trail
    assert "dispute_target_subscription" not in d.guard_trail


# ── Dispute-target = surprise recurring subscription (v10) ────────────────── #

def test_dispute_target_subscription_167304():
    # Ticket 167304: customer accepts 199+1990 (one-time), disputes the 5490 sub they
    # "don't recognize". stated amounts map to 3 types → v9 was AMBIGUOUS; v10 routes to
    # the subscription on the unauthorized signal.
    charges = [_sub("ch_sub", 5490, "2026-07-23"),
               _report(cid="ch_rep", amount=1990, dt="2026-07-16"),
               _first(cid="ch_fee", amount=199, dt="2026-07-16")]
    d = decide(_ctx(language="JP", nexus_data=_data(charges), as_of_date="2026-07-24T10:00:00Z",
                    ticket_text="7月22日の5,490円は身に覚えがありません。199円と1,990円は了承します"), CFG)
    assert d.would_be_refunded is True and d.refund_flow == "flow1_subscription"
    assert "dispute_target_subscription" in d.guard_trail
    assert d.candidate_charge_id == "ch_sub"


def test_dispute_target_subscription_en_conflict():
    # Both a subscription and a report exist; customer names small one-time amounts but the
    # complaint is "never subscribed / recurring" → route to subscription despite the mix.
    charges = [_sub("ch_sub", 5490, "2026-07-20"),
               _report(cid="ch_rep", amount=1990, dt="2026-07-12"),
               _first(cid="ch_fee", amount=199, dt="2026-07-12")]
    d = decide(_ctx(nexus_data=_data(charges), as_of_date="2026-07-24T10:00:00Z",
                    ticket_text="I only wanted the 199 test and my report — I never subscribed "
                                "to a recurring membership, please refund the unauthorized charge"), CFG)
    assert d.would_be_refunded is True and "dispute_target_subscription" in d.guard_trail


def test_dispute_target_needs_a_subscription():
    # Unauthorized-recurring wording but NO subscription among charges → must NOT invent one;
    # falls through to the one-time collapse (definite NO), not dispute_target.
    charges = [_report(cid="ch_rep", amount=1990, dt="2026-07-16"),
               _first(cid="ch_fee", amount=199, dt="2026-07-16")]
    d = decide(_ctx(nexus_data=_data(charges), as_of_date="2026-07-24T10:00:00Z",
                    ticket_text="I never authorized this recurring charge"), CFG)
    assert d.would_be_refunded is False
    assert "dispute_target_subscription" not in d.guard_trail
    assert "one_time_collapsed" in d.guard_trail


def test_dispute_target_still_gated_by_window():
    # Unauthorized-recurring + subscription, but the latest sub is OLD → routed to
    # subscription (not ambiguous) yet correctly NO on the window (no false YES).
    charges = [_sub("ch_old", 5490, "2026-06-20"),
               _first(cid="ch_fee", amount=199, dt="2026-06-18")]
    d = decide(_ctx(language="JP", nexus_data=_data(charges), as_of_date="2026-07-24T10:00:00Z",
                    ticket_text="身に覚えのない継続課金です、返金してください"), CFG)
    assert d.reason_code == re_.RC_OUTSIDE_REFUND_WINDOW
    assert "dispute_target_subscription" in d.guard_trail
