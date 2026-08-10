"""When the customer names a specific amount, it must match SOMETHING on the account.

Live incident #175712 (2026-08-10): customer wrote "charged 199 and 1990 yen on July 28,
refund the 1990". Nexus returned only two UNRELATED 5490 JPY renewals (a different site's
subscription). The engine's stated-amount routing (rule A) correctly found no match, but a
weaker fallback (keyword "subscription" in the ticket text) then confidently routed to the
5490 charge anyway. The bot replied in 12 seconds denying a charge the customer never asked
about, on the wrong brand's legal links. The customer had to argue the bot was "looking at a
different person's account" before a human stepped in and reconciled it manually.

Root cause: routing rules C/D/E/F (date, keyword, unauthorized-recurring heuristic, one-time
collapse) can override a hard amount mismatch with a much weaker signal. Fix: when the
customer names amount(s) and NONE of them match ANY refundable charge, stop immediately
(RC_STATED_AMOUNT_MISMATCH) — a human must check whether this is the right account/email
before any of the weaker fallbacks are allowed to run.
"""
import os

os.environ.setdefault("SKIP_WC_HEALTHCHECK", "true")

import main
import refund_engine


def _ctx(charges, text, as_of="2026-08-09", country="Japan"):
    return refund_engine.RefundContext(
        intent="REFUND_REQUEST", confidence=1.0, language="EN", country=country,
        as_of_date=as_of, nexus_available=True,
        nexus_data={"charges": charges}, nexus_lookup_failed=False,
        ticket_text=text,
    )


def _renewal(amount, d, cid="ch_1"):
    return {"charge_id": cid, "amount": amount, "currency": "JPY",
            "date": d, "type": "renewal", "status": "success"}


# ── The live incident, reproduced ─────────────────────────────────────────── #

def test_175712_mismatch_escalates_instead_of_answering_wrong_charge():
    """Customer named 199/1990; the account only has unrelated 5490 renewals."""
    charges = [_renewal("5490", "2026-07-22", "ch_a"), _renewal("5490", "2026-06-24", "ch_b")]
    text = ("I completed an IQ test and paid 199 yen. I was also charged an additional "
            "1990 yen on the exact same date. Please refund the unauthorized 1990 charge.")
    d = refund_engine.decide(_ctx(charges, text), main.REFUND_CONFIG)
    assert d.reason_code == refund_engine.RC_STATED_AMOUNT_MISMATCH
    assert d.would_be_refunded is False
    assert "199" in d.human_message and "1990" in d.human_message


def test_mismatch_never_auto_replies():
    """A human must verify — this reason code must NOT be in the auto-reply set."""
    assert refund_engine.RC_STATED_AMOUNT_MISMATCH not in main.reply_generator.REFUND_AUTOREPLY_CODES \
        if hasattr(main, "reply_generator") else True
    import importlib.util as ilu
    spec = ilu.spec_from_file_location("_rg", "reply_generator.py")
    rg = ilu.module_from_spec(spec)
    spec.loader.exec_module(rg)
    assert refund_engine.RC_STATED_AMOUNT_MISMATCH not in rg.REFUND_AUTOREPLY_CODES
    assert refund_engine.RC_STATED_AMOUNT_MISMATCH not in rg.REFUND_APPROVE_CODES


# ── The gate must not break correctly-matched cases ───────────────────────── #

def test_exact_amount_match_still_auto_approves():
    """Baseline: the ordinary in-window case must be completely unaffected."""
    charges = [_renewal("5490", "2026-08-07", "ch_a")]
    d = refund_engine.decide(_ctx(charges, "Please refund my 5490 yen charge"), main.REFUND_CONFIG)
    assert d.reason_code == refund_engine.RC_WOULD_BE_REFUNDED
    assert d.would_be_refunded is True


def test_exact_amount_match_still_denies_outside_window():
    charges = [_renewal("5490", "2026-07-01", "ch_a")]
    d = refund_engine.decide(_ctx(charges, "Please refund my 5490 yen charge"), main.REFUND_CONFIG)
    assert d.reason_code == refund_engine.RC_OUTSIDE_REFUND_WINDOW


def test_no_stated_amount_falls_through_to_normal_routing():
    """Customer names NO amount at all → gate must not fire; existing routing decides."""
    charges = [_renewal("5490", "2026-08-07", "ch_a")]
    d = refund_engine.decide(_ctx(charges, "Please cancel my subscription and refund me"),
                              main.REFUND_CONFIG)
    assert d.reason_code != refund_engine.RC_STATED_AMOUNT_MISMATCH
    assert d.reason_code == refund_engine.RC_WOULD_BE_REFUNDED


def test_stated_amount_matching_a_report_charge_routes_normally():
    """A named amount that DOES match a (different-type) charge must not be flagged."""
    charges = [
        _renewal("5490", "2026-08-07", "ch_a"),
        {"charge_id": "ch_r", "amount": "1990", "currency": "JPY", "date": "2026-08-05",
         "type": "cross_sale", "status": "success"},
    ]
    d = refund_engine.decide(_ctx(charges, "Please refund the 1990 yen report charge"),
                              main.REFUND_CONFIG)
    assert d.reason_code != refund_engine.RC_STATED_AMOUNT_MISMATCH
    assert d.reason_code == refund_engine.RC_REPORT_NOT_REFUNDABLE


def test_ambiguous_multi_type_match_is_not_treated_as_mismatch():
    """Stated amount matches MULTIPLE type groups → real ambiguity (rule G), not a mismatch."""
    charges = [
        _renewal("1990", "2026-08-07", "ch_a"),
        {"charge_id": "ch_f", "amount": "1990", "currency": "JPY", "date": "2026-08-05",
         "type": "first_sale", "status": "success"},
    ]
    d = refund_engine.decide(_ctx(charges, "Please refund my 1990 yen charge"), main.REFUND_CONFIG)
    assert d.reason_code != refund_engine.RC_STATED_AMOUNT_MISMATCH


def test_mismatch_checked_across_all_groups_not_just_subscription():
    """A stated amount matching a REPORT charge must not be flagged even when a subscription
    charge (different amount) is also present — the mismatch check spans every group."""
    charges = [
        _renewal("5490", "2026-08-07", "ch_a"),
        {"charge_id": "ch_r", "amount": "199", "currency": "JPY", "date": "2026-08-05",
         "type": "cross_sale", "status": "success"},
    ]
    d = refund_engine.decide(_ctx(charges, "Please refund the 199 yen test report"),
                              main.REFUND_CONFIG)
    assert d.reason_code != refund_engine.RC_STATED_AMOUNT_MISMATCH
