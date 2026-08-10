"""Multi-charge refund denials must NAME every charge they refuse.

Nastya (2026-08-10) reported the bot "doesn't write a denial about past payments" on
Sub Renewal Refund. Confirmed from BigQuery (zendesk_bot.cancellation_logs, 2026-08-09):
14 of 14 tickets where the bot saw more than one charge listed exactly ONE charge in the
reply. Worst live case #175502 — 14 renewals of 4990 JPY, the customer was told about
one. The deny template always had a `charges_list` slot; main.py fed it a single line.

These tests pin the fixed behaviour with the real production charge sets.
"""
import os
import re
from datetime import date, timedelta

import pytest

os.environ.setdefault("SKIP_WC_HEALTHCHECK", "true")   # bypass the live WC ping

import main
import refund_engine

# Several sibling test modules install a MagicMock for "reply_generator" in
# sys.modules (they only exercise main.py's flow), so a plain `import
# reply_generator` here would bind the mock depending on collection order and the
# template assertions below would silently pass against a mock. Load the REAL
# module from disk under a private name so these tests check real template text.
import importlib.util as _ilu
_spec = _ilu.spec_from_file_location(
    "_real_reply_generator",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                 "reply_generator.py"),
)
reply_generator = _ilu.module_from_spec(_spec)
_spec.loader.exec_module(reply_generator)


# ── Real charge sets from production ─────────────────────────────────────── #

def _charges_175502():
    """#175502 — 14 monthly renewals of 4990 JPY, newest 2026-07-31 (all out of window)."""
    dates = ["2026-07-31", "2026-04-30", "2026-03-31", "2026-02-28", "2026-01-28",
             "2025-12-28", "2025-11-28", "2025-10-28", "2025-09-28", "2025-08-28",
             "2025-07-28", "2025-06-28", "2025-05-28", "2025-04-28"]
    return [{"charge_id": f"ch_{i}", "amount": "4990", "currency": "JPY",
             "date": d, "type": "renewal", "status": "success"}
            for i, d in enumerate(dates)]


def _charges_175712():
    """#175712 — Anna's ticket: 2 renewals of 5490 JPY (2026-07-22, 2026-06-24)."""
    return [
        {"charge_id": "ch_a", "amount": "5490", "currency": "JPY",
         "date": "2026-07-22", "type": "renewal", "status": "success"},
        {"charge_id": "ch_b", "amount": "5490", "currency": "JPY",
         "date": "2026-06-24", "type": "renewal", "status": "success"},
    ]


def _ctx(charges, as_of):
    return refund_engine.RefundContext(
        intent="SUB_RENEWAL_REFUND", confidence=1.0, language="EN", country="Japan",
        as_of_date=as_of, nexus_available=True,
        nexus_data={"charges": charges}, nexus_lookup_failed=False,
        ticket_text="I want a refund for all of these payments",
    )


# ── Engine: denied_charges must cover every refused charge ───────────────── #

def test_outside_window_denies_all_14_charges():
    d = refund_engine.decide(_ctx(_charges_175502(), "2026-08-09"), main.REFUND_CONFIG)
    assert d.reason_code == refund_engine.RC_OUTSIDE_REFUND_WINDOW
    assert d.would_be_refunded is False
    ids = [p.split(":")[0] for p in d.denied_charges.split(";") if p]
    assert len(ids) == 14, f"expected all 14 charges denied, got {len(ids)}"


def test_denied_charges_are_newest_first():
    d = refund_engine.decide(_ctx(_charges_175502(), "2026-08-09"), main.REFUND_CONFIG)
    dates = [p.split(":")[2] for p in d.denied_charges.split(";") if p]
    assert dates == sorted(dates, reverse=True)
    assert dates[0] == "2026-07-31"


def test_last_only_denies_just_the_earlier_charges():
    """Latest charge is INSIDE the window → approve it, deny+list the earlier ones."""
    charges = _charges_175712()
    charges[0]["date"] = "2026-08-07"          # 2d old → within the 8d JP window
    d = refund_engine.decide(_ctx(charges, "2026-08-09"), main.REFUND_CONFIG)
    assert d.reason_code == refund_engine.RC_WOULD_BE_REFUNDED_LAST_ONLY
    assert d.would_be_refunded is True
    # Money/Refund Sum still covers ONLY the latest charge …
    assert d.candidate_charges.count(";") == 0
    assert "ch_a" in d.candidate_charges
    # … but the denial names the earlier one.
    assert "ch_b" in d.denied_charges
    assert "ch_a" not in d.denied_charges


def test_single_charge_denial_lists_that_one_charge():
    d = refund_engine.decide(_ctx(_charges_175712()[:1], "2026-08-09"), main.REFUND_CONFIG)
    assert d.reason_code == refund_engine.RC_OUTSIDE_REFUND_WINDOW
    assert len([p for p in d.denied_charges.split(";") if p]) == 1


def test_approved_single_charge_denies_nothing():
    charges = _charges_175712()[:1]
    charges[0]["date"] = (date.fromisoformat("2026-08-09") - timedelta(days=2)).isoformat()
    d = refund_engine.decide(_ctx(charges, "2026-08-09"), main.REFUND_CONFIG)
    assert d.reason_code == refund_engine.RC_WOULD_BE_REFUNDED
    assert not (d.denied_charges or "")


# ── Renderer: the bullet list main.py hands to the template ──────────────── #

def test_render_all_14_lines():
    d = refund_engine.decide(_ctx(_charges_175502(), "2026-08-09"), main.REFUND_CONFIG)
    text, n = main._denied_charges_list(d.denied_charges, "JPY")
    assert n == 14
    assert text.count("\n") == 13
    assert text.startswith("- 4990 JPY (charged 2026-07-31)")
    assert "- 4990 JPY (charged 2025-04-28)" in text


def test_render_empty_input_falls_back():
    assert main._denied_charges_list("", "JPY") == ("", 0)
    assert main._denied_charges_list(None, "") == ("", 0)


def test_render_skips_malformed_and_dedupes():
    raw = "ch_1:4990:2026-07-31;garbage;ch_1:4990:2026-07-31;ch_2::;ch_3:100:2026-01-01"
    text, n = main._denied_charges_list(raw, "JPY")
    assert n == 2                                   # ch_1 once, ch_3; ch_2 has no amount
    assert text.count("ch_") == 0                   # ids never leak to the customer
    assert "- 100 JPY (charged 2026-01-01)" in text


# ── Templates: the customer actually sees the charges ────────────────────── #

def test_deny_template_lists_every_charge_and_plural_grammar():
    d = refund_engine.decide(_ctx(_charges_175502(), "2026-08-09"), main.REFUND_CONFIG)
    lst, n = main._denied_charges_list(d.denied_charges, "JPY")
    body = reply_generator.refund_master_reply(
        "OUTSIDE_REFUND_WINDOW",
        {"charges_list": lst, "it_falls": "they fall" if n > 1 else "it falls",
         "refund_window_days": 8},
    )
    assert len(re.findall(r"(?m)^- ", body)) == 14, "every refused charge must be listed"
    assert "as they fall outside" in body, "plural grammar for a multi-charge denial"
    assert "as it falls outside" not in body


def test_deny_template_keeps_singular_grammar_for_one_charge():
    body = reply_generator.refund_master_reply(
        "OUTSIDE_REFUND_WINDOW",
        {"charges_list": "- 5490 JPY (charged 2026-06-24)", "it_falls": "it falls",
         "refund_window_days": 8},
    )
    assert len(re.findall(r"(?m)^- ", body)) == 1
    assert "as it falls outside" in body


def test_last_only_template_names_the_earlier_charges():
    body = reply_generator.refund_master_reply(
        "WOULD_BE_REFUNDED_LAST_ONLY",
        {"charge_amount": "5490 JPY", "charge_date": "2026-08-07", "refund_window_days": 8,
         "earlier_charges_list": "- 5490 JPY (charged 2026-06-24)"},
    )
    assert "- 5490 JPY (charged 2026-06-24)" in body
    assert "earlier charge(s) do not qualify" in body


def test_last_only_template_without_list_stays_readable():
    """No earlier-charge data → the sentence must still close cleanly (old wording)."""
    body = reply_generator.refund_master_reply(
        "WOULD_BE_REFUNDED_LAST_ONLY",
        {"charge_amount": "5490 JPY", "charge_date": "2026-08-07", "refund_window_days": 8},
    )
    assert "outside the applicable refund window. We understand" in body
    assert "::" not in body


@pytest.mark.parametrize("rc", ["OUTSIDE_REFUND_WINDOW", "WOULD_BE_REFUNDED_LAST_ONLY"])
def test_templates_never_leak_charge_ids(rc):
    body = reply_generator.refund_master_reply(
        rc,
        {"charge_amount": "5490 JPY", "charge_date": "2026-08-07", "refund_window_days": 8,
         "charges_list": "- 4990 JPY (charged 2026-07-31)",
         "earlier_charges_list": "- 4990 JPY (charged 2026-04-30)"},
    )
    assert "ch_" not in body


# ── Labelling: renewal refunds must report under Sub Renewal Refund ──────── #

def test_renewal_charge_gets_sub_renewal_topic():
    fields = main._build_refund_fields(False, "", "JPY", charge_type="renewal")
    assert main._ZENDESK_TOPIC_SUB_RENEWAL_REFUND_VALUE in fields.values()


def test_first_subscription_stays_plain_refund_topic():
    fields = main._build_refund_fields(False, "", "JPY", charge_type="subscription",
                                       charge_count=1)
    assert main._ZENDESK_TOPIC_REFUND_VALUE in fields.values()
    assert main._ZENDESK_TOPIC_SUB_RENEWAL_REFUND_VALUE not in fields.values()


def test_multi_charge_subscription_is_labelled_as_renewal():
    """Safety net: 2+ subscription charges IS a 2nd+ payment, whatever the type says."""
    fields = main._build_refund_fields(False, "", "JPY", charge_type="subscription",
                                       charge_count=3)
    assert main._ZENDESK_TOPIC_SUB_RENEWAL_REFUND_VALUE in fields.values()


def test_cross_sale_is_not_a_renewal_refund():
    fields = main._build_refund_fields(False, "", "JPY", charge_type="cross_sale",
                                       charge_count=5)
    assert main._ZENDESK_TOPIC_SUB_RENEWAL_REFUND_VALUE not in fields.values()
