"""The refund engine used to route on the customer's opening line only.

All three `_refund_would_be_eval` call sites pass `subject + body`, and `body`
is the Zendesk description — the FIRST comment, which never changes. So
`parse_stated_amounts`, `_route_by_date` and `_route_by_type_keyword` never saw
a follow-up comment, and on a live-chat / messaging ticket the description is
empty by design, which left routing to work off a subject alone.

`refund_ask_in_text` in `_process` was already computed over subject + body +
every public customer comment, and the suppression guards compare against it.
So the flow could suppress a reply because of a demand it had read, while the
engine had routed the ticket without ever seeing that text.

This matters because of where those tickets end up: a ticket that says nothing
useful up front and names the charge in the second comment falls through amount,
date and type routing into the heuristic/LLM route — and is then suppressed by
Guard 2b for having been routed by heuristic. 421 tickets per 20 days sit in
that bucket, and 130 of them had named an amount somewhere.

The widening is best-effort on purpose: a Zendesk hiccup must leave the old
subject+body behaviour exactly as it was, never fail the eval.
"""
from unittest.mock import MagicMock, patch

import main
import refund_engine as re_


def _charge(amount, cur, ctype, d, cid):
    return {"charge_id": cid, "amount": amount, "currency": cur,
            "date": d, "type": ctype, "status": "success"}


# A mixed account: a subscription AND a report. Nothing in subject+body points at
# either, so without the comments this is the AMBIGUOUS_FLOW residual that Guard
# 2b then suppresses.
_MIXED = [_charge("29.99", "EUR", "renewal", "2026-08-19", "ch_sub"),
          _charge("9.99", "EUR", "cross_sale", "2026-08-12", "ch_rep")]


def _eval(body, comments, charges=None, alt_charges=None, comments_raise=False):
    result = {}
    nexus = MagicMock()
    if alt_charges is None:
        nexus.search_subscription.return_value = {"charges": charges if charges is not None else _MIXED}
    else:
        nexus.search_subscription.side_effect = lambda e: (
            {"charges": alt_charges} if e != "u@e.com" else {"charges": []})
    zd = MagicMock()
    zd.get_ticket_image_attachments.return_value = []
    if comments_raise:
        zd.get_all_customer_comments_text.side_effect = RuntimeError("zendesk 503")
    else:
        zd.get_all_customer_comments_text.return_value = comments
    with patch.object(main, "USE_NEXUS_FOR_LOOKUP", True), \
         patch.object(main, "nexus_client", nexus), \
         patch.object(main, "zendesk", zd), \
         patch.object(main.refund_ocr, "is_enabled", return_value=False), \
         patch.object(main.refund_disambiguate, "is_enabled", return_value=False):
        main._refund_would_be_eval(
            "7001", "u@e.com", "REFUND_REQUEST",
            {"confidence": 0.98, "language": "DE"}, result,
            ticket_text=f"Rückerstattung\n{body}",
            as_of_date="2026-08-20T00:00:00Z", country="Germany", brand="wwiqtest")
    return result, zd


def test_without_the_comments_this_ticket_is_the_ambiguous_residual():
    # The baseline the fix is measured against — no comment, nothing to route on.
    result, _zd = _eval("Bitte um Rückerstattung.", "")
    assert result["refund_reason_code"] == re_.RC_AMBIGUOUS_FLOW


def test_an_amount_named_in_a_later_comment_routes_the_ticket():
    result, _zd = _eval("Bitte um Rückerstattung.",
                        "Nachtrag: Es wurden 29,90 EUR abgebucht.")
    assert result["refund_reason_code"] != re_.RC_AMBIGUOUS_FLOW
    trail = result["refund_guard_trail"]
    assert "route:subscription" in trail
    # Hard-routed by amount — not by the heuristic route Guard 2b suppresses.
    assert "dispute_target_subscription" not in trail
    assert "llm_disambiguated" not in trail


def test_a_charge_type_named_in_a_later_comment_routes_the_ticket():
    result, _zd = _eval("Bitte um Rückerstattung.",
                        "Ich meine den Bericht, nicht das Abo.")
    assert result["refund_reason_code"] == re_.RC_REPORT_NOT_REFUNDABLE
    assert "type_routed" in result["refund_guard_trail"]


def test_live_chat_ticket_with_an_empty_description_still_routes():
    # Messaging tickets carry no description; everything the customer said is in
    # the comments. Before, the engine got a subject and nothing else.
    result, _zd = _eval("", "Es wurden 29,90 EUR abgebucht, bitte zurück.")
    assert "route:subscription" in result["refund_guard_trail"]


def test_zendesk_failing_leaves_the_old_behaviour_untouched():
    result, _zd = _eval("Bitte um Rückerstattung.", "", comments_raise=True)
    assert result["refund_reason_code"] == re_.RC_AMBIGUOUS_FLOW  # evaluated, not crashed
    assert result.get("refund_engine_version")


def test_alt_email_retry_still_sees_comment_only_addresses_and_fetches_once():
    # The 2026-07-27 audit found 7 of 25 genuine NOT_FOUND misses had the charge
    # under an email that appeared only in a comment. That retry used to do its
    # own fetch; it now reuses the widened text, so the call happens once.
    result, zd = _eval("Ich finde meine Zahlung nicht.",
                       "Bezahlt habe ich mit paid@other.com",
                       alt_charges=[_charge("29.99", "EUR", "renewal", "2026-08-19", "ch_s")])
    assert result.get("refund_lookup_email") == "paid@other.com"
    assert zd.get_all_customer_comments_text.call_count == 1
