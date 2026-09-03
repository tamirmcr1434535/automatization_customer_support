"""Guard 2b, relaxed for the one slice that was measured.

Guard 2b suppresses any ticket whose charge TYPE was resolved by heuristic — the
unauthorized-recurring rule or the LLM disambiguator — rather than by a stated
amount, a date or a type word. 421 tickets per 20 days, of which not one has
ever been auto-answered, while every one of the 579 auto-answered approvals was
hard-routed.

The 2026-09-03 study asked the only question that matters: did the human refund
the charge the ENGINE targeted? 40 tickets from 2026-08-15..27, stratified by
brand x language; ground truth = Nexus charge-detail `refunded_at`, cross-checked
against the Zendesk refund tags, the two agreeing on 39 of 40. 39 of 40 tickets
were closed with an agent reply, so a "no" is a human decision rather than an
unopened ticket.

    llm_disambiguated            16/20 = 80.0%  [58.4-91.9]
    dispute_target_subscription  15/20 = 75.0%  [53.1-88.8]   (no LLM at all)
    overall                      31/40 = 77.5%  [62.5-87.7]

Two things follow. The claim in main.py that every llm-disambiguated approve was
a false positive is dead — the interval's lower bound is 58% — and the two arms
are indistinguishable, so the LLM was never the thing worth gating. But 77.5% is
short of the >=90% that would justify opening the gate wholesale: roughly one in
four would become a refund the human did not grant.

Precision split hard by language — 14/14 for EN/NL/KR/VI/ZH against 17/26 for
JP+DE. That split was not pre-registered, and 14/14 is [78%, 100%] rather than
zero errors. So this is a hypothesis under test on live traffic, one language set
at a time, gated by REFUND_SOFT_ROUTE_APPROVE_LANGS, empty by default.

What these tests pin is mostly what the relaxation must NOT do: denials stay
suppressed at any precision (bot denials score 18.5% good against 46-50% for a
human, p~2e-05), unlisted languages stay suppressed, and the two suppression
rules either side of Guard 2b in the elif chain still apply.
"""
from unittest.mock import MagicMock, patch

import main

_AUTO = {"WOULD_BE_REFUNDED", "WOULD_BE_REFUNDED_LAST_ONLY", "OUTSIDE_REFUND_WINDOW"}


def _mixed_account(sub_date="2026-08-18", sub_amount=5490):
    """A subscription plus a one-time fee: the mixed-type account that makes a
    ticket soft-routable in the first place."""
    return {"subscription_id": "1", "source": "stripe", "charges": [
        {"charge_id": "ch_sub", "amount": sub_amount, "currency": "JPY",
         "type": "subscription", "status": "success", "refundable": True, "date": sub_date},
        {"charge_id": "ch_fee", "amount": 199, "currency": "JPY", "type": "first_sale",
         "status": "success", "refundable": True, "date": "2026-08-10"},
    ]}


def _run(*, langs="", ticket_lang="EN",
         text="This charge was unauthorised. Please refund me.",
         as_of="2026-08-20T00:00:00Z",
         ask_in_text=True, llm_pick=None, sub_date="2026-08-18"):
    """Drive the eval with the flag set to `langs`. `llm_pick` set → the LLM
    disambiguator route; otherwise the deterministic unauthorized-recurring one."""
    result = {}
    if ask_in_text is not None:
        result["refund_ask_in_text"] = ask_in_text
    nexus = MagicMock()
    nexus.search_subscription.return_value = _mixed_account(sub_date=sub_date)
    rcm = MagicMock()
    rcm.is_configured.return_value = True
    rcm.get_charge_detail.return_value = {"disputed": False, "refundable": True,
                                          "amount": 5490, "currency": "JPY",
                                          "refunded_at": ""}
    rcm.create_refund.return_value = {"status": "refunded", "executed": True,
                                      "refunded_amount": 5490}
    zd = MagicMock()
    zd.get_ticket_image_attachments.return_value = []
    zd.get_all_customer_comments_text.return_value = ""
    langs_set = {x.strip().upper() for x in langs.split(",") if x.strip()}
    with patch.object(main, "REFUND_SOFT_ROUTE_APPROVE_LANGS", langs_set), \
         patch.object(main, "USE_NEXUS_FOR_LOOKUP", True), \
         patch.object(main, "nexus_client", nexus), \
         patch.object(main, "refund_client", rcm), \
         patch.object(main, "REFUNDS_ENABLED", True), \
         patch.object(main, "REFUNDS_ENABLED_BRANDS", set()), \
         patch.object(main, "zendesk", zd), \
         patch.object(main.refund_ocr, "is_enabled", return_value=False), \
         patch.object(main.refund_abuse, "check", return_value=(True, "")), \
         patch.object(main.refund_disambiguate, "is_enabled",
                      return_value=llm_pick is not None), \
         patch.object(main.refund_disambiguate, "pick_target_charge_id",
                      return_value=llm_pick), \
         patch.object(main.reply_generator, "REFUND_AUTOREPLY_CODES", _AUTO), \
         patch.object(main.reply_generator, "generate_refund_reply", return_value="DRAFT"):
        main._refund_would_be_eval(
            "9500", "u@e.com", "REFUND_REQUEST",
            {"confidence": 0.98, "language": ticket_lang}, result,
            ticket_text=text, as_of_date=as_of, brand="wwiqtest")
    return result, rcm, zd


# ── Default: nothing changes ────────────────────────────────────────────── #

def test_flag_empty_leaves_guard_2b_exactly_as_it_was():
    r, rcm, zd = _run(langs="")
    assert r["refund_decision"] == "YES"
    assert r["refund_reply_suppressed"] == "cross_sale_ambiguous_route"
    assert r["refund_soft_route_relaxed"] is False
    assert "refund_draft_reply" not in r
    rcm.create_refund.assert_not_called()


# ── The slice under test ────────────────────────────────────────────────── #

def test_listed_language_approve_is_answered_and_the_money_moves():
    r, rcm, _zd = _run(langs="EN,NL,KR,VI,ZH", ticket_lang="EN")
    assert r["refund_soft_route_relaxed"] is True
    assert "refund_reply_suppressed" not in r
    assert r.get("refund_draft_reply") == "DRAFT"
    assert r.get("refund_execution_status") == "refunded"
    rcm.create_refund.assert_called_once()


def test_the_llm_route_is_opened_for_the_same_slice():
    # Relaxing Guard 2b alone would free nothing here: the ticket would reach a
    # draft and then die on the older `skipped_llm_disambiguated` gate, because
    # send_ok demands `executed` for an approve. Both gates key off one predicate.
    r, rcm, _zd = _run(langs="EN", ticket_lang="EN", llm_pick="ch_sub",
                       text="please refund the charge I never agreed to")
    assert "llm_disambiguated" in r["refund_guard_trail"]
    assert r.get("refund_execution_status") == "refunded"
    rcm.create_refund.assert_called_once()


def test_the_llm_gate_still_stands_outside_the_slice():
    r, rcm, _zd = _run(langs="EN", ticket_lang="JP", llm_pick="ch_sub",
                       text="返金をお願いします")
    assert r["refund_reply_suppressed"] == "cross_sale_ambiguous_route"
    rcm.create_refund.assert_not_called()


# ── What the relaxation must never do ───────────────────────────────────── #

def test_unlisted_language_stays_suppressed():
    r, rcm, _zd = _run(langs="EN,NL", ticket_lang="DE",
                       text="Ich habe das Abo nicht bestellt, bitte Rückerstattung")
    assert r["refund_soft_route_relaxed"] is False
    assert r["refund_reply_suppressed"] == "cross_sale_ambiguous_route"
    rcm.create_refund.assert_not_called()


def test_denials_stay_suppressed_even_in_a_listed_language():
    # Approve-only, permanently. A bot denial on this cohort scores 18.5% good
    # against 46-50% for a human — a bad trade at any precision. Same ticket,
    # charge pushed outside the refund window so the verdict flips to a denial.
    r, rcm, _zd = _run(langs="EN", ticket_lang="EN", sub_date="2026-06-01")
    assert r["refund_reason_code"] == "OUTSIDE_REFUND_WINDOW"
    assert r["refund_soft_route_relaxed"] is False
    assert r["refund_reply_suppressed"] == "cross_sale_ambiguous_route"


def test_a_ticket_with_no_refund_ask_at_all_is_still_suppressed():
    # Rule (a) sits ABOVE Guard 2b in the elif chain and is untouched — #175987,
    # the customer who never asked for money and was sent a refund verdict.
    r, _rcm, _zd = _run(langs="EN", ticket_lang="EN", ask_in_text=False)
    assert r["refund_reply_suppressed"] == "no_refund_request_in_text"


def test_an_explanation_without_a_demand_is_still_suppressed():
    # Rule (c) sits BELOW Guard 2b, so skipping (b) falls through to it — #172054,
    # the unrecognised-charge report that is a request to explain, not to refund.
    r, rcm, _zd = _run(langs="EN", ticket_lang="EN",
                       text="I see a charge on my card I do not recognize. What is this?")
    assert r["refund_reply_suppressed"] == "explanation_only_no_refund_demand"
    rcm.create_refund.assert_not_called()


def test_the_money_guards_downstream_are_still_consulted():
    # Routing confidence is what opens, not a money guard. An open dispute on the
    # charge must still stop everything, inside the relaxed slice.
    result = {"refund_ask_in_text": True}
    nexus = MagicMock()
    nexus.search_subscription.return_value = _mixed_account()
    rcm = MagicMock()
    rcm.is_configured.return_value = True
    rcm.get_charge_detail.return_value = {"disputed": True, "refundable": True,
                                          "amount": 5490, "currency": "JPY",
                                          "refunded_at": ""}
    zd = MagicMock()
    zd.get_ticket_image_attachments.return_value = []
    zd.get_all_customer_comments_text.return_value = ""
    with patch.object(main, "REFUND_SOFT_ROUTE_APPROVE_LANGS", {"EN"}), \
         patch.object(main, "USE_NEXUS_FOR_LOOKUP", True), \
         patch.object(main, "nexus_client", nexus), \
         patch.object(main, "refund_client", rcm), \
         patch.object(main, "REFUNDS_ENABLED", True), \
         patch.object(main, "REFUNDS_ENABLED_BRANDS", set()), \
         patch.object(main, "zendesk", zd), \
         patch.object(main.refund_ocr, "is_enabled", return_value=False), \
         patch.object(main.reply_generator, "REFUND_AUTOREPLY_CODES", _AUTO), \
         patch.object(main.reply_generator, "generate_refund_reply", return_value="DRAFT"):
        main._refund_would_be_eval(
            "9501", "u@e.com", "REFUND_REQUEST",
            {"confidence": 0.98, "language": "EN"}, result,
            ticket_text="This charge was unauthorised. Please refund me.",
            as_of_date="2026-08-20T00:00:00Z", brand="wwiqtest")
    assert result["refund_decision"] == "NO"
    rcm.create_refund.assert_not_called()


# ── The predicate itself ────────────────────────────────────────────────── #

def test_predicate_is_approve_only_and_case_insensitive():
    with patch.object(main, "REFUND_SOFT_ROUTE_APPROVE_LANGS", {"EN", "NL"}):
        assert main._soft_route_approve_allowed("en", "WOULD_BE_REFUNDED")
        assert main._soft_route_approve_allowed(" NL ", "WOULD_BE_REFUNDED_LAST_ONLY")
        assert not main._soft_route_approve_allowed("EN", "OUTSIDE_REFUND_WINDOW")
        assert not main._soft_route_approve_allowed("JP", "WOULD_BE_REFUNDED")
        assert not main._soft_route_approve_allowed("", "WOULD_BE_REFUNDED")
        assert not main._soft_route_approve_allowed(None, "WOULD_BE_REFUNDED")


def test_it_is_an_allowlist_so_an_unmeasured_language_is_never_opened():
    # A JP/DE denylist would have opened TR, ID, TH and anything added later
    # without anyone deciding to.
    with patch.object(main, "REFUND_SOFT_ROUTE_APPROVE_LANGS", {"EN"}):
        for lang in ("TR", "ID", "TH", "AR", "PL", "IT", "JP", "DE"):
            assert not main._soft_route_approve_allowed(lang, "WOULD_BE_REFUNDED"), lang
