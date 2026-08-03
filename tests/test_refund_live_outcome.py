"""
Unit tests for the LIVE-resolved refund path (2026-07-30)
=========================================================
When the bot actually resolves a refund end-to-end for a refunds-enabled brand
(posts the approve/deny reply), the ticket must:
  • report status success_refund_approved / success_refund_denied (→ auto_resolved
    in the report), NOT skipped_refund_request;
  • surface the reply in Slack like a cancellation (reply_text set + "Refund …" line);
  • fill the Zendesk topic-screen fields (Topic / Refund Status / Refund Sum /
    Currency / Country) the way an agent would.

All heavy modules are mocked before importing main (same pattern as test_main_flow).
"""

import os
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

os.environ.setdefault("ZENDESK_SUBDOMAIN", "wwiqtest")
os.environ.setdefault("ZENDESK_EMAIL", "bot@test.com")
os.environ.setdefault("ZENDESK_API_TOKEN", "token")
os.environ.setdefault("WOO_SITE_URL", "https://iqbooster.org")
os.environ.setdefault("WOO_CONSUMER_KEY", "ck_test")
os.environ.setdefault("WOO_CONSUMER_SECRET", "cs_test")
os.environ.setdefault("STRIPE_SECRET_KEY", "sk_test")
os.environ.setdefault("ANTHROPIC_API_KEY", "sk-ant-test")
os.environ.setdefault("SLACK_WEBHOOK_URL", "https://hooks.slack.com/test")
os.environ.setdefault("SKIP_WC_HEALTHCHECK", "true")

sys.modules.setdefault("classifier", MagicMock())
sys.modules.setdefault("reply_generator", MagicMock())
sys.modules.setdefault("bq_logger", MagicMock())

import main  # noqa: E402

from slack_client import SlackClient  # noqa: E402


# ── _refund_outcome_status ──────────────────────────────────────────────────

def test_outcome_status_approved_when_reply_sent_and_yes():
    r = {"refund_reply_sent": True, "refund_decision": "YES"}
    assert main._refund_outcome_status(r) == "success_refund_approved"


def test_outcome_status_denied_when_reply_sent_and_no():
    r = {"refund_reply_sent": True, "refund_decision": "NO"}
    assert main._refund_outcome_status(r) == "success_refund_denied"


def test_outcome_status_skipped_when_no_reply_sent():
    # Drafted-only / shadow / left-to-human → stays skipped_refund_request.
    assert main._refund_outcome_status({"refund_decision": "YES"}) == "skipped_refund_request"
    assert main._refund_outcome_status({}) == "skipped_refund_request"


# ── _refund_sum_string ──────────────────────────────────────────────────────

def test_refund_sum_single_charge_uses_computed_amount():
    d = SimpleNamespace(computed_amount="5490", candidate_charges="ch_1:5490:2026-07-28")
    assert main._refund_sum_string(d) == "5490"


def test_refund_sum_multi_charge_joined_with_plus():
    d = SimpleNamespace(computed_amount="199",
                        candidate_charges="ch_1:199:2026-07-28;ch_2:1990:2026-07-28")
    assert main._refund_sum_string(d) == "199+1990"


def test_refund_sum_empty_when_no_amount():
    d = SimpleNamespace(computed_amount=None, candidate_charges=None)
    assert main._refund_sum_string(d) == ""


# ── _build_refund_fields ────────────────────────────────────────────────────

def test_build_refund_fields_approved_full_set_from_charge():
    # Approved renewal on wwpersonalitytest JP with a cross-sale on the account.
    fields = main._build_refund_fields(
        approved=True, sum_text="5490", currency="JPY",
        host="jp.wwpersonalitytest.com", host_brand="wwpersonalitytest",
        cross_sale=True, provider="Stripe", charge_type="renewal")
    assert fields[main._ZENDESK_TOPIC_FIELD_ID] == "refund"
    assert fields[main._ZENDESK_REFUND_STATUS_FIELD_ID] == "refund_approved"
    assert fields[main._ZENDESK_REFUND_SUM_FIELD_ID] == "5490"
    assert fields[main._ZENDESK_CURRENCY_FIELD_ID] == "jpy"
    assert fields[main._ZENDESK_COUNTRY_FIELD_ID] == "jp"          # from host locale
    assert fields[main._ZENDESK_PROCESSOR_FIELD_ID] == "stripe"    # from provider
    assert fields[main._ZENDESK_REGISTERED_FIELD_ID] == "pt_cross" # brand + cross_sale
    assert fields[main._ZENDESK_REFUND_TYPE_FIELD_ID] == "sub"     # renewal → Sub


def test_build_refund_fields_denied_leaves_sum_and_currency_out():
    fields = main._build_refund_fields(
        approved=False, sum_text="", currency="JPY",
        host="", host_brand="", cross_sale=False, provider="", charge_type="")
    assert fields[main._ZENDESK_REFUND_STATUS_FIELD_ID] == "refund_denied"
    assert main._ZENDESK_REFUND_SUM_FIELD_ID not in fields   # denied → no sum
    assert main._ZENDESK_CURRENCY_FIELD_ID not in fields
    assert main._ZENDESK_COUNTRY_FIELD_ID not in fields      # no host → no country
    assert main._ZENDESK_PROCESSOR_FIELD_ID not in fields    # unknown → skipped
    assert main._ZENDESK_REGISTERED_FIELD_ID not in fields


def test_refund_field_derivers():
    # host → locale → language / country
    assert main._host_locale("jp.wwpersonalitytest.com") == "jp"
    assert main._host_locale("16types.ai/ja") == "ja"
    assert main._host_locale("16types.ai") == ""
    assert main._locale_lang("de.wwiqtest.com") == "DE"
    assert main._locale_country("kor.wwiqtest.com") == "kr"
    # Processor
    assert main._processor_value("Stripe") == "stripe"
    assert main._processor_value("", paypal_order_id="PO-1") == "paypal"
    assert main._processor_value("weird") == ""
    # Registered (base vs +Cross)
    assert main._registered_value("16types", True) == "16_types_test_cross"
    assert main._registered_value("16types", False) == "16_types_test"
    assert main._registered_value("iqbooster", True) == ""      # no option → human
    # Refund Type (brand-mapped)
    assert main._refund_type_value("renewal", "wwpersonalitytest") == "sub"
    assert main._refund_type_value("cross_sale", "16personas") == "16persons_report"
    assert main._refund_type_value("cross_sale", "wwiqtest") == "iq_test_report"
    assert main._refund_type_value("first_sale", "iqpro") == "iq_test_certificate"
    assert main._refund_type_value("cross_sale", "iqbooster") == "iq_test_report"


def test_reply_solve_and_set_fields_is_one_atomic_put_with_tags():
    from zendesk_client import ZendeskClient
    zc = ZendeskClient("sub", "e@e.com", "tok", dry_run=False)
    with patch.object(zc, "_request_with_retry") as req:
        zc.reply_solve_and_set_fields(
            "5", "hello", {111: "a", 222: "", 333: "b"}, additional_tags=["bot_handled"])
    req.assert_called_once()   # ONE PUT: reply + solve + fields + tag together
    body = req.call_args.kwargs["json"]["ticket"]
    assert body["status"] == "solved"
    assert body["comment"] == {"body": "hello", "public": True}
    assert {e["id"]: e["value"] for e in body["custom_fields"]} == {111: "a", 333: "b"}
    # bot_handled added via additional_tags (ADD, never replace) — no separate
    # add_tag POST that could race and wipe the field-tags (#171200).
    assert body["additional_tags"] == ["bot_handled"]


# ── #3 charge-host → product/link brand ─────────────────────────────────────

def test_host_to_brand_maps_known_domains():
    assert main._host_to_brand("jp.wwpersonalitytest.com") == "wwpersonalitytest"
    assert main._host_to_brand("16types.ai/ja") == "16types"
    assert main._host_to_brand("16persons.com") == "16personas"
    assert main._host_to_brand("de.wwiqtest.com") == "wwiqtest"
    assert main._host_to_brand("iqpro.ai") == "iqpro"
    assert main._host_to_brand("iqbooster.org/ko") == "iqbooster"
    assert main._host_to_brand("") == ""
    assert main._host_to_brand("unknown.example.com") == ""


def test_charge_host_brand_prefers_candidate_charge():
    # Cross-site: contacted brand differs from the charge host → follow the charge.
    nexus = {"host": "16persons.com", "charges": [
        {"charge_id": "ch_1", "host": "16persons.com", "type": "cross_sale"},
        {"charge_id": "ch_2", "host": "16types.ai/ja", "type": "subscription"},
    ]}
    assert main._charge_host_brand(nexus, "ch_2") == "16types"


def test_charge_host_brand_falls_back_to_any_then_data_then_empty():
    # No candidate match → first charge with a host.
    assert main._charge_host_brand(
        {"charges": [{"charge_id": "x", "host": "iqpro.ai"}]}, "nope") == "iqpro"
    # No charge host → subscription-level host.
    assert main._charge_host_brand({"host": "16types.ai", "charges": []}, "") == "16types"
    # No host anywhere (old records) → "" so caller uses the Zendesk brand.
    assert main._charge_host_brand({"charges": [{"charge_id": "x"}]}, "x") == ""
    assert main._charge_host_brand({}, "") == ""


# ── Slack card ──────────────────────────────────────────────────────────────

def _blocks_for(result):
    sc = SlackClient(bot_token="xoxb-test", target_email="ops@test.com", dry_run=True)
    captured = {}
    with patch.object(sc, "_post", lambda text, blocks=None: captured.update(text=text, blocks=blocks) or True):
        sc.notify_ticket_result("999", result, "wwiqtest", shadow=False)
    return captured


def test_slack_live_approved_shows_refunded_line_brand_and_reply_preview():
    result = {
        "status": "success_refund_approved", "intent": "REFUND_REQUEST",
        "email": "a@b.com", "language": "JA",
        "refund_decision": "YES", "refund_reason_code": "WOULD_BE_REFUNDED",
        "refund_amount": "5490", "refund_currency": "JPY", "refund_brand": "iqpro",
        "refunds_enabled_for_brand": True,
        "refund_reply_sent": True, "reply_text": "こんにちは。返金いたします。",
    }
    blob = str(_blocks_for(result)["blocks"])
    assert "REFUNDED" in blob and "approved & money moved" in blob
    assert "Brand:" in blob and "iqpro" in blob and "refunds ✅ON" in blob
    assert "Reply preview" in blob


def test_slack_denied_sent_shows_denied_line():
    result = {
        "status": "success_refund_denied", "intent": "REFUND_REQUEST",
        "email": "a@b.com", "refund_decision": "NO",
        "refund_reason_code": "OUTSIDE_REFUND_WINDOW", "refund_brand": "iqpro",
        "refunds_enabled_for_brand": True, "refund_reply_sent": True,
        "reply_text": "返金はできません。",
    }
    blob = str(_blocks_for(result)["blocks"])
    assert "Refund DENIED" in blob


def test_slack_suppressed_shows_held_for_human_with_reason():
    # Guard 2b: enabled brand, engine would refund, but cross-sale ambiguity → human.
    result = {
        "status": "skipped_refund_request", "intent": "REFUND_REQUEST",
        "email": "a@b.com", "refund_decision": "YES",
        "refund_reason_code": "WOULD_BE_REFUNDED", "refund_amount": "5490",
        "refund_currency": "JPY", "refund_brand": "iqpro",
        "refunds_enabled_for_brand": True,
        "refund_reply_suppressed": "cross_sale_ambiguous_route",
        "refund_human_message": "Latest subscription 5490 JPY is within the window → would refund this charge.",
    }
    blob = str(_blocks_for(result)["blocks"])
    assert "Held for a human" in blob
    assert "cross-sale" in blob                       # explains WHY
    assert "refunds ✅ON" in blob                      # brand is live
    # No misleading "would-be YES" / "would refund this charge" on a HELD ticket (#170821)
    assert "would-be" not in blob.lower()
    assert "would refund this charge" not in blob


# ── #2 cancellation-after-refund + #4 solve ─────────────────────────────────

def test_cancel_after_refund_already_cancelled_skips_wc():
    r = {}
    w = MagicMock()
    with patch.object(main, "woo", w):
        ok = main._cancel_subscription_after_refund(
            "1", {"was_already_cancelled": True, "subscription_id": 9}, r)
    assert ok is True and r["refund_subscription_cancelled"] is True
    w._cancel_sub_by_id.assert_not_called()          # already cancelled → no WC call


def test_cancel_after_refund_calls_wc_by_id():
    r = {}
    w = MagicMock()
    w._cancel_sub_by_id.return_value = {"cancelled": True, "status": "success"}
    with patch.object(main, "woo", w):
        ok = main._cancel_subscription_after_refund("1", {"subscription_id": 555}, r)
    assert ok is True and r["refund_subscription_cancelled"] is True
    w._cancel_sub_by_id.assert_called_once_with(555)


def test_cancel_after_refund_failure_returns_false():
    r = {}
    w = MagicMock()
    w._cancel_sub_by_id.return_value = {"cancelled": False, "status": "error"}
    with patch.object(main, "woo", w):
        ok = main._cancel_subscription_after_refund("1", {"subscription_id": 555}, r)
    assert ok is False and r["refund_subscription_cancelled"] is False


def test_cancel_after_refund_no_subid_returns_false():
    r = {}
    with patch.object(main, "woo", MagicMock()):
        ok = main._cancel_subscription_after_refund("1", {}, r)
    assert ok is False


def test_has_explicit_refund_demand():
    # Explicit "money back" demands → True
    assert main._has_explicit_refund_demand("返金してください")
    assert main._has_explicit_refund_demand("Please refund me")
    assert main._has_explicit_refund_demand("환불 요청합니다")
    # Unrecognised-charge inquiry WITHOUT a refund word → False (= explanation)
    assert not main._has_explicit_refund_demand("テストの後に身に覚えのない請求あり")
    assert not main._has_explicit_refund_demand("I don't recognize this charge")
    assert not main._has_explicit_refund_demand("")


def test_e2e_explanation_only_charge_is_not_auto_refunded():
    # Customer reported an unrecognised charge (would-be YES) but never asked for
    # a refund → suppressed to a human, NOT auto-refunded (#172054).
    result, zd = _drive_eval(
        "WOULD_BE_REFUNDED", True, ticket_text="身に覚えのない請求があります")
    assert not result.get("refund_reply_sent")           # no auto-refund reply
    assert result.get("refund_reply_suppressed") == "explanation_only_no_refund_demand"
    assert main._refund_outcome_status(result) == "skipped_refund_request"  # → human
    # internal note explains it's an explanation request
    assert any("EXPLANATION request" in str(c) for c in zd.add_internal_note.call_args_list)


def test_e2e_explicit_refund_demand_still_auto_refunds():
    # Same window decision but WITH a refund demand → normal auto-refund.
    result, zd = _drive_eval(
        "WOULD_BE_REFUNDED", True, ticket_text="身に覚えのない請求、返金してください")
    assert result.get("refund_reply_sent") is True
    assert result.get("refund_reply_suppressed") is None


def test_e2e_approved_solves_ticket_and_flags_unconfirmed_cancel():
    # In the harness nexus_data is None → cancel can't be confirmed → the bot
    # must (a) reply+solve+fields in the one atomic call, (b) leave a manual-
    # cancel warning note. solve_ticket is NOT called separately (folded in).
    result, zd = _drive_eval("WOULD_BE_REFUNDED", True)
    assert zd.reply_solve_and_set_fields.called
    zd.solve_ticket.assert_not_called()
    assert any("AUTO-CANCEL" in str(c) for c in zd.add_internal_note.call_args_list)


def test_slack_would_be_only_when_brand_not_enabled():
    result = {
        "status": "skipped_refund_request", "intent": "REFUND_REQUEST",
        "email": "a@b.com", "refund_decision": "YES",
        "refund_reason_code": "WOULD_BE_REFUNDED", "refund_amount": "29.99",
        "refund_currency": "EUR", "refund_brand": "iqbooster",
        "refunds_enabled_for_brand": False,
    }
    blob = str(_blocks_for(result)["blocks"])
    assert "Would-be only" in blob
    assert "refunds NOT enabled" in blob
    assert "iqbooster" in blob and "refunds ⭕off" in blob


# ── End-to-end wiring of _refund_would_be_eval (approved + denied) ──────────

def _fake_decision(reason_code, would_be, amount="5490", currency="JPY"):
    return SimpleNamespace(
        would_be_refunded=would_be, reason_code=reason_code,
        human_message="msg", guard_trail=[], computed_amount=amount,
        currency=currency, customer_stated_amount=None, customer_stated_amounts="",
        candidate_charge_id="ch_1", charge_type="subscription",
        candidate_charges=f"ch_1:{amount}:2026-07-28", refund_flow="flow1_subscription",
        source="wwiqtest", engine_version="test",
    )


def _drive_eval(reason_code, would_be, refund_ret=None, ticket_text="返金してください"):
    """Run _refund_would_be_eval live-enabled with all collaborators stubbed,
    returning (result, zendesk_mock). `refund_ret` overrides create_refund's
    return; `ticket_text` is the customer text (default has an explicit refund
    demand so the auto-refund is not suppressed)."""
    result = {}
    fake_reply_gen = SimpleNamespace(
        REFUND_AUTOREPLY_CODES={"WOULD_BE_REFUNDED", "OUTSIDE_REFUND_WINDOW"},
        generate_refund_reply=lambda rc, lang, data: "こんにちは。ご対応します。",
    )
    zd = MagicMock()
    rc_client = MagicMock()
    rc_client.is_configured.return_value = False           # skip charge-detail guard
    rc_client.create_refund.return_value = refund_ret if refund_ret is not None else {
        "status": "refunded", "executed": True, "refunded_amount": "5490"}
    with patch.object(main.refund_engine, "decide", return_value=_fake_decision(reason_code, would_be)), \
         patch.object(main, "reply_generator", fake_reply_gen), \
         patch.object(main, "refunds_enabled_for", return_value=True), \
         patch.object(main, "_refund_xhost", return_value="host"), \
         patch.object(main, "refund_client", rc_client), \
         patch.object(main, "refund_abuse", SimpleNamespace(check=lambda b, e: (True, ""))), \
         patch.object(main, "nexus_client", None), \
         patch.object(main, "zendesk", zd):
        main._refund_would_be_eval(
            "555", "cust@x.com", "REFUND_REQUEST",
            {"confidence": 0.95, "language": "JA"}, result,
            ticket_text=ticket_text, as_of_date="2026-07-30T00:00:00Z",
            brand="wwiqtest",
        )
    return result, zd


def test_e2e_approved_refund_reports_success_and_sets_fields():
    result, zd = _drive_eval("WOULD_BE_REFUNDED", True)
    assert result.get("refund_reply_sent") is True
    assert result.get("reply_text")                       # reply mirrored for Slack
    assert main._refund_outcome_status(result) == "success_refund_approved"
    # reply + solve + fields in ONE atomic call: (ticket_id, body, fields_dict)
    args = zd.reply_solve_and_set_fields.call_args.args
    fields = args[2]
    assert args[1]                                            # reply body present
    assert fields[main._ZENDESK_TOPIC_FIELD_ID] == "refund"
    assert fields[main._ZENDESK_REFUND_STATUS_FIELD_ID] == "refund_approved"
    assert fields[main._ZENDESK_REFUND_SUM_FIELD_ID] == "5490"
    assert fields[main._ZENDESK_CURRENCY_FIELD_ID] == "jpy"
    zd.solve_ticket.assert_not_called()                       # solve folded into the atomic PUT


def test_e2e_denied_refund_reports_success_denied_no_sum():
    result, zd = _drive_eval("OUTSIDE_REFUND_WINDOW", False)
    assert result.get("refund_reply_sent") is True
    assert main._refund_outcome_status(result) == "success_refund_denied"
    fields = zd.reply_solve_and_set_fields.call_args.args[2]
    assert fields[main._ZENDESK_REFUND_STATUS_FIELD_ID] == "refund_denied"
    assert main._ZENDESK_REFUND_SUM_FIELD_ID not in fields   # denied → no sum


# ── refund_failed: approved refund that could NOT be carried out ────────────

def test_outcome_status_failed_when_execution_attempted_but_not_sent():
    # API returned a non-refunded status → executed False, reply never sent.
    r = {"refund_decision": "YES", "refund_execution_status": "error"}
    assert main._refund_outcome_status(r) == "refund_failed"


def test_outcome_status_failed_covers_guard_blocks():
    for exec_s in ("skipped_no_xhost", "skipped_abuse_guard:daily_cap",
                   "skipped_llm_disambiguated", "dispute_open", "already_refunded"):
        r = {"refund_decision": "YES", "refund_execution_status": exec_s}
        assert main._refund_outcome_status(r) == "refund_failed", exec_s


def test_outcome_status_failed_when_money_moved_but_reply_failed():
    # post_reply raised after the money moved: executed but reply not sent.
    r = {"refund_decision": "YES", "refund_execution_status": "refunded",
         "refund_executed": True}
    assert main._refund_outcome_status(r) == "refund_failed"


def test_outcome_status_denied_not_attempted_stays_skipped():
    # A NO decision with no execution attempt is a plain skip, not a failure.
    assert main._refund_outcome_status(
        {"refund_decision": "NO"}) == "skipped_refund_request"


def test_e2e_approved_refund_execution_fails_reports_refund_failed():
    result, zd = _drive_eval(
        "WOULD_BE_REFUNDED", True,
        refund_ret={"status": "error", "executed": False, "message": "gateway 500"})
    assert not result.get("refund_reply_sent")
    assert result.get("refund_execution_status") == "error"
    assert main._refund_outcome_status(result) == "refund_failed"
    # No customer reply, no refund topic fields written on a failed execution.
    zd.post_reply.assert_not_called()
    fields = {int(c.args[1]): c.args[2] for c in zd.set_custom_field.call_args_list}
    assert int(main._ZENDESK_REFUND_STATUS_FIELD_ID) not in fields


def test_slack_refund_failed_shows_needs_human_line():
    result = {
        "status": "refund_failed", "intent": "REFUND_REQUEST", "email": "a@b.com",
        "refund_decision": "YES", "refund_reason_code": "WOULD_BE_REFUNDED",
        "refund_amount": "5490", "refund_currency": "JPY",
        "refund_execution_status": "error",
    }
    blob = str(_blocks_for(result)["blocks"])
    assert "COULD NOT be completed" in blob
    assert "execution status: `error`" in blob
    assert "Would be refunded" not in blob
