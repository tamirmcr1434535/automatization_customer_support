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


# ── _set_refund_fields_for_ticket ───────────────────────────────────────────

def test_set_fields_approved_sets_topic_status_sum_currency_country():
    zd = MagicMock()
    with patch.object(main, "zendesk", zd), \
         patch.object(main, "_set_country_for_ticket") as country:
        main._set_refund_fields_for_ticket(
            "111", approved=True, sum_text="5490", currency="JPY", country="Japan")
    set_calls = {int(c.args[1]): c.args[2] for c in zd.set_custom_field.call_args_list}
    assert set_calls[int(main._ZENDESK_TOPIC_FIELD_ID)] == "refund"
    assert set_calls[int(main._ZENDESK_REFUND_STATUS_FIELD_ID)] == "refund_approved"
    assert set_calls[int(main._ZENDESK_REFUND_SUM_FIELD_ID)] == "5490"
    assert set_calls[int(main._ZENDESK_CURRENCY_FIELD_ID)] == "jpy"   # lowercased tag
    country.assert_called_once_with("111", "Japan")


def test_set_fields_denied_leaves_sum_and_currency_empty():
    zd = MagicMock()
    with patch.object(main, "zendesk", zd), \
         patch.object(main, "_set_country_for_ticket"):
        main._set_refund_fields_for_ticket(
            "222", approved=False, sum_text="", currency="JPY", country="")
    set_calls = {int(c.args[1]): c.args[2] for c in zd.set_custom_field.call_args_list}
    assert set_calls[int(main._ZENDESK_REFUND_STATUS_FIELD_ID)] == "refund_denied"
    # Denied → no Sum / Currency written (mirrors how agents fill the form)
    assert int(main._ZENDESK_REFUND_SUM_FIELD_ID) not in set_calls
    assert int(main._ZENDESK_CURRENCY_FIELD_ID) not in set_calls


def test_set_fields_skips_country_when_unknown():
    zd = MagicMock()
    with patch.object(main, "zendesk", zd), \
         patch.object(main, "_set_country_for_ticket") as country:
        main._set_refund_fields_for_ticket(
            "333", approved=True, sum_text="10", currency="EUR", country="")
    country.assert_not_called()


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
    }
    blob = str(_blocks_for(result)["blocks"])
    assert "Held for a human" in blob
    assert "cross-sale" in blob                       # explains WHY
    assert "refunds ✅ON" in blob                      # brand is live


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


def _drive_eval(reason_code, would_be, refund_ret=None):
    """Run _refund_would_be_eval live-enabled with all collaborators stubbed,
    returning (result, zendesk_mock). `refund_ret` overrides create_refund's
    return (default = a successful money move)."""
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
            ticket_text="返金してください", as_of_date="2026-07-30T00:00:00Z",
            brand="wwiqtest",
        )
    return result, zd


def test_e2e_approved_refund_reports_success_and_sets_fields():
    result, zd = _drive_eval("WOULD_BE_REFUNDED", True)
    assert result.get("refund_reply_sent") is True
    assert result.get("reply_text")                       # reply mirrored for Slack
    assert main._refund_outcome_status(result) == "success_refund_approved"
    fields = {int(c.args[1]): c.args[2] for c in zd.set_custom_field.call_args_list}
    assert fields[int(main._ZENDESK_TOPIC_FIELD_ID)] == "refund"
    assert fields[int(main._ZENDESK_REFUND_STATUS_FIELD_ID)] == "refund_approved"
    assert fields[int(main._ZENDESK_REFUND_SUM_FIELD_ID)] == "5490"
    assert fields[int(main._ZENDESK_CURRENCY_FIELD_ID)] == "jpy"


def test_e2e_denied_refund_reports_success_denied_no_sum():
    result, zd = _drive_eval("OUTSIDE_REFUND_WINDOW", False)
    assert result.get("refund_reply_sent") is True
    assert main._refund_outcome_status(result) == "success_refund_denied"
    fields = {int(c.args[1]): c.args[2] for c in zd.set_custom_field.call_args_list}
    assert fields[int(main._ZENDESK_REFUND_STATUS_FIELD_ID)] == "refund_denied"
    assert int(main._ZENDESK_REFUND_SUM_FIELD_ID) not in fields   # denied → no sum


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
