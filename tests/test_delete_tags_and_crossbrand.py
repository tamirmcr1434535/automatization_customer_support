"""Four live defects reported by Anna on 2026-09-09/10.

  #193095 — DELETE_ALL_DATA was silently skipped
  #193082 — a skipped ticket carried no marker at all
  #192984 — a refund request answered as a plain cancellation
  #191696 — a cancellation reply naming the wrong brand's product

Each test names the ticket it comes from, because each fix is a rule about
production behaviour, not about the shape of the code.
"""
import os
import sys
from unittest.mock import MagicMock, patch

os.environ.setdefault("SKIP_WC_HEALTHCHECK", "true")
os.environ.setdefault("ZENDESK_SUBDOMAIN", "wwiqtest")
os.environ.setdefault("ZENDESK_EMAIL", "bot@test.com")
os.environ.setdefault("ZENDESK_API_TOKEN", "token")
os.environ.setdefault("WOO_SITE_URL", "https://iqbooster.org")
os.environ.setdefault("WOO_CONSUMER_KEY", "ck_test")
os.environ.setdefault("WOO_CONSUMER_SECRET", "cs_test")
os.environ.setdefault("STRIPE_SECRET_KEY", "sk_test")
os.environ.setdefault("ANTHROPIC_API_KEY", "sk-ant-test")
os.environ.setdefault("SLACK_WEBHOOK_URL", "https://hooks.slack.com/test")

sys.modules.setdefault("classifier", MagicMock())
sys.modules.setdefault("reply_generator", MagicMock())
sys.modules.setdefault("bq_logger", MagicMock())

import main  # noqa: E402

main.reply_generator.REFUND_APPROVE_CODES = {
    "WOULD_BE_REFUNDED", "WOULD_BE_REFUNDED_LAST_ONLY"}

from tests.conftest import make_zendesk_ticket  # noqa: E402


def _classification(intent="TRIAL_CANCELLATION", confidence=0.92, language="EN"):
    return {
        "intent": intent,
        "confidence": confidence,
        "language": language,
        "chargeback_risk": False,
        "reasoning": "test",
    }


def _setup_zd(mock_zd, ticket=None):
    """Same neutral harness as tests/test_main_flow.py — a bare MagicMock is
    truthy, so every guard that reads a Zendesk helper has to be given a real
    value or it fires and skips the ticket before the code under test runs."""
    ticket = ticket or make_zendesk_ticket()
    mock_zd.get_ticket.return_value = ticket
    mock_zd.get_ticket_tags.return_value = list(ticket.get("tags", []))
    mock_zd.get_all_customer_comments_text.return_value = ""
    mock_zd.get_first_customer_comment.return_value = ""
    mock_zd.last_public_comment_is_from_agent.return_value = False
    mock_zd.find_active_tickets_for_email.return_value = []
    mock_zd.find_active_tickets_for_requester.return_value = []
    mock_zd.count_bot_replies.return_value = 0


# ── #193095: DELETE_ALL_DATA is a deletion request, not a nothing ────────── #

def test_delete_all_data_is_a_delete_intent():
    """The classifier's own name for the intent must reach the delete branch."""
    assert "DELETE_ALL_DATA" in main._DELETE_INTENTS
    assert "DELETE_ACCOUNT" in main._DELETE_INTENTS


@patch.object(main, "log_result")
@patch.object(main, "classify_ticket",
              return_value=_classification(intent="DELETE_ALL_DATA",
                                           confidence=0.95, language="JP"))
@patch.object(main, "zendesk")
def test_193095_delete_all_data_escalates_and_tags(mock_zd, mock_cls, mock_log):
    """The live ticket: cancel the subscription AND delete every account detail.

    Before the fix this returned skipped_not_handled with no tag, no note and
    no agent — 1107 times in the 90 days to 2026-09-10.
    """
    _setup_zd(mock_zd, ticket=make_zendesk_ticket(
        subject='IQ Booster "Help Form"',
        body="サブスクリプションの停止と、全てのアカウント情報を削除してください。",
    ))
    result = main._process("193095")

    assert result["status"] == "escalated_delete_account"
    tagged = [c.args[1] for c in mock_zd.add_tag.call_args_list]
    assert "delete_account" in tagged
    assert "needs_manual_review" in tagged
    assert "bot_handled" in tagged
    mock_zd.add_internal_note.assert_called_once()


# ── #193082: a skip must still leave a trace ─────────────────────────────── #

@patch.object(main, "log_result")
@patch.object(main, "classify_ticket",
              return_value=_classification(intent="GENERAL_QUESTION",
                                           confidence=0.82, language="DE"))
@patch.object(main, "zendesk")
def test_193082_skipped_ticket_is_marked(mock_zd, mock_cls, mock_log):
    """A German customer asking for their results overview — correctly skipped,
    but the agent must be able to see the bot triaged it."""
    _setup_zd(mock_zd, ticket=make_zendesk_ticket(
        subject='WW IQ TEST "Contact Form"',
        body="Bitte senden Sie mir meine Leistungsübersicht zu.",
    ))
    result = main._process("193082")

    assert result["status"] == "skipped_not_handled"
    tagged = [c.args[1] for c in mock_zd.add_tag.call_args_list]
    assert "bot_skipped" in tagged


@patch.object(main, "log_result")
@patch.object(main, "classify_ticket",
              return_value=_classification(intent="GENERAL_QUESTION"))
@patch.object(main, "zendesk")
def test_skip_marker_is_written_once(mock_zd, mock_cls, mock_log):
    """Every add_tag fires another Zendesk webhook, and this marker does not
    short-circuit the next run — so a ticket that already carries it must not
    be written again, or the re-fires never stop."""
    _setup_zd(mock_zd, ticket=make_zendesk_ticket(
        tags=["automation_test", "bot_skipped"]))
    result = main._process("193082")

    assert result["status"] == "skipped_not_handled"
    assert [c for c in mock_zd.add_tag.call_args_list
            if c.args[1] == "bot_skipped"] == []


@patch.object(main, "log_result")
@patch.object(main, "classify_ticket",
              return_value=_classification(intent="GENERAL_QUESTION"))
@patch.object(main, "zendesk")
def test_skip_marker_is_not_the_idempotency_lock(mock_zd, mock_cls, mock_log):
    """`bot_handled` blocks all re-processing AND arms the 24h per-requester
    spam guard. A skip must never set it, or a customer whose question was
    skipped could no longer get an automatic cancellation."""
    _setup_zd(mock_zd)
    main._process("193082")

    tagged = [c.args[1] for c in mock_zd.add_tag.call_args_list]
    assert "bot_handled" not in tagged


# ── #192984: the amount lived in the subject ─────────────────────────────── #

_SUBJECT_192984 = "1990원 결제했는데 14990은 뭐죠?"
_BODY_192984 = "당장 취소해주세요!!!"


def test_192984_amount_is_only_in_the_subject():
    """Both halves of the premise: the currency IS recognised, and the body
    alone carries nothing — so scanning the body only had to miss it."""
    assert main._contains_amount_with_currency(_SUBJECT_192984) is True
    assert main._contains_amount_with_currency(_BODY_192984) is False
    assert main._contains_cancel_signal(_BODY_192984) is True
    # The customer never wrote 환불 — this is why refund_ask_in_text was False.
    assert main._contains_refund_request(
        _SUBJECT_192984 + " " + _BODY_192984) is False


@patch.object(main, "log_result")
@patch.object(main, "woo")
@patch.object(main, "classify_ticket",
              return_value=_classification(intent="REFUND_REQUEST",
                                           confidence=0.88, language="KR"))
@patch.object(main, "zendesk")
def test_192984_charge_named_in_subject_blocks_the_cancel_remap(
    mock_zd, mock_cls, mock_woo, mock_log
):
    """Paid 1,990 KRW, charged 14,990, "what is this?" — a money question.

    The bot must not cancel the trial and send a confirmation that says
    nothing about the 14,990.
    """
    _setup_zd(mock_zd, ticket=make_zendesk_ticket(
        subject=_SUBJECT_192984, body=_BODY_192984))
    result = main._process("192984")

    assert result["status"] != "success"
    assert mock_woo.cancel_subscription.called is False
    assert mock_woo.cancel_subscription_via_nexus.called is False


@patch.object(main, "log_result")
@patch.object(main, "validate_reply", return_value=(True, ""))
@patch.object(main, "generate_reply", return_value="Cancelled.")
@patch.object(main, "woo")
@patch.object(main, "classify_ticket",
              return_value=_classification(intent="REFUND_REQUEST",
                                           confidence=0.88, language="EN"))
@patch.object(main, "zendesk")
def test_180292_remap_still_fires_without_an_amount(
    mock_zd, mock_cls, mock_woo, mock_reply, mock_validate, mock_log
):
    """Regression guard for the fix this one narrows.

    #180292: "I already asked to cancel and I'm still being charged" with no
    refund ask and no amount must STILL run a real cancellation — leaving the
    subscription running was the original incident.
    """
    _setup_zd(mock_zd, ticket=make_zendesk_ticket(
        subject="Cancellation",
        body="I already asked you to cancel my subscription and it is still "
             "active. Please cancel it now.",
    ))
    mock_woo.cancel_subscription.return_value = {
        "status": "trial_cancelled", "email": "user@example.com",
        "cancelled": True, "subscription_type": "trial",
        "subscription_id": 101, "plan": "IQ Test Monthly",
        "source": "woocommerce",
    }
    result = main._process("180292")
    assert result["status"] == "success"


# ── #191696: name the product the customer actually owns ─────────────────── #

def test_191696_product_follows_the_subscription_not_the_inbox():
    """Emailed IQ Pro, owns a 16 Types Growth Plan."""
    brand, via = main._cancelled_product_brand(
        {"nexus_host": "16types.ai/ja", "plan": "16 Types Growth Plan"},
        zendesk_brand="iqpro",
    )
    assert (brand, via) == ("16types", "nexus_host")


def test_plan_name_resolves_the_brand_when_nexus_has_no_host():
    """Nexus returns host=None on older WooCommerce / PayPal records."""
    brand, via = main._cancelled_product_brand(
        {"nexus_host": "", "plan": "16 Types Growth Plan"},
        zendesk_brand="iqpro",
    )
    assert (brand, via) == ("16types", "plan_name")


def test_unknown_plan_falls_back_to_the_contacted_brand():
    """An unrecognised plan name must never be quoted at the customer."""
    brand, via = main._cancelled_product_brand(
        {"nexus_host": "", "plan": "IQ Test Subscription"},
        zendesk_brand="wwiqtest",
    )
    assert (brand, via) == ("wwiqtest", "zendesk_brand")


@patch.object(main, "log_result")
@patch.object(main, "validate_reply", return_value=(True, ""))
@patch.object(main, "generate_reply", return_value="Cancelled.")
@patch.object(main, "zendesk")
def test_191696_reply_gets_the_right_product(
    mock_zd, mock_reply, mock_validate, mock_log
):
    cancel_result = {
        "status": "subscription_cancelled", "cancelled": True,
        "subscription_type": "subscription", "subscription_id": 4241312,
        "plan": "16 Types Growth Plan", "source": "woocommerce",
        "nexus_host": "16types.ai", "order_count": 1,
    }
    main._finish_cancellation(
        "191696", "Test User", "JA", "SUB_CANCELLATION",
        cancel_result, {}, zendesk_brand="iqpro",
    )

    # The reply names the plan the customer owns, not the deployment default.
    assert cancel_result["brand_phrase"] == "16 Types Growth Plan"
    assert mock_reply.call_args.kwargs["cancel_result"]["brand_phrase"] == \
        "16 Types Growth Plan"

    # No cross-brand warning note: the reply is correct, so there is nothing
    # for an agent to do. The warning is reserved for the case where we could
    # NOT name the product — see tests/test_startup_alert_and_note_noise.py.
    note = " ".join(str(c.args[1]) for c in mock_zd.add_internal_note.call_args_list)
    assert "no confirmed product name" not in note


@patch.object(main, "log_result")
@patch.object(main, "validate_reply", return_value=(True, ""))
@patch.object(main, "generate_reply", return_value="Cancelled.")
@patch.object(main, "zendesk")
def test_same_brand_cancellation_is_untouched(
    mock_zd, mock_reply, mock_validate, mock_log
):
    """No mismatch → no phrase override, no note, wording exactly as before."""
    cancel_result = {
        "status": "trial_cancelled", "cancelled": True,
        "subscription_type": "trial", "subscription_id": 1,
        "plan": "IQ Test Subscription", "source": "woocommerce",
        "nexus_host": "", "order_count": 1,
    }
    main._finish_cancellation(
        "1001", "Test User", "EN", "TRIAL_CANCELLATION",
        cancel_result, {}, zendesk_brand="wwiqtest",
    )
    assert "brand_phrase" not in cancel_result
    notes = " ".join(str(c.args[1]) for c in mock_zd.add_internal_note.call_args_list)
    assert "wrote to" not in notes
