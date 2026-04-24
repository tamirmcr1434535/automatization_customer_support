"""
Unit tests for main._process() and main._cancel()
===================================================
All external clients and heavy modules are mocked before main is imported,
so no real API calls or network connections are made.

Scenarios:
  A. Ticket not found in Zendesk
  B. TEST_MODE=true, ticket missing automation_test tag → skipped
  C. Intent not handled (REFUND_REQUEST) → skipped_not_handled
  D. Low confidence → escalated_low_confidence
  E. WooCommerce handles trial → success, cancel_source=woocommerce
  F. WooCommerce not_found → Stripe success → cancel_source=stripe
  G. WooCommerce no_active_sub → Stripe success → cancel_source=stripe
  H. Full SUB_CANCELLATION via WooCommerce paid sub → success
  I. Not found anywhere → ask for card digits, ticket set to pending
  J. Not found anywhere → awaiting_card_digits tag added, ticket NOT solved
  K. Card digits request called with correct ticket_id
"""

import os
import sys
from unittest.mock import MagicMock, patch

# ── Set env vars before any import ───────────────────────────────────────────
os.environ.setdefault("ZENDESK_SUBDOMAIN", "wwiqtest")
os.environ.setdefault("ZENDESK_EMAIL", "bot@test.com")
os.environ.setdefault("ZENDESK_API_TOKEN", "token")
os.environ.setdefault("WOO_SITE_URL", "https://iqbooster.org")
os.environ.setdefault("WOO_CONSUMER_KEY", "ck_test")
os.environ.setdefault("WOO_CONSUMER_SECRET", "cs_test")
os.environ.setdefault("STRIPE_SECRET_KEY", "sk_test")
os.environ.setdefault("ANTHROPIC_API_KEY", "sk-ant-test")
os.environ.setdefault("SLACK_WEBHOOK_URL", "https://hooks.slack.com/test")

# ── Pre-mock heavy modules so importing main doesn't trigger real connections ─
# classifier.py, reply_generator.py and bq_logger.py create real API clients
# at module level (Anthropic, BigQuery). We replace them with MagicMock BEFORE
# importing main so no network calls or missing-package errors occur.
sys.modules.setdefault("classifier", MagicMock())
sys.modules.setdefault("reply_generator", MagicMock())
sys.modules.setdefault("bq_logger", MagicMock())

import main  # noqa: E402 — must come after sys.modules patching

from tests.conftest import make_zendesk_ticket  # noqa: E402


# ── Helpers ──────────────────────────────────────────────────────────────────

def _classification(intent="TRIAL_CANCELLATION", confidence=0.92, language="EN"):
    return {
        "intent": intent,
        "confidence": confidence,
        "language": language,
        "chargeback_risk": False,
        "reasoning": "Customer wants to cancel.",
    }


def _woo_trial(email="user@example.com"):
    return {
        "status": "trial_cancelled", "email": email,
        "cancelled": True, "subscription_type": "trial",
        "subscription_id": 101, "plan": "IQ Test Monthly",
        "source": "woocommerce",
    }


def _woo_not_found(email="user@example.com"):
    return {"status": "not_found", "email": email, "cancelled": False}


def _woo_no_active(email="user@example.com"):
    return {"status": "no_active_sub", "email": email, "cancelled": False}


def _stripe_cancelled(email="user@example.com"):
    return {
        "status": "cancelled", "email": email,
        "subscription_id": "sub_abc", "plan": "IQ Test",
        "cancelled": True, "source": "stripe",
        "subscription_type": "subscription",
    }


def _stripe_not_found(email="user@example.com"):
    return {"status": "not_found", "email": email, "cancelled": False}


def _setup_zd(mock_zd, ticket=None, agent_replied=False):
    """
    Configure common ZendeskClient mock defaults.

    last_public_comment_is_from_agent must be explicitly set to False for
    most tests — a bare MagicMock() is truthy and would cause every ticket
    to be skipped with status='skipped_agent_already_replied'.
    """
    if ticket is None:
        ticket = make_zendesk_ticket()
    mock_zd.get_ticket.return_value = ticket
    mock_zd.last_public_comment_is_from_agent.return_value = agent_replied
    mock_zd.get_all_customer_comments_text.return_value = ""
    # No sibling tickets — avoid the merge-candidate guard kicking in
    # and skipping every test with 'skipped_merge_candidate'.
    mock_zd.find_active_tickets_for_email.return_value = []
    # Spam-guard needs an int return, not a MagicMock.
    mock_zd.count_bot_replies.return_value = 0
    # Race-condition guards read current tags — return the ticket's own tags.
    mock_zd.get_ticket_tags.return_value = list(ticket.get("tags", []))


# ── Tests ─────────────────────────────────────────────────────────────────── #

class TestProcess:

    # A. Ticket not found in Zendesk
    @patch.object(main, "log_result")
    @patch.object(main, "zendesk")
    def test_ticket_not_found(self, mock_zd, mock_log):
        mock_zd.get_ticket.return_value = None
        result = main._process("9999")
        assert result["status"] == "not_found"

    # B. TEST_MODE — missing tag
    @patch.object(main, "log_result")
    @patch.object(main, "zendesk")
    def test_test_mode_missing_tag(self, mock_zd, mock_log):
        _setup_zd(mock_zd, ticket=make_zendesk_ticket(tags=[]))
        with patch.object(main, "TEST_MODE", True):
            result = main._process("1001")
        assert result["status"] == "skipped_no_test_tag"

    # C. Unhandled intent
    @patch.object(main, "log_result")
    @patch.object(main, "classify_ticket", return_value=_classification(intent="REFUND_REQUEST"))
    @patch.object(main, "zendesk")
    def test_unhandled_intent(self, mock_zd, mock_cls, mock_log):
        _setup_zd(mock_zd)
        result = main._process("1002")
        assert result["status"] == "skipped_refund_request"

    # D. Low confidence → escalate
    @patch.object(main, "log_result")
    @patch.object(main, "classify_ticket", return_value=_classification(confidence=0.5))
    @patch.object(main, "zendesk")
    def test_low_confidence_escalated(self, mock_zd, mock_cls, mock_log):
        _setup_zd(mock_zd)
        result = main._process("1003")
        assert result["status"] == "escalated_low_confidence"
        mock_zd.add_tag.assert_any_call("1003", "bot_low_confidence")
        mock_zd.add_internal_note.assert_called_once()

    # D2. Cancellation + "what is this charge?" → escalate (bot can't explain)
    #
    # Real-case example: customer says "cancel my subscription, also what is
    # this 1990 yen charge?" — the cancel part is clear, but the bot can't
    # identify the unexplained charge. Must go to a human.
    @patch.object(main, "log_result")
    @patch.object(main, "classify_ticket", return_value=_classification())
    @patch.object(main, "zendesk")
    def test_explanation_question_escalated_jp(self, mock_zd, mock_cls, mock_log):
        _setup_zd(mock_zd, ticket=make_zendesk_ticket(
            subject="解約したい",
            body=(
                "199円払ったのですがこれはサブスクですか？もしそうなら解約してください。"
                "あと一緒に1990円引き落とされそうになったんですけどこれなに?"
            ),
        ))
        mock_zd.get_ticket_tags.return_value = ["automation_test"]
        result = main._process("1010")
        assert result["status"] == "escalated_explanation_question"
        mock_zd.add_tag.assert_any_call("1010", "needs_manual_review")
        mock_zd.set_open.assert_called_once_with("1010")
        mock_zd.post_reply.assert_not_called()

    # D3. Pure cancellation (no "what is this?" question) → still auto-cancels
    # Guards against the escalation rule being too broad.
    @patch.object(main, "log_result")
    @patch.object(main, "validate_reply", return_value=(True, ""))
    @patch.object(main, "generate_reply", return_value="Your trial has been cancelled.")
    @patch.object(main, "woo")
    @patch.object(main, "classify_ticket",
                  return_value=_classification(language="JP"))
    @patch.object(main, "zendesk")
    def test_pure_cancellation_still_auto_cancels(
        self, mock_zd, mock_cls, mock_woo, mock_reply, mock_validate, mock_log
    ):
        _setup_zd(mock_zd, ticket=make_zendesk_ticket(
            subject="解約について",
            body=(
                "結果を購入しましたが、もうキャンセルしたいです。"
                "これ以上支払いをしないようにしたいので教えて下さい。"
            ),
        ))
        mock_woo.cancel_subscription.return_value = _woo_trial()
        result = main._process("1011")
        assert result["status"] == "success"

    # D4. "I haven't received results" alongside cancel → escalate (JP)
    # Real ticket shape: customer paid for the IQ test, didn't get results,
    # asks to cancel. Bot can't know whether delivery actually failed.
    @patch.object(main, "log_result")
    @patch.object(main, "classify_ticket", return_value=_classification())
    @patch.object(main, "zendesk")
    def test_no_results_received_escalated_jp(self, mock_zd, mock_cls, mock_log):
        _setup_zd(mock_zd, ticket=make_zendesk_ticket(
            subject="解約したい",
            body=(
                "まだ結果を受け取っておらず、決済も完了しておりませんので、"
                "キャンセルしてください"
            ),
        ))
        result = main._process("1012")
        assert result["status"] == "escalated_no_results_received"
        mock_zd.add_tag.assert_any_call("1012", "needs_manual_review")
        mock_zd.set_open.assert_called_once_with("1012")
        mock_zd.post_reply.assert_not_called()

    # D5. "I did not consent to the charge" → refund keyword override
    # Real ticket: customer paid small charge voluntarily but refuses a
    # larger charge with "承諾しておりません" (did not consent). Must go
    # to a human as a refund/dispute ticket, not auto-cancel.
    @patch.object(main, "log_result")
    @patch.object(main, "classify_ticket", return_value=_classification())
    @patch.object(main, "zendesk")
    def test_did_not_consent_routed_to_refund(self, mock_zd, mock_cls, mock_log):
        _setup_zd(mock_zd, ticket=make_zendesk_ticket(
            subject="解約したい",
            body=(
                "IQテスト結果の199円は自らの意思でお支払いしましたが、"
                "フルレポート分1,990円は承諾しておりません"
            ),
        ))
        result = main._process("1013")
        assert result["status"] == "skipped_refund_request"
        mock_zd.post_reply.assert_not_called()

    # D6. Ukrainian "я не отримав результат" → escalate
    @patch.object(main, "log_result")
    @patch.object(main, "classify_ticket", return_value=_classification(language="UK"))
    @patch.object(main, "zendesk")
    def test_no_results_received_escalated_uk(self, mock_zd, mock_cls, mock_log):
        _setup_zd(mock_zd, ticket=make_zendesk_ticket(
            subject="Скасуйте підписку",
            body=(
                "Я ще не отримав(ла) результат і оплата також не була "
                "завершена, тому, будь ласка, скасуйте це."
            ),
        ))
        result = main._process("1014")
        assert result["status"] == "escalated_no_results_received"
        mock_zd.post_reply.assert_not_called()

    # D7. Successful cancel leaves an audit internal note on the ticket.
    @patch.object(main, "log_result")
    @patch.object(main, "validate_reply", return_value=(True, ""))
    @patch.object(main, "generate_reply", return_value="Your trial has been cancelled.")
    @patch.object(main, "woo")
    @patch.object(main, "classify_ticket", return_value=_classification())
    @patch.object(main, "zendesk")
    def test_success_posts_audit_internal_note(
        self, mock_zd, mock_cls, mock_woo, mock_reply, mock_validate, mock_log
    ):
        _setup_zd(mock_zd)
        mock_woo.cancel_subscription.return_value = _woo_trial()
        result = main._process("1015")
        assert result["status"] == "success"
        # An internal note must be posted even on the success path.
        mock_zd.add_internal_note.assert_called_once()
        args, _ = mock_zd.add_internal_note.call_args
        note = args[1]
        assert "Bot auto-cancelled" in note
        assert "TRIAL_CANCELLATION" in note

    # E. WooCommerce handles trial
    @patch.object(main, "log_result")
    @patch.object(main, "generate_reply", return_value="Your trial has been cancelled.")
    @patch.object(main, "woo")
    @patch.object(main, "classify_ticket", return_value=_classification())
    @patch.object(main, "zendesk")
    def test_woo_trial_success(self, mock_zd, mock_cls, mock_woo, mock_reply, mock_log):
        _setup_zd(mock_zd)
        mock_woo.cancel_subscription.return_value = _woo_trial()
        result = main._process("1004")
        assert result["status"] == "success"
        assert result["cancel_source"] == "woocommerce"

    # F. WooCommerce not_found → Stripe success
    @patch.object(main, "log_result")
    @patch.object(main, "generate_reply", return_value="Subscription cancelled.")
    @patch.object(main, "stripe_cli")
    @patch.object(main, "woo")
    @patch.object(main, "classify_ticket", return_value=_classification(intent="SUB_CANCELLATION"))
    @patch.object(main, "zendesk")
    def test_woo_not_found_stripe_fallback(
        self, mock_zd, mock_cls, mock_woo, mock_stripe, mock_reply, mock_log
    ):
        _setup_zd(mock_zd)
        mock_woo.cancel_subscription.return_value = _woo_not_found()
        mock_stripe.cancel_subscription.return_value = _stripe_cancelled()
        result = main._process("1005")
        assert result["status"] == "success"
        assert result["cancel_source"] == "stripe"

    # G. WooCommerce no_active_sub → Stripe success
    @patch.object(main, "log_result")
    @patch.object(main, "generate_reply", return_value="Subscription cancelled.")
    @patch.object(main, "stripe_cli")
    @patch.object(main, "woo")
    @patch.object(main, "classify_ticket", return_value=_classification(intent="SUB_CANCELLATION"))
    @patch.object(main, "zendesk")
    def test_woo_no_active_sub_stripe_fallback(
        self, mock_zd, mock_cls, mock_woo, mock_stripe, mock_reply, mock_log
    ):
        _setup_zd(mock_zd)
        mock_woo.cancel_subscription.return_value = _woo_no_active()
        mock_stripe.cancel_subscription.return_value = _stripe_cancelled()
        result = main._process("1006")
        assert result["cancel_source"] == "stripe"
        assert result["status"] == "success"

    # H. Full SUB_CANCELLATION via WooCommerce paid sub
    @patch.object(main, "log_result")
    @patch.object(main, "generate_reply", return_value="Your subscription has been cancelled.")
    @patch.object(main, "woo")
    @patch.object(main, "classify_ticket",
                  return_value=_classification(intent="SUB_CANCELLATION", language="JP"))
    @patch.object(main, "zendesk")
    def test_sub_cancellation_woo_paid(self, mock_zd, mock_cls, mock_woo, mock_reply, mock_log):
        _setup_zd(mock_zd)
        mock_woo.cancel_subscription.return_value = {
            "status": "subscription_cancelled", "email": "user@example.com",
            "cancelled": True, "subscription_type": "subscription",
            "subscription_id": 202, "plan": "IQ Test Monthly", "source": "woocommerce",
        }
        result = main._process("1007")
        assert result["status"] == "success"
        assert result["cancel_source"] == "woocommerce"
        mock_zd.post_reply.assert_called_once()
        mock_zd.solve_ticket.assert_called_once_with("1007")

    # I. Not found anywhere → ask for card digits, ticket set to pending
    @patch.object(main, "log_result")
    @patch.object(main, "slack")
    @patch.object(main, "stripe_cli")
    @patch.object(main, "woo")
    @patch.object(main, "classify_ticket", return_value=_classification())
    @patch.object(main, "zendesk")
    def test_not_found_anywhere_asks_card_digits(
        self, mock_zd, mock_cls, mock_woo, mock_stripe, mock_slack, mock_log
    ):
        _setup_zd(mock_zd, ticket=make_zendesk_ticket(email="ghost@example.com"))
        mock_woo.cancel_subscription.return_value = _woo_not_found("ghost@example.com")
        mock_stripe.cancel_subscription.return_value = _stripe_not_found("ghost@example.com")
        result = main._process("1008")
        assert result["status"] == "awaiting_card_digits"
        assert result["action"] == "asked_for_card_digits"
        mock_zd.post_reply_and_set_pending.assert_called_once()
        mock_zd.solve_ticket.assert_not_called()
        mock_slack.notify_manual_review.assert_not_called()

    # J. Not found anywhere → awaiting_card_digits tag added, ticket NOT solved
    @patch.object(main, "log_result")
    @patch.object(main, "slack")
    @patch.object(main, "stripe_cli")
    @patch.object(main, "woo")
    @patch.object(main, "classify_ticket", return_value=_classification())
    @patch.object(main, "zendesk")
    def test_not_found_ticket_stays_open(
        self, mock_zd, mock_cls, mock_woo, mock_stripe, mock_slack, mock_log
    ):
        _setup_zd(mock_zd)
        mock_woo.cancel_subscription.return_value = _woo_not_found()
        mock_stripe.cancel_subscription.return_value = _stripe_not_found()
        main._process("1009")
        mock_zd.solve_ticket.assert_not_called()
        # post_reply (solve-path) must NOT be called; post_reply_and_set_pending (card digits) IS
        mock_zd.post_reply.assert_not_called()
        mock_zd.post_reply_and_set_pending.assert_called_once()
        tags_added = [c.args[1] for c in mock_zd.add_tag.call_args_list]
        assert "awaiting_card_digits" in tags_added

    # K. Card digits request called with correct ticket_id
    @patch.object(main, "log_result")
    @patch.object(main, "slack")
    @patch.object(main, "stripe_cli")
    @patch.object(main, "woo")
    @patch.object(main, "classify_ticket", return_value=_classification())
    @patch.object(main, "zendesk")
    def test_card_digits_request_called_with_correct_args(
        self, mock_zd, mock_cls, mock_woo, mock_stripe, mock_slack, mock_log
    ):
        _setup_zd(mock_zd, ticket=make_zendesk_ticket(
            ticket_id="5555", email="specific@example.com"
        ))
        mock_woo.cancel_subscription.return_value = _woo_not_found("specific@example.com")
        mock_stripe.cancel_subscription.return_value = _stripe_not_found("specific@example.com")
        result = main._process("5555")
        assert result["status"] == "awaiting_card_digits"
        call_args = mock_zd.post_reply_and_set_pending.call_args
        assert "5555" in str(call_args)
