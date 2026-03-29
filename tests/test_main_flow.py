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
  I. Not found in WooCommerce AND Stripe → Slack alert, status=manual_review_required
  J. Not found anywhere → ticket NOT solved, tag needs_manual_review added
  K. Slack notify called with correct ticket_id and email
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
        mock_zd.get_ticket.return_value = make_zendesk_ticket(tags=[])
        with patch.object(main, "TEST_MODE", True):
            result = main._process("1001")
        assert result["status"] == "skipped_no_test_tag"

    # C. Unhandled intent
    @patch.object(main, "log_result")
    @patch.object(main, "classify_ticket", return_value=_classification(intent="REFUND_REQUEST"))
    @patch.object(main, "zendesk")
    def test_unhandled_intent(self, mock_zd, mock_cls, mock_log):
        mock_zd.get_ticket.return_value = make_zendesk_ticket()
        result = main._process("1002")
        assert result["status"] == "skipped_not_handled"

    # D. Low confidence → escalate
    @patch.object(main, "log_result")
    @patch.object(main, "classify_ticket", return_value=_classification(confidence=0.5))
    @patch.object(main, "zendesk")
    def test_low_confidence_escalated(self, mock_zd, mock_cls, mock_log):
        mock_zd.get_ticket.return_value = make_zendesk_ticket()
        result = main._process("1003")
        assert result["status"] == "escalated_low_confidence"
        mock_zd.add_tag.assert_called_with("1003", "bot_low_confidence")
        mock_zd.add_internal_note.assert_called_once()

    # E. WooCommerce handles trial
    @patch.object(main, "log_result")
    @patch.object(main, "generate_reply", return_value="Your trial has been cancelled.")
    @patch.object(main, "woo")
    @patch.object(main, "classify_ticket", return_value=_classification())
    @patch.object(main, "zendesk")
    def test_woo_trial_success(self, mock_zd, mock_cls, mock_woo, mock_reply, mock_log):
        mock_zd.get_ticket.return_value = make_zendesk_ticket()
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
        mock_zd.get_ticket.return_value = make_zendesk_ticket()
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
        mock_zd.get_ticket.return_value = make_zendesk_ticket()
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
        mock_zd.get_ticket.return_value = make_zendesk_ticket()
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

    # I. Not found anywhere → Slack alert, status=manual_review_required
    @patch.object(main, "log_result")
    @patch.object(main, "slack")
    @patch.object(main, "stripe_cli")
    @patch.object(main, "woo")
    @patch.object(main, "classify_ticket", return_value=_classification())
    @patch.object(main, "zendesk")
    def test_not_found_anywhere_slack_alert(
        self, mock_zd, mock_cls, mock_woo, mock_stripe, mock_slack, mock_log
    ):
        mock_zd.get_ticket.return_value = make_zendesk_ticket(email="ghost@example.com")
        mock_woo.cancel_subscription.return_value = _woo_not_found("ghost@example.com")
        mock_stripe.cancel_subscription.return_value = _stripe_not_found("ghost@example.com")
        result = main._process("1008")
        assert result["status"] == "manual_review_required"
        assert result["action"] == "slack_alerted"
        mock_slack.notify_manual_review.assert_called_once()

    # J. Not found anywhere → ticket NOT solved, needs_manual_review tag added
    @patch.object(main, "log_result")
    @patch.object(main, "slack")
    @patch.object(main, "stripe_cli")
    @patch.object(main, "woo")
    @patch.object(main, "classify_ticket", return_value=_classification())
    @patch.object(main, "zendesk")
    def test_not_found_ticket_stays_open(
        self, mock_zd, mock_cls, mock_woo, mock_stripe, mock_slack, mock_log
    ):
        mock_zd.get_ticket.return_value = make_zendesk_ticket()
        mock_woo.cancel_subscription.return_value = _woo_not_found()
        mock_stripe.cancel_subscription.return_value = _stripe_not_found()
        main._process("1009")
        mock_zd.solve_ticket.assert_not_called()
        mock_zd.post_reply.assert_not_called()
        tags_added = [c.args[1] for c in mock_zd.add_tag.call_args_list]
        assert "needs_manual_review" in tags_added

    # K. Slack called with correct ticket_id and email
    @patch.object(main, "log_result")
    @patch.object(main, "slack")
    @patch.object(main, "stripe_cli")
    @patch.object(main, "woo")
    @patch.object(main, "classify_ticket", return_value=_classification())
    @patch.object(main, "zendesk")
    def test_slack_called_with_correct_args(
        self, mock_zd, mock_cls, mock_woo, mock_stripe, mock_slack, mock_log
    ):
        mock_zd.get_ticket.return_value = make_zendesk_ticket(
            ticket_id="5555", email="specific@example.com"
        )
        mock_woo.cancel_subscription.return_value = _woo_not_found("specific@example.com")
        mock_stripe.cancel_subscription.return_value = _stripe_not_found("specific@example.com")
        main._process("5555")
        call_kwargs = mock_slack.notify_manual_review.call_args
        assert "5555" in str(call_kwargs)
        assert "specific@example.com" in str(call_kwargs)
