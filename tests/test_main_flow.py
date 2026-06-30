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
    # Thin-body classification path falls back to first customer comment;
    # return empty string so the body argument used downstream stays a
    # plain str instead of a MagicMock.
    mock_zd.get_first_customer_comment.return_value = ""
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

    # D1a. Low-confidence cancel boost: subject="キャンセルします", thin body,
    # classifier returned TRIAL_CANCELLATION at 0.72 → boost to 0.85 → auto-cancel.
    # This is the #120924-pattern from production.
    @patch.object(main, "log_result")
    @patch.object(main, "validate_reply", return_value=(True, ""))
    @patch.object(main, "generate_reply", return_value="Your trial has been cancelled.")
    @patch.object(main, "woo")
    @patch.object(main, "classify_ticket",
                  return_value=_classification(confidence=0.72, language="JP"))
    @patch.object(main, "zendesk")
    def test_low_confidence_boost_pattern_a_auto_cancels(
        self, mock_zd, mock_cls, mock_woo, mock_reply, mock_validate, mock_log
    ):
        _setup_zd(mock_zd, ticket=make_zendesk_ticket(
            subject="キャンセルします",
            body="iPhoneから送信",
        ))
        mock_woo.cancel_subscription.return_value = _woo_trial()
        result = main._process("1020")
        assert result["status"] == "success"
        assert result["confidence"] == 0.85  # boosted from 0.72

    # D1b. Low-confidence cancel boost — amount+currency disqualifier.
    # Same subject "キャンセル" but body mentions a specific charge (¥1,990) →
    # boost skips, ticket stays escalated. Protects refund-leaning customers
    # who used the word "cancel" but really want a charge dispute.
    @patch.object(main, "log_result")
    @patch.object(main, "classify_ticket",
                  return_value=_classification(confidence=0.72, language="JP"))
    @patch.object(main, "zendesk")
    def test_low_confidence_boost_skipped_on_amount(
        self, mock_zd, mock_cls, mock_log
    ):
        _setup_zd(mock_zd, ticket=make_zendesk_ticket(
            subject="キャンセル",
            body="1990円が引かれていた、キャンセルしたい",
        ))
        result = main._process("1021")
        # Boost MUST NOT fire because the body contains an amount+currency —
        # this is the strict-narrow guard against auto-cancelling refund cases.
        assert result["status"] == "escalated_low_confidence"

    # D1c. UNKNOWN safety-net bug fix: classifier returned UNKNOWN at 0.0,
    # body contains explicit cancel keyword → safety net overrides to
    # TRIAL_CANCELLATION AND bumps confidence to 0.85, so the downstream
    # low-confidence gate no longer kills the override. Before the fix this
    # combination escalated at confidence=0.0 despite the keyword match.
    @patch.object(main, "log_result")
    @patch.object(main, "validate_reply", return_value=(True, ""))
    @patch.object(main, "generate_reply", return_value="Your trial has been cancelled.")
    @patch.object(main, "woo")
    @patch.object(main, "classify_ticket",
                  return_value=_classification(intent="UNKNOWN", confidence=0.0, language="JP"))
    @patch.object(main, "zendesk")
    def test_unknown_safety_net_cancel_keyword_auto_handles(
        self, mock_zd, mock_cls, mock_woo, mock_reply, mock_validate, mock_log
    ):
        _setup_zd(mock_zd, ticket=make_zendesk_ticket(
            subject="解約",
            body="解約お願いします",
        ))
        mock_woo.cancel_subscription.return_value = _woo_trial()
        result = main._process("1022")
        assert result["status"] == "success"
        assert result["intent"] == "TRIAL_CANCELLATION"

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

    # D6b. Ticket was merged mid-flight → webhook handler returns
    # skipped_merged cleanly instead of surfacing a raw 422 as an error.
    @patch.object(main, "log_result")
    @patch.object(main, "_bq_log_result")
    @patch.object(main, "_report_slack")
    @patch.object(main, "classify_ticket", return_value=_classification())
    @patch.object(main, "zendesk")
    def test_merged_midflight_returns_skipped_merged(
        self, mock_zd, mock_cls, mock_slack, mock_bq, mock_log
    ):
        from zendesk_client import TicketNotWritableError
        _setup_zd(mock_zd)
        # First write (add_tag) fails with 422 — ticket was merged just now.
        mock_zd.add_tag.side_effect = TicketNotWritableError(
            ticket_id="1016", method="POST",
            url="https://wwiqtest.zendesk.com/api/v2/tickets/1016/tags.json",
            detail="RecordInvalid: Ticket is closed",
        )

        fake_request = MagicMock()
        fake_request.method = "POST"
        fake_request.get_json.return_value = {"ticket_id": "1016"}

        with patch.object(main, "_webhook_dedup", return_value=False):
            body, status_code, _ = main.zendesk_webhook(fake_request)

        import json as _json
        payload = _json.loads(body)
        assert status_code == 200
        assert payload["status"] == "skipped_merged"
        # Slack card MUST fire for skipped_merged so operators can see the
        # bot recognised the merge and skipped cleanly (rather than the
        # ticket silently vanishing from the Slack stream).
        mock_slack.notify_ticket_result.assert_called_once()
        # And it must carry a human-readable reason, not the raw 422.
        kwargs = mock_slack.notify_ticket_result.call_args.kwargs
        assert "merged" in kwargs["result"].get("reason", "").lower()

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
    @patch.object(main, "validate_reply", return_value=(True, ""))
    @patch.object(main, "generate_reply", return_value="Your trial has been cancelled.")
    @patch.object(main, "woo")
    @patch.object(main, "classify_ticket", return_value=_classification())
    @patch.object(main, "zendesk")
    def test_woo_trial_success(self, mock_zd, mock_cls, mock_woo, mock_reply, mock_validate, mock_log):
        _setup_zd(mock_zd)
        mock_woo.cancel_subscription.return_value = _woo_trial()
        result = main._process("1004")
        assert result["status"] == "success"
        assert result["cancel_source"] == "woocommerce"

    # F. WooCommerce not_found → Stripe success
    @patch.object(main, "log_result")
    @patch.object(main, "validate_reply", return_value=(True, ""))
    @patch.object(main, "generate_reply", return_value="Subscription cancelled.")
    @patch.object(main, "stripe_cli")
    @patch.object(main, "woo")
    @patch.object(main, "classify_ticket", return_value=_classification(intent="SUB_CANCELLATION"))
    @patch.object(main, "zendesk")
    def test_woo_not_found_stripe_fallback(
        self, mock_zd, mock_cls, mock_woo, mock_stripe, mock_reply, mock_validate, mock_log
    ):
        _setup_zd(mock_zd)
        mock_woo.cancel_subscription.return_value = _woo_not_found()
        mock_stripe.cancel_subscription.return_value = _stripe_cancelled()
        result = main._process("1005")
        assert result["status"] == "success"
        assert result["cancel_source"] == "stripe"

    # G. WooCommerce no_active_sub → Stripe success
    @patch.object(main, "log_result")
    @patch.object(main, "validate_reply", return_value=(True, ""))
    @patch.object(main, "generate_reply", return_value="Subscription cancelled.")
    @patch.object(main, "stripe_cli")
    @patch.object(main, "woo")
    @patch.object(main, "classify_ticket", return_value=_classification(intent="SUB_CANCELLATION"))
    @patch.object(main, "zendesk")
    def test_woo_no_active_sub_stripe_fallback(
        self, mock_zd, mock_cls, mock_woo, mock_stripe, mock_reply, mock_validate, mock_log
    ):
        _setup_zd(mock_zd)
        mock_woo.cancel_subscription.return_value = _woo_no_active()
        mock_stripe.cancel_subscription.return_value = _stripe_cancelled()
        result = main._process("1006")
        assert result["cancel_source"] == "stripe"
        assert result["status"] == "success"

    # H. Full SUB_CANCELLATION via WooCommerce paid sub
    @patch.object(main, "log_result")
    @patch.object(main, "validate_reply", return_value=(True, ""))
    @patch.object(main, "generate_reply", return_value="Your subscription has been cancelled.")
    @patch.object(main, "woo")
    @patch.object(main, "classify_ticket",
                  return_value=_classification(intent="SUB_CANCELLATION", language="JP"))
    @patch.object(main, "zendesk")
    def test_sub_cancellation_woo_paid(self, mock_zd, mock_cls, mock_woo, mock_reply, mock_validate, mock_log):
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

    # I. Not found anywhere → silent Slack escalation (card-digits flow was
    # retired as "unreliable and spammy" — see main.py:2274). The customer
    # gets NO reply; a human picks the ticket up from Slack.
    @patch.object(main, "log_result")
    @patch.object(main, "slack")
    @patch.object(main, "stripe_cli")
    @patch.object(main, "woo")
    @patch.object(main, "classify_ticket", return_value=_classification())
    @patch.object(main, "zendesk")
    def test_not_found_anywhere_escalates_silently(
        self, mock_zd, mock_cls, mock_woo, mock_stripe, mock_slack, mock_log
    ):
        _setup_zd(mock_zd, ticket=make_zendesk_ticket(email="ghost@example.com"))
        mock_woo.cancel_subscription.return_value = _woo_not_found("ghost@example.com")
        mock_stripe.cancel_subscription.return_value = _stripe_not_found("ghost@example.com")
        result = main._process("1008")
        assert result["status"] == "escalated_not_found"
        assert result["action"] == "slack_alerted_not_found"
        # No customer-facing message goes out on the not-found path.
        mock_zd.post_reply.assert_not_called()
        mock_zd.post_reply_and_set_pending.assert_not_called()
        mock_zd.solve_ticket.assert_not_called()

    # J. Not found anywhere → ticket re-opened with escalation tags, NOT solved.
    @patch.object(main, "log_result")
    @patch.object(main, "slack")
    @patch.object(main, "stripe_cli")
    @patch.object(main, "woo")
    @patch.object(main, "classify_ticket", return_value=_classification())
    @patch.object(main, "zendesk")
    def test_not_found_reopens_ticket_with_escalation_tags(
        self, mock_zd, mock_cls, mock_woo, mock_stripe, mock_slack, mock_log
    ):
        _setup_zd(mock_zd)
        mock_woo.cancel_subscription.return_value = _woo_not_found()
        mock_stripe.cancel_subscription.return_value = _stripe_not_found()
        main._process("1009")
        mock_zd.solve_ticket.assert_not_called()
        mock_zd.post_reply.assert_not_called()
        mock_zd.set_open.assert_called_once_with("1009")
        tags_added = [c.args[1] for c in mock_zd.add_tag.call_args_list]
        # Current escalation tag set — replaces the retired awaiting_card_digits.
        assert "bot_handled" in tags_added
        assert "needs_manual_review" in tags_added
        assert "ai_bot_failed" in tags_added

    # K. Not-found ticket: ticket_id is propagated correctly through the
    # escalation path (internal note + set_open + tags all target the same id).
    @patch.object(main, "log_result")
    @patch.object(main, "slack")
    @patch.object(main, "stripe_cli")
    @patch.object(main, "woo")
    @patch.object(main, "classify_ticket", return_value=_classification())
    @patch.object(main, "zendesk")
    def test_not_found_escalation_uses_correct_ticket_id(
        self, mock_zd, mock_cls, mock_woo, mock_stripe, mock_slack, mock_log
    ):
        _setup_zd(mock_zd, ticket=make_zendesk_ticket(
            ticket_id="5555", email="specific@example.com"
        ))
        mock_woo.cancel_subscription.return_value = _woo_not_found("specific@example.com")
        mock_stripe.cancel_subscription.return_value = _stripe_not_found("specific@example.com")
        result = main._process("5555")
        assert result["status"] == "escalated_not_found"
        mock_zd.set_open.assert_called_once_with("5555")
        # The escalation internal note must mention the looked-up email so a
        # human can pick up where the bot left off.
        note_call = mock_zd.add_internal_note.call_args
        assert "5555" == note_call.args[0]
        assert "specific@example.com" in note_call.args[1]


# ── Tests: billing-amount-complaint refund override ────────────────────── #
# Real failure: ticket #149230 (June 2026). Customer wrote a JP message
# about past charges (Apr–Jun) with a specific amount that rose, asking
# to "stop the payment" — classifier read this as SUB_RENEWAL_CANCELLATION
# at 82% and auto-cancelled. Correct intent is SUB_RENEWAL_REFUND; human
# had to apologise and offer goodwill compensation.
#
# Fix: compound check `_contains_billing_amount_complaint` requires THREE
# signals together (amount+currency, amount-rose phrase, stop-payment verb)
# and feeds into the existing refund-keyword override in _process.


class TestBillingAmountComplaint:
    """Unit tests for main._contains_billing_amount_complaint."""

    # ── True positives ────────────────────────────────────────────────── #

    def test_ticket_149230_exact_text_matches(self):
        # Verbatim from ticket #149230 — must trip the compound check.
        body = (
            "4月から6月にかけてWWIQTEST.COMの支払い金額が5490円と"
            "上がっているため、支払いを取りやめてほしい。"
        )
        assert main._contains_billing_amount_complaint(body) is True

    def test_amount_high_with_yame_verb_matches(self):
        body = "毎月の支払い金額が1990円と高くなっているので、支払いをやめてください。"
        assert main._contains_billing_amount_complaint(body) is True

    def test_amount_increased_with_stop_deduction_matches(self):
        body = "金額が増えているので、引き落としを止めてください。1500円も取られている。"
        assert main._contains_billing_amount_complaint(body) is True

    # ── False positives must NOT match ────────────────────────────────── #

    def test_plain_cancel_with_price_does_not_match(self):
        # "Cancel my 1990 yen subscription" — has amount, no rose-signal,
        # uses a SUBSCRIPTION-cancel verb (not stop-payment) → must NOT match.
        body = "1990円のサブスクをキャンセルしてください。"
        assert main._contains_billing_amount_complaint(body) is False

    def test_amount_only_without_complaint_does_not_match(self):
        body = "5490円を支払いました。解約したいです。"
        assert main._contains_billing_amount_complaint(body) is False

    def test_complaint_without_amount_does_not_match(self):
        body = "金額が上がっているので、支払いを取りやめてほしい。"
        # No amount+currency → must NOT match (3-of-3 rule).
        assert main._contains_billing_amount_complaint(body) is False

    def test_amount_and_complaint_without_stop_payment_does_not_match(self):
        # Has amount + rose-signal but no stop-payment verb → must NOT match.
        body = "5490円と上がっています。理由を教えてください。"
        assert main._contains_billing_amount_complaint(body) is False

    def test_empty_text_does_not_match(self):
        assert main._contains_billing_amount_complaint("") is False


class TestBillingComplaintIntegration:
    """Integration test: ticket #149230 pattern routes to refund."""

    @patch.object(main, "log_result")
    @patch.object(main, "classify_ticket",
                  return_value=_classification(
                      intent="SUB_RENEWAL_CANCELLATION",
                      confidence=0.82,
                      language="JP",
                  ))
    @patch.object(main, "zendesk")
    def test_ticket_149230_pattern_routes_to_refund(
        self, mock_zd, mock_cls, mock_log
    ):
        # Verbatim from ticket #149230. Classifier returned cancel at 0.82
        # (above the 80% gate, so no boost involved) but the new refund
        # override must intercept it as skipped_refund_request.
        _setup_zd(mock_zd, ticket=make_zendesk_ticket(
            subject="支払いを取りやめてほしい",
            body=(
                "4月から6月にかけてWWIQTEST.COMの支払い金額が5490円と"
                "上がっているため、支払いを取りやめてほしい。"
            ),
        ))
        result = main._process("149230")
        assert result["status"] == "skipped_refund_request"
        # And no cancel-side write should have happened.
        mock_zd.post_reply.assert_not_called()
        mock_zd.solve_ticket.assert_not_called()


# ── Tests: speculative subscription-lookup boost (4e) ───────────────────── #
# Covers _quick_subscription_check (unit) and the new 4e boost branch in
# _process (integration). Mirrors the ticket #149925 pattern: JP cancel
# subject + mobile-signature-only body → classifier returns
# TRIAL_CANCELLATION at 0.55, which the rescue path lifts to 0.85 ONLY
# when a real account/subscription exists for the email.


class TestQuickSubscriptionCheck:
    """Unit tests for main._quick_subscription_check."""

    def test_empty_email_returns_unknown(self):
        assert main._quick_subscription_check("", "1") == "unknown"

    # ── Nexus mode ────────────────────────────────────────────────────── #

    @patch.object(main, "USE_NEXUS_FOR_LOOKUP", True)
    def test_nexus_sub_found_returns_exists(self):
        fake_nexus = MagicMock()
        fake_nexus.search_subscription.return_value = {
            "subscription_id": 9001, "source": "iqbooster"
        }
        with patch.object(main, "nexus_client", fake_nexus):
            assert main._quick_subscription_check("a@b.com", "1") == "exists"
        fake_nexus.search_subscription.assert_called_once_with("a@b.com")

    @patch.object(main, "USE_NEXUS_FOR_LOOKUP", True)
    def test_nexus_returns_none_treated_as_unknown(self):
        # NexusClient conflates clean 404 with 5xx — we MUST NOT downgrade
        # the ticket on a None response (could be a transient outage).
        fake_nexus = MagicMock()
        fake_nexus.search_subscription.return_value = None
        with patch.object(main, "nexus_client", fake_nexus):
            assert main._quick_subscription_check("a@b.com", "1") == "unknown"

    @patch.object(main, "USE_NEXUS_FOR_LOOKUP", True)
    def test_nexus_exception_returns_unknown(self):
        fake_nexus = MagicMock()
        fake_nexus.search_subscription.side_effect = RuntimeError("boom")
        with patch.object(main, "nexus_client", fake_nexus):
            assert main._quick_subscription_check("a@b.com", "1") == "unknown"

    @patch.object(main, "USE_NEXUS_FOR_LOOKUP", True)
    def test_nexus_response_without_sub_id_returns_unknown(self):
        # Defensive: even if the wrapper somehow returned a dict without
        # subscription_id (contract violation), we must not call it "exists".
        fake_nexus = MagicMock()
        fake_nexus.search_subscription.return_value = {"source": "iqbooster"}
        with patch.object(main, "nexus_client", fake_nexus):
            assert main._quick_subscription_check("a@b.com", "1") == "unknown"

    # ── WooCommerce mode (default — USE_NEXUS_FOR_LOOKUP=False) ───────── #

    @patch.object(main, "USE_NEXUS_FOR_LOOKUP", False)
    @patch.object(main, "woo")
    def test_wc_customer_found_returns_exists(self, mock_woo):
        mock_woo.get_customer_by_email.return_value = {"id": 42, "email": "a@b.com"}
        assert main._quick_subscription_check("a@b.com", "1") == "exists"
        # Helper must pass the _errors list so we can distinguish missing vs error.
        _, kwargs = mock_woo.get_customer_by_email.call_args
        assert "_errors" in kwargs

    @patch.object(main, "USE_NEXUS_FOR_LOOKUP", False)
    @patch.object(main, "woo")
    def test_wc_no_customer_clean_returns_missing(self, mock_woo):
        # No customer, no errors → email genuinely has no history.
        def _fake_lookup(email, _errors=None):
            return None
        mock_woo.get_customer_by_email.side_effect = _fake_lookup
        assert main._quick_subscription_check("a@b.com", "1") == "missing"

    @patch.object(main, "USE_NEXUS_FOR_LOOKUP", False)
    @patch.object(main, "woo")
    def test_wc_no_customer_with_errors_returns_unknown(self, mock_woo):
        # Lookup errored (e.g. 504) → we don't know if the customer exists.
        # Must not boost; must escalate.
        def _fake_lookup(email, _errors=None):
            if _errors is not None:
                _errors.append({"step": "customer_email", "kind": "timeout_error", "detail": "504"})
            return None
        mock_woo.get_customer_by_email.side_effect = _fake_lookup
        assert main._quick_subscription_check("a@b.com", "1") == "unknown"

    @patch.object(main, "USE_NEXUS_FOR_LOOKUP", False)
    @patch.object(main, "woo")
    def test_wc_exception_returns_unknown(self, mock_woo):
        mock_woo.get_customer_by_email.side_effect = RuntimeError("network down")
        assert main._quick_subscription_check("a@b.com", "1") == "unknown"


class TestSpeculativeLookupBoost:
    """Integration tests for the 4e boost branch in main._process."""

    # 4e-1. Ticket #149925-pattern: JP subject "Request for Cancellation"
    # + iPhone-signature body, classifier returns 0.55, WC says "customer
    # exists" → boost to 0.85 → auto-cancel via WC.
    @patch.object(main, "log_result")
    @patch.object(main, "validate_reply", return_value=(True, ""))
    @patch.object(main, "generate_reply", return_value="Your trial has been cancelled.")
    @patch.object(main, "woo")
    @patch.object(main, "USE_NEXUS_FOR_LOOKUP", False)
    @patch.object(main, "classify_ticket",
                  return_value=_classification(confidence=0.55, language="JP"))
    @patch.object(main, "zendesk")
    def test_boost_fires_when_wc_confirms_customer(
        self, mock_zd, mock_cls, mock_woo, mock_reply, mock_validate, mock_log
    ):
        _setup_zd(mock_zd, ticket=make_zendesk_ticket(
            subject="Request for Cancellation of Subscription",
            body="Sent from my iPhone",
        ))
        # Speculative lookup says "customer exists".
        mock_woo.get_customer_by_email.return_value = {"id": 42, "email": "user@example.com"}
        # Downstream cancel actually happens.
        mock_woo.cancel_subscription.return_value = _woo_trial()
        result = main._process("4001")
        assert result["status"] == "success"
        assert result["confidence"] == 0.85  # boosted from 0.55
        # The speculative lookup must have been consulted.
        mock_woo.get_customer_by_email.assert_called()

    # 4e-2. Same shape, but Nexus mode is on and Nexus finds a sub →
    # boost still fires.
    @patch.object(main, "log_result")
    @patch.object(main, "validate_reply", return_value=(True, ""))
    @patch.object(main, "generate_reply", return_value="Your trial has been cancelled.")
    @patch.object(main, "woo")
    @patch.object(main, "USE_NEXUS_FOR_LOOKUP", True)
    @patch.object(main, "classify_ticket",
                  return_value=_classification(confidence=0.55, language="JP"))
    @patch.object(main, "zendesk")
    def test_boost_fires_when_nexus_confirms_subscription(
        self, mock_zd, mock_cls, mock_woo, mock_reply, mock_validate, mock_log
    ):
        _setup_zd(mock_zd, ticket=make_zendesk_ticket(
            subject="解約してください",
            body="iPhoneから送信",
        ))
        fake_nexus = MagicMock()
        fake_nexus.search_subscription.return_value = {
            "subscription_id": 9001, "source": "iqbooster"
        }
        mock_woo.cancel_subscription.return_value = _woo_trial()
        with patch.object(main, "nexus_client", fake_nexus):
            result = main._process("4002")
        assert result["status"] == "success"
        assert result["confidence"] == 0.85
        fake_nexus.search_subscription.assert_called()

    # 4e-3. Lookup returns "missing" → no boost → escalation as before.
    @patch.object(main, "log_result")
    @patch.object(main, "woo")
    @patch.object(main, "USE_NEXUS_FOR_LOOKUP", False)
    @patch.object(main, "classify_ticket",
                  return_value=_classification(confidence=0.55, language="JP"))
    @patch.object(main, "zendesk")
    def test_boost_skipped_when_lookup_missing(
        self, mock_zd, mock_cls, mock_woo, mock_log
    ):
        _setup_zd(mock_zd, ticket=make_zendesk_ticket(
            subject="Request for Cancellation of Subscription",
            body="Sent from my iPhone",
        ))
        # Clean miss — no customer, no errors.
        def _fake_lookup(email, _errors=None):
            return None
        mock_woo.get_customer_by_email.side_effect = _fake_lookup
        result = main._process("4003")
        assert result["status"] == "escalated_low_confidence"

    # 4e-4. Lookup errored ("unknown") → no boost — transient failures
    # must NOT auto-cancel. Mirrors WC 504 / Nexus 5xx behaviour.
    @patch.object(main, "log_result")
    @patch.object(main, "woo")
    @patch.object(main, "USE_NEXUS_FOR_LOOKUP", False)
    @patch.object(main, "classify_ticket",
                  return_value=_classification(confidence=0.55, language="JP"))
    @patch.object(main, "zendesk")
    def test_boost_skipped_when_lookup_unknown(
        self, mock_zd, mock_cls, mock_woo, mock_log
    ):
        _setup_zd(mock_zd, ticket=make_zendesk_ticket(
            subject="Request for Cancellation of Subscription",
            body="Sent from my iPhone",
        ))
        def _fake_lookup(email, _errors=None):
            if _errors is not None:
                _errors.append({"step": "customer_email", "kind": "timeout_error", "detail": "504"})
            return None
        mock_woo.get_customer_by_email.side_effect = _fake_lookup
        result = main._process("4004")
        assert result["status"] == "escalated_low_confidence"

    # 4e-5. Confidence below 0.50 floor → no boost even if sub exists.
    # Very-low confidence usually means the classifier was genuinely
    # unsure about the intent, not just thin context — we keep escalating.
    @patch.object(main, "log_result")
    @patch.object(main, "woo")
    @patch.object(main, "USE_NEXUS_FOR_LOOKUP", False)
    @patch.object(main, "classify_ticket",
                  return_value=_classification(confidence=0.30, language="JP"))
    @patch.object(main, "zendesk")
    def test_boost_skipped_below_floor(
        self, mock_zd, mock_cls, mock_woo, mock_log
    ):
        _setup_zd(mock_zd, ticket=make_zendesk_ticket(
            subject="Request for Cancellation of Subscription",
            body="Sent from my iPhone",
        ))
        mock_woo.get_customer_by_email.return_value = {"id": 42, "email": "user@example.com"}
        result = main._process("4005")
        assert result["status"] == "escalated_low_confidence"
        # Lookup must NOT even be attempted at this confidence — short-circuit.
        mock_woo.get_customer_by_email.assert_not_called()

    # 4e-6. Amount + currency in body → disqualifier kicks in → no boost.
    # Protects refund-leaning customers who mentioned a charge alongside
    # the cancel word.
    @patch.object(main, "log_result")
    @patch.object(main, "woo")
    @patch.object(main, "USE_NEXUS_FOR_LOOKUP", False)
    @patch.object(main, "classify_ticket",
                  return_value=_classification(confidence=0.55, language="JP"))
    @patch.object(main, "zendesk")
    def test_boost_skipped_on_amount_currency(
        self, mock_zd, mock_cls, mock_woo, mock_log
    ):
        _setup_zd(mock_zd, ticket=make_zendesk_ticket(
            subject="解約",
            body="1990円が引かれていた、解約したい",
        ))
        mock_woo.get_customer_by_email.return_value = {"id": 42, "email": "user@example.com"}
        result = main._process("4006")
        assert result["status"] == "escalated_low_confidence"
        # Disqualifier short-circuits the boost — lookup never runs.
        mock_woo.get_customer_by_email.assert_not_called()


# ── Tests: _resolve_intent in both legacy WC and new Nexus modes ────────── #
# Mirrors the three intent buckets the bot tags and topic-codes for renewal
# tracking. Under Nexus mode the dispatch reads native fields
# (renewal_subscriptions, subscription_start) — no order_count heuristic.

class TestResolveIntentNexusMode:
    """Nexus-mode dispatch: triggered when cancel_result carries
    `nexus_renewals` (set only by woo.cancel_subscription_via_nexus).
    These tests are the contract for the new classifier branch."""

    def test_no_sub_no_renewal_is_trial(self):
        # Rule 1: "людина просить відмінити підписку, а в неї немає саба,
        # чи реньювала" → TRIAL_CANCELLATION.
        cancel_result = {
            "subscription_type": "trial",  # ignored in Nexus dispatch
            "order_count": 1,              # ignored in Nexus dispatch
            "nexus_sub_started": False,
            "nexus_renewals": 0,
        }
        assert main._resolve_intent("SUB_CANCELLATION", cancel_result) == "TRIAL_CANCELLATION"

    def test_sub_started_no_renewal_is_sub(self):
        # Rule 2: "в неї тріал вже пройшов, а реньювала немає" → SUB_CANCELLATION.
        cancel_result = {
            "subscription_type": "subscription",
            "order_count": 1,
            "nexus_sub_started": True,
            "nexus_renewals": 0,
        }
        assert main._resolve_intent("TRIAL_CANCELLATION", cancel_result) == "SUB_CANCELLATION"

    def test_sub_started_with_renewal_is_renewal(self):
        # Rule 3: "тріал пройшов, і є реньювал" → SUB_RENEWAL_CANCELLATION.
        # Importantly: ONE renewal is enough — no order_count >= MAX_BOT_ORDERS
        # heuristic in Nexus mode, because Nexus tells us natively.
        cancel_result = {
            "subscription_type": "subscription",
            "order_count": 2,
            "nexus_sub_started": True,
            "nexus_renewals": 1,
        }
        assert main._resolve_intent("TRIAL_CANCELLATION", cancel_result) == "SUB_RENEWAL_CANCELLATION"

    def test_multiple_renewals_still_renewal(self):
        # Many renewals → SUB_RENEWAL_CANCELLATION (same bucket as 1 renewal).
        cancel_result = {
            "subscription_type": "subscription",
            "order_count": 7,
            "nexus_sub_started": True,
            "nexus_renewals": 5,
        }
        assert main._resolve_intent("TRIAL_CANCELLATION", cancel_result) == "SUB_RENEWAL_CANCELLATION"

    def test_unbounded_renewals_still_renewal(self):
        # Contract: the dispatch has NO upper ceiling on renewal count.
        # `n_renewals >= 1` matches everything from 1 to 2**31 and beyond.
        # We pick a deliberately large value (matches what a customer with
        # 8+ years of monthly renewals would have) plus a pathological
        # very-large value to lock the contract against any future "cap
        # at N" change sneaking in.
        for big in (12, 99, 9999, 10_000_000):
            cancel_result = {
                "nexus_sub_started": True,
                "nexus_renewals": big,
            }
            assert main._resolve_intent("TRIAL_CANCELLATION", cancel_result) == "SUB_RENEWAL_CANCELLATION", big

    def test_renewal_outranks_sub_started_false(self):
        # Defensive edge case: if Nexus ever reports renewals>=1 with
        # subscription_start=False, renewal still wins. We trust the
        # renewal count because that's what the customer is being
        # billed on right now.
        cancel_result = {
            "nexus_sub_started": False,
            "nexus_renewals": 2,
        }
        assert main._resolve_intent("TRIAL_CANCELLATION", cancel_result) == "SUB_RENEWAL_CANCELLATION"

    def test_nexus_mode_ignores_order_count_threshold(self):
        # Regression: under Nexus mode the legacy `order_count >= 3`
        # heuristic must NOT apply. A first-paid-period sub with high
        # order_count but zero renewals stays SUB_CANCELLATION.
        # (This shape isn't expected in real data — Nexus order_count
        # mirrors renewal count when sub_started — but the assertion
        # documents the dispatch contract.)
        cancel_result = {
            "subscription_type": "subscription",
            "order_count": 5,
            "nexus_sub_started": True,
            "nexus_renewals": 0,
        }
        assert main._resolve_intent("TRIAL_CANCELLATION", cancel_result) == "SUB_CANCELLATION"


class TestResolveIntentLegacyWCMode:
    """Legacy WooCommerce dispatch — when `nexus_renewals` is absent
    (either toggle off, or a Stripe-fallback cancel result). Asserts the
    pre-Nexus behaviour didn't shift."""

    def test_trial_subscription_type(self):
        cancel_result = {"subscription_type": "trial", "order_count": 1}
        assert main._resolve_intent("SUB_CANCELLATION", cancel_result) == "TRIAL_CANCELLATION"

    def test_subscription_under_threshold_is_sub(self):
        # order_count=2 < MAX_BOT_ORDERS(3) → SUB_CANCELLATION, NOT renewal.
        cancel_result = {"subscription_type": "subscription", "order_count": 2}
        assert main._resolve_intent("TRIAL_CANCELLATION", cancel_result) == "SUB_CANCELLATION"

    def test_subscription_at_threshold_is_renewal(self):
        # order_count >= 3 → SUB_RENEWAL_CANCELLATION (legacy WC heuristic).
        cancel_result = {"subscription_type": "subscription", "order_count": 3}
        assert main._resolve_intent("TRIAL_CANCELLATION", cancel_result) == "SUB_RENEWAL_CANCELLATION"

    def test_active_treated_like_subscription(self):
        cancel_result = {"subscription_type": "active", "order_count": 1}
        assert main._resolve_intent("TRIAL_CANCELLATION", cancel_result) == "SUB_CANCELLATION"

    def test_unknown_sub_type_falls_back_to_text_intent(self):
        # No sub_type, no Nexus signals → keep whatever the text
        # classifier returned.
        cancel_result = {"status": "not_found_anywhere"}
        assert main._resolve_intent("REFUND_REQUEST", cancel_result) == "REFUND_REQUEST"


# ── Tests: amount+currency disqualifier (regression for #152536) ────────── #
# Bot auto-cancelled a customer who wrote "Refund money autodebt IDR 499,990
# from my card" because the original regex was suffix-only AND didn't carry
# IDR / Rp / ฿ / ₱ / MYR / SGD / etc. These tests pin the expanded coverage
# so any future regex edit fails fast instead of silently letting refund-
# leaning tickets through the cancel boost.

class TestAmountCurrencyDetection:
    """Unit tests for main._contains_amount_with_currency."""

    def test_152536_real_body_matches(self):
        # The exact text that slipped through the disqualifier in prod.
        body = "Refund money autodebt IDR 499,990 from my card"
        assert main._contains_amount_with_currency(body) is True

    def test_idr_both_orders(self):
        assert main._contains_amount_with_currency("IDR 499,990") is True
        assert main._contains_amount_with_currency("499,990 IDR") is True
        assert main._contains_amount_with_currency("Rp 499.990") is True
        assert main._contains_amount_with_currency("Rp499.990") is True

    def test_prefix_style_symbols(self):
        # All prefix-style currencies the original suffix-only regex missed.
        for s in ("$5,490", "€49", "£25", "¥499", "₹999", "₱990",
                  "฿1,990", "₽1500", "₴800"):
            assert main._contains_amount_with_currency(s) is True, s

    def test_suffix_style_codes(self):
        for s in ("5490円", "1990 jpy", "1990 THB", "MYR 49", "49 SGD",
                  "PHP 990", "MXN 1500", "1500 zł", "1500 EUR"):
            assert main._contains_amount_with_currency(s) is True, s

    def test_does_not_match_random_text(self):
        # Negative cases — must not falsely flag every digit/letter combo.
        for s in ("no money here", "Hello, can you help?",
                  "rpm engine", "I have 3 cats", "version 1.2.3"):
            assert main._contains_amount_with_currency(s) is False, s


# ── Tests: live-chat boost regression for #152536 ───────────────────────── #
# Ticket #152536 produced a SUB_CANCELLATION (boosted 0.32→0.85) on a
# Messaging ticket whose body was EMPTY at classification time (the chat
# widget's "Request Description" field never landed in body/comments
# before the 45s delay elapsed). The customer was actually asking for a
# refund. Two fixes pinned here:
#   1. Live-chat boost no longer fires on a strictly-empty body.
#   2. Even if body had landed, "IDR 499,990" is now an amount-currency
#      disqualifier that blocks the boost.

class TestLiveChatBoostEmptyBody:
    """Live-chat boost must NOT auto-cancel an empty-body ticket."""

    @patch.dict(os.environ, {"MESSAGING_CLASSIFY_DELAY_SEC": "0"})
    @patch("time.sleep")
    @patch.object(main, "log_result")
    @patch.object(main, "validate_reply", return_value=(True, ""))
    @patch.object(main, "generate_reply", return_value="OK")
    @patch.object(main, "woo")
    @patch.object(main, "classify_ticket",
                  return_value=_classification(confidence=0.32, language="EN"))
    @patch.object(main, "zendesk")
    def test_empty_body_does_not_boost(
        self, mock_zd, mock_cls, mock_woo, mock_reply, mock_validate, mock_log,
        mock_sleep,
    ):
        # Exactly the #152536 shape: subject="Conversation with X", body="".
        _setup_zd(mock_zd, ticket=make_zendesk_ticket(
            subject="Conversation with Zefira",
            body="",
        ))
        # Body stays empty after the messaging-delay fallback — comments
        # are empty too (the chat-widget submission only filled a custom
        # field, which the bot doesn't currently read).
        mock_zd.get_all_customer_comments_text.return_value = ""
        mock_zd.get_first_customer_comment.return_value = ""

        result = main._process("152536")

        # Must NOT have auto-cancelled. Confidence stays at the classifier
        # output (0.32), low-confidence path takes over.
        assert result["status"] == "escalated_low_confidence", result
        mock_woo.cancel_subscription.assert_not_called()


class TestRequestDescriptionExtractor:
    """Unit tests for main._extract_request_description.

    Documents the contract: pull custom_field id 21564352562460 (wwiqtest
    Zendesk) string value from a ticket dict. Tolerate every shape of
    "field missing / value None / wrong type" without raising — the bot
    must never crash on a missing field; worst case is "no extra text".
    """

    _FID = 21564352562460  # matches default ZENDESK_REQUEST_DESCRIPTION_FIELD_ID

    def test_extracts_real_152536_payload(self):
        ticket = {
            "id": 152536,
            "subject": "Conversation with Zefira",
            "description": "Conversation with Web User abc",
            "custom_fields": [
                {"id": self._FID, "value": "Refund money autodebt IDR 499,990 from my card"},
                {"id": 999, "value": "unrelated"},
            ],
        }
        assert main._extract_request_description(ticket) == \
            "Refund money autodebt IDR 499,990 from my card"

    def test_missing_field_returns_empty(self):
        ticket = {"id": 1, "custom_fields": [{"id": 999, "value": "x"}]}
        assert main._extract_request_description(ticket) == ""

    def test_no_custom_fields_key_returns_empty(self):
        assert main._extract_request_description({"id": 1}) == ""

    def test_null_value_returns_empty(self):
        ticket = {"custom_fields": [{"id": self._FID, "value": None}]}
        assert main._extract_request_description(ticket) == ""

    def test_non_string_value_returns_empty(self):
        ticket = {"custom_fields": [{"id": self._FID, "value": 12345}]}
        assert main._extract_request_description(ticket) == ""

    def test_strips_whitespace(self):
        ticket = {"custom_fields": [{"id": self._FID, "value": "  refund please  \n"}]}
        assert main._extract_request_description(ticket) == "refund please"

    def test_disabled_via_empty_env_returns_empty(self):
        ticket = {"custom_fields": [{"id": self._FID, "value": "refund please"}]}
        with patch.object(main, "_ZENDESK_REQUEST_DESCRIPTION_FIELD_ID", ""):
            assert main._extract_request_description(ticket) == ""


class TestProcessFoldsRequestDescription:
    """End-to-end: #152536-shape ticket → bot sees the refund text from
    the custom field → REFUND keyword override fires → NO auto-cancel."""

    @patch.dict(os.environ, {"MESSAGING_CLASSIFY_DELAY_SEC": "0"})
    @patch("time.sleep")
    @patch.object(main, "log_result")
    @patch.object(main, "validate_reply", return_value=(True, ""))
    @patch.object(main, "generate_reply", return_value="OK")
    @patch.object(main, "woo")
    @patch.object(main, "classify_ticket",
                  return_value=_classification(confidence=0.32, language="EN"))
    @patch.object(main, "zendesk")
    def test_152536_full_shape_routes_to_refund(
        self, mock_zd, mock_cls, mock_woo, mock_reply, mock_validate, mock_log,
        mock_sleep,
    ):
        # Exact Zendesk-API shape captured from the real #152536 ticket.
        ticket = make_zendesk_ticket(
            ticket_id="152536",
            subject="Conversation with Zefira",
            body="Conversation with Web User 6a437732f05f2e15b9f83bf3",
        )
        ticket["custom_fields"] = [
            {"id": 21564352562460,
             "value": "Refund money autodebt IDR 499,990 from my card"},
        ]
        _setup_zd(mock_zd, ticket=ticket)
        mock_zd.get_all_customer_comments_text.return_value = ""
        mock_zd.get_first_customer_comment.return_value = ""

        result = main._process("152536")

        # Refund keyword override catches it (the body now contains the
        # custom-field text). No WC cancel must be issued.
        assert result["status"] == "skipped_refund_request", result
        mock_woo.cancel_subscription.assert_not_called()


class TestLiveChatBoostIDRRefund:
    """Live-chat boost must drop on amount+currency in body (IDR coverage)."""

    @patch.dict(os.environ, {"MESSAGING_CLASSIFY_DELAY_SEC": "0"})
    @patch("time.sleep")
    @patch.object(main, "log_result")
    @patch.object(main, "validate_reply", return_value=(True, ""))
    @patch.object(main, "generate_reply", return_value="OK")
    @patch.object(main, "woo")
    @patch.object(main, "classify_ticket",
                  return_value=_classification(confidence=0.45, language="EN"))
    @patch.object(main, "zendesk")
    def test_idr_amount_blocks_boost(
        self, mock_zd, mock_cls, mock_woo, mock_reply, mock_validate, mock_log,
        mock_sleep,
    ):
        # Same Conversation-with header, but THIS time the chat-widget text
        # did land in body — bot must see the IDR amount and route to a human.
        _setup_zd(mock_zd, ticket=make_zendesk_ticket(
            subject="Conversation with Zefira",
            body="Refund money autodebt IDR 499,990 from my card",
        ))
        mock_zd.get_all_customer_comments_text.return_value = ""

        result = main._process("152536b")

        # The expanded refund-keyword detector catches "Refund" in body
        # BEFORE the cancel boost can fire → ticket routes to a human as a
        # refund request. (Even if that path were skipped, the IDR amount-
        # currency disqualifier would still block the boost — belt and
        # suspenders, both verified by other tests in this module.)
        assert result["status"] in (
            "skipped_refund_request", "escalated_low_confidence",
        ), result
        mock_woo.cancel_subscription.assert_not_called()
