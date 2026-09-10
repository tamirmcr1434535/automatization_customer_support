"""#193481 — "I can't log in, please cancel my subscription".

The bot used to escalate without touching the subscription: the customer had
reported a login problem, and Anna's rule from #175987 sent those to an agent
because only a human can restore account access.

But cancelling needs no login — the bot does it server-side, and being locked
out is precisely WHY the customer wrote to support instead of cancelling
themselves. On #193481 an agent cancelled it by hand 13 minutes later and sent
the very confirmation the bot would have sent; 103 trial cancellations took
that route in 30 days, each leaving a subscription billing in the meantime.

Anna, 2026-09-10: the bot should cancel these, and agents help with access
afterwards, even after the cancellation. Refund intents are unchanged — there
the customer asked for nothing the bot may safely do alone.
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
os.environ.setdefault("ANTHROPIC_API_KEY", "sk-ant-test")
os.environ.setdefault("SLACK_WEBHOOK_URL", "https://hooks.slack.com/test")

sys.modules.setdefault("classifier", MagicMock())
sys.modules.setdefault("reply_generator", MagicMock())
sys.modules.setdefault("bq_logger", MagicMock())

import main  # noqa: E402

main.reply_generator.REFUND_APPROVE_CODES = {
    "WOULD_BE_REFUNDED", "WOULD_BE_REFUNDED_LAST_ONLY"}

from tests.conftest import make_zendesk_ticket  # noqa: E402


# The live ticket, verbatim.
TICKET_193481 = (
    "IQ Booster サポートご担当者様 お世話になっております。"
    "IQ Boosterの7日間トライアルに申し込みましたが、ログインできないため、"
    "サブスクリプションを解約したくご連絡いたしました。"
    "今後の自動更新および料金請求が発生しないよう、解約手続きをお願いいたします。"
    "登録に使用したメールアドレスは以下です。"
    "解約が完了しましたら、確認のご連絡をいただけますと幸いです。よろしくお願いいたします。"
)


def _classification(intent="TRIAL_CANCELLATION", confidence=0.97, language="JP"):
    return {"intent": intent, "confidence": confidence, "language": language,
            "chargeback_risk": False, "reasoning": "test"}


def _setup_zd(mock_zd, ticket=None):
    ticket = ticket or make_zendesk_ticket()
    mock_zd.get_ticket.return_value = ticket
    mock_zd.get_ticket_tags.return_value = list(ticket.get("tags", []))
    mock_zd.get_all_customer_comments_text.return_value = ""
    mock_zd.get_first_customer_comment.return_value = ""
    mock_zd.last_public_comment_is_from_agent.return_value = False
    mock_zd.find_active_tickets_for_email.return_value = []
    mock_zd.find_active_tickets_for_requester.return_value = []
    mock_zd.count_bot_replies.return_value = 0


def _woo_trial():
    return {"status": "trial_cancelled", "email": "user@example.com",
            "cancelled": True, "subscription_type": "trial",
            "subscription_id": 101, "plan": "IQ Test Subscription",
            "source": "woocommerce", "nexus_charge_types": ["subscription"]}


# ── The premise ──────────────────────────────────────────────────────────── #

def test_the_live_ticket_reads_as_both_a_login_problem_and_a_cancel():
    assert main._contains_login_problem(TICKET_193481) is True
    assert main._contains_cancel_signal(TICKET_193481) is True


# ── Cancellation: act, then hand over ───────────────────────────────────── #

@patch.object(main, "log_result")
@patch.object(main, "validate_reply", return_value=(True, ""))
@patch.object(main, "generate_reply", return_value="解約しました。")
@patch.object(main, "woo")
@patch.object(main, "classify_ticket", return_value=_classification())
@patch.object(main, "zendesk")
def test_193481_cancels_instead_of_only_escalating(
    mock_zd, mock_cls, mock_woo, mock_reply, mock_validate, mock_log
):
    _setup_zd(mock_zd, ticket=make_zendesk_ticket(
        subject="IQ Booster サブスクリプション解約のお願い", body=TICKET_193481))
    mock_woo.cancel_subscription.return_value = _woo_trial()
    result = main._process("193481")

    assert mock_woo.cancel_subscription.called is True
    assert result["status"] != "escalated_login_problem"
    mock_zd.post_reply.assert_called_once()          # they asked for confirmation


@patch.object(main, "log_result")
@patch.object(main, "validate_reply", return_value=(True, ""))
@patch.object(main, "generate_reply", return_value="解約しました。")
@patch.object(main, "woo")
@patch.object(main, "classify_ticket", return_value=_classification())
@patch.object(main, "zendesk")
def test_the_ticket_stays_open_for_the_access_problem(
    mock_zd, mock_cls, mock_woo, mock_reply, mock_validate, mock_log
):
    """Billing stops immediately, but the account is still unreachable and only
    a human can fix that — so this ticket must NOT be solved and closed."""
    _setup_zd(mock_zd, ticket=make_zendesk_ticket(
        subject="解約のお願い", body=TICKET_193481))
    mock_woo.cancel_subscription.return_value = _woo_trial()
    main._process("193481")

    mock_zd.solve_ticket.assert_not_called()
    tagged = [c.args[1] for c in mock_zd.add_tag.call_args_list]
    assert "login_problem" in tagged
    assert "needs_manual_review" in tagged
    notes = " ".join(str(c.args[1]) for c in mock_zd.add_internal_note.call_args_list)
    assert "cannot log in" in notes and "left open" in notes


# ── Refunds are untouched ───────────────────────────────────────────────── #

@patch.object(main, "log_result")
@patch.object(main, "woo")
@patch.object(main, "classify_ticket",
              return_value=_classification(intent="REFUND_REQUEST", confidence=0.9))
@patch.object(main, "zendesk")
def test_refund_with_a_login_problem_still_escalates_untouched(
    mock_zd, mock_cls, mock_woo, mock_log
):
    # Neutral subject on purpose: a refund keyword in the SUBJECT trips an
    # earlier guard (main.py "Refund keyword in subject line") and the flow
    # never reaches the login check we are testing here. Body long enough that
    # the short-description path does not swap it for the (empty) comments.
    _setup_zd(mock_zd, ticket=make_zendesk_ticket(
        subject="お問い合わせ",
        body="お世話になっております。アカウントにログインできません。"
             "パスワードを再設定しても入れませんでした。先日請求された"
             "料金の返金をお願いしたく、ご連絡いたしました。"
             "よろしくお願いいたします。"))
    result = main._process("175987")

    assert result["status"] == "escalated_login_problem"
    assert mock_woo.cancel_subscription.called is False
    mock_zd.post_reply.assert_not_called()


# ── An ordinary cancellation is unchanged ───────────────────────────────── #

@patch.object(main, "log_result")
@patch.object(main, "validate_reply", return_value=(True, ""))
@patch.object(main, "generate_reply", return_value="Cancelled.")
@patch.object(main, "woo")
@patch.object(main, "classify_ticket",
              return_value=_classification(language="EN"))
@patch.object(main, "zendesk")
def test_cancellation_without_a_login_problem_is_still_solved(
    mock_zd, mock_cls, mock_woo, mock_reply, mock_validate, mock_log
):
    _setup_zd(mock_zd)
    mock_woo.cancel_subscription.return_value = _woo_trial()
    result = main._process("1001")

    assert result["status"] == "success"
    mock_zd.solve_ticket.assert_called_once()
    assert "login_problem" not in [c.args[1] for c in mock_zd.add_tag.call_args_list]
