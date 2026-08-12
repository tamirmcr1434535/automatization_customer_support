"""A refund verdict must never answer a customer who never asked for a refund.

Live incident #175987 (2026-08-10, Anna): a Japanese customer wrote

    "記載のメールアドレスに紐づくアカウントは、5月に一度サブスクリプション解除を
     依頼ずみですが、未だに毎月請求がなされています。ログインを試みましたが、
     アカウントとメールアドレスの紐付けは解除済みのようで、再ログインと退会が
     できませんでした。至急サブスクリプション[解除]を願います"

    ("I already asked you to cancel in May but I'm still billed every month. I tried
     to log in, but the account seems unlinked from my email, so I could neither log
     back in nor unsubscribe. Please cancel urgently.")

There is NO refund word anywhere. The classifier still returned REFUND_REQUEST (0.82,
reasoning: "ongoing charges after a prior cancellation request"), the window check said
the 21 July charge was older than Japan's 8 days, and the bot sent a refund DENIAL. The
customer replied in the satisfaction survey that they would report us to the authorities
or the courts.

Two rules come out of it (Anna, 2026-08-11):
  1. no refund wording in the customer's own text → the bot sends NO refund verdict
     (neither approve nor deny) and a human answers what was actually asked;
  2. a customer who says they cannot log in goes to an agent — they must be able to
     reach their own subscription, and only a human can restore that access.
"""
import os
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

os.environ.setdefault("SKIP_WC_HEALTHCHECK", "true")

import main


# The live ticket, verbatim (Zendesk #175987 description).
TICKET_175987 = (
    "First Name: 内堀 Last Name: そよみ Email: uchiborisoyomi@gmail.com Message: "
    "記載のメールアドレスに紐づくアカウントは、5月に一度サブスクリプション解除を依頼ずみですが、"
    "未だに毎月請求がなされています ログインを試みましたが、アカウントとメールアドレスの紐付けは"
    "解除済みのようで、再ログインと退会ができませんでした 至急サブスクリプションを願います "
    "-- This e-mail was sent from a contact form on WW IQ TEST (https://jap.wwiqtest.com)"
)


# ── Rule 1: the customer never asked for a refund ────────────────────────── #

def test_175987_carries_no_refund_signal():
    """The deterministic detector must disagree with the classifier here."""
    assert main._contains_refund_request(TICKET_175987) is False


def test_175987_is_a_cancellation_ask():
    """It IS a cancel request — the guard's job is to route it there, not to a refund."""
    assert main._contains_cancel_signal(TICKET_175987) is True


def test_still_charged_after_cancelling_is_not_a_refund_ask():
    """The exact false-positive class: repeated charges, zero money-back wording."""
    for text in (
        "解約したはずなのに、ずっとIQPROから4990円引かれています。契約の状況確認して欲しいです。",
        "解約したいです。初回以外利用していないのに月額が請求されてます",
        "I cancelled my subscription in May but I am still being charged every month. "
        "Please stop the charges.",
    ):
        assert main._contains_refund_request(text) is False, text


def test_real_refund_asks_still_detected():
    """The guard must not swallow genuine refund requests (any language)."""
    for text in (
        "先日の請求について返金をお願いします。",
        "Please refund the 5490 JPY charged on July 21.",
        "환불 요청합니다",
        "Ich bitte um Rückerstattung der Abbuchung.",
        "私は登録した覚えがないのに引き落とされています",   # unauthorized-charge phrasing
    ):
        assert main._contains_refund_request(text) is True, text


# ── Rule 2: cannot log in → agent ────────────────────────────────────────── #

def test_175987_login_problem_detected():
    assert main._contains_login_problem(TICKET_175987) is True


def test_login_problem_phrasings():
    for text in (
        "ログインできません。パスワードもわかりません。",
        "アカウントにアクセスできない状態です",
        "再ログインと退会ができませんでした",
        "I can't log in to my account, it says the email is unknown.",
        "I am unable to access my account and want to cancel.",
        "I forgot my password and cannot sign in.",
        "로그인이 안 됩니다",
        "Ich kann mich nicht einloggen.",
        "No puedo iniciar sesión en mi cuenta.",
        "Je ne peux pas me connecter à mon compte.",
        "Kan niet inloggen op mijn account.",
        "Không thể đăng nhập được vào tài khoản.",
    ):
        assert main._contains_login_problem(text) is True, text


def test_login_problem_does_not_fire_on_successful_or_absent_login():
    """Narrow by design: only an INABILITY to reach the account escalates."""
    for text in (
        "ログインして解約しました。確認をお願いします。",           # logged in fine
        "I logged in and cancelled my subscription yesterday.",
        "How do I log in to see my results?",                      # a how-to question
        "解約したいです。よろしくお願いします。",                    # plain cancel
        "Please cancel my subscription.",
        "",
    ):
        assert main._contains_login_problem(text) is False, text


# ── End-to-end: the eval must not answer when no refund was asked ────────── #

def _decision(reason_code, would_be):
    return SimpleNamespace(
        would_be_refunded=would_be, reason_code=reason_code, human_message="msg",
        guard_trail=[], computed_amount="4990", currency="JPY",
        customer_stated_amount=None, customer_stated_amounts="",
        candidate_charge_id="ch_1", charge_type="subscription",
        candidate_charges="ch_1:4990:2026-07-21", refund_flow="flow1_subscription",
        source="wwiqtest", engine_version="test",
    )


def _drive(reason_code, would_be, result):
    """Run the eval refunds-enabled with every collaborator stubbed (same harness
    as test_refund_live_outcome), seeding `result` with the caller's flags."""
    fake_reply_gen = SimpleNamespace(
        REFUND_AUTOREPLY_CODES={"WOULD_BE_REFUNDED", "WOULD_BE_REFUNDED_LAST_ONLY",
                                "OUTSIDE_REFUND_WINDOW"},
        REFUND_APPROVE_CODES={"WOULD_BE_REFUNDED", "WOULD_BE_REFUNDED_LAST_ONLY"},
        generate_refund_reply=lambda rc, lang, data: "こんにちは。ご対応します。",
    )
    zd = MagicMock()
    rc_client = MagicMock()
    rc_client.is_configured.return_value = False
    rc_client.create_refund.return_value = {"status": "refunded", "executed": True,
                                            "refunded_amount": "4990"}
    with patch.object(main.refund_engine, "decide", return_value=_decision(reason_code, would_be)), \
         patch.object(main, "reply_generator", fake_reply_gen), \
         patch.object(main, "refunds_enabled_for", return_value=True), \
         patch.object(main, "_refund_xhost", return_value="host"), \
         patch.object(main, "refund_client", rc_client), \
         patch.object(main, "refund_abuse", SimpleNamespace(check=lambda b, e: (True, ""))), \
         patch.object(main, "nexus_client", None), \
         patch.object(main, "zendesk", zd):
        main._refund_would_be_eval(
            "175987", "uchiborisoyomi@gmail.com", "REFUND_REQUEST",
            {"confidence": 0.82, "language": "JA"}, result,
            ticket_text=TICKET_175987, as_of_date="2026-08-10T06:30:07Z",
            brand="wwiqtest",
        )
    return result, zd, rc_client


def test_175987_denial_is_not_sent_when_no_refund_was_asked():
    """The live regression: OUTSIDE_REFUND_WINDOW must NOT reach the customer."""
    result, zd, _ = _drive("OUTSIDE_REFUND_WINDOW", False,
                           {"refund_ask_in_text": False})
    assert result.get("refund_reply_suppressed") == "no_refund_request_in_text"
    assert not result.get("refund_reply_sent")
    zd.reply_solve_and_set_fields.assert_not_called()      # no customer reply
    zd.post_reply.assert_not_called()
    assert main._refund_outcome_status(result) == "skipped_refund_request"
    # …and the agent is told why, in the ticket (the suppression note; the generic
    # "left to a human" note is added after it, same as the other suppressions).
    notes = [c.args[1] for c in zd.add_internal_note.call_args_list]
    assert any("NEVER asked for a refund" in n for n in notes), notes


def test_no_refund_ask_also_blocks_an_approve_and_moves_no_money():
    result, zd, rc_client = _drive("WOULD_BE_REFUNDED", True,
                                   {"refund_ask_in_text": False})
    assert result.get("refund_reply_suppressed") == "no_refund_request_in_text"
    assert not result.get("refund_reply_sent")
    rc_client.create_refund.assert_not_called()            # no money movement
    zd.reply_solve_and_set_fields.assert_not_called()


def test_genuine_refund_ask_is_still_answered():
    """Guard is targeted: with the flag True the denial goes out exactly as before."""
    result, zd, _ = _drive("OUTSIDE_REFUND_WINDOW", False,
                           {"refund_ask_in_text": True})
    assert not result.get("refund_reply_suppressed")
    assert result.get("refund_reply_sent") is True
    zd.reply_solve_and_set_fields.assert_called_once()


def test_flag_absent_keeps_previous_behaviour():
    """Keyword-driven call sites (subject / body override) never set the flag —
    a missing flag must not suppress anything, or real refunds stop being answered."""
    result, zd, _ = _drive("OUTSIDE_REFUND_WINDOW", False, {})
    assert not result.get("refund_reply_suppressed")
    assert result.get("refund_reply_sent") is True
