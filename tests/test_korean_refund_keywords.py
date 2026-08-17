"""Korean "I never agreed — cancel that amount" is a refund ask, not a cancellation.

Live incident #179802 (2026-08-17, Anna: "чомусь на цьому випадку бот не обробив
як рефанд"). A Korean customer wrote, an hour after the first renewal charge:

    "1시간전 정기 구독료가 결제된 것을 확인했습니다.
     나는 IQ BOOSTER 프로그램 구독에 동의한적이 없으므로
     해당 금액을 취소해주시기 바랍니다."

    ("I see the renewal was charged an hour ago. I never agreed to subscribe to
     IQ BOOSTER, so please cancel that amount.")

The refund engine got it right — flow1_subscription, 39990 KRW, window
[currency=7d, age=0d] → WOULD_BE_REFUNDED. Then the ticket was suppressed by the
#175987 guard (`no_refund_request_in_text`), because the Korean text scored ZERO
hits in `_REFUND_KEYWORDS`: the list carried the Japanese 同意していません and the
English "never agreed to", but no Korean equivalent, and no Korean form of
"cancel that amount" (only 승인취소; 결제취소 had been removed as too ambiguous).
A human then cancelled and refunded — exactly what the bot had already decided.

Two families close the gap, both narrow on purpose:
  1. 동의한 적 없 / 동의하지 않았 — "I never agreed", the KR twin of 同意していません;
  2. 금액을 취소 / 금액 취소 — an *amount* is money already taken, so asking to
     cancel it can only mean giving it back. This is why it is safe where the
     broader 결제 취소 ("cancel the payment") is not — that one is routinely a
     plain forward-looking subscription cancel and stays out of the refund lists.

Family 2 has to land in `_REFUND_DEMAND_WORDS` as well: routing alone still hits
the `explanation_only_no_refund_demand` guard, which requires an explicit
money-back demand before any approve code may auto-answer.
"""
import os
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

os.environ.setdefault("SKIP_WC_HEALTHCHECK", "true")

import main


# The live ticket, verbatim (Zendesk #179802, customer's own words only —
# the quoted IQ Booster welcome e-mail below it carries no refund wording).
TICKET_179802 = (
    "안녕하세요.\n"
    "1시간전 정기 구독료가 결제된 것을 확인했습니다.\n"
    "나는 IQ BOOSTER 프로그램 구독에 동의한적이 없으므로\n"
    "해당 금액을 취소해주시기 바랍니다.\n"
    "나의 아이디는 acenote@naver.com 입니다.\n"
    "빠른처리 바랍니다."
)


# ── The three detectors the ticket has to clear ──────────────────────────── #

def test_179802_is_recognised_as_a_refund_request():
    """Routing: without this the no_refund_request_in_text guard suppresses it."""
    assert main._contains_refund_request(TICKET_179802) is True


def test_179802_is_an_explicit_refund_demand():
    """Auto-approve gate: 'cancel that amount' IS a demand for the money back."""
    assert main._has_explicit_refund_demand(TICKET_179802) is True


def test_179802_beats_the_cancel_signal():
    """The Korean cancel list carries a bare '취소', so a strong signal is needed
    or the ticket reads as a plain cancellation and the charge stays put."""
    assert main._contains_cancel_signal(TICKET_179802) is True
    assert main._contains_strong_refund_signal(TICKET_179802) is True


def test_korean_never_agreed_phrasings():
    for text in (
        "구독에 동의한적이 없습니다",
        "동의한 적 없는데 결제가 되었습니다",
        "저는 동의하지 않았습니다",
        "구독에 동의한 적이 없으므로 처리 부탁드립니다",
    ):
        assert main._contains_refund_request(text) is True, text


def test_korean_cancel_that_amount_phrasings():
    """An amount is money already taken → both routing AND money-back demand."""
    for text in (
        "해당 금액을 취소해주세요",
        "결제된 금액 취소 부탁드립니다",
        "결제 금액을 환급해 주세요",
    ):
        assert main._contains_refund_request(text) is True, text
        assert main._has_explicit_refund_demand(text) is True, text


def test_korean_cancel_the_billing_routes_but_does_not_auto_refund():
    """청구를 취소 ('cancel the billing') is routing-only on purpose: unlike an
    amount, a *billing* can plausibly mean 'stop invoicing me from now on', so it
    reaches the refund flow but never satisfies the auto-approve demand gate."""
    text = "이 청구를 취소해 주시기 바랍니다"
    assert main._contains_refund_request(text) is True
    assert main._has_explicit_refund_demand(text) is False


# ── Regression: a plain Korean cancellation must stay a cancellation ─────── #

def test_plain_korean_cancels_are_not_refund_asks():
    """'결제 취소' / '구독 취소' stay OUT of the refund lists — they were removed
    for exactly this reason (47 such tickets in 60 days, most of them ordinary
    forward-looking cancels). Only an *amount* being cancelled means money back."""
    for text in (
        "구독 취소해주세요",
        "결제 취소 부탁드립니다",
        "정기결제 취소하고 싶습니다",
        "해지하고 싶습니다. 방법을 알려주세요.",
        "무료 체험을 취소하고 싶어요",
    ):
        assert main._contains_refund_request(text) is False, text
        assert main._has_explicit_refund_demand(text) is False, text


def test_japanese_false_positive_class_still_suppressed():
    """#175987 must not regress: 'cancelled but still charged', zero money wording."""
    assert main._contains_refund_request(
        "解約したはずなのに、ずっとIQPROから4990円引かれています。契約の状況確認して欲しいです。"
    ) is False


# ── End-to-end: the approve now reaches the customer ─────────────────────── #

def _decision():
    return SimpleNamespace(
        would_be_refunded=True, reason_code="WOULD_BE_REFUNDED", human_message="msg",
        guard_trail=["route:subscription", "window[currency=7d,age=0d]"],
        computed_amount="39990", currency="KRW",
        customer_stated_amount=None, customer_stated_amounts="",
        candidate_charge_id="ch_1", charge_type="subscription",
        candidate_charges="ch_1:39990:2026-08-17", refund_flow="flow1_subscription",
        source="wwiqtest", engine_version="test",
    )


def _drive(result):
    fake_reply_gen = SimpleNamespace(
        REFUND_AUTOREPLY_CODES={"WOULD_BE_REFUNDED", "WOULD_BE_REFUNDED_LAST_ONLY",
                                "OUTSIDE_REFUND_WINDOW"},
        REFUND_APPROVE_CODES={"WOULD_BE_REFUNDED", "WOULD_BE_REFUNDED_LAST_ONLY"},
        generate_refund_reply=lambda rc, lang, data: "안녕하세요. 처리해 드리겠습니다.",
    )
    zd = MagicMock()
    rc_client = MagicMock()
    rc_client.is_configured.return_value = False
    rc_client.create_refund.return_value = {"status": "refunded", "executed": True,
                                            "refunded_amount": "39990"}
    with patch.object(main.refund_engine, "decide", return_value=_decision()), \
         patch.object(main, "reply_generator", fake_reply_gen), \
         patch.object(main, "refunds_enabled_for", return_value=True), \
         patch.object(main, "_refund_xhost", return_value="host"), \
         patch.object(main, "refund_client", rc_client), \
         patch.object(main, "refund_abuse", SimpleNamespace(check=lambda b, e: (True, ""))), \
         patch.object(main, "nexus_client", None), \
         patch.object(main, "zendesk", zd):
        main._refund_would_be_eval(
            "179802", "acenote@naver.com", "REFUND_REQUEST",
            {"confidence": 0.9, "language": "KO"}, result,
            ticket_text=TICKET_179802, as_of_date="2026-08-17T04:55:29Z",
            brand="wwiqtest",
        )
    return result, zd, rc_client


def test_179802_approve_is_no_longer_suppressed():
    """The live regression: with the keywords in place the bot answers itself."""
    result, zd, _ = _drive(
        {"refund_ask_in_text": main._contains_refund_request(TICKET_179802)}
    )
    assert not result.get("refund_reply_suppressed")
    assert result.get("refund_reply_sent") is True
    zd.reply_solve_and_set_fields.assert_called_once()
