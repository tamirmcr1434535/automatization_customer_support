"""Three live misses reported by Anna on 2026-08-22.

#182034 (JP) — a Zendesk FOLLOW-UP ("this is a supplementary comment on request
    #167119"). Policy is that follow-ups belong to an agent, and the code has a
    skip for them — but it matched on ENGLISH phrases only, so the Japanese
    preamble sailed past it. The bot cancelled again and sent a stock
    confirmation that answered neither of the customer's questions ("when does
    the cancellation take effect?" / "why was August charged?"). The
    satisfaction survey came back negative.

#181704 (NL) — "Ik heb direct mijn abonnement beëindigd, maar er wordt nog
    steeds geld afgeschreven" ("I cancelled immediately but money is STILL being
    taken"). Classified as a plain SUB_CANCELLATION, so the bot cancelled and
    replied "no further charges will be made" — never addressing the money
    already taken.

#181924 (KR) — Country left empty because WooCommerce had none on file, so Anna
    filled it in by hand. The language (KR) was an unambiguous signal.
"""
import os
from unittest.mock import MagicMock, patch

os.environ.setdefault("SKIP_WC_HEALTHCHECK", "true")

import main


# ── #182034: follow-up detection must not depend on language ─────────────── #

def test_zendesk_followup_flag_is_detected_structurally():
    """via_followup_source_id / via.source.rel are language-independent."""
    assert main._is_followup_ticket({"via_followup_source_id": 167119}) is True
    assert main._is_followup_ticket(
        {"via": {"source": {"rel": "follow_up"}}}) is True
    assert main._is_followup_ticket({"via": {"channel": "email"}}) is False
    assert main._is_followup_ticket({}) is False


def test_182034_japanese_followup_is_skipped_not_cancelled():
    result = _process_ticket(
        ticket_id="182034",
        subject='Re: [WW Personality Test]: WW PERSONALITY TEST "Contact Form"',
        body="以前いただいたリクエストNo.#167119に対する補足コメントです しつれいします。"
             "この解約はいつから有効でしょうか？解約の手続きをさせていただきましたが、"
             "８月分引き落としがされていますので、質問です。",
        extra_ticket={"via_followup_source_id": 167119},
        intent="SUB_RENEWAL_CANCELLATION",
        language="JA",
    )
    assert result["status"] == "skipped_followup"


def test_english_followup_phrase_still_skipped():
    """The pre-existing English path must keep working."""
    result = _process_ticket(
        ticket_id="1900",
        subject="Re: my request",
        body="This is a follow-up to your previous request #123. Please cancel.",
    )
    assert result["status"] == "skipped_followup"


def test_ordinary_ticket_is_not_treated_as_a_followup():
    result = _process_ticket(
        ticket_id="1901",
        subject="Cancel",
        body="Please cancel my subscription.",
    )
    assert result["status"] != "skipped_followup"


# ── #181704: "already cancelled, still being charged" ────────────────────── #

def test_181704_dutch_text_is_flagged():
    assert main._is_charged_after_cancelling(
        "Ik heb direct mijn abonnement beëindigd, maar er wordt nog steeds "
        "geld afgeschreven. Kunnen jullie dit stoppen?") is True


def test_charged_after_cancelling_across_languages():
    for text in (
        "I already cancelled my subscription but I am still being charged.",
        "解約手続きをしましたが、まだ引き落とされています。",
        "구독을 해지했는데 계속 결제가 되었습니다.",
        "Ich habe bereits gekündigt, trotzdem abgebucht.",
        "Ya cancelé mi suscripción pero me siguen cobrando.",
    ):
        assert main._is_charged_after_cancelling(text) is True, text


def test_plain_cancellation_is_not_flagged():
    """Both halves are required — ordinary cancellations keep their instant reply."""
    for text in (
        "Please cancel my subscription.",
        "解約したいです。方法を教えて下さい",
        "구독 해지 요청합니다",
        "Ik wil mijn abonnement opzeggen.",
        "I was charged 29.99, what is this?",          # charge complaint, no prior cancel
        "I already cancelled, thanks for confirming.",  # prior cancel, no charge complaint
        "",
    ):
        assert main._is_charged_after_cancelling(text) is False, text


def test_181704_cancels_then_escalates_instead_of_replying():
    """Billing must still be stopped (#147892), but the money question goes to a human."""
    result = _process_ticket(
        ticket_id="181704",
        subject='WW IQ TEST "Contact Form"',
        body="Beste, Ik heb direct mijn abonnement beëindigd, maar er wordt nog "
             "steeds geld afgeschreven. Kunnen jullie dit stoppen?",
        intent="SUB_CANCELLATION",
        language="NL",
    )
    assert result["status"] == "escalated_charged_after_cancelling"
    assert result["cancel_outcome"] == "success"      # subscription WAS cancelled
    assert _MOCKS["woo"].cancel_subscription.called
    _MOCKS["zd"].post_reply.assert_not_called()       # no stock confirmation
    notes = [c.args[1] for c in _MOCKS["zd"].add_internal_note.call_args_list]
    assert any("did NOT reply" in n for n in notes), notes


# ── #181924: country falls back to the language ──────────────────────────── #

def test_country_falls_back_to_language_when_wc_has_none():
    result = _process_ticket(
        ticket_id="181924",
        subject="구독 해지 요청 (구독 취소)",
        body="본 메일은 제 IQ 부스터 구독을 즉시 해지해 달라는 요청입니다.",
        intent="TRIAL_CANCELLATION",
        language="KR",
        wc_country="",           # WooCommerce has no country on file
    )
    assert result["status"] == "success"
    # Language (KR) filled the Country field that WooCommerce left empty,
    # and it is mirrored into BQ.
    assert result["country"] == "kr"


def test_wc_country_still_wins_over_the_language_fallback():
    result = _process_ticket(
        ticket_id="1902",
        subject="Cancel",
        body="Please cancel my subscription.",
        intent="TRIAL_CANCELLATION",
        language="KR",
        wc_country="JP",         # WC knows better than the reply language
    )
    assert result["country"] == "jp"


# ── harness ──────────────────────────────────────────────────────────────── #

_MOCKS = {}


def _process_ticket(ticket_id, subject, body, intent="TRIAL_CANCELLATION",
                    language="EN", extra_ticket=None, wc_country=""):
    """Same mocking contract as tests/test_main_flow.py::_setup_zd — the guards
    in _process (agent-replied, merge-candidate, spam, race) each need an
    explicit non-MagicMock default or every ticket short-circuits."""
    ticket = {
        "id": ticket_id, "subject": subject, "description": body,
        "tags": ["automation_test"],
        "requester": {"email": "user@example.com", "name": "Test User"},
    }
    ticket.update(extra_ticket or {})

    zd = MagicMock()
    zd.get_ticket.return_value = ticket
    zd.last_public_comment_is_from_agent.return_value = False
    zd.get_all_customer_comments_text.return_value = ""
    zd.get_first_customer_comment.return_value = ""
    zd.find_active_tickets_for_email.return_value = []
    zd.count_bot_replies.return_value = 0
    zd.get_ticket_tags.return_value = list(ticket.get("tags", []))
    zd.get_ticket_image_attachments.return_value = []

    woo = MagicMock()
    woo.cancel_subscription.return_value = {
        "status": "trial_cancelled", "email": "user@example.com",
        "cancelled": True, "subscription_type": "trial", "subscription_id": 101,
        "plan": "IQ Test Monthly", "source": "woocommerce", "country": wc_country,
        "order_count": 1,
    }
    stripe = MagicMock()
    stripe.find_active_subscription.return_value = {"status": "no_active_sub"}
    _MOCKS.update(zd=zd, woo=woo, stripe=stripe)

    classification = {"intent": intent, "confidence": 0.95, "language": language,
                      "chargeback_risk": False, "reasoning": "test"}

    with patch.object(main, "zendesk", zd), \
         patch.object(main, "woo", woo), \
         patch.object(main, "stripe_cli", stripe), \
         patch.object(main, "classify_ticket", return_value=classification), \
         patch.object(main, "generate_reply", return_value="Cancelled."), \
         patch.object(main, "validate_reply", return_value=(True, "")), \
         patch.object(main, "log_result"), \
         patch.object(main, "slack"), \
         patch.object(main.ticket_merger, "merge_user_tickets",
                      return_value={"status": "no_action"}), \
         patch.object(main, "TEST_MODE", False):
        return main._process(ticket_id)
