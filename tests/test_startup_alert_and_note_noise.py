"""An alert that cries outage during a non-outage, and a note nobody would read.

Both found on 2026-09-10 by watching the first minutes of live traffic after a
deploy, not by reading the code.
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
from slack_client import SlackClient  # noqa: E402

# `main.reply_generator` is a MagicMock shared across every test module, so
# this assignment is global. It must carry the SAME set the other modules use —
# narrowing it here silently broke test_soft_route_approve_relaxation, which
# reads it through main._soft_route_approve_allowed.
main.reply_generator.REFUND_APPROVE_CODES = {
    "WOULD_BE_REFUNDED", "WOULD_BE_REFUNDED_LAST_ONLY"}


def _blocks_text(mock_post):
    """Every bit of text the alert would show a human."""
    text, blocks = mock_post.call_args.args[0], mock_post.call_args.args[1]
    parts = [text]
    for b in blocks:
        t = b.get("text")
        if isinstance(t, dict):
            parts.append(t.get("text", ""))
        for f in b.get("fields", []) or []:
            parts.append(f.get("text", ""))
    return " ".join(parts)


# ── The alert must not claim an outage that is not happening ─────────────── #

def test_degraded_startup_does_not_claim_the_bot_exited():
    """A WooCommerce read timeout at startup is not fatal — the bot serves
    tickets. The 2026-09-10 alert said "Bot will exit and no tickets will be
    processed" in the same message whose Detail said it had continued."""
    sc = SlackClient(bot_token="x", target_email="a@b.c", dry_run=False)
    with patch.object(sc, "_post", return_value=True) as mock_post:
        sc.notify_startup_failure(
            service="WooCommerce", error_kind="timeout_error",
            error_detail="Read timed out.\n\nBot continued startup "
                         "(non-auth errors are not fatal).",
            fatal=False,
        )
    said = _blocks_text(mock_post)
    assert "will exit" not in said
    assert "Deploy is broken" not in said
    assert "IS processing tickets" in said


def test_fatal_startup_still_says_the_deploy_is_broken():
    """An auth_error really does sys.exit — that alert must stay alarming."""
    sc = SlackClient(bot_token="x", target_email="a@b.c", dry_run=False)
    with patch.object(sc, "_post", return_value=True) as mock_post:
        sc.notify_startup_failure(
            service="WooCommerce", error_kind="auth_error",
            error_detail="401 Unauthorized",
        )
    said = _blocks_text(mock_post)
    assert "Deploy is broken" in said
    assert "NOT processing tickets" in said


def test_non_auth_healthcheck_passes_fatal_false():
    """The wiring, not just the wording: main.py must tell Slack it survived."""
    import inspect
    src = inspect.getsource(main._run_wc_healthcheck_once)
    non_fatal = src.split("else:", 1)[1]
    assert "fatal=False" in non_fatal


# ── The note must appear only when there is something to do ──────────────── #

def _cancel_result(plan, host):
    return {
        "status": "trial_cancelled", "cancelled": True,
        "subscription_type": "trial", "subscription_id": 1,
        "plan": plan, "source": "woocommerce", "nexus_host": host,
        "order_count": 1,
    }


@patch.object(main, "log_result")
@patch.object(main, "validate_reply", return_value=(True, ""))
@patch.object(main, "generate_reply", return_value="Cancelled.")
@patch.object(main, "zendesk")
def test_cross_brand_with_a_known_product_needs_no_note(
    mock_zd, mock_reply, mock_validate, mock_log
):
    """Reply names the right product → nothing for a human to fix → no
    cross-brand note. iqbooster/wwiqtest turned out to be routine traffic; a
    note on every one of those is how agents learn to skip notes.

    The routine audit note every cancellation gets is untouched — and it
    already carries the plan name, so an agent who wants the detail has it.
    """
    cr = _cancel_result("16 Types Growth Plan", "16types.ai")
    main._finish_cancellation("1", "T", "EN", "TRIAL_CANCELLATION", cr, {},
                              zendesk_brand="iqpro")
    assert cr["brand_phrase"] == "16 Types Growth Plan"
    notes = " ".join(str(c.args[1]) for c in mock_zd.add_internal_note.call_args_list)
    assert "wrote to" not in notes
    assert "no confirmed product name" not in notes
    # the ordinary audit note is still there
    assert "auto-cancelled this ticket" in notes


@patch.object(main, "log_result")
@patch.object(main, "validate_reply", return_value=(True, ""))
@patch.object(main, "generate_reply", return_value="Cancelled.")
@patch.object(main, "zendesk")
def test_cross_brand_adds_no_note_even_when_the_product_is_unnamed(
    mock_zd, mock_reply, mock_validate, mock_log
):
    """The live #193375/#193377 case: contacted iqbooster, owns wwiqtest.

    3 of the first 5 cancellations after the 2026-09-10 deploy were this exact
    pair, and product names for the remaining brands are not being added
    (Yaroslav) — so a per-ticket note here could never be actioned or retired.
    It stays in the log, not on the ticket.
    """
    cr = _cancel_result("WW IQ Test 1 Week Trial Then 28 days Stripe",
                        "jap.wwiqtest.com")
    main._finish_cancellation("193375", "T", "NL", "TRIAL_CANCELLATION", cr, {},
                              zendesk_brand="iqbooster")
    assert "brand_phrase" not in cr
    notes = " ".join(str(c.args[1]) for c in mock_zd.add_internal_note.call_args_list)
    assert "no confirmed product name" not in notes
    assert "wrote to" not in notes
    # the ordinary audit note is untouched
    assert "auto-cancelled this ticket" in notes


@patch.object(main, "log_result")
@patch.object(main, "validate_reply", return_value=(True, ""))
@patch.object(main, "generate_reply", return_value="Cancelled.")
@patch.object(main, "zendesk")
def test_cross_brand_cancellation_is_never_escalated(
    mock_zd, mock_reply, mock_validate, mock_log
):
    """Yaroslav 2026-09-10: a cross-brand trial cancellation is an ordinary
    cancellation. Cancel it and close the ticket — do not hand it to an agent."""
    cr = _cancel_result("WW IQ Test 1 Week Trial", "wwiqtest.com")
    result = {}
    main._finish_cancellation("193377", "T", "NL", "TRIAL_CANCELLATION", cr,
                              result, zendesk_brand="iqbooster")
    tagged = [c.args[1] for c in mock_zd.add_tag.call_args_list]
    assert "needs_manual_review" not in tagged
    assert "ai_bot_failed" not in tagged
    assert not str(result.get("status", "")).startswith("escalated")
