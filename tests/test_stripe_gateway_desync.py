"""WooCommerce saying "cancelled" must not be trusted as "the money stopped".

Live incident #147892 (2026-06-23): a Japanese customer asked to cancel. The bot
cancelled WooCommerce subscription #3522734, reported success, and replied
"no further charges will be made". Stripe then charged 5490 JPY on 2026-07-12
and again on 2026-08-09 (provider=Stripe). A human found it two months later and
had to refund both cycles.

Root cause: the Stripe lookup only ran as a FALLBACK when WooCommerce found
nothing. Once WC reported a successful cancel, the gateway was never checked, so
a WC/Stripe desync went straight to the customer as a false promise.

The subtlety that makes this worth a test file: a gracefully cancelled Stripe
subscription STAYS status="active" until the period ends. Checking the status
alone would flag every healthy cancellation as a desync — `cancel_at_period_end`
is the flag that actually distinguishes "will keep billing" from "winding down".
"""
import os
from unittest.mock import MagicMock, patch

os.environ.setdefault("SKIP_WC_HEALTHCHECK", "true")

import main


def _wc_cancelled():
    return {"status": "cancelled", "email": "hope10354649@yahoo.co.jp",
            "cancelled": True, "subscription_type": "subscription",
            "subscription_id": 3522734, "source": "woocommerce"}


def _run(find_result, cancel_result=None):
    """Drive the safety net with a stubbed Stripe client; returns (result, zd, stripe)."""
    result = {}
    zd = MagicMock()
    stripe = MagicMock()
    stripe.find_active_subscription.return_value = find_result
    stripe.cancel_subscription_by_id.return_value = (
        cancel_result if cancel_result is not None
        else {"status": "cancelled", "subscription_id": "sub_live", "cancelled": True}
    )
    with patch.object(main, "stripe_cli", stripe), patch.object(main, "zendesk", zd):
        main._cancel_leftover_stripe_sub(
            "147892", "hope10354649@yahoo.co.jp", _wc_cancelled(), result)
    return result, zd, stripe


# ── The incident: Stripe still billing after WC said cancelled ───────────── #

def test_leftover_billing_subscription_is_cancelled():
    result, zd, stripe = _run(
        {"status": "billing", "subscription_id": "sub_live",
         "subscription_status": "active", "plan": "IQ Booster"})
    stripe.cancel_subscription_by_id.assert_called_once_with("sub_live")
    assert result["stripe_leftover_cancelled"] is True
    assert result["stripe_leftover_subscription_id"] == "sub_live"
    note = zd.add_internal_note.call_args.args[1]
    assert "Gateway desync" in note and "sub_live" in note


# ── The false-positive trap: healthy cancellations must stay silent ──────── #

def test_subscription_already_winding_down_is_not_flagged():
    """cancel_at_period_end=True is reported as no_active_sub by the client —
    the gateway agrees with WC, so nothing should happen."""
    result, zd, stripe = _run({"status": "no_active_sub"})
    stripe.cancel_subscription_by_id.assert_not_called()
    zd.add_internal_note.assert_not_called()
    assert result == {}


def test_no_stripe_customer_is_not_flagged():
    result, zd, stripe = _run({"status": "not_found"})
    stripe.cancel_subscription_by_id.assert_not_called()
    zd.add_internal_note.assert_not_called()
    assert result == {}


# ── Failure handling: never break the cancellation, always tell a human ──── #

def test_stripe_lookup_error_is_non_blocking_and_silent_on_the_ticket():
    result, zd, stripe = _run({"status": "error", "error": "rate limited"})
    stripe.cancel_subscription_by_id.assert_not_called()
    zd.add_internal_note.assert_not_called()
    assert result == {}


def test_failed_stripe_cancel_escalates_loudly():
    """Worst case: desync found, but we could not stop it — the customer has
    already been promised no further charges, so a human must be told."""
    result, zd, _ = _run(
        {"status": "billing", "subscription_id": "sub_live"},
        cancel_result={"status": "error", "error": "card_declined",
                       "cancelled": False})
    assert result["stripe_leftover_cancel_failed"] is True
    assert "stripe_leftover_cancelled" not in result
    note = zd.add_internal_note.call_args.args[1]
    assert "could NOT cancel" in note and "manually" in note


def test_lookup_exception_never_propagates():
    """The WC cancellation already happened — a Stripe hiccup must not undo it."""
    result = {}
    zd = MagicMock()
    stripe = MagicMock()
    stripe.find_active_subscription.side_effect = RuntimeError("boom")
    with patch.object(main, "stripe_cli", stripe), patch.object(main, "zendesk", zd):
        main._cancel_leftover_stripe_sub("147892", "a@b.com", _wc_cancelled(), result)
    assert result == {}
    zd.add_internal_note.assert_not_called()


def test_missing_email_is_a_noop():
    result, zd, stripe = {}, MagicMock(), MagicMock()
    with patch.object(main, "stripe_cli", stripe), patch.object(main, "zendesk", zd):
        main._cancel_leftover_stripe_sub("147892", "", _wc_cancelled(), result)
    stripe.find_active_subscription.assert_not_called()
    assert result == {}
