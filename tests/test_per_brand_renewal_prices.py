"""iqtest.jp sells a yearly plan; the amount guard only knew monthly prices.

`_RENEWAL_PRICE` is keyed by currency alone, which was right while every brand
charged the same. iqtest.jp is the only one of the 37 rows in Anna's pricing
sheet with a yearly option — ¥29,990 / $199 next to the usual ¥5,490 / $29.99 —
so every yearly refund would have been escalated as REFUND_AMOUNT_ANOMALY.

Adding those to the shared table would have widened the band for every brand on
those currencies. They are per-brand, and ADDITIVE: a brand can gain a valid
price, never lose one.
"""
import os
import sys
from unittest.mock import MagicMock

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


# ── What the change is for ───────────────────────────────────────────────── #

def test_iqtestjp_yearly_passes():
    assert main._amount_guard(29990, "JPY", "iqtestjp")[0] is True
    assert main._amount_guard(199, "USD", "iqtestjp")[0] is True


def test_iqtestjp_monthly_still_passes():
    """The brand layer adds to the currency defaults, it does not replace them."""
    assert main._amount_guard(5490, "JPY", "iqtestjp")[0] is True
    assert main._amount_guard(29.99, "USD", "iqtestjp")[0] is True


# ── What it must NOT do ──────────────────────────────────────────────────── #

def test_other_brands_keep_the_narrow_band():
    """The whole point of not putting 199 in the shared USD list: a $199 charge
    on WW IQ Test is still an anomaly a human should look at."""
    ok, why = main._amount_guard(199, "USD", "wwiqtest")
    assert ok is False and "deviates" in why
    assert main._amount_guard(29990, "JPY", "wwiqtest")[0] is False


def test_no_brand_given_behaves_exactly_as_before():
    assert main._amount_guard(29.99, "USD")[0] is True
    assert main._amount_guard(199, "USD")[0] is False


def test_unknown_brand_is_not_an_error_just_no_extra_prices():
    assert main._amount_guard(29.99, "USD", "brand-we-never-heard-of")[0] is True
    assert main._amount_guard(199, "USD", "brand-we-never-heard-of")[0] is False


# ── Guard invariants that must survive the refactor ─────────────────────── #

def test_zero_null_and_junk_are_still_rejected():
    for amount in (None, 0, -5, "abc"):
        assert main._amount_guard(amount, "JPY", "iqtestjp")[0] is False


def test_unknown_currency_still_fails_closed():
    ok, why = main._amount_guard(1000, "XYZ", "iqtestjp")
    assert ok is False and "no reference renewal price" in why


def test_band_is_still_twenty_percent():
    """±20% of ¥29,990 → 23,992..35,988."""
    assert main._amount_guard(24000, "JPY", "iqtestjp")[0] is True
    assert main._amount_guard(23000, "JPY", "iqtestjp")[0] is False


def test_currency_and_brand_lookups_are_case_insensitive():
    assert main._amount_guard(199, "usd", "IQTESTJP")[0] is True
