"""iqtest.jp — brand resolution and legal links.

Groundwork only. The brand is NOT wired for refund execution on purpose: no
x-host is mapped, so `_refund_xhost` returns "" and main.py's guard refuses to
move money for it (skipped_no_xhost → a human decides). Cancellations need
nothing brand-specific — Nexus `search-subscription` posts only {"email": ...}
and searches across every migrated brand, and iqtest.jp is migrated (34
subscriptions / 24 emails / 17 active as of 2026-09-10, site_url
https://funnel.iqtest.jp, every row carrying an email).
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
import refund_reply_links  # noqa: E402


# ── Brand from the ticket text (the fallback when brand_id is unmapped) ──── #

def test_contact_form_footer_resolves_iqtestjp():
    assert main._brand_key(
        "-- This e-mail was sent from a contact form on IQtest.jp "
        "(https://iqtest.jp)") == "iqtestjp"


def test_funnel_subdomain_also_resolves():
    """Purchases happen on funnel.iqtest.jp, so the footer may carry either."""
    assert main._brand_key("sent from https://funnel.iqtest.jp/") == "iqtestjp"


def test_wwiqtest_is_not_swallowed_by_the_new_marker():
    """"wwiqtest.com" contains "iqtest" — the marker must be the full
    "iqtest.jp" or every WW IQ Test ticket would resolve to the wrong brand."""
    assert main._brand_key(
        "sent from a contact form on WW IQ TEST (https://jap.wwiqtest.com)"
    ) == "wwiqtest"
    assert main._brand_key("https://wwiqtest.com/pricing/") == "wwiqtest"


# ── Brand from the Nexus charge host ─────────────────────────────────────── #

def test_charge_host_resolves_iqtestjp():
    for host in ("iqtest.jp", "funnel.iqtest.jp", "https://funnel.iqtest.jp"):
        assert main._host_to_brand(host) == "iqtestjp", host


def test_charge_host_still_resolves_the_neighbours():
    assert main._host_to_brand("jap.wwiqtest.com") == "wwiqtest"
    assert main._host_to_brand("16types.ai/ja") == "16types"


# ── Legal links: the customer must not be shown another brand's terms ────── #

def test_japanese_customer_gets_iqtestjp_terms():
    terms, sub = refund_reply_links.links_for("iqtestjp", "JP")
    assert terms == "https://iqtest.jp/terms"
    assert sub == "https://iqtest.jp/subscription-policy"


def test_english_customer_gets_the_en_docs():
    terms, sub = refund_reply_links.links_for("iqtestjp", "EN")
    assert terms == "https://iqtest.jp/en/terms"


def test_a_third_language_falls_back_within_the_brand():
    """iqtest.jp publishes only EN + JA. The fallback order is brand+lang →
    brand+EN → wwiqtest — so an unlisted language must land on iqtest.jp's
    English docs, never on WW IQ Test's."""
    terms, sub = refund_reply_links.links_for("iqtestjp", "DE")
    assert "iqtest.jp" in terms and "wwiqtest" not in terms
    assert "iqtest.jp" in sub


# ── Money stays blocked until the backend confirms the scope ─────────────── #

def test_no_xhost_means_refunds_cannot_execute():
    """A resolved-but-wrong x-host would refund against another brand's scope
    and main.py cannot detect that. No mapping → skipped_no_xhost → human."""
    assert main._refund_xhost("iqtestjp") == ""
