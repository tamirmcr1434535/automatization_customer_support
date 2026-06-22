"""
Unit tests for Country custom field (BUG-10)
=============================================
Covers:
  A.  `_set_country_for_ticket` helper in main.py — normalises ISO-2,
      silently skips invalid/empty values, never raises on Zendesk errors.
  B.  WooCommerceClient.cancel_subscription — includes `country` in result
      dict, extracted from customer.billing.country (or sub.billing.country
      as fallback).

External clients (Zendesk, Anthropic, BigQuery) are mocked so no network
calls happen during the test run.
"""
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

# ── env + heavy-module stubs (must precede `import main`) ────────────────
os.environ.setdefault("ZENDESK_SUBDOMAIN", "wwiqtest")
os.environ.setdefault("ZENDESK_EMAIL", "bot@test.com")
os.environ.setdefault("ZENDESK_API_TOKEN", "token")
os.environ.setdefault("WOO_SITE_URL", "https://iqbooster.org")
os.environ.setdefault("WOO_CONSUMER_KEY", "ck_test")
os.environ.setdefault("WOO_CONSUMER_SECRET", "cs_test")
os.environ.setdefault("STRIPE_SECRET_KEY", "sk_test")
os.environ.setdefault("ANTHROPIC_API_KEY", "sk-ant-test")
os.environ.setdefault("SLACK_WEBHOOK_URL", "https://hooks.slack.com/test")
os.environ.setdefault("SKIP_WC_HEALTHCHECK", "true")

sys.modules.setdefault("classifier", MagicMock())
sys.modules.setdefault("reply_generator", MagicMock())
sys.modules.setdefault("bq_logger", MagicMock())

import main  # noqa: E402
from woocommerce_client import WooCommerceClient  # noqa: E402
from datetime import datetime, timezone, timedelta  # noqa: E402


def _make_sub(sub_id=101, country=None):
    """Local helper — builds a minimal active trial sub dict.

    Doesn't rely on tests/conftest.make_wc_subscription which has a stale
    signature in the current repo (missing `days_since_start`).
    """
    start_dt = datetime.now(timezone.utc) - timedelta(days=3)
    sub = {
        "id": sub_id,
        "status": "active",
        "trial_end_date_gmt": "0000-00-00 00:00:00",
        "start_date_gmt": start_dt.strftime("%Y-%m-%dT%H:%M:%S"),
        "line_items": [{"name": "IQ Test Monthly"}],
    }
    if country is not None:
        sub["billing"] = {"email": "test@example.com", "country": country}
    return sub


# ────────────────────────────────────────────────────────────────────────
#  A. _set_country_for_ticket
# ────────────────────────────────────────────────────────────────────────

def test_country_iso2_lowercased_and_sent():
    """Valid ISO-2 → forwarded to Zendesk set_custom_field as lowercase."""
    with patch.object(main, "zendesk") as zd:
        main._set_country_for_ticket("12345", "JP")
        zd.set_custom_field.assert_called_once_with(
            "12345", int(main._ZENDESK_COUNTRY_FIELD_ID), "jp",
        )


def test_country_already_lowercase_passed_through():
    with patch.object(main, "zendesk") as zd:
        main._set_country_for_ticket("12345", "ua")
        zd.set_custom_field.assert_called_once_with(
            "12345", int(main._ZENDESK_COUNTRY_FIELD_ID), "ua",
        )


def test_country_empty_string_silent_skip():
    with patch.object(main, "zendesk") as zd:
        main._set_country_for_ticket("12345", "")
        zd.set_custom_field.assert_not_called()


def test_country_none_silent_skip():
    with patch.object(main, "zendesk") as zd:
        main._set_country_for_ticket("12345", None)  # type: ignore[arg-type]
        zd.set_custom_field.assert_not_called()


def test_country_invalid_length_skipped(caplog):
    """3+ letter codes (e.g. ISO-3 "JPN") are rejected — not sent to ZD."""
    with patch.object(main, "zendesk") as zd:
        main._set_country_for_ticket("12345", "JPN")
        zd.set_custom_field.assert_not_called()


def test_country_non_alpha_skipped():
    """Numeric codes / garbage are rejected — not sent to ZD."""
    with patch.object(main, "zendesk") as zd:
        main._set_country_for_ticket("12345", "12")
        zd.set_custom_field.assert_not_called()


def test_country_zendesk_failure_does_not_raise():
    """Zendesk API failure must not raise — country is reporting-only."""
    with patch.object(main, "zendesk") as zd:
        zd.set_custom_field.side_effect = RuntimeError("zd down")
        # Should not raise:
        main._set_country_for_ticket("12345", "JP")


def test_country_field_id_empty_disables_call(monkeypatch):
    """Explicit empty ZENDESK_COUNTRY_FIELD_ID disables the integration."""
    monkeypatch.setattr(main, "_ZENDESK_COUNTRY_FIELD_ID", "")
    with patch.object(main, "zendesk") as zd:
        main._set_country_for_ticket("12345", "JP")
        zd.set_custom_field.assert_not_called()


def test_country_whitespace_stripped():
    """Stray whitespace around the ISO code is tolerated."""
    with patch.object(main, "zendesk") as zd:
        main._set_country_for_ticket("12345", "  jp  ")
        zd.set_custom_field.assert_called_once_with(
            "12345", int(main._ZENDESK_COUNTRY_FIELD_ID), "jp",
        )


# ────────────────────────────────────────────────────────────────────────
#  B. WooCommerceClient.cancel_subscription includes country
# ────────────────────────────────────────────────────────────────────────

def _wc_client():
    return WooCommerceClient(
        site_url="https://test.example.com",
        consumer_key="ck_test",
        consumer_secret="cs_test",
        dry_run=False,
    )


def _dispatcher(responses: dict):
    """
    URL-substring dispatcher for both `requests.get` and `requests.request`
    paths in WooCommerceClient. The orders endpoint is matched BEFORE the
    "subscriptions" key (URL "subscriptions/101/orders" contains both).
    """
    def _h(*args, **kwargs):
        # requests.get(url, ...)  →  url is args[0]
        # requests.request(method, url, ...)  →  url is args[1]
        if len(args) >= 2 and isinstance(args[0], str) and args[0].isupper():
            url = args[1]
        elif args:
            url = args[0]
        else:
            url = kwargs.get("url", "")
        resp = MagicMock()
        resp.ok = True
        resp.status_code = 200
        resp.reason = "OK"
        resp.headers.get.return_value = None
        if "/orders" in url:
            resp.json.return_value = responses.get("orders", [])
            return resp
        for key, value in responses.items():
            if key in url:
                resp.json.return_value = value
                return resp
        resp.json.return_value = []
        return resp
    return _h


def _make_mock_put(status_code=200):
    resp = MagicMock()
    resp.ok = status_code < 400
    resp.status_code = status_code
    resp.text = "OK"
    return resp


@patch("woocommerce_client.requests.put")
@patch("woocommerce_client.requests.request")
@patch("woocommerce_client.requests.get")
def test_country_extracted_from_customer_billing(mock_get, mock_request, mock_put):
    """billing.country on the customer → propagates into result['country']."""
    customer = {
        "id": 42, "email": "test@example.com",
        "billing": {"email": "test@example.com", "country": "JP"},
    }
    sub = _make_sub()
    dispatch = _dispatcher({
        "customers": [customer],
        "subscriptions": [sub],
    })
    mock_get.side_effect = dispatch
    mock_request.side_effect = dispatch
    mock_put.return_value = _make_mock_put(200)

    result = _wc_client().cancel_subscription("test@example.com")

    assert result["status"] == "trial_cancelled", result
    # Country preserved as-is (WC returns uppercase ISO-2; main.py normalises).
    assert result["country"] == "JP", result


@patch("woocommerce_client.requests.put")
@patch("woocommerce_client.requests.request")
@patch("woocommerce_client.requests.get")
def test_country_fallback_from_subscription_billing(mock_get, mock_request, mock_put):
    """When customer has no billing.country, fall back to sub.billing.country."""
    customer = {"id": 42, "email": "test@example.com", "billing": {}}
    sub = _make_sub(country="KR")
    dispatch = _dispatcher({
        "customers": [customer],
        "subscriptions": [sub],
    })
    mock_get.side_effect = dispatch
    mock_request.side_effect = dispatch
    mock_put.return_value = _make_mock_put(200)

    result = _wc_client().cancel_subscription("test@example.com")
    assert result["country"] == "KR", result


@patch("woocommerce_client.requests.request")
@patch("woocommerce_client.requests.get")
def test_country_empty_when_customer_missing(mock_get, mock_request):
    """No customer at all → country defaults to '' (silent skip downstream)."""
    dispatch = _dispatcher({})
    mock_get.side_effect = dispatch
    mock_request.side_effect = dispatch

    result = _wc_client().cancel_subscription("nobody@example.com")
    assert result["status"] == "not_found", result
    assert result["country"] == "", result


@patch("woocommerce_client.requests.request")
@patch("woocommerce_client.requests.get")
def test_country_empty_when_customer_billing_missing(mock_get, mock_request):
    """Customer exists but no billing.country anywhere → country is ''."""
    customer = {"id": 42, "email": "test@example.com", "billing": {}}
    dispatch = _dispatcher({
        "customers": [customer],
        "subscriptions": [],
    })
    mock_get.side_effect = dispatch
    mock_request.side_effect = dispatch

    result = _wc_client().cancel_subscription("test@example.com")
    assert result["status"] == "no_active_sub", result
    assert result["country"] == "", result
