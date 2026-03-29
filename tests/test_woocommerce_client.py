"""
Unit tests for WooCommerceClient
==================================
All HTTP calls are mocked — no real network requests.

Test scenarios:
  1.  DRY_RUN mode — no real calls, returns dry_run status
  2.  Customer not found → not_found
  3.  Customer found, no subscriptions → no_active_sub
  4.  Customer found, active TRIAL → trial_cancelled
  5.  Customer found, paid subscription (no trial) → subscription_cancelled
  6.  Customer found, expired trial → treated as paid subscription
  7.  Customer found, pending-cancel subscription → subscription_cancelled
  8.  Cancel API returns error → propagates error status
  9.  Trial detection: future date returns True
  10. Trial detection: past date returns False
  11. Trial detection: "0000-00-00 00:00:00" returns False
  12. Trial detection: missing field returns False
"""

import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone, timedelta

from woocommerce_client import WooCommerceClient
from tests.conftest import make_wc_customer, make_wc_subscription


# ── Helpers ──────────────────────────────────────────────────────────────────

def make_client(dry_run=False):
    return WooCommerceClient(
        site_url="https://test.example.com",
        consumer_key="ck_test",
        consumer_secret="cs_test",
        dry_run=dry_run,
    )


def mock_get(responses: dict):
    """
    Build a requests.get mock that dispatches based on URL substring.
    `responses` maps URL substring → return value (list or dict).
    """
    def _get(url, **kwargs):
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.status_code = 200
        for key, value in responses.items():
            if key in url:
                mock_resp.json.return_value = value
                return mock_resp
        mock_resp.json.return_value = []
        return mock_resp
    return _get


def mock_put(status_code=200):
    mock_resp = MagicMock()
    mock_resp.ok = status_code < 400
    mock_resp.status_code = status_code
    mock_resp.text = "Error" if status_code >= 400 else "OK"
    return mock_resp


# ── 1. DRY_RUN mode ───────────────────────────────────────────────────────── #

def test_dry_run_returns_dry_run_status():
    client = make_client(dry_run=True)
    result = client.cancel_subscription("anyone@example.com")
    assert result["status"] == "dry_run"
    assert result["cancelled"] is True
    assert result["source"] == "woocommerce"


# ── 2. Customer not found ─────────────────────────────────────────────────── #

@patch("woocommerce_client.requests.get")
def test_customer_not_found(mock_get_fn):
    mock_resp = MagicMock()
    mock_resp.ok = True
    mock_resp.json.return_value = []       # empty list → no customer
    mock_get_fn.return_value = mock_resp

    client = make_client()
    result = client.cancel_subscription("nobody@example.com")
    assert result["status"] == "not_found"
    assert result["cancelled"] is False


# ── 3. Customer found but no subscriptions ────────────────────────────────── #

@patch("woocommerce_client.requests.get")
def test_no_subscriptions(mock_get_fn):
    mock_get_fn.side_effect = mock_get({
        "customers": [make_wc_customer()],
        "subscriptions": [],
    })
    client = make_client()
    result = client.cancel_subscription("test@example.com")
    assert result["status"] == "no_active_sub"
    assert result["cancelled"] is False


# ── 4. Active trial → trial_cancelled ─────────────────────────────────────── #

@patch("woocommerce_client.requests.put")
@patch("woocommerce_client.requests.get")
def test_active_trial_cancelled(mock_get_fn, mock_put_fn):
    trial_sub = make_wc_subscription(trial_days_from_now=7)
    mock_get_fn.side_effect = mock_get({
        "customers": [make_wc_customer()],
        "subscriptions": [trial_sub],
    })
    mock_put_fn.return_value = mock_put(200)

    client = make_client()
    result = client.cancel_subscription("test@example.com")
    assert result["status"] == "trial_cancelled"
    assert result["cancelled"] is True
    assert result["subscription_type"] == "trial"
    assert result["subscription_id"] == trial_sub["id"]


# ── 5. Paid subscription (no trial) → subscription_cancelled ─────────────── #

@patch("woocommerce_client.requests.put")
@patch("woocommerce_client.requests.get")
def test_paid_subscription_cancelled(mock_get_fn, mock_put_fn):
    paid_sub = make_wc_subscription(trial_days_from_now=0)
    mock_get_fn.side_effect = mock_get({
        "customers": [make_wc_customer()],
        "subscriptions": [paid_sub],
    })
    mock_put_fn.return_value = mock_put(200)

    client = make_client()
    result = client.cancel_subscription("test@example.com")
    assert result["status"] == "subscription_cancelled"
    assert result["cancelled"] is True
    assert result["subscription_type"] == "subscription"


# ── 6. Expired trial → treated as paid subscription ───────────────────────── #

@patch("woocommerce_client.requests.put")
@patch("woocommerce_client.requests.get")
def test_expired_trial_treated_as_subscription(mock_get_fn, mock_put_fn):
    past = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
    sub = make_wc_subscription()
    sub["trial_end_date_gmt"] = past

    mock_get_fn.side_effect = mock_get({
        "customers": [make_wc_customer()],
        "subscriptions": [sub],
    })
    mock_put_fn.return_value = mock_put(200)

    client = make_client()
    result = client.cancel_subscription("test@example.com")
    assert result["subscription_type"] == "subscription"
    assert result["status"] == "subscription_cancelled"


# ── 7. pending-cancel subscription ───────────────────────────────────────── #

@patch("woocommerce_client.requests.put")
@patch("woocommerce_client.requests.get")
def test_pending_cancel_subscription(mock_get_fn, mock_put_fn):
    sub = make_wc_subscription(status="pending-cancel")
    mock_get_fn.side_effect = mock_get({
        "customers": [make_wc_customer()],
        "subscriptions": [sub],
    })
    mock_put_fn.return_value = mock_put(200)

    client = make_client()
    result = client.cancel_subscription("test@example.com")
    assert result["cancelled"] is True


# ── 8. Cancel API returns error ───────────────────────────────────────────── #

@patch("woocommerce_client.requests.put")
@patch("woocommerce_client.requests.get")
def test_cancel_api_error_propagated(mock_get_fn, mock_put_fn):
    paid_sub = make_wc_subscription()
    mock_get_fn.side_effect = mock_get({
        "customers": [make_wc_customer()],
        "subscriptions": [paid_sub],
    })
    mock_put_fn.return_value = mock_put(500)

    client = make_client()
    result = client.cancel_subscription("test@example.com")
    assert result["cancelled"] is False
    assert result["status"] == "error"


# ── 9–12. Trial detection unit tests ─────────────────────────────────────── #

class TestIsTrialActive:
    def test_future_date_returns_true(self):
        future = (datetime.now(timezone.utc) + timedelta(days=5)).isoformat()
        sub = {"trial_end_date_gmt": future}
        assert WooCommerceClient._is_trial_active(sub) is True

    def test_past_date_returns_false(self):
        past = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        sub = {"trial_end_date_gmt": past}
        assert WooCommerceClient._is_trial_active(sub) is False

    def test_zero_date_returns_false(self):
        sub = {"trial_end_date_gmt": "0000-00-00 00:00:00"}
        assert WooCommerceClient._is_trial_active(sub) is False

    def test_missing_field_returns_false(self):
        sub = {}
        assert WooCommerceClient._is_trial_active(sub) is False

    def test_z_suffix_parsed_correctly(self):
        future = (datetime.now(timezone.utc) + timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
        sub = {"trial_end_date_gmt": future}
        assert WooCommerceClient._is_trial_active(sub) is True
