"""
Unit tests for WooCommerceClient
==================================
All HTTP calls are mocked — no real network requests.

Test scenarios:
  1.  DRY_RUN mode — reads are mocked, returns dry_run status
  2.  Customer not found → not_found
  3.  Customer found, no subscriptions → no_active_sub
  4.  Customer found, active TRIAL (days_since_start ≤ 8) → trial_cancelled
  5.  Customer found, paid subscription (days_since_start > 8) → subscription_cancelled
  6.  Customer found, expired trial (no start_date, past trial_end) → subscription_cancelled
  7.  Customer found, pending-cancel subscription → subscription_cancelled
  8.  Cancel API returns error → propagates error status
  9.  Subscription already cancelled → already_cancelled
  10. _get_sub_type: order_count > 1 → subscription
  11. _get_sub_type: order_count=1 + days ≤ 8 → trial
  12. _get_sub_type: order_count=1 + days > 8 → subscription
  13. _get_sub_type: order_count=None + days ≤ 8 → trial
  14. _get_sub_type: order_count=None + days > 8 → subscription
  15. _get_sub_type: no start_date + future trial_end → trial
  16. _get_sub_type: no start_date + expired trial_end → subscription
  17. _get_sub_type: no start_date + zero trial_end → subscription
  18. _get_sub_type: no start_date, no trial_end → subscription
  19. _get_sub_type: start_date with Z suffix parsed correctly
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

    Handles the /orders endpoint BEFORE /subscriptions to avoid the
    substring collision (subscriptions/101/orders contains "subscriptions").

    The "orders" key in responses controls what the orders endpoint returns;
    defaults to [] (no orders → order_count = 0 via len fallback).

    Sets headers.get("X-WP-Total") to return None so get_order_count()
    falls back to len(data) rather than int(MagicMock).
    """
    def _get(url, **kwargs):
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.status_code = 200
        # Explicitly no X-WP-Total header → get_order_count() uses len(data)
        mock_resp.headers.get.return_value = None

        # Orders endpoint must be checked BEFORE "subscriptions" (substring match)
        if "/orders" in url:
            data = responses.get("orders", [])
            mock_resp.json.return_value = data
            return mock_resp

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

@patch("woocommerce_client.requests.get")
def test_dry_run_returns_dry_run_status(mock_get_fn):
    """DRY_RUN still performs real reads; if sub found, returns dry_run."""
    active_sub = make_wc_subscription(days_since_start=3)
    mock_get_fn.side_effect = mock_get({
        "customers": [make_wc_customer()],
        "subscriptions": [active_sub],
    })
    client = make_client(dry_run=True)
    result = client.cancel_subscription("test@example.com")
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


# ── 4. Active trial (days ≤ 8) → trial_cancelled ──────────────────────────── #

@patch("woocommerce_client.requests.put")
@patch("woocommerce_client.requests.get")
def test_active_trial_cancelled(mock_get_fn, mock_put_fn):
    # days_since_start=3 → primary signal: 3 ≤ 8 → trial
    trial_sub = make_wc_subscription(days_since_start=3)
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


# ── 5. Paid subscription (days > 8) → subscription_cancelled ─────────────── #

@patch("woocommerce_client.requests.put")
@patch("woocommerce_client.requests.get")
def test_paid_subscription_cancelled(mock_get_fn, mock_put_fn):
    # days_since_start=40 → 40 > 8 → subscription
    paid_sub = make_wc_subscription(days_since_start=40)
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


# ── 6. Expired trial (no start_date, past trial_end) → subscription ────────── #

@patch("woocommerce_client.requests.put")
@patch("woocommerce_client.requests.get")
def test_expired_trial_treated_as_subscription(mock_get_fn, mock_put_fn):
    """trial_end in the past + no start_date → subscription (not a trial)."""
    past = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
    sub = make_wc_subscription()   # no days_since_start → no start_date_gmt
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
    sub = make_wc_subscription(status="pending-cancel", days_since_start=40)
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
    paid_sub = make_wc_subscription(days_since_start=40)
    mock_get_fn.side_effect = mock_get({
        "customers": [make_wc_customer()],
        "subscriptions": [paid_sub],
    })
    mock_put_fn.return_value = mock_put(500)

    client = make_client()
    result = client.cancel_subscription("test@example.com")
    assert result["cancelled"] is False
    assert result["status"] == "error"


# ── 9. Already-cancelled subscription → already_cancelled ────────────────── #

@patch("woocommerce_client.requests.get")
def test_already_cancelled_subscription(mock_get_fn):
    """If WC returns a cancelled sub (no active), bot should confirm cancellation."""
    cancelled_sub = make_wc_subscription(status="cancelled", days_since_start=40)
    mock_get_fn.side_effect = mock_get({
        "customers": [make_wc_customer()],
        "subscriptions": [cancelled_sub],
    })

    client = make_client()
    result = client.cancel_subscription("test@example.com")
    assert result["status"] == "already_cancelled"
    assert result["cancelled"] is True
    assert result["subscription_type"] == "subscription"


# ── 10–19. _get_sub_type unit tests ──────────────────────────────────────── #

class TestGetSubType:
    """Unit tests for WooCommerceClient._get_sub_type (static method)."""

    # ── order_count is the primary signal ────────────────────────────────── #

    def test_order_count_gt_1_returns_subscription(self):
        start = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
        sub = {"start_date_gmt": start}
        # Even if days ≤ 8, more than one order → definitely a subscription
        assert WooCommerceClient._get_sub_type(sub, order_count=2) == "subscription"

    def test_order_count_1_days_le_8_returns_trial(self):
        start = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
        sub = {"start_date_gmt": start}
        assert WooCommerceClient._get_sub_type(sub, order_count=1) == "trial"

    def test_order_count_1_days_gt_8_returns_subscription(self):
        start = (datetime.now(timezone.utc) - timedelta(days=15)).isoformat()
        sub = {"start_date_gmt": start}
        assert WooCommerceClient._get_sub_type(sub, order_count=1) == "subscription"

    def test_order_count_none_days_le_8_returns_trial(self):
        """order_count=None (API timeout) + days ≤ 8 → still classified as trial."""
        start = (datetime.now(timezone.utc) - timedelta(days=5)).isoformat()
        sub = {"start_date_gmt": start}
        assert WooCommerceClient._get_sub_type(sub, order_count=None) == "trial"

    def test_order_count_none_days_gt_8_returns_subscription(self):
        start = (datetime.now(timezone.utc) - timedelta(days=20)).isoformat()
        sub = {"start_date_gmt": start}
        assert WooCommerceClient._get_sub_type(sub, order_count=None) == "subscription"

    # ── fallback path: no start_date, use trial_end_date ─────────────────── #

    def test_no_start_date_future_trial_end_returns_trial(self):
        """No start_date + trial_end still in future + order_count=1 → trial."""
        future = (datetime.now(timezone.utc) + timedelta(days=5)).isoformat()
        sub = {"trial_end_date_gmt": future}
        assert WooCommerceClient._get_sub_type(sub, order_count=1) == "trial"

    def test_no_start_date_expired_trial_end_returns_subscription(self):
        """No start_date + trial_end already past → subscription (expired trial)."""
        past = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        sub = {"trial_end_date_gmt": past}
        assert WooCommerceClient._get_sub_type(sub, order_count=1) == "subscription"

    def test_no_start_date_zero_trial_end_returns_subscription(self):
        """'0000-00-00 00:00:00' sentinel → no trial → subscription."""
        sub = {"trial_end_date_gmt": "0000-00-00 00:00:00"}
        assert WooCommerceClient._get_sub_type(sub, order_count=1) == "subscription"

    def test_no_start_date_no_trial_end_returns_subscription(self):
        """No date fields at all → safe default is subscription."""
        sub = {}
        assert WooCommerceClient._get_sub_type(sub, order_count=None) == "subscription"

    def test_z_suffix_start_date_parsed_correctly(self):
        """start_date_gmt with Z suffix (UTC) should parse without error."""
        start = (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
        sub = {"start_date_gmt": start}
        assert WooCommerceClient._get_sub_type(sub, order_count=1) == "trial"
