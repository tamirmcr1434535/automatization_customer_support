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
  10. _get_sub_type: order_count=1 → trial (Parent only)
  11. _get_sub_type: order_count=2 → subscription (Parent + 1 Renewal)
  12. _get_sub_type: order_count=3 → renewal_subscription
  13. _get_sub_type: order_count=10 → renewal_subscription (any 3+)
  14. _get_sub_type: order_count=0 → trial (no orders → safe trial label)
  15. _get_sub_type: order_count=None → unknown (escalate, do not guess type)
  20. get_subscriptions_by_billing_email: ?search= returns empty billing.email →
      individual detail fetch resolves meta_data._billing_email match
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


# ── 5. Paid subscription (2 related orders) → subscription_cancelled ─────── #

@patch("woocommerce_client.requests.put")
@patch("woocommerce_client.requests.get")
def test_paid_subscription_cancelled(mock_get_fn, mock_put_fn):
    # 2 related orders (Parent + 1 Renewal) → "subscription"
    paid_sub = make_wc_subscription(days_since_start=40)
    mock_get_fn.side_effect = mock_get({
        "customers": [make_wc_customer()],
        "subscriptions": [paid_sub],
        "orders": [{"id": 1}, {"id": 2}],
    })
    mock_put_fn.return_value = mock_put(200)

    client = make_client()
    result = client.cancel_subscription("test@example.com")
    assert result["status"] == "subscription_cancelled"
    assert result["cancelled"] is True
    assert result["subscription_type"] == "subscription"


# ── 6. Renewal subscription (3+ related orders) → subscription_cancelled ─── #

@patch("woocommerce_client.requests.put")
@patch("woocommerce_client.requests.get")
def test_renewal_subscription_cancelled(mock_get_fn, mock_put_fn):
    """3+ related orders → renewal_subscription, but reuses subscription_cancelled status."""
    sub = make_wc_subscription(days_since_start=120)
    mock_get_fn.side_effect = mock_get({
        "customers": [make_wc_customer()],
        "subscriptions": [sub],
        "orders": [{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}],
    })
    mock_put_fn.return_value = mock_put(200)

    client = make_client()
    # No max_auto_cancel_orders passed → renewal-gate inactive → PUT happens.
    result = client.cancel_subscription("test@example.com")
    assert result["subscription_type"] == "renewal_subscription"
    assert result["status"] == "subscription_cancelled"
    assert result["cancelled"] is True


# ── 7. pending-cancel subscription ───────────────────────────────────────── #

@patch("woocommerce_client.requests.put")
@patch("woocommerce_client.requests.get")
def test_pending_cancel_subscription(mock_get_fn, mock_put_fn):
    sub = make_wc_subscription(status="pending-cancel", days_since_start=40)
    mock_get_fn.side_effect = mock_get({
        "customers": [make_wc_customer()],
        "subscriptions": [sub],
        "orders": [{"id": 1}, {"id": 2}],
    })
    mock_put_fn.return_value = mock_put(200)

    client = make_client()
    result = client.cancel_subscription("test@example.com")
    assert result["cancelled"] is True
    assert result["subscription_type"] == "subscription"


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
        "orders": [{"id": 1}, {"id": 2}],
    })

    client = make_client()
    result = client.cancel_subscription("test@example.com")
    assert result["status"] == "already_cancelled"
    assert result["cancelled"] is True
    assert result["subscription_type"] == "subscription"


# ── 10–15. _get_sub_type unit tests (related-order count rule) ──────────── #

class TestGetSubType:
    """Unit tests for WooCommerceClient._get_sub_type (static method).

    The classification rule is:
      • 1 order  (Parent only)         → "trial"
      • 2 orders (Parent + 1 Renewal)  → "subscription"
      • 3+ orders                      → "renewal_subscription"
      • None (lookup failure)          → "unknown" (caller must escalate)
    """

    def test_order_count_1_returns_trial(self):
        """Parent order only → still in trial."""
        assert WooCommerceClient._get_sub_type({}, order_count=1) == "trial"

    def test_order_count_2_returns_subscription(self):
        """Parent + 1 Renewal → first-period paid subscription."""
        assert WooCommerceClient._get_sub_type({}, order_count=2) == "subscription"

    def test_order_count_3_returns_renewal_subscription(self):
        """Parent + 2 Renewals → renewal_subscription."""
        assert WooCommerceClient._get_sub_type({}, order_count=3) == "renewal_subscription"

    def test_order_count_high_returns_renewal_subscription(self):
        """Any 3+ count → renewal_subscription regardless of how many."""
        assert WooCommerceClient._get_sub_type({}, order_count=10) == "renewal_subscription"

    def test_order_count_zero_returns_trial(self):
        """0 successful orders → safe trial label (sub created but nothing charged yet)."""
        assert WooCommerceClient._get_sub_type({}, order_count=0) == "trial"

    def test_order_count_none_returns_unknown(self):
        """API lookup failed (None) → 'unknown' so caller escalates.

        Guessing the type when we cannot count orders risks sending the
        wrong reply (e.g. trial copy 'nothing was charged' to a paying
        customer). cancel_subscription() detects 'unknown' and returns
        a transient_error, which existing escalation translates to a
        Slack alert.
        """
        assert WooCommerceClient._get_sub_type({}, order_count=None) == "unknown"

    def test_subscription_dict_is_unused(self):
        """Subscription payload contents must not influence classification."""
        sub = {
            "start_date_gmt": "2020-01-01T00:00:00",
            "trial_end_date_gmt": "2099-12-31T00:00:00",
            "status": "active",
        }
        assert WooCommerceClient._get_sub_type(sub, order_count=1) == "trial"
        assert WooCommerceClient._get_sub_type(sub, order_count=2) == "subscription"
        assert WooCommerceClient._get_sub_type(sub, order_count=5) == "renewal_subscription"


# ── 20. get_subscriptions_by_billing_email detail-fetch fallback ─────────── #

@patch("woocommerce_client.requests.get")
def test_billing_email_search_detail_fetch_fallback(mock_get_fn):
    """
    When ?search= returns a subscription with empty billing.email,
    the client should fetch the individual subscription detail and
    match via meta_data._billing_email.

    Simulates: WC REST list responses omit billing.email (stored only in
    WP post meta _billing_email), but the detail endpoint returns it.
    This was the root cause for satoru_fighting_forever@yahoo.co.jp not
    being found even though the subscription existed in WC admin.
    """
    email = "satoru_fighting_forever@yahoo.co.jp"
    sub_id = 3305071
    start = (datetime.now(timezone.utc) - timedelta(days=11)).isoformat()

    # List response has empty billing.email (post-meta-only billing)
    list_sub = {
        "id": sub_id,
        "status": "active",
        "billing": {"email": ""},   # empty — stored in post meta only
        "meta_data": [],             # not populated in list responses
        "start_date_gmt": start,
        "line_items": [{"name": "WW Personality Test 1 Week Trial Then 28 days"}],
    }

    # Detail response includes meta_data with _billing_email
    detail_sub = {
        **list_sub,
        "billing": {"email": ""},    # still empty in billing obj
        "meta_data": [
            {"key": "_billing_email", "value": email},
        ],
    }

    call_count = [0]

    def _mock_get(url, **kwargs):
        resp = MagicMock()
        resp.ok = True
        resp.status_code = 200
        resp.headers.get.return_value = None

        # 1st call: customers endpoint → no customer found
        if "/customers" in url:
            resp.json.return_value = []
            return resp

        # 2nd call: ?billing_email= on subscriptions → empty (filter not supported)
        if "/subscriptions" in url and "billing_email" in str(kwargs.get("params", {})):
            resp.json.return_value = []
            return resp

        # 3rd call: ?search= on subscriptions → returns sub with empty billing.email
        if "/subscriptions" in url and "search" in str(kwargs.get("params", {})):
            resp.json.return_value = [list_sub]
            return resp

        # 4th call: individual detail GET /subscriptions/{id}
        if f"/subscriptions/{sub_id}" in url:
            resp.json.return_value = detail_sub
            return resp

        resp.json.return_value = []
        return resp

    mock_get_fn.side_effect = _mock_get

    client = make_client()
    result = client.cancel_subscription(email)

    # Should have found the subscription via detail-fetch fallback
    assert result["status"] != "not_found", (
        f"Expected subscription to be found via detail-fetch, got: {result}"
    )
    assert result["subscription_id"] == sub_id
