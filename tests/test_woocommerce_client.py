"""
Unit tests for WooCommerceClient
==================================
All HTTP calls are mocked — no real network requests.

Test scenarios:
  1.  DRY_RUN mode — reads are mocked, returns dry_run status
  2.  Customer not found → not_found
  3.  Customer found, no subscriptions → no_active_sub
  4.  Customer found, no completed renewals → trial_cancelled
  5.  Customer found, 1 completed renewal → subscription_cancelled
  7.  Customer found, pending-cancel subscription → subscription_cancelled
  8.  Cancel API returns error → propagates error status
  9.  Subscription already cancelled → already_cancelled
  10. _get_sub_type: renewal_count=0 → trial (Parent only)
  11. _get_sub_type: renewal_count=1 → subscription (Parent + 1 Renewal)
  12. _get_sub_type: renewal_count=2 → renewal_subscription
  13. _get_sub_type: renewal_count=10 → renewal_subscription (any 2+)
  14. _get_sub_type: renewal_count=None → unknown (escalate, do not guess)
  16. _get_completed_orders_breakdown: detects renewal via meta_data
  17. _get_completed_orders_breakdown: excludes processing/failed
  20. get_subscriptions_by_billing_email: ?search= returns empty billing.email →
      individual detail fetch resolves meta_data._billing_email match
"""

import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone, timedelta

from woocommerce_client import WooCommerceClient
from tests.conftest import make_wc_customer, make_wc_order, make_wc_subscription


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

    The "orders" key in responses controls what the orders endpoint
    returns; defaults to [] (no related orders → 0 renewals → trial).
    Pass make_wc_order(is_renewal=True) entries to simulate renewals.
    """
    def _get(url, **kwargs):
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.status_code = 200
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


# ── 5. Paid subscription (Parent + 1 Renewal) → subscription_cancelled ─── #

@patch("woocommerce_client.requests.put")
@patch("woocommerce_client.requests.get")
def test_paid_subscription_cancelled(mock_get_fn, mock_put_fn):
    # 1 Parent + 1 Renewal (both completed) → "subscription"
    paid_sub = make_wc_subscription(days_since_start=40)
    mock_get_fn.side_effect = mock_get({
        "customers": [make_wc_customer()],
        "subscriptions": [paid_sub],
        "orders": [
            make_wc_order(1, is_renewal=False),
            make_wc_order(2, is_renewal=True),
        ],
    })
    mock_put_fn.return_value = mock_put(200)

    client = make_client()
    result = client.cancel_subscription("test@example.com")
    assert result["status"] == "subscription_cancelled"
    assert result["cancelled"] is True
    assert result["subscription_type"] == "subscription"


# ── 6. Renewal subscription (Parent + 2+ Renewals) → renewal_subscription ── #

@patch("woocommerce_client.requests.put")
@patch("woocommerce_client.requests.get")
def test_renewal_subscription_cancelled(mock_get_fn, mock_put_fn):
    """Parent + 3 Renewals → renewal_subscription; reuses subscription_cancelled status."""
    sub = make_wc_subscription(days_since_start=120)
    mock_get_fn.side_effect = mock_get({
        "customers": [make_wc_customer()],
        "subscriptions": [sub],
        "orders": [
            make_wc_order(1, is_renewal=False),
            make_wc_order(2, is_renewal=True),
            make_wc_order(3, is_renewal=True),
            make_wc_order(4, is_renewal=True),
        ],
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
        "orders": [
            make_wc_order(1, is_renewal=False),
            make_wc_order(2, is_renewal=True),
        ],
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
        "orders": [
            make_wc_order(1, is_renewal=False),
            make_wc_order(2, is_renewal=True),
        ],
    })

    client = make_client()
    result = client.cancel_subscription("test@example.com")
    assert result["status"] == "already_cancelled"
    assert result["cancelled"] is True
    assert result["subscription_type"] == "subscription"


# ── 10–15. _get_sub_type unit tests (renewal-count rule) ────────────────── #

class TestGetSubType:
    """Unit tests for WooCommerceClient._get_sub_type (static method).

    Classification rule mirrors the Relationship column in the WC admin's
    Related Orders panel (Parent Order vs Renewal Order):
      • 0 renewals (Parent only)         → "trial"
      • 1 renewal  (Parent + 1 Renewal)  → "subscription"
      • 2+ renewals                      → "renewal_subscription"
      • None (orders lookup failure)     → "unknown" (caller escalates)
    """

    def test_zero_renewals_returns_trial(self):
        """No completed renewals (Parent only) → still in trial."""
        assert WooCommerceClient._get_sub_type({}, renewal_count=0) == "trial"

    def test_one_renewal_returns_subscription(self):
        """Parent + 1 Renewal → first-period paid subscription."""
        assert WooCommerceClient._get_sub_type({}, renewal_count=1) == "subscription"

    def test_two_renewals_returns_renewal_subscription(self):
        """Parent + 2 Renewals → renewal_subscription."""
        assert WooCommerceClient._get_sub_type({}, renewal_count=2) == "renewal_subscription"

    def test_many_renewals_returns_renewal_subscription(self):
        """Any 2+ renewals → renewal_subscription regardless of count."""
        assert WooCommerceClient._get_sub_type({}, renewal_count=10) == "renewal_subscription"

    def test_renewal_count_none_returns_unknown(self):
        """Orders lookup failed (None) → 'unknown' so caller escalates.

        Guessing the type when we cannot read the related orders risks
        sending the wrong reply (e.g. trial copy 'nothing was charged'
        to a paying customer). cancel_subscription() detects 'unknown'
        and returns a transient_error, which existing escalation
        translates to a Slack alert.
        """
        assert WooCommerceClient._get_sub_type({}, renewal_count=None) == "unknown"

    def test_subscription_dict_is_unused(self):
        """Subscription payload contents must not influence classification."""
        sub = {
            "start_date_gmt": "2020-01-01T00:00:00",
            "trial_end_date_gmt": "2099-12-31T00:00:00",
            "status": "active",
        }
        assert WooCommerceClient._get_sub_type(sub, renewal_count=0) == "trial"
        assert WooCommerceClient._get_sub_type(sub, renewal_count=1) == "subscription"
        assert WooCommerceClient._get_sub_type(sub, renewal_count=4) == "renewal_subscription"


# ── 16–17. _get_completed_orders_breakdown unit tests ────────────────────── #

class TestCompletedOrdersBreakdown:
    """Unit tests for WooCommerceClient._get_completed_orders_breakdown.

    Mirrors the WC admin's Related Orders panel: a Parent Order has no
    `_subscription_renewal` meta, every Renewal Order does. Only orders
    with status="completed" are counted — failed retries, cancelled,
    refunded, processing, pending, on-hold are excluded so a half-paid
    renewal doesn't bump the sub into "renewal_subscription".
    """

    @patch("woocommerce_client.requests.get")
    def test_renewal_meta_distinguishes_parent_from_renewal(self, mock_get_fn):
        """Parent + 2 Renewals → renewals=2, parents=1, total=3."""
        mock_get_fn.side_effect = mock_get({
            "orders": [
                make_wc_order(1, is_renewal=False),
                make_wc_order(2, is_renewal=True),
                make_wc_order(3, is_renewal=True),
            ],
        })
        client = make_client()
        breakdown = client._get_completed_orders_breakdown(101)
        assert breakdown == {"renewals": 2, "parents": 1, "total": 3}

    @patch("woocommerce_client.requests.get")
    def test_non_completed_statuses_excluded(self, mock_get_fn):
        """Failed/processing/refunded/etc. don't count.

        The "I only paid 2 times while bot said orders=4" reports came
        from counting failed retry attempts as paid renewals — guard
        against a regression here.
        """
        mock_get_fn.side_effect = mock_get({
            "orders": [
                make_wc_order(1, is_renewal=False, status="completed"),
                make_wc_order(2, is_renewal=True, status="completed"),
                make_wc_order(3, is_renewal=True, status="failed"),
                make_wc_order(4, is_renewal=True, status="processing"),
                make_wc_order(5, is_renewal=True, status="refunded"),
            ],
        })
        client = make_client()
        breakdown = client._get_completed_orders_breakdown(101)
        assert breakdown == {"renewals": 1, "parents": 1, "total": 2}

    @patch("woocommerce_client.requests.get")
    def test_request_failure_returns_none(self, mock_get_fn):
        """Network error → None so caller escalates instead of guessing."""
        import requests as _requests
        mock_get_fn.side_effect = _requests.exceptions.Timeout("boom")
        client = make_client()
        assert client._get_completed_orders_breakdown(101) is None


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
