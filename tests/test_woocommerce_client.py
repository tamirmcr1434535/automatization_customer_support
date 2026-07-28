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
  20. ?search= step disabled (2026-05-21) — when 1/1b/2a/2b/2c all miss,
      bot returns not_found so the higher-level Stripe fallback (last-4-card)
      can find a different email and retry the WC lookup.
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


def wc_http(responses: dict, put_status: int = 200):
    """Unified HTTP stand-in patched onto EVERY seam the WC client uses.

    The client mixes seams: customer + subscription lookups and the cancel PUT
    go through requests.request (via _request_with_retry); the health check,
    orders and order-count use requests.get directly. Patching only requests.get
    (as the old tests did) let the customer lookup leak to the real network. We
    route requests.request / requests.get / requests.put through one URL-based
    dispatcher so no step can ever hit the network.

    `responses` maps a URL substring → JSON payload:
      {"customers": [...], "subscriptions": [...], "orders": [...]}
    A PUT (the cancel write) returns `put_status`.
    """
    def _respond(method: str, url: str):
        r = MagicMock()
        r.ok = True
        r.status_code = 200
        r.reason = "OK"
        r.text = "OK"
        r.headers.get.return_value = None  # no X-WP-Total → len() fallback

        if method == "PUT":  # the cancel write
            r.status_code = put_status
            r.ok = put_status < 400
            r.reason = "OK" if put_status < 400 else "Error"
            r.text = "OK" if put_status < 400 else "Error"
            r.json.return_value = {"id": 101, "status": "cancelled"}
            return r

        # /orders must be matched BEFORE /subscriptions (substring collision:
        # ".../subscriptions/101/orders" contains "subscriptions").
        if "/orders" in url:
            r.json.return_value = responses.get("orders", [])
        elif "customers" in url:
            r.json.return_value = responses.get("customers", [])
        elif "subscriptions" in url:
            r.json.return_value = responses.get("subscriptions", [])
        else:
            r.json.return_value = []
        return r

    def fake_request(method, url, **kwargs):
        return _respond(str(method).upper(), url)

    def fake_get(url, **kwargs):
        return _respond("GET", url)

    def fake_put(url, **kwargs):
        return _respond("PUT", url)

    return patch.multiple(
        "woocommerce_client.requests",
        request=MagicMock(side_effect=fake_request),
        get=MagicMock(side_effect=fake_get),
        put=MagicMock(side_effect=fake_put),
    )


# ── 1. DRY_RUN mode ───────────────────────────────────────────────────────── #

def test_dry_run_returns_dry_run_status():
    """DRY_RUN still performs real reads; if sub found, returns dry_run."""
    active_sub = make_wc_subscription(days_since_start=3)
    client = make_client(dry_run=True)
    with wc_http({"customers": [make_wc_customer()], "subscriptions": [active_sub]}):
        result = client.cancel_subscription("test@example.com")
    assert result["status"] == "dry_run"
    assert result["cancelled"] is True
    assert result["source"] == "woocommerce"


# ── 2. Customer not found ─────────────────────────────────────────────────── #

def test_customer_not_found():
    client = make_client()
    with wc_http({"customers": [], "subscriptions": []}):
        result = client.cancel_subscription("nobody@example.com")
    assert result["status"] == "not_found"
    assert result["cancelled"] is False


# ── 3. Customer found but no subscriptions ────────────────────────────────── #

def test_no_subscriptions():
    client = make_client()
    with wc_http({"customers": [make_wc_customer()], "subscriptions": []}):
        result = client.cancel_subscription("test@example.com")
    assert result["status"] == "no_active_sub"
    assert result["cancelled"] is False


# ── 4. Active trial (days ≤ 8) → trial_cancelled ──────────────────────────── #

def test_active_trial_cancelled():
    # days_since_start=3 → primary signal: 3 ≤ 8 → trial
    trial_sub = make_wc_subscription(days_since_start=3)
    client = make_client()
    with wc_http({"customers": [make_wc_customer()], "subscriptions": [trial_sub]}):
        result = client.cancel_subscription("test@example.com")
    assert result["status"] == "trial_cancelled"
    assert result["cancelled"] is True
    assert result["subscription_type"] == "trial"
    assert result["subscription_id"] == trial_sub["id"]


# ── 5. Paid subscription (days > 8) → subscription_cancelled ─────────────── #

def test_paid_subscription_cancelled():
    # days_since_start=40 → 40 > 8 → subscription
    paid_sub = make_wc_subscription(days_since_start=40)
    client = make_client()
    with wc_http({"customers": [make_wc_customer()], "subscriptions": [paid_sub]}):
        result = client.cancel_subscription("test@example.com")
    assert result["status"] == "subscription_cancelled"
    assert result["cancelled"] is True
    assert result["subscription_type"] == "subscription"


# ── 6. Expired trial (no start_date, past trial_end) → subscription ────────── #

def test_expired_trial_treated_as_subscription():
    """trial_end in the past + no start_date → subscription (not a trial)."""
    past = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
    sub = make_wc_subscription()   # no days_since_start → no start_date_gmt
    sub["trial_end_date_gmt"] = past

    client = make_client()
    with wc_http({"customers": [make_wc_customer()], "subscriptions": [sub]}):
        result = client.cancel_subscription("test@example.com")
    assert result["subscription_type"] == "subscription"
    assert result["status"] == "subscription_cancelled"


# ── 7. pending-cancel subscription ───────────────────────────────────────── #

def test_pending_cancel_subscription():
    sub = make_wc_subscription(status="pending-cancel", days_since_start=40)
    client = make_client()
    with wc_http({"customers": [make_wc_customer()], "subscriptions": [sub]}):
        result = client.cancel_subscription("test@example.com")
    assert result["cancelled"] is True


# ── 8. Cancel API returns error ───────────────────────────────────────────── #

def test_cancel_api_error_propagated():
    paid_sub = make_wc_subscription(days_since_start=40)
    client = make_client()
    with wc_http({"customers": [make_wc_customer()], "subscriptions": [paid_sub]},
                 put_status=500):
        result = client.cancel_subscription("test@example.com")
    assert result["cancelled"] is False
    # A failed cancel PUT propagates a typed error status (a 500 → "api_error";
    # the older code collapsed all failures to a bare "error").
    assert result["status"] in {"api_error", "error"}


# ── 9. Already-cancelled subscription → already_cancelled ────────────────── #

def test_already_cancelled_subscription():
    """If WC returns a cancelled sub (no active), bot should confirm cancellation."""
    cancelled_sub = make_wc_subscription(status="cancelled", days_since_start=40)
    client = make_client()
    with wc_http({"customers": [make_wc_customer()], "subscriptions": [cancelled_sub]}):
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


# ── 20. get_subscriptions_by_billing_email detail-fetch fallback ─────────── #

@patch("woocommerce_client.requests.request")
def test_search_endpoint_disabled_returns_not_found(mock_request_fn):
    """
    The ?search= step (2d) was DISABLED on 2026-05-21 to reduce load on the
    iqbooster.org WC server. When 1 (/customers?email=), 1b (?search=),
    2a (meta_data direct), 2b (?customer=), and 2c (?billing_email=) all
    miss, the bot must now return "not_found" so the higher-level Stripe
    fallback (ask customer for last-4 card digits → Stripe finds email →
    retry WC) can take over.

    Previously, a subscription whose billing.email lived only in WP post
    meta could be found by the ?search= → detail-fetch path. That path is
    now gone — those tickets reach the Stripe fallback instead.
    """
    email = "satoru_fighting_forever@yahoo.co.jp"

    def _mock_request(method, url, **kwargs):
        resp = MagicMock()
        resp.ok = True
        resp.status_code = 200
        resp.reason = "OK"
        resp.headers.get.return_value = None
        # Every lookup returns empty — exercises the disabled-?search= path
        resp.json.return_value = []
        return resp

    mock_request_fn.side_effect = _mock_request

    client = make_client()
    result = client.cancel_subscription(email)

    # ?search= is disabled — no fallback inside WC client. Caller (bot) is
    # expected to handle this via Stripe last-4 lookup.
    assert result["status"] == "not_found", (
        f"Expected not_found after disabling ?search=, got: {result}"
    )

    # Verify the ?search= subscription endpoint was NOT called.
    def _is_subs_search(call):
        # _request_with_retry calls requests.request("GET", url, ...) so
        # the URL is the 2nd positional argument.
        url = call.args[1] if len(call.args) > 1 else call.kwargs.get("url", "")
        params = call.kwargs.get("params", {})
        return (
            "/subscriptions" in str(url)
            and "search" in params
            and "billing_email" not in params
            and "customer" not in params
        )

    assert not any(_is_subs_search(c) for c in mock_request_fn.call_args_list), (
        "?search= on /subscriptions should not be called — it was disabled"
    )
