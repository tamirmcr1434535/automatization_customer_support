"""
Unit tests for NexusClient + WC.cancel_subscription_via_nexus
==============================================================
Covers:
  A.  NexusClient.search_subscription — happy path, 404, 5xx, network
      error, malformed body, meta.success=false, missing sub_id.
  B.  WooCommerceClient.cancel_subscription_via_nexus — full flow with
      a fake NexusClient: trial cancel, sub cancel, already-cancelled
      short-circuit (no PUT), renewal gate, Nexus 404 → not_found,
      country pulled from customer.meta_data, plan from WC by-id.

External HTTP (requests.*) is mocked. No network calls.
"""
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

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

from nexus_client import NexusClient  # noqa: E402
from woocommerce_client import WooCommerceClient  # noqa: E402


# ────────────────────────────────────────────────────────────────────────
#  A. NexusClient.search_subscription
# ────────────────────────────────────────────────────────────────────────

def _nexus(x_host: str = ""):
    return NexusClient(
        "https://apinexus.cellon.ai",
        "test_token",
        x_host=x_host,
    )


def _resp(status_code, json_body=None, raise_on_read=False):
    r = MagicMock()
    r.status_code = status_code
    r.ok = 200 <= status_code < 400
    r.text = "" if json_body is None else "{}"
    if raise_on_read:
        r.json.side_effect = ValueError("non-json")
    else:
        r.json.return_value = json_body or {}
    return r


@patch("nexus_client.requests.post")
def test_search_happy_path_returns_data(mock_post):
    mock_post.return_value = _resp(200, {
        "meta": {"success": True, "status": 200},
        "data": {
            "subscription_id": "3750501",
            "source": "stripe",
            "order_count": "0",
            "subscription_start": False,
            "renewal_subscriptions": None,
            "was_already_cancelled": False,
            "status_before": "active",
        },
    })
    out = _nexus().search_subscription("foo@example.com")
    assert out is not None
    assert out["subscription_id"] == "3750501"


@patch("nexus_client.requests.post")
def test_search_404_returns_none(mock_post):
    mock_post.return_value = _resp(404, {
        "meta": {"success": False, "status": 404, "message": "Subscription not found"},
        "data": None,
    })
    assert _nexus().search_subscription("missing@example.com") is None


@patch("nexus_client.requests.post")
def test_search_5xx_returns_none(mock_post):
    mock_post.return_value = _resp(503, {})
    assert _nexus().search_subscription("foo@example.com") is None


@patch("nexus_client.requests.post")
def test_search_network_error_returns_none(mock_post):
    import requests as _r
    mock_post.side_effect = _r.exceptions.ConnectionError("dns failed")
    assert _nexus().search_subscription("foo@example.com") is None


@patch("nexus_client.requests.post")
def test_search_timeout_returns_none(mock_post):
    import requests as _r
    mock_post.side_effect = _r.exceptions.Timeout("timed out")
    assert _nexus().search_subscription("foo@example.com") is None


@patch("nexus_client.requests.post")
def test_search_meta_success_false_returns_none(mock_post):
    """Defensive: API was reported to return meta.success=true on bad
    input early in dev. We re-check it explicitly."""
    mock_post.return_value = _resp(200, {
        "meta": {"success": False, "message": "Please provide email"},
        "data": None,
    })
    assert _nexus().search_subscription("foo@example.com") is None


@patch("nexus_client.requests.post")
def test_search_missing_sub_id_returns_none(mock_post):
    mock_post.return_value = _resp(200, {
        "meta": {"success": True},
        "data": {"customer_email": "foo@example.com"},  # no subscription_id
    })
    assert _nexus().search_subscription("foo@example.com") is None


@patch("nexus_client.requests.post")
def test_search_malformed_json_returns_none(mock_post):
    mock_post.return_value = _resp(200, raise_on_read=True)
    assert _nexus().search_subscription("foo@example.com") is None


def test_search_empty_email_returns_none():
    """No outbound call when email is falsy."""
    with patch("nexus_client.requests.post") as mock_post:
        assert _nexus().search_subscription("") is None
        mock_post.assert_not_called()


# ── x-host header behaviour ──────────────────────────────────────────────

@patch("nexus_client.requests.post")
def test_x_host_omitted_by_default(mock_post):
    """Default constructor sends NO x-host header. Confirmed
    empirically on 2026-06-23 that the current API build ignores the
    header — we omit it for cleaner config."""
    mock_post.return_value = _resp(200, {
        "meta": {"success": True}, "data": {"subscription_id": "1"},
    })
    _nexus().search_subscription("foo@x.com")
    sent_headers = mock_post.call_args.kwargs["headers"]
    assert "x-host" not in sent_headers, sent_headers


@patch("nexus_client.requests.post")
def test_x_host_sent_when_explicitly_set(mock_post):
    """If brand scoping is ever enforced, callers can opt in via
    env var NEXUS_X_HOST. Header is forwarded as-is."""
    mock_post.return_value = _resp(200, {
        "meta": {"success": True}, "data": {"subscription_id": "1"},
    })
    _nexus(x_host="16_persons").search_subscription("foo@x.com")
    sent_headers = mock_post.call_args.kwargs["headers"]
    assert sent_headers.get("x-host") == "16_persons"


@patch("nexus_client.requests.post")
def test_x_host_empty_string_omitted(mock_post):
    """Empty string is treated the same as not-set — header omitted."""
    mock_post.return_value = _resp(200, {
        "meta": {"success": True}, "data": {"subscription_id": "1"},
    })
    _nexus(x_host="").search_subscription("foo@x.com")
    sent_headers = mock_post.call_args.kwargs["headers"]
    assert "x-host" not in sent_headers


# ────────────────────────────────────────────────────────────────────────
#  B. WooCommerceClient.cancel_subscription_via_nexus
# ────────────────────────────────────────────────────────────────────────

def _wc():
    return WooCommerceClient(
        site_url="https://test.example.com",
        consumer_key="ck_test",
        consumer_secret="cs_test",
        dry_run=False,
    )


class FakeNexus:
    """Hand-rolled Nexus stand-in. Returns canned data."""
    def __init__(self, data):
        self.data = data
        self.calls: list[str] = []

    def search_subscription(self, email):
        self.calls.append(email)
        return self.data


def _wc_dispatch(responses: dict):
    """Mock requests.get / requests.request so WC sees stub data.

    Distinguishes PUT to /subscriptions/{id} as a cancel-call: returns
    `{"status": "cancelled"}` instead of the stub sub object (which
    represents the BEFORE state and would fail _cancel_sub_by_id's
    "did WC actually cancel" sanity check).
    """
    def _h(*args, **kwargs):
        if len(args) >= 2 and isinstance(args[0], str) and args[0].isupper():
            method, url = args[0], args[1]
        elif args:
            method, url = "GET", args[0]
        else:
            method = kwargs.get("method", "GET")
            url = kwargs.get("url", "")
        resp = MagicMock()
        resp.ok = True
        resp.status_code = 200
        resp.reason = "OK"
        resp.text = ""
        resp.headers.get.return_value = None
        if method == "PUT" and "/subscriptions/" in url:
            resp.json.return_value = {"status": "cancelled"}
            resp.text = '{"status":"cancelled"}'
            return resp
        for key, value in responses.items():
            if key in url:
                resp.json.return_value = value
                return resp
        resp.json.return_value = []
        return resp
    return _h


def _mock_put_ok():
    r = MagicMock()
    r.ok = True
    r.status_code = 200
    r.text = '{"status":"cancelled"}'
    r.json.return_value = {"status": "cancelled"}
    return r


@patch("woocommerce_client.requests.put")
@patch("woocommerce_client.requests.request")
@patch("woocommerce_client.requests.get")
def test_via_nexus_trial_happy_path(mock_get, mock_request, mock_put):
    """Nexus says "trial-ish" → wc PUT fires → result has trial_cancelled."""
    customer = {
        "id": 42, "email": "test@x.com",
        "billing": {"email": "test@x.com"},
        "meta_data": [{"key": "country", "value": "Japan"}],
    }
    sub_object = {
        "id": 3750501, "status": "active",
        "line_items": [{"name": "IQ Test 1 Week Trial Then 28 days"}],
        "billing": {"country": ""},
    }
    dispatch = _wc_dispatch({
        "/customers": [customer],
        "/subscriptions/3750501": sub_object,
    })
    mock_get.side_effect = dispatch
    mock_request.side_effect = dispatch
    mock_put.return_value = _mock_put_ok()

    nexus = FakeNexus({
        "subscription_id": "3750501",
        "source": "stripe",
        "order_count": "0",
        "subscription_start": False,
        "renewal_subscriptions": None,
        "was_already_cancelled": False,
        "status_before": "active",
    })

    out = _wc().cancel_subscription_via_nexus("test@x.com", nexus)
    assert out["status"] == "trial_cancelled", out
    assert out["cancelled"] is True
    assert out["subscription_type"] == "trial"
    assert out["subscription_id"] == 3750501
    assert out["plan"].startswith("IQ Test 1 Week Trial")
    assert out["country"] == "Japan"
    # order_count parity with legacy WC client: signup is +1 even on
    # trial (Nexus's order_count omits it).
    assert out["order_count"] == 1, out
    assert nexus.calls == ["test@x.com"]


@patch("woocommerce_client.requests.put")
@patch("woocommerce_client.requests.request")
@patch("woocommerce_client.requests.get")
def test_via_nexus_subscription_happy_path(mock_get, mock_request, mock_put):
    """Nexus says renewal happened → sub_type=subscription, PUT fires."""
    customer = {
        "id": 42, "email": "test@x.com",
        "billing": {"email": "test@x.com", "country": "JP"},
        "meta_data": [],
    }
    dispatch = _wc_dispatch({
        "/customers": [customer],
        "/subscriptions/3524487": {
            "id": 3524487, "status": "active",
            "line_items": [{"name": "IQ Test Monthly"}],
        },
    })
    mock_get.side_effect = dispatch
    mock_request.side_effect = dispatch
    mock_put.return_value = _mock_put_ok()

    nexus = FakeNexus({
        "subscription_id": "3524487",
        "source": "stripe",
        "order_count": "2",
        "subscription_start": True,
        "renewal_subscriptions": "1",
        "was_already_cancelled": False,
    })

    out = _wc().cancel_subscription_via_nexus("test@x.com", nexus)
    assert out["status"] == "subscription_cancelled", out
    assert out["subscription_type"] == "subscription"
    assert out["order_count"] == 2
    assert out["country"] == "JP"


@patch("woocommerce_client.requests.put")
@patch("woocommerce_client.requests.request")
@patch("woocommerce_client.requests.get")
def test_via_nexus_already_cancelled_skips_put(mock_get, mock_request, mock_put):
    """was_already_cancelled=true → no PUT, status=already_cancelled."""
    dispatch = _wc_dispatch({
        "/customers": [{"id": 1, "email": "x@y.com", "billing": {}, "meta_data": []}],
        "/subscriptions/9999": {"id": 9999, "status": "cancelled", "line_items": [{"name": "Foo"}]},
    })
    mock_get.side_effect = dispatch
    mock_request.side_effect = dispatch

    nexus = FakeNexus({
        "subscription_id": "9999",
        "source": "woocommerce",
        "order_count": "0",
        "subscription_start": False,
        "renewal_subscriptions": None,
        "was_already_cancelled": True,
        "status_before": "cancelled",
    })

    out = _wc().cancel_subscription_via_nexus("x@y.com", nexus)
    assert out["status"] == "already_cancelled", out
    assert out["cancelled"] is True
    assert out["subscription_id"] == 9999
    mock_put.assert_not_called(), "PUT must not fire when already cancelled"


@patch("woocommerce_client.requests.put")
@patch("woocommerce_client.requests.request")
@patch("woocommerce_client.requests.get")
def test_via_nexus_renewal_gate_blocks_put(mock_get, mock_request, mock_put):
    """3+ renewals → renewal_too_many_orders, no PUT."""
    dispatch = _wc_dispatch({
        "/customers": [{"id": 1, "email": "x@y.com", "billing": {}, "meta_data": []}],
        "/subscriptions/5555": {"id": 5555, "status": "active", "line_items": [{"name": "Foo"}]},
    })
    mock_get.side_effect = dispatch
    mock_request.side_effect = dispatch

    nexus = FakeNexus({
        "subscription_id": "5555",
        "source": "stripe",
        "order_count": "5",
        "subscription_start": True,
        "renewal_subscriptions": "4",
        "was_already_cancelled": False,
    })

    out = _wc().cancel_subscription_via_nexus(
        "x@y.com", nexus, max_auto_cancel_orders=3,
    )
    assert out["status"] == "renewal_too_many_orders", out
    assert out["cancelled"] is False
    assert out["order_count"] >= 3
    mock_put.assert_not_called()


@patch("woocommerce_client.requests.request")
@patch("woocommerce_client.requests.get")
def test_via_nexus_not_found_returns_not_found(mock_get, mock_request):
    """Nexus 404 (returns None) + no WC customer → status=not_found."""
    # No customer in WC, no subscription anywhere
    dispatch = _wc_dispatch({"/customers": []})
    mock_get.side_effect = dispatch
    mock_request.side_effect = dispatch

    nexus = FakeNexus(None)  # Nexus also says "not found"
    out = _wc().cancel_subscription_via_nexus("ghost@nowhere.com", nexus)
    assert out["status"] == "not_found", out
    assert out["country"] == ""


@patch("woocommerce_client.requests.request")
@patch("woocommerce_client.requests.get")
def test_via_nexus_customer_found_but_nexus_404_returns_no_active_sub(mock_get, mock_request):
    """WC customer exists but Nexus has no sub → status=no_active_sub."""
    dispatch = _wc_dispatch({
        "/customers": [{
            "id": 99, "email": "x@y.com", "billing": {"country": "DE"},
            "meta_data": [],
        }],
    })
    mock_get.side_effect = dispatch
    mock_request.side_effect = dispatch

    out = _wc().cancel_subscription_via_nexus("x@y.com", FakeNexus(None))
    assert out["status"] == "no_active_sub", out
    assert out["country"] == "DE"


@patch("woocommerce_client.requests.put")
@patch("woocommerce_client.requests.request")
@patch("woocommerce_client.requests.get")
def test_via_nexus_non_numeric_sub_id_treated_as_not_found(mock_get, mock_request, mock_put):
    dispatch = _wc_dispatch({"/customers": [{"id": 1, "email": "x@y.com", "billing": {}, "meta_data": []}]})
    mock_get.side_effect = dispatch
    mock_request.side_effect = dispatch

    nexus = FakeNexus({
        "subscription_id": "not-a-number",
        "source": "stripe",
        "order_count": "0",
        "subscription_start": False,
        "renewal_subscriptions": None,
    })
    out = _wc().cancel_subscription_via_nexus("x@y.com", nexus)
    assert out["status"] == "not_found", out
    mock_put.assert_not_called()
