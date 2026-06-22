"""
Unit tests for Country custom field (BUG-10 + meta_data follow-up)
====================================================================
Covers:
  A.  `_set_country_for_ticket` — accepts both ISO-2 codes ("JP") and
      full English names ("Japan"), looks up names via the lazy-loaded
      Zendesk Country field options. Silently skips on unrecognised
      input. Never raises.
  B.  `_load_country_name_to_tag` — lazy loader hits the Zendesk
      ticket_field endpoint once, caches the {name: tag} map.
  C.  WooCommerceClient.cancel_subscription — pulls `country` from
      customer.meta_data['country'] first (full name), falls back to
      billing.country, then subscription.billing.country.

External clients (Zendesk, Anthropic, BigQuery) are mocked so no
network calls happen during the test run.
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
    """Minimal active trial sub dict.

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


# Pretend Zendesk field options (subset of real 241; enough for tests).
_FAKE_COUNTRY_FIELD = {
    "custom_field_options": [
        {"name": "Japan", "value": "jp"},
        {"name": "Korea, Republic of", "value": "kr"},
        {"name": "United States", "value": "us"},
        {"name": "Germany", "value": "de"},
    ]
}


@pytest.fixture(autouse=True)
def _reset_country_cache():
    """Clear the lazy name→tag cache between tests."""
    main._COUNTRY_NAME_TO_TAG = None
    yield
    main._COUNTRY_NAME_TO_TAG = None


# ────────────────────────────────────────────────────────────────────────
#  A. _set_country_for_ticket — ISO-2 path
# ────────────────────────────────────────────────────────────────────────

def test_iso2_uppercase_lowercased_and_sent():
    with patch.object(main, "zendesk") as zd:
        main._set_country_for_ticket("1", "JP")
        zd.set_custom_field.assert_called_once_with(
            "1", int(main._ZENDESK_COUNTRY_FIELD_ID), "jp",
        )
        zd.get_ticket_field.assert_not_called()  # no map lookup needed


def test_iso2_lowercase_passed_through():
    with patch.object(main, "zendesk") as zd:
        main._set_country_for_ticket("1", "ua")
        zd.set_custom_field.assert_called_once_with(
            "1", int(main._ZENDESK_COUNTRY_FIELD_ID), "ua",
        )


def test_iso2_with_whitespace_stripped():
    with patch.object(main, "zendesk") as zd:
        main._set_country_for_ticket("1", "  jp  ")
        zd.set_custom_field.assert_called_once_with(
            "1", int(main._ZENDESK_COUNTRY_FIELD_ID), "jp",
        )


# ────────────────────────────────────────────────────────────────────────
#  A. _set_country_for_ticket — full-name path
# ────────────────────────────────────────────────────────────────────────

def test_full_name_japan_resolved_to_jp():
    with patch.object(main, "zendesk") as zd:
        zd.get_ticket_field.return_value = _FAKE_COUNTRY_FIELD
        main._set_country_for_ticket("1", "Japan")
        zd.set_custom_field.assert_called_once_with(
            "1", int(main._ZENDESK_COUNTRY_FIELD_ID), "jp",
        )
        zd.get_ticket_field.assert_called_once()  # lazy load fired


def test_full_name_case_insensitive():
    with patch.object(main, "zendesk") as zd:
        zd.get_ticket_field.return_value = _FAKE_COUNTRY_FIELD
        main._set_country_for_ticket("1", "JAPAN")
        zd.set_custom_field.assert_called_once_with(
            "1", int(main._ZENDESK_COUNTRY_FIELD_ID), "jp",
        )


def test_full_name_multiword_resolved():
    with patch.object(main, "zendesk") as zd:
        zd.get_ticket_field.return_value = _FAKE_COUNTRY_FIELD
        main._set_country_for_ticket("1", "Korea, Republic of")
        zd.set_custom_field.assert_called_once_with(
            "1", int(main._ZENDESK_COUNTRY_FIELD_ID), "kr",
        )


def test_unknown_full_name_silent_skip():
    with patch.object(main, "zendesk") as zd:
        zd.get_ticket_field.return_value = _FAKE_COUNTRY_FIELD
        main._set_country_for_ticket("1", "Atlantis")
        zd.set_custom_field.assert_not_called()


def test_name_to_tag_cached_after_first_lookup():
    """get_ticket_field is called only once even with multiple set calls."""
    with patch.object(main, "zendesk") as zd:
        zd.get_ticket_field.return_value = _FAKE_COUNTRY_FIELD
        main._set_country_for_ticket("1", "Japan")
        main._set_country_for_ticket("2", "Germany")
        main._set_country_for_ticket("3", "Japan")
        assert zd.get_ticket_field.call_count == 1
        assert zd.set_custom_field.call_count == 3


def test_load_failure_results_in_empty_map_no_crash():
    """If Zendesk option fetch fails, set silently skips and the
    function never raises."""
    with patch.object(main, "zendesk") as zd:
        zd.get_ticket_field.side_effect = RuntimeError("zd down")
        main._set_country_for_ticket("1", "Japan")
        zd.set_custom_field.assert_not_called()


# ────────────────────────────────────────────────────────────────────────
#  A. _set_country_for_ticket — empty / disabled
# ────────────────────────────────────────────────────────────────────────

def test_empty_string_silent_skip():
    with patch.object(main, "zendesk") as zd:
        main._set_country_for_ticket("1", "")
        zd.set_custom_field.assert_not_called()


def test_none_silent_skip():
    with patch.object(main, "zendesk") as zd:
        main._set_country_for_ticket("1", None)  # type: ignore[arg-type]
        zd.set_custom_field.assert_not_called()


def test_field_id_unset_disables_call(monkeypatch):
    monkeypatch.setattr(main, "_ZENDESK_COUNTRY_FIELD_ID", "")
    with patch.object(main, "zendesk") as zd:
        main._set_country_for_ticket("1", "Japan")
        zd.set_custom_field.assert_not_called()


def test_zendesk_set_failure_does_not_raise():
    with patch.object(main, "zendesk") as zd:
        zd.set_custom_field.side_effect = RuntimeError("zd 503")
        main._set_country_for_ticket("1", "JP")  # must not raise


# ────────────────────────────────────────────────────────────────────────
#  C. WooCommerceClient.cancel_subscription — country priority chain
# ────────────────────────────────────────────────────────────────────────

def _wc_client():
    return WooCommerceClient(
        site_url="https://test.example.com",
        consumer_key="ck_test",
        consumer_secret="cs_test",
        dry_run=False,
    )


def _dispatcher(responses: dict):
    def _h(*args, **kwargs):
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
def test_country_from_meta_data_takes_priority(mock_get, mock_request, mock_put):
    """meta_data 'country' is preferred over billing.country."""
    customer = {
        "id": 42, "email": "test@example.com",
        "billing": {"email": "test@example.com", "country": "ZZ"},  # bogus billing
        "meta_data": [
            {"key": "language", "value": "ja"},
            {"key": "country", "value": "Japan"},     # winner
        ],
    }
    dispatch = _dispatcher({"customers": [customer], "subscriptions": [_make_sub()]})
    mock_get.side_effect = dispatch
    mock_request.side_effect = dispatch
    mock_put.return_value = _make_mock_put(200)

    result = _wc_client().cancel_subscription("test@example.com")
    assert result["country"] == "Japan", result


@patch("woocommerce_client.requests.put")
@patch("woocommerce_client.requests.request")
@patch("woocommerce_client.requests.get")
def test_country_falls_back_to_billing_when_meta_empty(mock_get, mock_request, mock_put):
    customer = {
        "id": 42, "email": "test@example.com",
        "billing": {"email": "test@example.com", "country": "JP"},
        "meta_data": [
            {"key": "language", "value": "ja"},
            # no country meta
        ],
    }
    dispatch = _dispatcher({"customers": [customer], "subscriptions": [_make_sub()]})
    mock_get.side_effect = dispatch
    mock_request.side_effect = dispatch
    mock_put.return_value = _make_mock_put(200)

    result = _wc_client().cancel_subscription("test@example.com")
    assert result["country"] == "JP", result


@patch("woocommerce_client.requests.put")
@patch("woocommerce_client.requests.request")
@patch("woocommerce_client.requests.get")
def test_country_falls_back_to_sub_billing(mock_get, mock_request, mock_put):
    customer = {"id": 42, "email": "test@example.com", "billing": {}, "meta_data": []}
    sub = _make_sub(country="KR")
    dispatch = _dispatcher({"customers": [customer], "subscriptions": [sub]})
    mock_get.side_effect = dispatch
    mock_request.side_effect = dispatch
    mock_put.return_value = _make_mock_put(200)

    result = _wc_client().cancel_subscription("test@example.com")
    assert result["country"] == "KR", result


@patch("woocommerce_client.requests.request")
@patch("woocommerce_client.requests.get")
def test_country_empty_when_no_source(mock_get, mock_request):
    customer = {"id": 42, "email": "test@example.com", "billing": {}, "meta_data": []}
    dispatch = _dispatcher({"customers": [customer], "subscriptions": []})
    mock_get.side_effect = dispatch
    mock_request.side_effect = dispatch

    result = _wc_client().cancel_subscription("test@example.com")
    assert result["country"] == "", result
