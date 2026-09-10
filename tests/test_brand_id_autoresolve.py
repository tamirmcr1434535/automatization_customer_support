"""A brand nobody pasted an id for must still be recognised.

`_ZENDESK_BRAND_TO_KEY` is hand-maintained: someone opens the Zendesk admin UI,
copies a numeric brand id and pastes it into the source. IQTEST.JP showed what
happens when nobody does — the brand resolves to "", and `links_for` then falls
back to another brand's legal documents, so a Japanese customer is quoted WW IQ
Test's terms. The bot holds a Zendesk token, so it can ask instead of waiting
for a human to copy a number.
"""
import os
import sys
from unittest.mock import MagicMock, patch

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
import pytest  # noqa: E402


@pytest.fixture(autouse=True)
def _clear_brand_cache():
    main._ZENDESK_BRAND_ID_TO_KEY_FETCHED = None
    yield
    main._ZENDESK_BRAND_ID_TO_KEY_FETCHED = None


IQTESTJP_ID = 99999999999999          # not in the hand-maintained table


@patch.object(main, "zendesk")
def test_known_brand_needs_no_api_call(mock_zd):
    """The seven pasted ids keep working offline — and stay the fast path."""
    assert main._zendesk_brand_key({"brand_id": 27151310564636}) == "16types"
    mock_zd.get_brands.assert_not_called()


@patch.object(main, "zendesk")
def test_unknown_brand_resolves_from_its_url(mock_zd):
    mock_zd.get_brands.return_value = [
        {"id": IQTESTJP_ID, "name": "IQTEST.JP", "brand_url": "https://iqtest.jp"},
    ]
    assert main._zendesk_brand_key({"brand_id": IQTESTJP_ID}) == "iqtestjp"


@patch.object(main, "zendesk")
def test_display_name_is_the_second_chance(mock_zd):
    """Some brands' brand_url is the Zendesk subdomain, not the product site."""
    mock_zd.get_brands.return_value = [
        {"id": IQTESTJP_ID, "name": "IQTEST.JP",
         "brand_url": "https://wwiqtest1234.zendesk.com"},
    ]
    assert main._zendesk_brand_key({"brand_id": IQTESTJP_ID}) == "iqtestjp"


@patch.object(main, "zendesk")
def test_a_brand_matching_nothing_stays_unresolved(mock_zd):
    """Never a wrong guess: an unrecognised domain resolves to "", exactly as
    an unlisted brand does today."""
    mock_zd.get_brands.return_value = [
        {"id": 5, "name": "Some New Thing", "brand_url": "https://example.com"},
    ]
    assert main._zendesk_brand_key({"brand_id": 5}) == ""


@patch.object(main, "zendesk")
def test_api_failure_is_silent(mock_zd):
    mock_zd.get_brands.side_effect = RuntimeError("Zendesk 500")
    assert main._zendesk_brand_key({"brand_id": IQTESTJP_ID}) == ""


@patch.object(main, "zendesk")
def test_brands_are_fetched_once_per_process(mock_zd):
    """Zendesk fires 5-15 webhooks per ticket — this must not be 15 API calls."""
    mock_zd.get_brands.return_value = [
        {"id": IQTESTJP_ID, "name": "IQTEST.JP", "brand_url": "https://iqtest.jp"},
    ]
    for _ in range(5):
        main._zendesk_brand_key({"brand_id": IQTESTJP_ID})
    assert mock_zd.get_brands.call_count == 1


@patch.object(main, "zendesk")
def test_missing_or_junk_brand_id_never_raises(mock_zd):
    for ticket in ({}, {"brand_id": None}, {"brand_id": "abc"}):
        assert main._zendesk_brand_key(ticket) == ""
    mock_zd.get_brands.assert_not_called()


@patch.object(main, "zendesk")
def test_helpdesk_url_never_files_a_brand_under_the_account_name(mock_zd):
    """The Zendesk account is `wwiqtest`, so every brand_url on
    *.zendesk.com contains "wwiqtest". Matching on that would file 16 Types,
    IQ Pro and IQTEST.JP all under WW IQ Test — silently, and only visible as
    wrong legal links in a customer's refund email."""
    mock_zd.get_brands.return_value = [
        {"id": 7, "name": "Some New Thing",
         "brand_url": "https://wwiqtest1234.zendesk.com"},
    ]
    assert main._zendesk_brand_key({"brand_id": 7}) == ""
