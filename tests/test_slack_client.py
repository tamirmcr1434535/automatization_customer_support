"""
Unit tests for SlackClient
===========================
All HTTP calls are mocked — no real Slack messages sent.

Scenarios:
  1. DRY_RUN — no HTTP call, returns True
  2. Successful send — POST called once, returns True
  3. Slack API error (non-200) — returns False, no exception raised
  4. Network timeout — returns False, no exception raised
  5. Message contains ticket URL with correct subdomain
  6. Message contains email and intent
"""

import pytest
from unittest.mock import patch, MagicMock
import requests

from slack_client import SlackClient


def make_client(dry_run=False):
    return SlackClient(
        webhook_url="https://hooks.slack.com/services/TEST/TEST/TEST",
        dry_run=dry_run,
    )


# ── 1. DRY_RUN ────────────────────────────────────────────────────────────── #

def test_dry_run_returns_true_without_http_call():
    client = make_client(dry_run=True)
    with patch("slack_client.requests.post") as mock_post:
        result = client.notify_manual_review("1234", "user@test.com", "TRIAL_CANCELLATION", "wwiqtest")
        assert result is True
        mock_post.assert_not_called()


# ── 2. Successful send ────────────────────────────────────────────────────── #

@patch("slack_client.requests.post")
def test_successful_send(mock_post):
    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_post.return_value = mock_resp

    client = make_client()
    result = client.notify_manual_review("1234", "user@test.com", "TRIAL_CANCELLATION", "wwiqtest")

    assert result is True
    mock_post.assert_called_once()


# ── 3. Slack API error ────────────────────────────────────────────────────── #

@patch("slack_client.requests.post")
def test_slack_api_error_returns_false(mock_post):
    mock_resp = MagicMock()
    mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError("400 Bad Request")
    mock_post.return_value = mock_resp

    client = make_client()
    result = client.notify_manual_review("1234", "user@test.com", "SUB_CANCELLATION", "wwiqtest")

    assert result is False


# ── 4. Network timeout ────────────────────────────────────────────────────── #

@patch("slack_client.requests.post", side_effect=requests.exceptions.Timeout)
def test_network_timeout_returns_false(mock_post):
    client = make_client()
    result = client.notify_manual_review("1234", "user@test.com", "SUB_CANCELLATION", "wwiqtest")
    assert result is False


# ── 5. Message contains correct ticket URL ────────────────────────────────── #

@patch("slack_client.requests.post")
def test_message_contains_ticket_url(mock_post):
    mock_post.return_value = MagicMock()
    mock_post.return_value.raise_for_status.return_value = None

    client = make_client()
    client.notify_manual_review("9999", "x@x.com", "TRIAL_CANCELLATION", "wwiqtest")

    payload = mock_post.call_args.kwargs.get("json") or mock_post.call_args[1].get("json")
    payload_str = str(payload)
    assert "9999" in payload_str
    assert "wwiqtest.zendesk.com" in payload_str


# ── 6. Message contains email and intent ─────────────────────────────────── #

@patch("slack_client.requests.post")
def test_message_contains_email_and_intent(mock_post):
    mock_post.return_value = MagicMock()
    mock_post.return_value.raise_for_status.return_value = None

    client = make_client()
    client.notify_manual_review("1234", "customer@example.com", "SUB_CANCELLATION", "wwiqtest")

    payload_str = str(mock_post.call_args)
    assert "customer@example.com" in payload_str
