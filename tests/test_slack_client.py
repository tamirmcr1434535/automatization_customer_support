"""
Unit tests for SlackClient
===========================
All HTTP calls are mocked — no real Slack messages sent.

SlackClient sends DMs via a Slack **bot token** (not a webhook):
  users.lookupByEmail (GET) → conversations.open (POST) → chat.postMessage (POST).
`notify_ticket_result` is the per-ticket report used by the bot.

Scenarios:
  1. DRY_RUN — no HTTP call, returns True
  2. Successful send — chat.postMessage called, returns True
  3. Slack API error (chat.postMessage ok:false) — returns False, no exception
  4. Network exception during lookup — returns False, no exception
  5. Message contains the ticket URL with the correct subdomain
  6. Message contains the email and intent
"""

import pytest
from unittest.mock import patch, MagicMock
import requests

from slack_client import SlackClient


_RESULT = {
    "status": "manual_review_required",
    "intent": "TRIAL_CANCELLATION",
    "email": "user@test.com",
    "language": "EN",
    "confidence": 0.92,
}


def make_client(dry_run=False):
    return SlackClient(
        bot_token="xoxb-test-token",
        target_email="ops@example.com",
        dry_run=dry_run,
    )


def slack_http(user_ok=True, open_ok=True, post_ok=True, get_exc=None, capture=None):
    """Patch the three Slack HTTP seams onto URL-routed fakes.

    users.lookupByEmail → GET; conversations.open + chat.postMessage → POST.
    `capture`, if given, collects each chat.postMessage JSON payload.
    `get_exc`, if given, is raised from the users.lookupByEmail GET.
    """
    def fake_get(url, **kwargs):
        if get_exc is not None:
            raise get_exc
        r = MagicMock()
        r.json.return_value = (
            {"ok": True, "user": {"id": "U1"}} if user_ok
            else {"ok": False, "error": "users_not_found"}
        )
        return r

    def fake_post(url, **kwargs):
        r = MagicMock()
        if url.endswith("conversations.open"):
            r.json.return_value = (
                {"ok": True, "channel": {"id": "D1"}} if open_ok
                else {"ok": False, "error": "cannot_dm_bot"}
            )
        else:  # chat.postMessage
            if capture is not None:
                capture.append(kwargs.get("json", {}))
            r.json.return_value = {"ok": True} if post_ok else {"ok": False, "error": "channel_not_found"}
        return r

    return patch.multiple(
        "slack_client.requests",
        get=MagicMock(side_effect=fake_get),
        post=MagicMock(side_effect=fake_post),
    )


# ── 1. DRY_RUN ────────────────────────────────────────────────────────────── #

def test_dry_run_returns_true_without_http_call():
    client = make_client(dry_run=True)
    with patch("slack_client.requests.get") as mock_get, \
         patch("slack_client.requests.post") as mock_post:
        result = client.notify_ticket_result("1234", _RESULT, "wwiqtest")
        assert result is True
        mock_get.assert_not_called()
        mock_post.assert_not_called()


# ── 2. Successful send ────────────────────────────────────────────────────── #

def test_successful_send():
    cap: list = []
    client = make_client()
    with slack_http(capture=cap):
        result = client.notify_ticket_result("1234", _RESULT, "wwiqtest")
    assert result is True
    assert len(cap) == 1          # chat.postMessage fired exactly once


# ── 3. Slack API error (chat.postMessage ok:false) ───────────────────────── #

def test_slack_api_error_returns_false():
    client = make_client()
    with slack_http(post_ok=False):
        result = client.notify_ticket_result("1234", _RESULT, "wwiqtest")
    assert result is False


# ── 4. Network exception during lookup → False (no raise) ─────────────────── #

def test_network_timeout_returns_false():
    client = make_client()
    with slack_http(get_exc=requests.exceptions.Timeout("boom")):
        result = client.notify_ticket_result("1234", _RESULT, "wwiqtest")
    assert result is False


# ── 5. Message contains ticket URL with correct subdomain ─────────────────── #

def test_message_contains_ticket_url():
    cap: list = []
    client = make_client()
    with slack_http(capture=cap):
        client.notify_ticket_result("1234", _RESULT, "mysub")
    text = cap[0]["text"]
    assert "https://mysub.zendesk.com/agent/tickets/1234" in text


# ── 6. Message contains email and intent ──────────────────────────────────── #

def test_message_contains_email_and_intent():
    cap: list = []
    client = make_client()
    with slack_http(capture=cap):
        client.notify_ticket_result("1234", _RESULT, "wwiqtest")
    text = cap[0]["text"]
    assert "user@test.com" in text
    assert "TRIAL_CANCELLATION" in text
