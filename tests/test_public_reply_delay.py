"""PUBLIC_REPLY_DELAY_SEC — hold the customer-visible reply, never the action.

Anna, 2026-08-27: the bot replies inside the same minute, which reads as a
machine and looks like an auto-responder to spam filters. These tests pin
the three things that make the knob safe to turn in production:
  1. it is a DEADLINE from webhook arrival, not an extra sleep on top;
  2. it never delays internal notes, tags or status changes;
  3. unset / garbage env = exactly today's behaviour (zero sleep).
"""
import time
from unittest.mock import patch

import pytest

import zendesk_client as zc


@pytest.fixture
def client():
    c = zc.ZendeskClient("sub", "a@b.c", "tok", dry_run=False)
    # Never touch the network in these tests.
    with patch.object(c, "_request_with_retry") as req:
        c._req = req
        yield c


@pytest.fixture(autouse=True)
def _clean_clock():
    """Each test starts with no arrival stamp unless it sets one."""
    if hasattr(zc._request_clock, "t0"):
        del zc._request_clock.t0
    yield
    if hasattr(zc._request_clock, "t0"):
        del zc._request_clock.t0


def _slept(monkeypatch):
    """Record time.sleep calls instead of actually sleeping."""
    calls = []
    monkeypatch.setattr(zc.time, "sleep", lambda s: calls.append(s))
    return calls


# ── knob off / malformed ────────────────────────────────────────────────

def test_unset_knob_does_not_sleep(client, monkeypatch):
    monkeypatch.delenv("PUBLIC_REPLY_DELAY_SEC", raising=False)
    calls = _slept(monkeypatch)
    client.post_reply("1", "hi")
    assert calls == []
    client._req.assert_called_once()


@pytest.mark.parametrize("value", ["", "  ", "abc", "-30"])
def test_malformed_or_negative_knob_does_not_sleep(client, monkeypatch, value):
    monkeypatch.setenv("PUBLIC_REPLY_DELAY_SEC", value)
    calls = _slept(monkeypatch)
    client.post_reply("1", "hi")
    assert calls == []          # garbage must never block a reply
    client._req.assert_called_once()   # ...and must never swallow one either


# ── deadline semantics ──────────────────────────────────────────────────

def test_no_arrival_stamp_sleeps_full_target(client, monkeypatch):
    monkeypatch.setenv("PUBLIC_REPLY_DELAY_SEC", "180")
    calls = _slept(monkeypatch)
    client.post_reply("1", "hi")
    assert calls == [pytest.approx(180, abs=1)]


def test_time_already_spent_is_subtracted(client, monkeypatch):
    """The classifier's 50s counts toward the 180s, it does not stack."""
    monkeypatch.setenv("PUBLIC_REPLY_DELAY_SEC", "180")
    calls = _slept(monkeypatch)
    zc.ZendeskClient.begin_request()
    monkeypatch.setattr(zc.time, "monotonic", lambda: zc._request_clock.t0 + 50)
    client.post_reply("1", "hi")
    assert calls == [pytest.approx(130, abs=1)]


def test_slow_ticket_replies_immediately(client, monkeypatch):
    """A ticket that already took longer than the target is not held at all."""
    monkeypatch.setenv("PUBLIC_REPLY_DELAY_SEC", "180")
    calls = _slept(monkeypatch)
    zc.ZendeskClient.begin_request()
    monkeypatch.setattr(zc.time, "monotonic", lambda: zc._request_clock.t0 + 400)
    client.post_reply("1", "hi")
    assert calls == []
    client._req.assert_called_once()


def test_jitter_stays_inside_its_band(client, monkeypatch):
    monkeypatch.setenv("PUBLIC_REPLY_DELAY_SEC", "120")
    monkeypatch.setenv("PUBLIC_REPLY_DELAY_JITTER_SEC", "60")
    calls = _slept(monkeypatch)
    for _ in range(50):
        client.post_reply("1", "hi")
    assert len(calls) == 50
    assert all(120 <= c <= 180 for c in calls)
    assert len(set(round(c) for c in calls)) > 1   # actually varying


# ── which writes are paced ──────────────────────────────────────────────

def test_all_three_public_reply_paths_are_paced(client, monkeypatch):
    monkeypatch.setenv("PUBLIC_REPLY_DELAY_SEC", "90")
    calls = _slept(monkeypatch)
    client.post_reply("1", "hi")
    client.post_reply_and_set_pending("1", "hi")
    client.reply_solve_and_set_fields("1", "hi", {123: "refund"})
    assert calls == [pytest.approx(90, abs=1)] * 3


def test_internal_writes_are_never_paced(client, monkeypatch):
    """Notes, tags and status changes must stay instant — support watches these."""
    monkeypatch.setenv("PUBLIC_REPLY_DELAY_SEC", "600")
    calls = _slept(monkeypatch)
    client.add_internal_note("1", "escalated")
    client.add_tag("1", "bot_handled")
    client.set_open("1")
    client.solve_ticket("1")
    client.set_custom_fields("1", {123: "refund"})
    assert calls == []


def test_dry_run_never_sleeps(monkeypatch):
    """Tests and DRY_RUN deploys must not sit for minutes per ticket."""
    monkeypatch.setenv("PUBLIC_REPLY_DELAY_SEC", "600")
    calls = _slept(monkeypatch)
    dry = zc.ZendeskClient("sub", "a@b.c", "tok", dry_run=True)
    dry.post_reply("1", "hi")
    dry.reply_solve_and_set_fields("1", "hi", {})
    assert calls == []


# ── concurrency ─────────────────────────────────────────────────────────

def test_arrival_clock_is_per_thread(client, monkeypatch):
    """Cloud Run runs 80 concurrent requests against ONE shared client —
    one ticket's arrival stamp must not leak into another's."""
    import threading

    monkeypatch.setenv("PUBLIC_REPLY_DELAY_SEC", "100")
    seen = {}

    def worker(name, age):
        zc.ZendeskClient.begin_request()
        t0 = zc._request_clock.t0
        with patch.object(zc.time, "monotonic", lambda: t0 + age), \
             patch.object(zc.time, "sleep", lambda s: seen.__setitem__(name, s)):
            client.post_reply(name, "hi")

    threads = [
        threading.Thread(target=worker, args=("fast", 10)),
        threading.Thread(target=worker, args=("slow", 80)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert seen["fast"] == pytest.approx(90, abs=1)
    assert seen["slow"] == pytest.approx(20, abs=1)
