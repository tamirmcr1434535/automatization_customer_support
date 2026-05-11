"""
Unit tests for ticket_merger.merge_user_tickets.

The merger is intentionally simple — most logic is delegated to
ZendeskClient — so tests use a hand-rolled fake client instead of
patching individual methods. Each test seeds the fake with a fixed set
of tickets and asserts the resulting merge_calls / status.
"""
import pytest
from ticket_merger import merge_user_tickets, EMAIL_BLACKLIST


# ── Fake Zendesk client ─────────────────────────────────────────────────────

class FakeZendesk:
    """
    Minimal in-memory stand-in for ZendeskClient. Implements only the
    methods ticket_merger touches.
    """

    def __init__(self, tickets: dict[int, dict]):
        # tickets keyed by id; each entry is a ticket dict
        self.tickets = tickets
        self.merge_calls: list[dict] = []
        self.status_updates: list[tuple[str, str]] = []

    def get_ticket(self, ticket_id):
        return self.tickets.get(int(ticket_id))

    def search_user_tickets(self, requester_id, **_kwargs):
        return {
            tid: t for tid, t in self.tickets.items()
            if t.get("requester_id") == int(requester_id)
        }

    def merge_tickets(
        self, target_id, source_ids, target_comment="",
        source_comment="", **_kwargs,
    ):
        self.merge_calls.append({
            "target_id": int(target_id),
            "source_ids": [int(s) for s in source_ids],
            "target_comment": target_comment,
            "source_comment": source_comment,
        })
        # Simulate Zendesk's behavior: source tickets become closed +
        # tagged "merge". Target stays as it was (status updated separately).
        for sid in source_ids:
            if int(sid) in self.tickets:
                self.tickets[int(sid)]["status"] = "closed"
                tags = self.tickets[int(sid)].get("tags") or []
                if "merge" not in tags:
                    tags = tags + ["merge"]
                self.tickets[int(sid)]["tags"] = tags
        return {"job_status": {"status": "queued"}}

    def update_ticket_status(self, ticket_id, status):
        self.status_updates.append((str(ticket_id), status))
        if int(ticket_id) in self.tickets:
            self.tickets[int(ticket_id)]["status"] = status
        return {"ticket": {"id": int(ticket_id), "status": status}}


def _ticket(tid, requester_id=42, status="new", created_at="2025-01-01T00:00:00Z",
            email="user@example.com", subject="Cancel", description="cancel pls"):
    return {
        "id": tid,
        "requester_id": requester_id,
        "status": status,
        "created_at": created_at,
        "subject": subject,
        "description": description,
        "via": {"source": {"from": {"address": email}}},
        "tags": [],
    }


# ── Tests ───────────────────────────────────────────────────────────────────

def test_no_requester_id_returns_guard_status():
    fake = FakeZendesk({})
    out = merge_user_tickets("123", "", fake)
    assert out["status"] == "no_requester_id"
    assert fake.merge_calls == []


def test_ticket_not_found_returns_guard_status():
    fake = FakeZendesk({})  # current ticket absent
    out = merge_user_tickets("123", "42", fake)
    assert out["status"] == "ticket_not_found"
    assert fake.merge_calls == []


def test_blacklisted_email_skips_merge():
    blacklisted = next(iter(EMAIL_BLACKLIST))
    tickets = {
        100: _ticket(100, email=blacklisted, created_at="2025-01-02T00:00:00Z"),
        101: _ticket(101, email=blacklisted, created_at="2025-01-01T00:00:00Z"),
    }
    fake = FakeZendesk(tickets)
    out = merge_user_tickets("100", "42", fake)
    assert out["status"] == "skipped_blacklist"
    assert out["email"] == blacklisted
    assert fake.merge_calls == [], "must NOT call merge for blacklisted requester"


def test_no_action_when_only_one_active_ticket():
    tickets = {100: _ticket(100, status="open")}
    fake = FakeZendesk(tickets)
    out = merge_user_tickets("100", "42", fake)
    assert out["status"] == "no_action"
    assert out["active_count"] == 1
    assert fake.merge_calls == []


def test_no_action_when_other_tickets_are_closed():
    tickets = {
        100: _ticket(100, status="open", created_at="2025-01-03T00:00:00Z"),
        101: _ticket(101, status="closed", created_at="2025-01-01T00:00:00Z"),
        102: _ticket(102, status="closed", created_at="2025-01-02T00:00:00Z"),
    }
    fake = FakeZendesk(tickets)
    out = merge_user_tickets("100", "42", fake)
    assert out["status"] == "no_action"
    assert fake.merge_calls == []


def test_current_is_newer_gets_merged_into_oldest():
    # Two active tickets, current is the newer → it gets folded into older.
    tickets = {
        100: _ticket(100, status="new", created_at="2025-01-03T00:00:00Z"),
        101: _ticket(101, status="open", created_at="2025-01-01T00:00:00Z"),
    }
    fake = FakeZendesk(tickets)
    out = merge_user_tickets("100", "42", fake)
    assert out["status"] == "merged"
    assert out["target_id"] == 101
    assert 100 in out["merged_ids"]
    assert out["current_was_target"] is False
    assert len(fake.merge_calls) == 1
    assert fake.merge_calls[0]["target_id"] == 101
    assert fake.merge_calls[0]["source_ids"] == [100]
    # Oldest was "open" → target_status stays "open"
    assert fake.status_updates == [("101", "open")]


def test_current_is_oldest_survives_as_target():
    # Current is oldest, newer ones get folded into it.
    tickets = {
        100: _ticket(100, status="new", created_at="2025-01-01T00:00:00Z"),
        101: _ticket(101, status="open", created_at="2025-01-02T00:00:00Z"),
        102: _ticket(102, status="pending", created_at="2025-01-03T00:00:00Z"),
    }
    fake = FakeZendesk(tickets)
    out = merge_user_tickets("100", "42", fake)
    assert out["status"] == "merged"
    assert out["target_id"] == 100
    assert out["current_was_target"] is True
    assert sorted(out["merged_ids"]) == [101, 102]
    # Two merge calls (one per newer ticket)
    assert len(fake.merge_calls) == 2
    # Oldest was "new" → target_status becomes "new" after each merge
    assert fake.status_updates == [("100", "new"), ("100", "new")]


def test_oldest_open_keeps_open_status_after_merge():
    tickets = {
        100: _ticket(100, status="new", created_at="2025-01-02T00:00:00Z"),
        101: _ticket(101, status="open", created_at="2025-01-01T00:00:00Z"),
    }
    fake = FakeZendesk(tickets)
    merge_user_tickets("100", "42", fake)
    # Oldest #101 status was "open" → target_status="open"
    assert fake.status_updates == [("101", "open")]


def test_search_misses_current_ticket_still_included():
    """
    Search API may not yet have indexed the just-created current ticket.
    The merger explicitly adds it to the candidate set, so the merge can
    still happen.
    """
    tickets = {
        # ONLY the older ticket is in search results (current not indexed)
        101: _ticket(101, status="open", created_at="2025-01-01T00:00:00Z"),
        # 100 exists in get_ticket but NOT in search
        100: _ticket(100, status="new", created_at="2025-01-03T00:00:00Z",
                     requester_id=999),  # search filter won't return it
    }
    fake = FakeZendesk(tickets)
    out = merge_user_tickets("100", "42", fake)
    # Even though search didn't return #100, merger added it manually
    assert out["status"] == "merged"
    assert out["target_id"] == 101


def test_merge_api_failure_returns_error_status():
    class FailingZendesk(FakeZendesk):
        def merge_tickets(self, **kwargs):
            raise RuntimeError("Zendesk 500")

    tickets = {
        100: _ticket(100, status="new", created_at="2025-01-02T00:00:00Z"),
        101: _ticket(101, status="open", created_at="2025-01-01T00:00:00Z"),
    }
    fake = FailingZendesk(tickets)
    out = merge_user_tickets("100", "42", fake)
    assert out["status"] == "error"
    assert out["merged_ids"] == []
    assert len(out["errors"]) == 1
    assert "Zendesk 500" in out["errors"][0]


def test_email_extraction_is_lowercased_and_stripped():
    """Case/whitespace variations of blacklisted emails are normalized."""
    blacklisted = next(iter(EMAIL_BLACKLIST))
    upper = blacklisted.upper()
    tickets = {
        100: _ticket(100, email=f"  {upper}  ",
                     created_at="2025-01-02T00:00:00Z"),
        101: _ticket(101, email=upper, created_at="2025-01-01T00:00:00Z"),
    }
    fake = FakeZendesk(tickets)
    out = merge_user_tickets("100", "42", fake)
    assert out["status"] == "skipped_blacklist"
