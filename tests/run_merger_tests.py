"""
Standalone runner for merger integration tests.

Mirrors the dryrun_monitor_v6 guard-test style but runs LOCALLY against
the local bot repo — no git clone, no Colab, no Anthropic key needed.
All external clients are mocked.

Usage from the bot repo root:

    /opt/homebrew/bin/python3.11 tests/run_merger_tests.py

(Python 3.10+ required because conftest.py uses `int | None` syntax.
On macOS the default `python3` is 3.8 — use the homebrew 3.11+ binary.)
"""
import os
import sys
import copy
import logging
from contextlib import ExitStack
from unittest.mock import patch, MagicMock

# ── 1. Make repo importable ─────────────────────────────────────────────────
REPO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_DIR not in sys.path:
    sys.path.insert(0, REPO_DIR)

# ── 2. Env vars — fake but valid-shaped so main.py imports cleanly ──────────
os.environ.setdefault("DRY_RUN", "true")
os.environ.setdefault("TEST_MODE", "false")
os.environ.setdefault("SKIP_WC_HEALTHCHECK", "true")  # bypass live WC ping
os.environ.setdefault("ZENDESK_SUBDOMAIN", "iqbooster")
os.environ.setdefault("ZENDESK_EMAIL", "test@example.com")
os.environ.setdefault("ZENDESK_API_TOKEN", "fake-token")
os.environ.setdefault("WOO_SITE_URL", "https://iqbooster.org")
os.environ.setdefault("WOO_CONSUMER_KEY", "fake-ck")
os.environ.setdefault("WOO_CONSUMER_SECRET", "fake-cs")
os.environ.setdefault("STRIPE_SECRET_KEY", "sk_test_fake")
os.environ.setdefault("ANTHROPIC_API_KEY", "sk-ant-fake")
os.environ.setdefault("SLACK_BOT_TOKEN", "xoxb-fake")
os.environ.setdefault("SLACK_TARGET_EMAIL", "fake@fake.test")
os.environ.setdefault("BQ_PROJECT", "fake-project")
os.environ.setdefault("BQ_DATASET", "fake_dataset")
os.environ.setdefault("BQ_TABLE", "fake_table")

# ── 3. Mock heavy modules BEFORE importing main ─────────────────────────────
# main.py imports classifier / reply_generator / bq_logger which would
# otherwise instantiate real Anthropic / BigQuery clients at import time.
# reply_generator's `validate_reply` returns (bool, str) — must seed proper
# return values so tuple-unpack in _finish_cancellation doesn't blow up.
_classifier_mock = MagicMock()
_classifier_mock.classify_ticket = MagicMock(return_value={
    "intent": "TRIAL_CANCELLATION", "language": "EN",
    "confidence": 0.95, "chargeback_risk": "",
})
_reply_gen_mock = MagicMock()
_reply_gen_mock.generate_reply = MagicMock(return_value="Your subscription has been cancelled. — Team")
_reply_gen_mock.validate_reply = MagicMock(return_value=(True, "ok"))
_reply_gen_mock.english_fallback_reply = MagicMock(
    return_value="Your subscription has been cancelled. — Team")
sys.modules.setdefault("classifier", _classifier_mock)
sys.modules.setdefault("reply_generator", _reply_gen_mock)
sys.modules.setdefault("bq_logger", MagicMock())

# Quiet down logs unless something fails
logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s | %(message)s")

import main as bot  # noqa: E402
assert bot.DRY_RUN, "DRY_RUN must be True for tests"
assert hasattr(bot, "ticket_merger"), "main must expose ticket_merger"
print(f"bot loaded  DRY_RUN={bot.DRY_RUN}  ticket_merger=OK")
print()


# ── 4. Test helpers (mirrors dryrun_monitor_v6 cell 3 + cell 4 style) ───────

class WriteTracker:
    def __init__(self):
        self.replies = []
        self.tags_added = []
        self.tags_removed = []
        self.solved = False
        self.set_pending = False
        self.internal_notes = []
        self.slack_calls = []

    @property
    def reply_count(self):
        return len(self.replies)


def _make_ticket(tid="99999", subject="Cancel subscription",
                 body="I want to cancel my subscription",
                 email="test@example.com", tags=None, status="open"):
    return {
        "id": tid, "subject": subject, "description": body,
        "tags": tags or [], "status": status,
        "requester": {"email": email, "name": "Test User"},
        "custom_fields": [],
    }


_PASS = 0
_FAIL = 0
_RESULTS = []


def _run_merger_test(
    name, ticket_data, siblings, merger_status,
    refetch_overrides=None, expect_status=None,
    expect_no_reply=True, requester_id=42,
    siblings_after_merge=None,
):
    global _PASS, _FAIL
    tracker = WriteTracker()
    tid = ticket_data.get("id", "99999")
    if isinstance(expect_status, str):
        expect_status = [expect_status]

    ticket_data = copy.deepcopy(ticket_data)
    if requester_id is not None:
        ticket_data.setdefault("requester_id", requester_id)

    refetched = copy.deepcopy(ticket_data)
    if refetch_overrides:
        for k, v in refetch_overrides.items():
            if k == "tags":
                refetched["tags"] = (refetched.get("tags") or []) + list(v)
            else:
                refetched[k] = v

    get_ticket_seq = [copy.deepcopy(ticket_data), refetched]

    def _get_ticket_side(*_a, **_kw):
        return get_ticket_seq.pop(0) if len(get_ticket_seq) > 1 else refetched

    original_test_mode = bot.TEST_MODE
    bot.TEST_MODE = False

    # The bot calls find_active_tickets_for_email TWICE in the merge guard:
    # once to detect siblings before merger runs, again after merger acted
    # (to confirm whether merger folded siblings into current ticket). If
    # the merger merged-INTO-current successfully, the 2nd call must return
    # an empty list — otherwise the bot falls through to skipped_merge_candidate.
    if siblings_after_merge is None:
        siblings_after_merge = siblings
    find_active_seq = [
        copy.deepcopy(siblings),
        copy.deepcopy(siblings_after_merge),
    ]

    def _find_active_side(*_a, **_kw):
        return find_active_seq.pop(0) if len(find_active_seq) > 1 else copy.deepcopy(siblings_after_merge)

    with ExitStack() as stack:
        stack.enter_context(patch.object(bot.zendesk, "get_ticket",
            side_effect=_get_ticket_side))
        stack.enter_context(patch.object(bot.zendesk, "find_active_tickets_for_email",
            side_effect=_find_active_side))

        if merger_status is None and siblings and requester_id:
            merger_mock = MagicMock(side_effect=RuntimeError("simulated merger crash"))
        else:
            merger_mock = MagicMock(return_value=merger_status or {"status": "no_action"})
        stack.enter_context(patch("main.ticket_merger.merge_user_tickets", merger_mock))

        def _on_reply(t, b): tracker.replies.append(b)
        def _on_add_tag(t, tag): tracker.tags_added.append(tag)
        def _on_note(t, body): tracker.internal_notes.append(body)
        stack.enter_context(patch.object(bot.zendesk, "post_reply", side_effect=_on_reply))
        stack.enter_context(patch.object(bot.zendesk, "post_reply_and_set_pending",
            side_effect=lambda t, b: tracker.replies.append(b)))
        stack.enter_context(patch.object(bot.zendesk, "add_tag", side_effect=_on_add_tag))
        stack.enter_context(patch.object(bot.zendesk, "remove_tag", side_effect=lambda *a: None))
        stack.enter_context(patch.object(bot.zendesk, "solve_ticket", side_effect=lambda t: None))
        stack.enter_context(patch.object(bot.zendesk, "set_open", return_value=None))
        stack.enter_context(patch.object(bot.zendesk, "add_internal_note", side_effect=_on_note))
        stack.enter_context(patch.object(bot.zendesk, "count_bot_replies", return_value=0))
        stack.enter_context(patch.object(bot.zendesk, "get_ticket_tags",
            return_value=ticket_data.get("tags", [])))
        stack.enter_context(patch.object(bot.zendesk, "last_public_comment_is_from_agent",
            return_value=False))
        stack.enter_context(patch.object(bot.zendesk, "get_first_customer_comment",
            return_value=None))
        stack.enter_context(patch.object(bot.zendesk, "get_all_customer_comments_text",
            return_value=ticket_data.get("description", "")))
        stack.enter_context(patch.object(bot.zendesk, "get_last_customer_comment",
            return_value=None))
        stack.enter_context(patch.object(bot.zendesk, "was_recently_handled",
            return_value=False))
        # Replace the entire slack triplet with MagicMocks — current
        # SlackClient only has notify_ticket_result / notify_startup_failure,
        # but older code paths may reference other notify_* methods. We
        # don't assert on slack from merger tests, so blanket-mock is fine.
        stack.enter_context(patch.object(bot, "slack", MagicMock()))
        stack.enter_context(patch.object(bot, "_report_slack", MagicMock()))
        stack.enter_context(patch.object(bot, "_alert_slack", MagicMock()))
        stack.enter_context(patch("main.classify_ticket", return_value={
            "intent": "TRIAL_CANCELLATION", "language": "EN",
            "confidence": 0.95, "chargeback_risk": "",
        }))
        stack.enter_context(patch.object(bot.woo, "cancel_subscription", return_value={
            "status": "dry_run", "cancelled": True, "subscription_type": "trial",
            "subscription_id": 999, "order_count": 1, "plan": "IQ Test",
        }))
        stack.enter_context(patch.object(bot.stripe_cli, "cancel_subscription",
            return_value={"status": "not_found"}))
        stack.enter_context(patch.object(bot.stripe_cli, "find_email_by_last4",
            return_value=None))
        stack.enter_context(patch("main.log_result", return_value=None))

        try:
            result = bot._process(str(tid))
        except Exception as e:
            import traceback
            result = {"status": "EXCEPTION", "error": f"{type(e).__name__}: {e}",
                      "trace": traceback.format_exc()}

    bot.TEST_MODE = original_test_mode

    errors = []
    got_status = result.get("status", "?")
    if got_status not in expect_status:
        errors.append(f"status: expected {expect_status}, got '{got_status}'")
    if expect_no_reply and tracker.reply_count > 0:
        errors.append(f"expected NO reply but got {tracker.reply_count}")
    should_call_merger = bool(siblings) and bool(requester_id)
    if should_call_merger and merger_mock.call_count == 0:
        errors.append("expected merger to be invoked but it was not")
    if not should_call_merger and merger_mock.call_count > 0:
        errors.append(f"expected merger NOT invoked but it ran {merger_mock.call_count}x")

    if got_status == "EXCEPTION":
        errors.append(f"raw error: {result.get('error', '?')}")

    passed = len(errors) == 0
    if passed:
        _PASS += 1
    else:
        _FAIL += 1
    _RESULTS.append({
        "name": name, "passed": passed, "errors": errors,
        "trace": result.get("trace") if got_status == "EXCEPTION" else None,
    })


# ── 5. Tests ────────────────────────────────────────────────────────────────

print("=" * 74)
print("MERGER INTEGRATION TESTS")
print("=" * 74)
print("--- 12. Merger Integration ---")

_run_merger_test(
    name="12.1 No siblings -> merger NOT called, normal flow proceeds",
    ticket_data=_make_ticket(),
    siblings=[],
    merger_status=None,
    expect_status=["success", "skipped_not_handled", "awaiting_card_digits",
                   "manual_review_required"],
    expect_no_reply=False)

_run_merger_test(
    name="12.2 Siblings exist, current merged INTO oldest -> skipped_merged",
    ticket_data=_make_ticket(tid="200"),
    siblings=[{"id": 199, "subject": "Older ticket", "status": "open"}],
    merger_status={"status": "merged", "target_id": 199,
                   "merged_ids": [200], "current_was_target": False},
    refetch_overrides={"tags": ["merge"]},
    expect_status="skipped_merged")

_run_merger_test(
    name="12.3 Siblings + current closed during merge -> skipped_closed",
    ticket_data=_make_ticket(tid="201"),
    siblings=[{"id": 198, "subject": "Older", "status": "open"}],
    merger_status={"status": "merged", "target_id": 198, "merged_ids": [201]},
    refetch_overrides={"status": "closed"},
    expect_status="skipped_closed")

_run_merger_test(
    name="12.4 Current IS oldest -> siblings folded INTO current, bot continues",
    ticket_data=_make_ticket(tid="150"),
    siblings=[{"id": 151, "subject": "Newer dup", "status": "new"}],
    siblings_after_merge=[],  # merger folded #151 into #150 -> no more siblings
    merger_status={"status": "merged", "target_id": 150,
                   "merged_ids": [151], "current_was_target": True},
    refetch_overrides=None,
    expect_status=["success", "skipped_not_handled", "awaiting_card_digits",
                   "manual_review_required"],
    expect_no_reply=False)

_run_merger_test(
    name="12.5 Merger raises -> falls through to skipped_merge_candidate",
    ticket_data=_make_ticket(tid="250"),
    siblings=[{"id": 249, "subject": "Older", "status": "open"}],
    merger_status=None,
    refetch_overrides=None,
    expect_status="skipped_merge_candidate")

_run_merger_test(
    name="12.6 Siblings but no requester_id -> merger NOT called, fallback",
    ticket_data=_make_ticket(tid="260"),
    siblings=[{"id": 259, "subject": "Older", "status": "open"}],
    merger_status=None,
    refetch_overrides=None,
    expect_status="skipped_merge_candidate",
    requester_id=None)

_run_merger_test(
    name="12.7 Merger returns no_action -> falls through to skipped_merge_candidate",
    ticket_data=_make_ticket(tid="270"),
    siblings=[{"id": 269, "subject": "Older", "status": "open"}],
    merger_status={"status": "no_action", "active_count": 1},
    refetch_overrides=None,
    expect_status="skipped_merge_candidate")

# ── 6. Summary ──────────────────────────────────────────────────────────────

print()
print("=" * 74)
print(f"RESULTS: {_PASS} passed, {_FAIL} failed out of {_PASS + _FAIL} tests")
print("=" * 74)
for r in _RESULTS:
    icon = "PASS" if r["passed"] else "FAIL"
    print(f"  [{icon}] {r['name']}")
    if not r["passed"]:
        for err in r["errors"]:
            print(f"         {err}")
        if r.get("trace"):
            for line in r["trace"].splitlines()[-12:]:
                print(f"         {line}")
print()
if _FAIL > 0:
    print(f"DEPLOYMENT BLOCKED: {_FAIL} merger test(s) failed.")
    sys.exit(1)
else:
    print("ALL MERGER TESTS PASSED. Safe to push feature/internal-merger.")
    sys.exit(0)
