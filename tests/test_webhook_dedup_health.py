"""
The distributed half of webhook dedup must not fail silently
============================================================
Layer 2 of `_webhook_dedup` is a Firestore atomic create() acting as a mutex
across Cloud Run instances. It was DEAD in production from the day it was
written until 2026-09-02 — the project had no Firestore database, so every
request logged `404 The database (default) does not exist` and fell through to
the per-instance dict, which cannot dedup across instances at all.

Nobody noticed because a per-request WARNING that never stops reads as noise.
Measured cost over Aug-2026: 5 tickets got 2+ bot replies (#181910 got four
cancellation emails), duplicates 0-5 seconds apart — races that beat the
`bot_handled` tag, since add_tag is a separate POST with an eventually
consistent read.

Fail-open is still correct: a dedup outage must not stop the bot answering
tickets. These tests pin that it is no longer free.
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
os.environ.setdefault("SKIP_WC_HEALTHCHECK", "true")

sys.modules.setdefault("classifier", MagicMock())
sys.modules.setdefault("reply_generator", MagicMock())
sys.modules.setdefault("bq_logger", MagicMock())

import main  # noqa: E402


# The real 404 the project emitted on every request for months.
MISSING_DB = RuntimeError(
    "404 The database (default) does not exist for project "
    "powerful-vine-426615-r2 Please visit https://console.cloud.google.com/"
    "datastore/setup?project=powerful-vine-426615-r2 to add a Cloud Datastore "
    "or Cloud Firestore database.")


@pytest.fixture(autouse=True)
def _reset():
    main._dedup_seen.clear()
    main._fs_degraded_reported = False
    main._fs_transient_reported_at = 0.0
    yield
    main._dedup_seen.clear()
    main._fs_degraded_reported = False
    main._fs_transient_reported_at = 0.0


def _with_firestore_raising(exc):
    """Firestore unavailable — the create() call blows up."""
    db = MagicMock()
    db.collection.return_value.document.return_value.create.side_effect = exc
    return patch.object(main, "_get_firestore_db", return_value=db)


# ── fail-open is preserved ───────────────────────────────────────────────── #

def test_a_dead_lock_still_lets_the_first_webhook_through():
    # Fail-CLOSED here would mean a Firestore outage stops the bot replying.
    with _with_firestore_raising(MISSING_DB), \
         patch.object(main, "_alert_slack", MagicMock()):
        assert main._webhook_dedup("500") is False


def test_in_memory_layer_still_catches_same_instance_duplicates():
    with _with_firestore_raising(MISSING_DB), \
         patch.object(main, "_alert_slack", MagicMock()):
        assert main._webhook_dedup("501") is False   # first claim
        assert main._webhook_dedup("501") is True    # dupe, same instance


# ── the outage is now reported, once ─────────────────────────────────────── #

def test_missing_database_alerts_slack_and_logs_error():
    slack = MagicMock()
    with _with_firestore_raising(MISSING_DB), \
         patch.object(main, "_alert_slack", slack), \
         patch.object(main.log, "error") as log_error:
        main._webhook_dedup("502")

    slack._post.assert_called_once()
    text = slack._post.call_args.args[0]
    assert "degraded" in text.lower()
    assert "duplicate" in text.lower()          # names the actual consequence
    log_error.assert_called_once()
    assert "DEGRADED" in log_error.call_args.args[0]


def test_permanent_failure_is_reported_once_not_once_per_request():
    # 3,736 webhooks arrived on 2026-08-10 alone. One report, not 3,736.
    slack = MagicMock()
    with _with_firestore_raising(MISSING_DB), \
         patch.object(main, "_alert_slack", slack), \
         patch.object(main.log, "error") as log_error:
        for i in range(200):
            main._webhook_dedup(f"perm-{i}")

    assert slack._post.call_count == 1
    assert log_error.call_count == 1


@pytest.mark.parametrize("msg", [
    "403 Permission denied on resource project x",
    "Cloud Firestore API has not been used in project x before or it is disabled",
])
def test_other_permanent_misconfigurations_are_classified_the_same_way(msg):
    slack = MagicMock()
    with _with_firestore_raising(RuntimeError(msg)), \
         patch.object(main, "_alert_slack", slack), \
         patch.object(main.log, "error"):
        main._webhook_dedup("503")
    slack._post.assert_called_once()


# ── transient failures stay warnings, throttled ──────────────────────────── #

def test_transient_error_warns_but_does_not_page():
    slack = MagicMock()
    with _with_firestore_raising(RuntimeError("503 connection reset by peer")), \
         patch.object(main, "_alert_slack", slack), \
         patch.object(main.log, "warning") as log_warning, \
         patch.object(main.log, "error") as log_error:
        main._webhook_dedup("504")

    slack._post.assert_not_called()   # a blip must not cry wolf
    log_error.assert_not_called()
    log_warning.assert_called_once()


def test_transient_warnings_are_throttled_to_one_per_five_minutes():
    slack = MagicMock()
    with _with_firestore_raising(RuntimeError("503 connection reset")), \
         patch.object(main, "_alert_slack", slack), \
         patch.object(main.log, "warning") as log_warning:
        for i in range(100):
            main._webhook_dedup(f"trans-{i}")
    assert log_warning.call_count == 1

    # …and comes back once the window has passed.
    main._fs_transient_reported_at = 0.0
    with _with_firestore_raising(RuntimeError("503 connection reset")), \
         patch.object(main, "_alert_slack", slack), \
         patch.object(main.log, "warning") as log_warning:
        main._webhook_dedup("trans-later")
    assert log_warning.call_count == 1


def test_a_slack_outage_cannot_break_dedup():
    slack = MagicMock()
    slack._post.side_effect = RuntimeError("Slack down")
    with _with_firestore_raising(MISSING_DB), \
         patch.object(main, "_alert_slack", slack), \
         patch.object(main.log, "error"):
        assert main._webhook_dedup("505") is False   # must not raise


# ── the healthy path is untouched ────────────────────────────────────────── #

def test_healthy_firestore_claims_the_ticket_and_reports_nothing():
    db = MagicMock()
    slack = MagicMock()
    with patch.object(main, "_get_firestore_db", return_value=db), \
         patch.object(main, "_alert_slack", slack), \
         patch.object(main.log, "error") as log_error:
        assert main._webhook_dedup("506") is False
    db.collection.return_value.document.return_value.create.assert_called_once()
    slack._post.assert_not_called()
    log_error.assert_not_called()


def test_second_instance_loses_the_race_and_is_told_it_is_a_duplicate():
    # The whole point of Layer 2: another instance already claimed this ticket.
    db = MagicMock()
    doc = db.collection.return_value.document.return_value
    doc.create.side_effect = RuntimeError("409 ALREADY_EXISTS: entity already exists")
    doc.get.return_value = MagicMock(exists=True, to_dict=lambda: {})
    slack = MagicMock()
    with patch.object(main, "_get_firestore_db", return_value=db), \
         patch.object(main, "_alert_slack", slack):
        assert main._webhook_dedup("507") is True
    slack._post.assert_not_called()   # not an outage — dedup working as designed
