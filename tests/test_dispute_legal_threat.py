"""
Unit tests for the explicit-legal-threat exception to the dispute→cancellation
remap (fix 2026-07-25, ticket #167445).
=============================================================================
A CHARGEBACK_THREAT / PAYPAL_DISPUTE ticket that merely asks to cancel is
re-mapped to a normal cancellation. But when the customer makes an EXPLICIT
legal / authority / chargeback threat, the remap must NOT fire — the ticket
stays a pure dispute and escalates to a human.

Heavy modules are mocked before importing main so no network calls happen.
"""
import os
import sys
from unittest.mock import MagicMock

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


# ── _contains_explicit_legal_threat ─────────────────────────────────────── #

class TestContainsExplicitLegalThreat:
    def test_jp_report_to_authorities(self):
        # The exact #167445 wording.
        assert main._contains_explicit_legal_threat("日本の当局に通報します")

    def test_jp_consumer_center(self):
        assert main._contains_explicit_legal_threat("消費者センターに相談します")

    def test_jp_lawyer_and_lawsuit(self):
        assert main._contains_explicit_legal_threat("弁護士に相談します")
        assert main._contains_explicit_legal_threat("訴えます")
        assert main._contains_explicit_legal_threat("法的措置を取ります")

    def test_en_chargeback_and_bank(self):
        assert main._contains_explicit_legal_threat("I will file a chargeback")
        assert main._contains_explicit_legal_threat("I will dispute this with my bank")
        assert main._contains_explicit_legal_threat("I'll contact my credit card company")

    def test_en_authorities_and_legal(self):
        assert main._contains_explicit_legal_threat("I will report you to the authorities")
        assert main._contains_explicit_legal_threat("I'm going to take legal action")
        assert main._contains_explicit_legal_threat("I'll get a lawyer")

    def test_case_insensitive(self):
        assert main._contains_explicit_legal_threat("CHARGEBACK NOW")

    def test_plain_cancel_is_not_a_threat(self):
        assert not main._contains_explicit_legal_threat("解約してください")
        assert not main._contains_explicit_legal_threat("Please cancel my subscription")

    def test_plain_charge_mention_is_not_a_threat(self):
        # Mentioning a charge/amount without a threat must not trip the guard.
        assert not main._contains_explicit_legal_threat("199円と1990円を引き落とされました")


# ── _dispute_is_really_cancellation ─────────────────────────────────────── #

class TestDisputeIsReallyCancellation:
    def test_remaps_when_cancel_and_no_threat(self):
        # Mild dispute wording + a genuine cancel ask → remap to cancellation.
        assert main._dispute_is_really_cancellation(
            "CHARGEBACK_THREAT", "解約したいです"
        )

    def test_no_remap_when_explicit_threat_present(self):
        # #167445: cancel word キャンセル present, but explicit "report to the
        # authorities" threat → must NOT remap (stays a dispute for a human).
        body = (
            "199円とは別に1990円を引き落とされました。"
            "1990円の方を速やかにキャンセルしなさい。"
            "さもなければ、日本の当局に通報します。"
        )
        assert not main._dispute_is_really_cancellation("CHARGEBACK_THREAT", body)

    def test_no_remap_when_no_cancel_signal(self):
        assert not main._dispute_is_really_cancellation(
            "CHARGEBACK_THREAT", "I want my money back"
        )

    def test_no_remap_for_non_dispute_intent(self):
        assert not main._dispute_is_really_cancellation(
            "TRIAL_CANCELLATION", "解約したいです"
        )

    def test_threat_in_subject_blocks_remap_via_full_text(self):
        # Cancel word in body, threat only in the subject → full_text carries
        # the threat and blocks the remap.
        body = "解約したいです"
        full_text = "I will file a chargeback -- 解約したいです"
        assert main._dispute_is_really_cancellation("CHARGEBACK_THREAT", body) is True
        assert (
            main._dispute_is_really_cancellation("CHARGEBACK_THREAT", body, full_text)
            is False
        )

    def test_paypal_dispute_with_threat_not_remapped(self):
        body = "サブスクを解約して。弁護士に相談します。"
        assert not main._dispute_is_really_cancellation("PAYPAL_DISPUTE", body)
