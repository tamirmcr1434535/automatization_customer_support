"""Unit tests for the AN-192 refund reply templates (reply_generator).
EN language → _translate is a no-op, so no Claude call is made."""
import os
import importlib.util

# test_main_flow installs a MagicMock for `reply_generator` in sys.modules; load the
# REAL module from source under a unique name so this test is order-independent.
_spec = importlib.util.spec_from_file_location(
    "reply_generator_under_test",
    os.path.join(os.path.dirname(__file__), "..", "reply_generator.py"),
)
rg = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(rg)


def test_report_template_mentions_non_refundable():
    out = rg.generate_refund_reply(
        "REPORT_NOT_REFUNDABLE_PER_TOS", "EN",
        {"brand": "IQ Booster", "report_price": "¥1,990", "currency": "JPY"})
    assert out and "not eligible for a refund" in out
    assert "¥1,990" in out and "IQ Booster" in out


def test_outside_window_template_states_window():
    out = rg.generate_refund_reply(
        "OUTSIDE_REFUND_WINDOW", "EN",
        {"brand": "IQ Booster", "window_days": 8, "renewal_price": "¥5,490", "currency": "JPY"})
    assert out and "8-day refund window" in out
    assert "not eligible for a refund" in out


def test_approved_template_states_processed():
    out = rg.generate_refund_reply(
        "WOULD_BE_REFUNDED", "EN",
        {"brand": "IQ Booster", "refund_amount": "5490", "currency": "JPY"})
    assert out and "approved and processed" in out
    assert "5490 JPY" in out and "10 business days" in out


def test_unhandled_reason_returns_none():
    assert rg.generate_refund_reply("AMBIGUOUS_FLOW", "EN", {}) is None
    assert rg.generate_refund_reply("NOT_FOUND_IN_NEXUS", "EN", {}) is None


def test_autoreply_codes_set():
    assert rg.REFUND_AUTOREPLY_CODES == {
        "WOULD_BE_REFUNDED", "REPORT_NOT_REFUNDABLE_PER_TOS", "OUTSIDE_REFUND_WINDOW"}
