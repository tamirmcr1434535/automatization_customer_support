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


def test_generic_denied_is_window_based():
    # Canonical Generic Refund Denied — window-based wording, no "Report per ToS".
    out = rg.generate_refund_reply(
        "OUTSIDE_REFUND_WINDOW", "EN",
        {"refund_window_days": 8, "charges_list": "- 5490 JPY (charged 2026-06-03)"})
    assert out and "do not qualify for a refund" in out
    assert "outside the applicable refund window (8 days" in out
    assert "5490 JPY (charged 2026-06-03)" in out
    assert "Subscription Policies" in out
    assert "Report" not in out and "Terms of Use" not in out  # no ToS-report claim


def test_generic_approved_states_processed_and_window():
    out = rg.generate_refund_reply(
        "WOULD_BE_REFUNDED", "EN",
        {"charge_amount": "5490 JPY", "charge_date": "2026-07-25", "refund_window_days": 8})
    assert out and "approved and processed" in out
    assert "5490 JPY" in out and "charge dated 2026-07-25" in out
    assert "within our applicable refund window (8 days" in out
    assert "10 business days" in out


def test_report_no_longer_auto_answered():
    # Anna: no canonical "report per ToS" template → REPORT escalates, no draft.
    assert rg.generate_refund_reply("REPORT_NOT_REFUNDABLE_PER_TOS", "EN", {}) is None


def test_unhandled_reason_returns_none():
    assert rg.generate_refund_reply("AMBIGUOUS_FLOW", "EN", {}) is None
    assert rg.generate_refund_reply("NOT_FOUND_IN_NEXUS", "EN", {}) is None


def test_autoreply_codes_set():
    assert rg.REFUND_AUTOREPLY_CODES == {"WOULD_BE_REFUNDED", "OUTSIDE_REFUND_WINDOW"}


# ── "Explained" variant (ticket #169403, Anna 2026-07-29) ────────────────── #

def test_explained_variant_only_when_explain_charge_set():
    # Without explain_charge → short template (no explanation section).
    plain = rg.generate_refund_reply(
        "OUTSIDE_REFUND_WINDOW", "EN",
        {"refund_window_days": 14, "charges_list": "- 29.99 EUR (charged 2026-06-03)"})
    assert "Explanation of the charge:" not in plain
    # With explain_charge → explained template with the dedicated section.
    explained = rg.generate_refund_reply(
        "OUTSIDE_REFUND_WINDOW", "EN",
        {"refund_window_days": 14, "charges_list": "- 29.99 EUR (charged 2026-06-03)",
         "explain_charge": True, "brand": "iqpro"})
    assert "Explanation of the charge:" in explained
    assert "Outcome of your request:" in explained
    assert "automatically converted to a paid subscription" in explained


def test_explained_plan_name_varies_by_brand():
    data = {"charge_amount": "29.99 EUR", "charge_date": "2026-07-25",
            "refund_window_days": 14, "explain_charge": True}
    iqpro = rg.generate_refund_reply("WOULD_BE_REFUNDED", "EN", {**data, "brand": "iqpro"})
    types16 = rg.generate_refund_reply("WOULD_BE_REFUNDED", "EN", {**data, "brand": "16types"})
    assert "IQ Booster brain training plan" in iqpro
    assert "16 Types Growth Plan" not in iqpro
    assert "16 Types Growth Plan" in types16
    assert "IQ Booster brain training plan" not in types16


def test_explained_has_no_markdown_brackets():
    # Links must render as "Terms and Conditions: URL" (plain text), no [ ]( ).
    for code in ("WOULD_BE_REFUNDED", "OUTSIDE_REFUND_WINDOW"):
        out = rg.generate_refund_reply(
            code, "EN",
            {"charge_amount": "29.99 EUR", "charge_date": "2026-07-25",
             "refund_window_days": 14, "charges_list": "- 29.99 EUR (charged 2026-06-03)",
             "explain_charge": True, "brand": "iqpro",
             "terms_url": "https://x/terms.pdf", "subscription_url": "https://x/sub.pdf"})
        assert "](" not in out and "[" not in out and "]" not in out
        assert "Terms and Conditions: https://x/terms.pdf" in out
