"""Per-brand + per-language legal-link lookup (from the WWIQTEST Legal sheet)."""
import refund_reply_links as L


def test_exact_brand_language():
    terms, sub = L.links_for("wwiqtest", "JP")
    assert "jap.wwiqtest.com" in terms and "subscription-policy" in sub


def test_brand_specific_domain():
    terms, _ = L.links_for("iqbooster", "DE")
    assert "iqbooster.org" in terms
    terms, _ = L.links_for("16types", "EN")
    assert "16types.ai" in terms


def test_language_fallback_to_brand_en():
    # A brand that lacks the exact language falls back to that brand's EN.
    terms, _ = L.links_for("16personas", "DE")  # 16personas has only EN/JP
    assert "16persons.com" in terms


def test_unknown_brand_falls_back_to_wwiqtest():
    terms, sub = L.links_for("unknown", "JP")
    assert "jap.wwiqtest.com" in terms  # wwiqtest + JP
    terms, sub = L.links_for("unknown", "XX")
    assert "wwiqtest.com" in terms       # wwiqtest + EN default
