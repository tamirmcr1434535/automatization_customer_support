"""Unit tests for refund_ocr — payment-screenshot amount extraction (AN-192).

The model call is mocked; these verify parsing, fail-closed behavior, the enable
toggle, and media-type filtering. No network, no real Claude call.
"""

import os
from types import SimpleNamespace
from unittest.mock import patch

import refund_ocr as ocr


def _resp(text):
    return SimpleNamespace(content=[SimpleNamespace(type="text", text=text)])


PNG = (b"\x89PNG\r\n\x1a\n_fake_", "image/png")


def test_extracts_amount_from_json():
    with patch.object(ocr._client.messages, "create",
                      return_value=_resp('{"amount":"5,490","currency":"¥","card_last4":"7905","txn_id":"85E27127"}')):
        out = ocr.extract_amount_from_images([PNG])
    assert out == {"amount": "5,490", "currency": "¥", "card_last4": "7905", "txn_id": "85E27127"}


def test_json_embedded_in_prose_still_parses():
    with patch.object(ocr._client.messages, "create",
                      return_value=_resp('Here you go:\n{"amount":"9.99","currency":"$"}\nHope that helps')):
        out = ocr.extract_amount_from_images([PNG])
    assert out["amount"] == "9.99" and out["currency"] == "$" and out["card_last4"] is None


def test_null_amount_returns_none():
    with patch.object(ocr._client.messages, "create",
                      return_value=_resp('{"amount":null,"currency":"¥"}')):
        assert ocr.extract_amount_from_images([PNG]) is None


def test_unparseable_response_returns_none():
    with patch.object(ocr._client.messages, "create", return_value=_resp("sorry, I can't read this")):
        assert ocr.extract_amount_from_images([PNG]) is None


def test_api_exception_fails_closed():
    with patch.object(ocr._client.messages, "create", side_effect=RuntimeError("boom")):
        assert ocr.extract_amount_from_images([PNG]) is None


def test_no_images_returns_none():
    assert ocr.extract_amount_from_images([]) is None


def test_non_image_media_filtered_out():
    # A PDF attachment must never be sent; with no valid images the call is skipped.
    with patch.object(ocr._client.messages, "create") as m:
        assert ocr.extract_amount_from_images([(b"%PDF-1.4", "application/pdf")]) is None
        m.assert_not_called()


def test_disabled_toggle_returns_none():
    with patch.dict(os.environ, {"REFUND_OCR_ENABLED": "false"}):
        with patch.object(ocr._client.messages, "create") as m:
            assert ocr.extract_amount_from_images([PNG]) is None
            m.assert_not_called()


def test_caps_to_max_images():
    imgs = [PNG] * 10
    captured = {}
    def fake_create(**kw):
        captured["n_images"] = sum(1 for b in kw["messages"][0]["content"] if b.get("type") == "image")
        return _resp('{"amount":"100","currency":"$"}')
    with patch.object(ocr._client.messages, "create", side_effect=fake_create):
        ocr.extract_amount_from_images(imgs, max_images=2)
    assert captured["n_images"] == 2
