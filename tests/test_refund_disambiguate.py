"""Unit tests for refund_disambiguate — LLM picks the disputed charge (AN-192).
The model call is mocked; verify parsing, id validation, abstain, fail-closed."""

import os
from types import SimpleNamespace
from unittest.mock import patch

import refund_disambiguate as dis


def _resp(text):
    return SimpleNamespace(content=[SimpleNamespace(type="text", text=text)])


CHARGES = [
    {"charge_id": "ch_sub", "amount": 5490, "currency": "JPY", "type": "subscription",
     "date": "2026-07-22", "refundable": True},
    {"charge_id": "ch_fee", "amount": 199, "currency": "JPY", "type": "first_sale",
     "date": "2026-07-15", "refundable": True},
]


def test_picks_valid_charge():
    with patch.object(dis._client.messages, "create", return_value=_resp('{"charge_id":"ch_sub"}')):
        assert dis.pick_target_charge_id("refund all except the first", CHARGES) == "ch_sub"


def test_abstain_null_returns_none():
    with patch.object(dis._client.messages, "create", return_value=_resp('{"charge_id":null}')):
        assert dis.pick_target_charge_id("what are these charges?", CHARGES) is None


def test_invalid_id_rejected():
    # Model hallucinated an id not on the account → reject (never invent a charge).
    with patch.object(dis._client.messages, "create", return_value=_resp('{"charge_id":"ch_zzz"}')):
        assert dis.pick_target_charge_id("refund", CHARGES) is None


def test_prose_wrapped_json_parses():
    with patch.object(dis._client.messages, "create",
                      return_value=_resp('Sure: {"charge_id":"ch_sub"} — that one')):
        assert dis.pick_target_charge_id("refund", CHARGES) == "ch_sub"


def test_single_charge_no_llm_call():
    with patch.object(dis._client.messages, "create") as m:
        assert dis.pick_target_charge_id("refund", CHARGES[:1]) is None
        m.assert_not_called()


def test_disabled_returns_none():
    with patch.dict(os.environ, {"REFUND_DISAMBIG_ENABLED": "false"}):
        with patch.object(dis._client.messages, "create") as m:
            assert dis.pick_target_charge_id("refund", CHARGES) is None
            m.assert_not_called()


def test_exception_fails_closed():
    with patch.object(dis._client.messages, "create", side_effect=RuntimeError("boom")):
        assert dis.pick_target_charge_id("refund", CHARGES) is None


def test_empty_inputs():
    assert dis.pick_target_charge_id("", CHARGES) is None
    assert dis.pick_target_charge_id("refund", []) is None
