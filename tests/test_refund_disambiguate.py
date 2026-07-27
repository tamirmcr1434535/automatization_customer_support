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


def test_non_refundable_charges_excluded_from_pick():
    # Only refundable charges are offered: a most-recent FAILED renewal must not be
    # pickable (it would be dropped by the engine → spurious AMBIGUOUS). (Live #168524.)
    charges = [
        {"charge_id": "ch_fail", "amount": 5490, "currency": "JPY", "type": "renewal",
         "status": "failed", "date": "2026-07-22", "refundable": False},
        {"charge_id": "ch_ok", "amount": 5490, "currency": "JPY", "type": "renewal",
         "status": "success", "date": "2026-06-03", "refundable": True},
        {"charge_id": "ch_fs", "amount": 199, "currency": "JPY", "type": "first_sale",
         "status": "success", "date": "2026-04-29", "refundable": True},
    ]
    # Even if the model names the failed charge, it isn't a valid id → rejected.
    with patch.object(dis._client.messages, "create", return_value=_resp('{"charge_id":"ch_fail"}')):
        assert dis.pick_target_charge_id("unauthorized recurring charge, refund", charges) is None
    # A refundable pick still works.
    with patch.object(dis._client.messages, "create", return_value=_resp('{"charge_id":"ch_ok"}')):
        assert dis.pick_target_charge_id("unauthorized recurring charge, refund", charges) == "ch_ok"


def test_single_refundable_charge_no_llm_call():
    # After filtering to refundable, only one remains → nothing to disambiguate.
    charges = [
        {"charge_id": "ch_fail", "type": "renewal", "status": "failed", "refundable": False},
        {"charge_id": "ch_ok", "type": "renewal", "status": "success", "refundable": True},
    ]
    with patch.object(dis._client.messages, "create") as m:
        assert dis.pick_target_charge_id("refund", charges) is None
        m.assert_not_called()


def test_max_tokens_headroom():
    # Regression: at max_tokens=120 a reply that reasoned in prose before the
    # JSON got truncated pre-brace → spurious abstain (real ticket #167946).
    # Guard the budget so the closing brace is always reachable.
    with patch.object(dis._client.messages, "create",
                      return_value=_resp('{"charge_id":"ch_sub"}')) as m:
        dis.pick_target_charge_id("refund the recurring charge", CHARGES)
        assert m.call_args.kwargs["max_tokens"] >= 400
