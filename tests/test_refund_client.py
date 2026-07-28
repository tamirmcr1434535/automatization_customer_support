"""RefundClient — charge-detail (read) + refund (gated). requests is mocked."""
from unittest.mock import patch, MagicMock

import refund_client as rc_mod


def _client(enabled=False, configured=True):
    c = rc_mod.RefundClient(enabled=enabled)
    c.base = "https://dev.example" if configured else ""
    c.token = "tok" if configured else ""
    c.x_host = "16_persons"
    return c


def _resp(status, body):
    r = MagicMock()
    r.status_code = status
    r.ok = 200 <= status < 300
    r.json.return_value = body
    return r


def test_charge_detail_returns_chargedata():
    c = _client()
    body = {"data": {"chargeData": {"charge_id": "ch", "disputed": True, "refundable": False}}}
    with patch.object(rc_mod.requests, "post", return_value=_resp(200, body)):
        d = c.get_charge_detail("ch")
    assert d["disputed"] is True and d["refundable"] is False


def test_charge_detail_none_when_unconfigured():
    c = _client(configured=False)
    with patch.object(rc_mod.requests, "post") as p:
        assert c.get_charge_detail("ch") is None
        p.assert_not_called()          # no network when not configured


def test_create_refund_noop_when_disabled():
    c = _client(enabled=False)          # configured but not enabled
    with patch.object(rc_mod.requests, "post") as p:
        out = c.create_refund(charge_id="ch")
        assert out["executed"] is False and out["status"] == "would_refund"
        p.assert_not_called()          # SAFETY: money endpoint never hit


def test_create_refund_noop_when_unconfigured():
    c = _client(enabled=True, configured=False)   # enabled but no API wired
    with patch.object(rc_mod.requests, "post") as p:
        out = c.create_refund(charge_id="ch")
        assert out["executed"] is False
        p.assert_not_called()


def test_create_refund_executes_when_enabled_and_configured():
    c = _client(enabled=True)
    body = {"data": {"status": "refunded", "refunded_amount": 9.99, "charge_id": "ch"}}
    with patch.object(rc_mod.requests, "post", return_value=_resp(200, body)) as p:
        out = c.create_refund(charge_id="ch")
        assert out["executed"] is True and out["status"] == "refunded"
        p.assert_called_once()


def test_create_refund_dispute_rejected_not_executed():
    c = _client(enabled=True)
    body = {"data": {"status": "rejected", "reason": "dispute_open", "refunded_amount": 0}}
    with patch.object(rc_mod.requests, "post", return_value=_resp(409, body)):
        out = c.create_refund(charge_id="ch")
        assert out["executed"] is False and out["status"] == "rejected"
