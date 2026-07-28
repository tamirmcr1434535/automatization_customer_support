"""Unit tests for refund_abuse — per-brand daily cap + per-email velocity, fail-closed."""
from unittest.mock import patch
import refund_abuse


class _Job:
    def __init__(self, rows): self._rows = rows
    def result(self): return self._rows

class _Client:
    """Fake BQ client. Returns one row of counts, or raises if boom=True."""
    def __init__(self, brand_today=0, email_recent=0, boom=False):
        self._row = [{"brand_today": brand_today, "email_recent": email_recent}]
        self._boom = boom
    def query(self, q, job_config=None):
        if self._boom:
            raise RuntimeError("bq down")
        return _Job(self._row)


def test_ok_when_under_limits():
    with patch.object(refund_abuse, "is_enabled", return_value=True), \
         patch.object(refund_abuse, "MAX_PER_DAY_PER_BRAND", 30), \
         patch.object(refund_abuse, "MAX_PER_EMAIL", 1):
        ok, why = refund_abuse.check("iqpro", "a@e.com", client=_Client(brand_today=5, email_recent=0))
    assert ok is True and why == ""


def test_blocks_on_brand_daily_cap():
    with patch.object(refund_abuse, "is_enabled", return_value=True), \
         patch.object(refund_abuse, "MAX_PER_DAY_PER_BRAND", 30), \
         patch.object(refund_abuse, "MAX_PER_EMAIL", 1):
        ok, why = refund_abuse.check("iqpro", "a@e.com", client=_Client(brand_today=30, email_recent=0))
    assert ok is False and why.startswith("brand_daily_cap")


def test_blocks_on_email_velocity():
    with patch.object(refund_abuse, "is_enabled", return_value=True), \
         patch.object(refund_abuse, "MAX_PER_DAY_PER_BRAND", 30), \
         patch.object(refund_abuse, "MAX_PER_EMAIL", 1):
        # customer already refunded once in the window → second one blocked
        ok, why = refund_abuse.check("iqpro", "a@e.com", client=_Client(brand_today=1, email_recent=1))
    assert ok is False and why.startswith("email_velocity")


def test_fail_closed_on_bq_error():
    with patch.object(refund_abuse, "is_enabled", return_value=True):
        ok, why = refund_abuse.check("iqpro", "a@e.com", client=_Client(boom=True))
    assert ok is False and why == "abuse_guard_error"


def test_disabled_passes():
    with patch.object(refund_abuse, "is_enabled", return_value=False):
        ok, why = refund_abuse.check("iqpro", "a@e.com", client=_Client(brand_today=9999, email_recent=9999))
    assert ok is True and why == ""
