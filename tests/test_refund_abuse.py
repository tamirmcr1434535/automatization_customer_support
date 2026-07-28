"""Unit tests for refund_abuse — per-brand hourly rate + daily cap + per-email velocity, fail-closed."""
from unittest.mock import patch
import refund_abuse


class _Job:
    def __init__(self, rows): self._rows = rows
    def result(self): return self._rows

class _Client:
    """Fake BQ client. Returns one row of counts, or raises if boom=True."""
    def __init__(self, brand_hour=0, brand_today=0, email_recent=0, boom=False):
        self._row = [{"brand_hour": brand_hour, "brand_today": brand_today, "email_recent": email_recent}]
        self._boom = boom
    def query(self, q, job_config=None):
        if self._boom:
            raise RuntimeError("bq down")
        return _Job(self._row)


def _cfg(**kw):
    base = {"MAX_PER_HOUR_PER_BRAND": 5, "MAX_PER_DAY_PER_BRAND": 30, "MAX_PER_EMAIL": 2}
    base.update(kw)
    return [patch.object(refund_abuse, "is_enabled", return_value=True)] + \
           [patch.object(refund_abuse, k, v) for k, v in base.items()]


def _run(client, **thresholds):
    import contextlib
    with contextlib.ExitStack() as es:
        for p in _cfg(**thresholds):
            es.enter_context(p)
        return refund_abuse.check("iqpro", "a@e.com", client=client)


def test_ok_when_under_all_limits():
    ok, why = _run(_Client(brand_hour=2, brand_today=5, email_recent=1))
    assert ok is True and why == ""


def test_blocks_on_brand_hourly_rate():
    ok, why = _run(_Client(brand_hour=5, brand_today=5, email_recent=0))
    assert ok is False and why.startswith("brand_rate")


def test_blocks_on_brand_daily_cap():
    ok, why = _run(_Client(brand_hour=1, brand_today=30, email_recent=0))
    assert ok is False and why.startswith("brand_daily_cap")


def test_blocks_on_email_velocity_2_per_day():
    # 2 already refunded in the window → 3rd blocked (cap = 2)
    ok, why = _run(_Client(brand_hour=1, brand_today=3, email_recent=2))
    assert ok is False and why.startswith("email_velocity")


def test_email_second_refund_allowed():
    # 1 prior refund → a 2nd is still allowed (cap = 2)
    ok, why = _run(_Client(brand_hour=1, brand_today=3, email_recent=1))
    assert ok is True and why == ""


def test_fail_closed_on_bq_error():
    with patch.object(refund_abuse, "is_enabled", return_value=True):
        ok, why = refund_abuse.check("iqpro", "a@e.com", client=_Client(boom=True))
    assert ok is False and why == "abuse_guard_error"


def test_disabled_passes():
    with patch.object(refund_abuse, "is_enabled", return_value=False):
        ok, why = refund_abuse.check("iqpro", "a@e.com",
                                     client=_Client(brand_hour=999, brand_today=999, email_recent=999))
    assert ok is True and why == ""
