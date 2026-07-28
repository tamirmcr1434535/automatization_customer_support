"""Unit tests for refund_abuse — hourly burst rate + ADAPTIVE daily cap (learned from
history) + per-email velocity, fail-closed."""
import contextlib
from unittest.mock import patch
import refund_abuse


class _Job:
    def __init__(self, rows): self._rows = rows
    def result(self): return self._rows

class _Client:
    """Fake BQ client returning one counts row, or raising if boom=True."""
    def __init__(self, brand_hour=0, brand_today=0, email_recent=0,
                 wb_baseline_total=0, wb_baseline_days=0, boom=False):
        self._row = [{"brand_hour": brand_hour, "brand_today": brand_today,
                      "email_recent": email_recent,
                      "wb_baseline_total": wb_baseline_total,
                      "wb_baseline_days": wb_baseline_days}]
        self._boom = boom
    def query(self, q, job_config=None):
        if self._boom:
            raise RuntimeError("bq down")
        return _Job(self._row)


def _run(client, **thresholds):
    base = {"MAX_PER_HOUR_PER_BRAND": 5, "MAX_PER_EMAIL": 2,
            "DAILY_FACTOR": 3.0, "DAILY_FLOOR": 5, "DAILY_HARD_MAX": 100}
    base.update(thresholds)
    with contextlib.ExitStack() as es:
        es.enter_context(patch.object(refund_abuse, "is_enabled", return_value=True))
        for k, v in base.items():
            es.enter_context(patch.object(refund_abuse, k, v))
        return refund_abuse.check("iqpro", "a@e.com", client=client)


# ── adaptive_daily_cap unit ─────────────────────────────────────────────── #

def test_adaptive_cap_math():
    with patch.object(refund_abuse, "DAILY_FACTOR", 3.0), \
         patch.object(refund_abuse, "DAILY_FLOOR", 5), \
         patch.object(refund_abuse, "DAILY_HARD_MAX", 100):
        assert refund_abuse.adaptive_daily_cap(0, 0) == 5        # new brand → floor
        assert refund_abuse.adaptive_daily_cap(28, 14) == 6      # 2/day × 3 = 6
        assert refund_abuse.adaptive_daily_cap(1000, 10) == 100  # clamped to hard max


# ── check() ─────────────────────────────────────────────────────────────── #

def test_ok_when_under_all_limits():
    # baseline 2/day → cap 6; today=3 is under
    ok, why = _run(_Client(brand_hour=2, brand_today=3, email_recent=1,
                           wb_baseline_total=28, wb_baseline_days=14))
    assert ok is True and why == ""


def test_blocks_on_brand_hourly_rate():
    ok, why = _run(_Client(brand_hour=5, brand_today=1, email_recent=0,
                           wb_baseline_total=28, wb_baseline_days=14))
    assert ok is False and why.startswith("brand_rate")


def test_adaptive_daily_cap_blocks_small_brand():
    # new/tiny brand (no baseline) → cap = floor 5; today already 5 → blocked
    ok, why = _run(_Client(brand_hour=1, brand_today=5, email_recent=0,
                           wb_baseline_total=0, wb_baseline_days=0))
    assert ok is False and why.startswith("brand_daily_adaptive")


def test_adaptive_daily_cap_scales_with_history():
    # busy brand: baseline 10/day → cap 30; today=20 still allowed
    ok, why = _run(_Client(brand_hour=1, brand_today=20, email_recent=0,
                           wb_baseline_total=140, wb_baseline_days=14))
    assert ok is True and why == ""


def test_blocks_on_email_velocity_2_per_day():
    ok, why = _run(_Client(brand_hour=1, brand_today=1, email_recent=2,
                           wb_baseline_total=140, wb_baseline_days=14))
    assert ok is False and why.startswith("email_velocity")


def test_email_second_refund_allowed():
    ok, why = _run(_Client(brand_hour=1, brand_today=1, email_recent=1,
                           wb_baseline_total=140, wb_baseline_days=14))
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
