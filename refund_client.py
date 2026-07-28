"""
Refund client — the money-operation boundary (AN-192)
=====================================================
The single, auditable place where a refund can ever be executed, plus the
read-only charge-detail lookup used to guard it.

TWO calls to the Cellon Nexus refund API (base/x-host/token from env):

  * get_charge_detail(charge_id)  — READ ONLY. Returns the charge's
    {disputed, refundable, refunded_at, ...}. Always safe; used to block
    refunds on disputed / non-refundable charges. Works whenever the API is
    configured, regardless of `enabled`.

  * create_refund(charge_id)      — the money move. Executes ONLY when BOTH
    `enabled` (mirrors REFUNDS_ENABLED) is true AND the API is configured.
    Otherwise it is a NO-OP that returns executed=False and moves nothing.

SAFETY: with REFUNDS_ENABLED=false (prod default) `enabled` is false, so
create_refund never posts to the refund endpoint. The refund API itself also
rejects disputed charges (`dispute_open`) and double refunds (`already_refunded`)
— a second, server-side guard. "Can this move money?" is answerable here: only
when enabled AND configured AND the charge passes both guards.
"""

import os
import logging

import requests

log = logging.getLogger("refund_client")


class RefundClient:
    """Charge-detail (read) + refund (gated) against the Nexus refund API."""

    def __init__(self, provider: str = "nexus", enabled: bool = False):
        self.provider = provider
        self.enabled = enabled  # mirrors REFUNDS_ENABLED — gates create_refund only
        # PROD refund/charge-detail API (same host as Nexus search-subscription).
        # base defaults to prod; the TOKEN is intentionally NOT defaulted — until it
        # is set the client is "not configured", so the guard is skipped and nothing
        # is called. Execution additionally needs `enabled` (REFUNDS_ENABLED).
        self.base = os.getenv("REFUND_API_BASE_URL", "https://apinexus.cellon.ai").rstrip("/")
        self.token = os.getenv("REFUND_API_TOKEN", "").strip()
        self.x_host = os.getenv("REFUND_API_X_HOST", "").strip()
        self.timeout = int(os.getenv("REFUND_API_TIMEOUT", "30"))

    def is_configured(self) -> bool:
        """True when the charge-detail / refund API is wired (base + token)."""
        return bool(self.base and self.token)

    def _headers(self, x_host: str | None) -> dict:
        h = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        host = (x_host or self.x_host).strip()
        if host:
            h["x-host"] = host
        return h

    def get_charge_detail(self, charge_id: str, *, x_host: str | None = None) -> dict | None:
        """Read-only charge lookup for the dispute guard. Returns the chargeData
        dict ({disputed, refundable, refunded_at, amount, ...}) or None on any
        failure / not-configured. NEVER moves money; safe to call always."""
        if not charge_id or not self.is_configured():
            return None
        try:
            r = requests.post(
                f"{self.base}/api/v1/customer/charge-detail",
                headers=self._headers(x_host),
                json={"charge_id": charge_id},
                timeout=self.timeout,
            )
        except requests.exceptions.RequestException as e:
            log.warning("charge-detail network error for %s: %s", charge_id, e)
            return None
        if r.status_code == 404:
            return None
        if not r.ok:
            log.warning("charge-detail %s → HTTP %s: %s", charge_id, r.status_code, r.text[:200])
            return None
        try:
            data = (r.json().get("data") or {}).get("chargeData")
        except ValueError:
            return None
        return data if isinstance(data, dict) else None

    def create_refund(self, *, charge_id: str, x_host: str | None = None,
                      idempotency_key: str = "", reason: str = "") -> dict:
        """Execute a refund for `charge_id` — ONLY when enabled AND configured.
        Otherwise a no-op ({executed:False}). Returns the API's data dict
        ({status, refunded_amount, ...}) on execution."""
        if not (self.enabled and self.is_configured()):
            log.info("[would_refund] charge=%s — NOT executed (enabled=%s configured=%s)",
                     charge_id, self.enabled, self.is_configured())
            return {"status": "would_refund", "executed": False,
                    "provider": self.provider, "charge_id": charge_id, "reason": reason}
        try:
            r = requests.post(
                f"{self.base}/api/v1/customer/refund",
                headers=self._headers(x_host),
                json={"charge_id": charge_id},
                timeout=self.timeout,
            )
            body = r.json()
        except (requests.exceptions.RequestException, ValueError) as e:
            log.warning("refund error for %s: %s", charge_id, e)
            return {"status": "error", "executed": False, "charge_id": charge_id, "message": str(e)}
        data = body.get("data") or {}
        data["executed"] = (data.get("status") == "refunded")
        log.info("[refund] charge=%s → status=%s refunded_amount=%s",
                 charge_id, data.get("status"), data.get("refunded_amount"))
        return data
