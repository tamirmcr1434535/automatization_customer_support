"""
Refund client — the money-operation boundary (STUB ONLY) (AN-192)
==================================================================
This module is the single, auditable place where a real refund would ever be
executed. In THIS iteration it has **no live path at all**:

  * There is NO call to `stripe.Refund.create`.
  * There is NO POST to any Nexus refund endpoint.
  * `create_refund(...)` always returns a `would_refund` result with
    `executed=False` and never moves money.

"Can this move money?" must be answerable by reading this one small file.
The answer, on this branch, is: no.

Future execution (separate, gated iteration — see docs/nexus_refund_api_spec.md)
will route through the Nexus refund endpoint and MUST additionally require the
deferred safety checks (Stripe/PayPal dispute check, amount cap, two-source
verify). The insertion point is marked below. It is intentionally left as a
no-op so that even `REFUNDS_ENABLED=true` cannot move money today.
"""

import logging

log = logging.getLogger("refund_client")


class RefundClient:
    """Provider-agnostic refund boundary. Execution target (future) = Nexus."""

    def __init__(self, provider: str = "nexus", enabled: bool = False):
        # `enabled` mirrors REFUNDS_ENABLED but is inert here — there is no live
        # branch to enable. Kept so the future wiring reads naturally.
        self.provider = provider
        self.enabled = enabled

    def create_refund(
        self,
        *,
        idempotency_key: str,
        order_id: str | int | None,
        amount,
        currency: str,
        reason: str = "",
    ) -> dict:
        """Would-be refund. NEVER executes — always returns `would_refund`.

        This deliberately has no live branch. Do not add one here without the
        deferred safety prerequisites (dispute/PayPal/amount-cap) and an explicit
        product decision.
        """
        log.info(
            "[would_refund] provider=%s order=%s amount=%s %s (idem=%s) — NOT executed",
            self.provider, order_id, amount, currency, idempotency_key,
        )

        # ── FUTURE live path goes here, gated behind REFUNDS_ENABLED *and* the
        # deferred safety checks. Intentionally absent on this branch. ──
        # if self.enabled and _all_deferred_safety_checks_pass(...):
        #     return _execute_via_nexus_refund_endpoint(...)

        return {
            "status": "would_refund",
            "executed": False,
            "provider": self.provider,
            "order_id": order_id,
            "amount": str(amount) if amount is not None else None,
            "currency": currency,
            "idempotency_key": idempotency_key,
            "reason": reason,
        }
