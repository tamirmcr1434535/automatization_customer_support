"""
Nexus API client (apinexus.cellon.ai)
======================================
Read-only subscription search used as a drop-in replacement for the
slow WooCommerce ?customer={id} lookup, which was intermittently
returning 504 Gateway Timeout (cf. BUG-2*, ticket #146875 et al.).

The cancellation PUT itself still goes through WooCommerce — Nexus
returns the subscription_id and the bot then calls
`wc._cancel_sub_by_id(sub_id)` directly. This keeps the WC subscription
state authoritative for audit / reporting and limits the blast radius
of the swap to the lookup phase only.

Activated by env var `USE_NEXUS_FOR_LOOKUP=true`. When the flag is
off (default) the client is never instantiated and the bot behaves
exactly as before.
"""
import logging
import os

import requests

log = logging.getLogger("nexus")


class NexusClient:
    """Thin wrapper around the `/api/v1/customer/search-subscription` endpoint.

    Read-only. Returns the raw `data` dict from a successful response,
    or None on 404 / network error / 5xx / malformed body — the caller
    treats None as "not found" and falls back to its own escalation
    path (no silent partial state).
    """

    def __init__(
        self,
        base_url: str,
        api_token: str,
        *,
        x_host: str = "",
        timeout: int = 30,
    ):
        """
        `x_host` is OPTIONAL. The 23/06/26 build of search-subscription
        ignores it — empirical test with no header, empty string,
        `iqbooster`, `all`, and `16_persons` all returned the identical
        payload. The endpoint searches by email across every brand
        Nexus has migrated. We keep the parameter as a defensive escape
        hatch in case the API starts enforcing it later; default is to
        not send the header at all.
        """
        self.base = base_url.rstrip("/")
        self.timeout = timeout
        self.headers = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json",
            "User-Agent": "automatization-customer-support",
        }
        if x_host:
            self.headers["x-host"] = x_host

    def search_subscription(self, email: str) -> dict | None:
        """Look up subscription state for `email`.

        Returns the API's `data` dict on success (sub_id, source,
        order_count, subscription_start, renewal_subscriptions,
        was_already_cancelled, status_before, trial flags, etc.).

        Returns None for:
          - HTTP 404 (subscription not found — caller escalates)
          - HTTP 5xx / network error (caller falls back to WC path)
          - meta.success == false (defensive: the API was reported to
            return meta.success=true on bad input early in dev; we
            re-check it explicitly even though that contract is now
            fixed)
          - malformed JSON body

        Never raises — Nexus is the lookup layer; the bot must always
        be able to escalate to a human if the lookup is unreliable.
        """
        if not email:
            return None

        url = f"{self.base}/api/v1/customer/search-subscription"
        try:
            resp = requests.post(
                url,
                json={"email": email},
                headers=self.headers,
                timeout=self.timeout,
            )
        except requests.exceptions.Timeout:
            log.warning(
                f"nexus.search_subscription: timeout ({self.timeout}s) "
                f"for {email}"
            )
            return None
        except requests.exceptions.RequestException as e:
            log.warning(
                f"nexus.search_subscription: network error for {email}: {e}"
            )
            return None

        if resp.status_code == 404:
            log.info(f"nexus.search_subscription: {email} → 404 not_found")
            return None

        if not resp.ok:
            log.warning(
                f"nexus.search_subscription: {email} → HTTP "
                f"{resp.status_code}: {resp.text[:200]}"
            )
            return None

        try:
            body = resp.json()
        except ValueError:
            log.warning(
                f"nexus.search_subscription: {email} returned non-JSON: "
                f"{resp.text[:200]}"
            )
            return None

        meta = body.get("meta") or {}
        if not meta.get("success"):
            log.info(
                f"nexus.search_subscription: {email} meta.success=false "
                f"({meta.get('message')!r})"
            )
            return None

        data = body.get("data")
        if not isinstance(data, dict) or not data.get("subscription_id"):
            log.warning(
                f"nexus.search_subscription: {email} returned success "
                "but no subscription_id in data"
            )
            return None

        return data


# Module-level factory — main.py wires this up at startup ONLY when the
# `USE_NEXUS_FOR_LOOKUP` flag is on, to avoid loading config in the
# common (flag-off) path.
def build_from_env() -> NexusClient | None:
    """Build a NexusClient from env vars, or return None if the API
    token is missing.

    Env vars (read at startup):
      NEXUS_API_TOKEN  REQUIRED (load from Secret Manager binding)
      NEXUS_BASE_URL   optional, default: https://apinexus.cellon.ai
      NEXUS_X_HOST     optional, default: "" (header omitted) —
                       only set this if Nexus starts enforcing brand
                       scoping; the current build ignores the header.
    """
    token = os.getenv("NEXUS_API_TOKEN", "").strip()
    if not token:
        log.warning("NEXUS_API_TOKEN not configured — Nexus client disabled")
        return None
    return NexusClient(
        base_url=os.getenv("NEXUS_BASE_URL", "https://apinexus.cellon.ai"),
        api_token=token,
        x_host=os.getenv("NEXUS_X_HOST", "").strip(),
    )
