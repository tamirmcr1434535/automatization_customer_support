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
import time

import requests

log = logging.getLogger("nexus")


class NexusLookupError(Exception):
    """Nexus could not be queried reliably (5xx / gateway 52x / timeout /
    network error / non-404 4xx).

    Raised so callers can tell a *transient/ambiguous* failure apart from a
    clean 404 "no subscription". A 404 still returns None (the customer
    genuinely has no sub); this exception means "we don't know — do NOT
    tell the customer they have no subscription, escalate instead".
    """


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
        max_retries: int = 2,
        retry_backoff: float = 0.5,
    ):
        """
        `x_host` is OPTIONAL. The 23/06/26 build of search-subscription
        ignores it — empirical test with no header, empty string,
        `iqbooster`, `all`, and `16_persons` all returned the identical
        payload. The endpoint searches by email across every brand
        Nexus has migrated. We keep the parameter as a defensive escape
        hatch in case the API starts enforcing it later; default is to
        not send the header at all.

        `max_retries` / `retry_backoff` control retry behaviour on
        *transient* failures only (5xx incl. Cloudflare 52x, timeout,
        network error). Nexus sits behind Cloudflare and returns
        intermittent 520/522/525 origin errors that succeed on a second
        attempt; a clean 404 is never retried. Backoff is linear:
        `retry_backoff * attempt` seconds between tries.
        """
        self.base = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max(0, int(max_retries))
        self.retry_backoff = max(0.0, float(retry_backoff))
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

        Returns None ONLY for outcomes that mean "no usable subscription
        for this email" — i.e. the customer genuinely has nothing here:
          - HTTP 404 (subscription not found)
          - meta.success == false (defensive: the API was reported to
            return meta.success=true on bad input early in dev; we
            re-check it explicitly even though that contract is now
            fixed)
          - HTTP 200 but no subscription_id in `data`
          - malformed JSON body

        Raises `NexusLookupError` for *transient/ambiguous* failures
        where the bot did NOT get a definitive answer:
          - HTTP 5xx (incl. Cloudflare 520/522/525 origin errors)
          - request timeout
          - network error
          - any other non-404 4xx (401/403/422/…)
        Transient failures are retried up to `max_retries` times before
        the exception is raised. Callers that cannot tolerate a raise
        (refund eval, quick check) already wrap this call; the cancel
        path maps the exception to a "nexus_lookup_error" escalation so
        the bot never tells a paying customer they have no subscription
        just because Nexus hiccupped.
        """
        if not email:
            return None

        url = f"{self.base}/api/v1/customer/search-subscription"
        attempt = 0
        while True:
            attempt += 1
            try:
                resp = requests.post(
                    url,
                    json={"email": email},
                    headers=self.headers,
                    timeout=self.timeout,
                )
            except requests.exceptions.Timeout as e:
                if attempt <= self.max_retries:
                    log.warning(
                        f"nexus.search_subscription: timeout ({self.timeout}s) "
                        f"for {email} (attempt {attempt}) — retrying"
                    )
                    self._backoff(attempt)
                    continue
                raise NexusLookupError(
                    f"timeout ({self.timeout}s) for {email} after "
                    f"{attempt} attempt(s)"
                ) from e
            except requests.exceptions.RequestException as e:
                if attempt <= self.max_retries:
                    log.warning(
                        f"nexus.search_subscription: network error for {email} "
                        f"(attempt {attempt}): {e} — retrying"
                    )
                    self._backoff(attempt)
                    continue
                raise NexusLookupError(
                    f"network error for {email} after {attempt} attempt(s): {e}"
                ) from e

            # Clean 404 — the sub genuinely isn't here. Never retried.
            if resp.status_code == 404:
                log.info(f"nexus.search_subscription: {email} → 404 not_found")
                return None

            # 5xx (incl. Cloudflare 520/522/525) — transient, retry then raise.
            if resp.status_code >= 500:
                if attempt <= self.max_retries:
                    log.warning(
                        f"nexus.search_subscription: {email} → HTTP "
                        f"{resp.status_code} (attempt {attempt}) — retrying"
                    )
                    self._backoff(attempt)
                    continue
                raise NexusLookupError(
                    f"{email} → HTTP {resp.status_code} after {attempt} "
                    f"attempt(s): {resp.text[:200]}"
                )

            # Other non-2xx (401/403/422/…) — not a "no sub" answer and a
            # retry won't help. Raise so the caller escalates rather than
            # silently treating it as "customer has no subscription".
            if not resp.ok:
                raise NexusLookupError(
                    f"{email} → HTTP {resp.status_code}: {resp.text[:200]}"
                )

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

    def _backoff(self, attempt: int) -> None:
        """Sleep between transient-failure retries (linear backoff)."""
        if self.retry_backoff > 0:
            time.sleep(self.retry_backoff * attempt)


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
      NEXUS_MAX_RETRIES    optional, default: 2 — retries on transient
                           (5xx / 52x / timeout / network) failures.
      NEXUS_RETRY_BACKOFF  optional, default: 0.5 — linear backoff base
                           (seconds) between transient retries.
    """
    token = os.getenv("NEXUS_API_TOKEN", "").strip()
    if not token:
        log.warning("NEXUS_API_TOKEN not configured — Nexus client disabled")
        return None

    def _int_env(name: str, default: int) -> int:
        try:
            return int(os.getenv(name, str(default)))
        except (TypeError, ValueError):
            return default

    def _float_env(name: str, default: float) -> float:
        try:
            return float(os.getenv(name, str(default)))
        except (TypeError, ValueError):
            return default

    return NexusClient(
        base_url=os.getenv("NEXUS_BASE_URL", "https://apinexus.cellon.ai"),
        api_token=token,
        x_host=os.getenv("NEXUS_X_HOST", "").strip(),
        max_retries=_int_env("NEXUS_MAX_RETRIES", 2),
        retry_backoff=_float_env("NEXUS_RETRY_BACKOFF", 0.5),
    )
