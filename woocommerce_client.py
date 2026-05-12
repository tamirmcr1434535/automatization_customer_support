"""
WooCommerce Subscriptions Client
=================================
Handles trial and subscription cancellations via WooCommerce REST API v3.
Requires the "WooCommerce Subscriptions" plugin to be active on the site.

IMPORTANT — performance notes for iqbooster.org:
  The WC server is VERY SLOW (any endpoint can take 5-70s).
  Cloud Function timeout is 3600s — use GENEROUS timeouts everywhere.

  FAST endpoints (usually < 1s, but can spike to 15s):
    GET /customers?email=             — exact email lookup, indexed
    GET /subscriptions/{id}           — direct single-row lookup

  SLOW endpoints (10–90s):
    GET /subscriptions?customer=      — customer_id filter (90s timeout, MAIN PATH)
    GET /subscriptions?search=        — full-text search, not indexed (45s timeout)
    GET /customers?search=            — full-text search, not indexed (30s timeout)

  BROKEN endpoints (use with caution):
    GET /subscriptions?billing_email= — returns ALL subs on iqbooster.org (server bug)
                                        → we paginate + client-side filter (up to 5 pages)
    GET /orders?customer={id}         — always times out on iqbooster.org (removed)

  Lookup strategy (CF timeout = 3600s, 15-min SLA budget = 900s):
    1.  /customers?email=              (15s timeout, indexed but server can be slow)
    1b. /customers?search=             (30s timeout, PayPal fallback)
    2a. customer meta_data → /subscriptions/{id}  (15s timeout)
    2b. /subscriptions?customer=       (3 attempts: 90→120→150s + 3s/5s backoff, MOST RELIABLE)
    2c. /subscriptions?billing_email=  (60s timeout per page, up to 5 pages if filter broken)
    2d. /subscriptions?search=         (120s timeout, per_page=100, last resort)
    Worst case: 15 + 30 + 15 + 368(2b w/retries) + 300 + 120 = 848s ≈ 14.1 min (fits 15-min SLA)
"""

import logging
import time
import requests
from datetime import datetime, timezone

log = logging.getLogger("woocommerce")


# HTTP statuses that come from the gateway / load balancer / CDN in front of
# the WC server, not from WC itself. They are almost always transient — the
# upstream was busy or briefly down. Worth a retry before giving up, since
# the alternative is the bot telling support "no subscription found" when
# really the lookup never reached WooCommerce.
_TRANSIENT_HTTP_STATUSES = (502, 503, 504)


def _request_with_retry(
    method, url, *,
    max_retries=1,
    timeout,
    timeout_schedule=None,
    backoff_sleeps=None,
    **kwargs,
):
    """
    HTTP request with automatic retry on timeout / network errors / gateway
    transient 5xx (502/503/504). Returns the response (which may itself be a
    non-OK transient response if all retries failed), or raises the last
    exception if all retries failed with an exception.

    Two retry modes:
      - Default: timeout doubles on each retry (max_retries=1 → [t, 2t]).
        Used by every caller that doesn't pass timeout_schedule.
      - Explicit schedule: timeout_schedule=(90, 120, 150) gives precisely
        those per-attempt timeouts and overrides max_retries. Used by the
        2b ?customer= path to stay under the 15-min SLA while still
        getting 3 attempts at WC.

    backoff_sleeps=(3, 5) pauses 3s before retry #2 and 5s before retry #3.
    Pauses run on any retry trigger (transient HTTP, timeout, network error)
    to let the upstream gateway recover instead of immediately re-hammering it.
    """
    if timeout_schedule is not None:
        timeouts = list(timeout_schedule)
    else:
        timeouts = [timeout * (2 ** i) for i in range(1 + max_retries)]
    attempts = len(timeouts)
    sleeps = list(backoff_sleeps) if backoff_sleeps else []

    last_exc = None
    last_resp = None
    for attempt in range(attempts):
        current_timeout = timeouts[attempt]
        is_last = attempt == attempts - 1
        try:
            resp = requests.request(
                method, url, timeout=current_timeout, **kwargs
            )
            if resp.status_code in _TRANSIENT_HTTP_STATUSES and not is_last:
                last_resp = resp
                log.warning(
                    f"WC: transient {resp.status_code} ({current_timeout}s) → "
                    f"retrying: {method} {url}"
                )
                if attempt < len(sleeps):
                    time.sleep(sleeps[attempt])
                continue
            return resp
        except requests.exceptions.Timeout as e:
            last_exc = e
            log.warning(
                f"WC: request timed out ({current_timeout}s) → "
                f"{'giving up' if is_last else 'retrying'}: "
                f"{method} {url}"
            )
            if not is_last and attempt < len(sleeps):
                time.sleep(sleeps[attempt])
        except requests.exceptions.RequestException as e:
            last_exc = e
            log.warning(
                f"WC: request error → "
                f"{'giving up' if is_last else 'retrying'}: "
                f"{method} {url}: {e}"
            )
            if not is_last and attempt < len(sleeps):
                time.sleep(sleeps[attempt])
    if last_resp is not None:
        # All retries exhausted with transient HTTP status — return the last
        # response so the caller can classify it via _error_kind_from_response.
        return last_resp
    raise last_exc


def _error_kind_from_response(resp) -> tuple[str, str] | None:
    """
    Classify an HTTP response as an error, if it is one.

    Returns (kind, detail) where kind is one of:
      - "auth_error"      (401, 403)
      - "transient_error" (502, 503, 504 — gateway / load balancer)
      - "api_error"       (other 5xx or non-2xx)

    Returns None for OK (2xx) responses.

    The `transient_error` kind is distinct from `api_error` so the bot can
    tell support "WC was temporarily unreachable, retry" instead of the
    misleading "no subscription found" when the upstream gateway flaked.
    """
    if resp is None:
        return None
    if resp.ok:
        return None
    detail = f"{resp.status_code} {resp.reason or ''}".strip()
    if resp.status_code in (401, 403):
        return ("auth_error", detail)
    if resp.status_code == 404:
        return None  # treat 404 as "not found", not as an error
    if resp.status_code in _TRANSIENT_HTTP_STATUSES:
        return ("transient_error", detail)
    return ("api_error", detail)


def _error_kind_from_exception(exc) -> tuple[str, str]:
    """
    Classify a requests exception.

    Returns (kind, detail) where kind is one of:
      - "timeout_error"  (requests.Timeout)
      - "api_error"      (other RequestException / network)
    """
    if isinstance(exc, requests.exceptions.Timeout):
        return ("timeout_error", f"timeout: {exc}"[:200])
    return ("api_error", f"network: {exc}"[:200])


def _worst_error_kind(errors: list[dict]) -> str | None:
    """
    Pick the most severe error kind from a list of lookup errors.

    Priority: auth_error > api_error > timeout_error > transient_error.
    auth_error means credentials are broken — highest severity because the
    bot cannot function at all until ops fixes it. transient_error is the
    least severe (gateway flake) so a single 504 in step 2b doesn't hide a
    real timeout in step 2c.
    """
    if not errors:
        return None
    kinds = {e.get("kind") for e in errors}
    for k in ("auth_error", "api_error", "timeout_error", "transient_error"):
        if k in kinds:
            return k
    return None


ACTIVE_STATUSES = {"active", "pending-cancel", "on-hold", "pending"}


class WooCommerceClient:
    def __init__(
        self,
        site_url: str,
        consumer_key: str,
        consumer_secret: str,
        dry_run: bool = True,
    ):
        self.base = f"{site_url.rstrip('/')}/wp-json/wc/v3"
        self.auth = (consumer_key, consumer_secret)
        self.dry_run = dry_run
        if dry_run:
            log.info("WooCommerceClient: DRY_RUN — no writes")

    # ================================================================== #
    #  Health check — used at startup to fail fast on bad credentials     #
    # ================================================================== #

    def health_check(self) -> dict:
        """
        Quick connectivity + credentials check.

        Makes a single GET /customers?per_page=1 call with a short timeout.
        Returns a dict:
          {"ok": True,  "status": "ok",            "detail": "200 OK"}
          {"ok": False, "status": "auth_error",    "detail": "401 Unauthorized"}
          {"ok": False, "status": "timeout_error", "detail": "timeout: ..."}
          {"ok": False, "status": "api_error",     "detail": "500 ..." or "network: ..."}
        """
        try:
            resp = requests.get(
                f"{self.base}/customers",
                params={"per_page": 1},
                auth=self.auth,
                timeout=30,
            )
        except requests.exceptions.Timeout as e:
            return {"ok": False, "status": "timeout_error",
                    "detail": f"timeout: {e}"[:200]}
        except requests.exceptions.RequestException as e:
            return {"ok": False, "status": "api_error",
                    "detail": f"network: {e}"[:200]}

        err = _error_kind_from_response(resp)
        if err:
            kind, detail = err
            return {"ok": False, "status": kind, "detail": detail}
        return {"ok": True, "status": "ok", "detail": f"{resp.status_code} {resp.reason or ''}".strip()}

    # ================================================================== #
    #  READ — customer lookup                                            #
    # ================================================================== #

    def get_customer_by_email(self, email: str, _errors: list | None = None) -> dict | None:
        """
        Return the first WooCommerce customer matching *email* (exact), or None.

        Uses ?email= (fast, indexed). Tries ?role=all first; if the server
        rejects it (4xx), falls back to no role filter.
        If ?role=all returns 200 OK with empty results → email simply doesn't
        exist, no point retrying without role.
        Timeout: 30s base + 1 retry at 60s (server regularly takes 15-30s).
        """
        t0 = time.time()
        for i, params in enumerate([
            {"email": email, "per_page": 1, "role": "all"},
            {"email": email, "per_page": 1},
        ]):
            try:
                resp = _request_with_retry(
                    "GET",
                    f"{self.base}/customers",
                    params=params,
                    auth=self.auth,
                    timeout=30,
                    max_retries=1,
                )
            except requests.exceptions.RequestException as e:
                log.warning(f"WC customer lookup error for {email}: {e}")
                if _errors is not None:
                    kind, detail = _error_kind_from_exception(e)
                    _errors.append({"step": "customer_email", "kind": kind, "detail": detail})
                continue

            if resp.status_code == 401 or resp.status_code == 403:
                log.error(f"WooCommerce {resp.status_code} Unauthorized — check consumer key/secret")
                if _errors is not None:
                    _errors.append({
                        "step": "customer_email",
                        "kind": "auth_error",
                        "detail": f"{resp.status_code} {resp.reason or ''}".strip(),
                    })
                return None
            if resp.status_code == 404:
                continue
            if not resp.ok:
                log.warning(f"WC customer lookup {resp.status_code} for {email}")
                if _errors is not None:
                    err = _error_kind_from_response(resp)
                    if err:
                        kind, detail = err
                        _errors.append({"step": "customer_email", "kind": kind, "detail": detail})
                continue

            data = resp.json()
            if data:
                log.info(
                    f"WC: found customer for {email} (id={data[0]['id']}) "
                    f"in {time.time()-t0:.1f}s"
                )
                return data[0]

            # 200 OK with empty data → email not in WC, skip second pass
            if i == 0:
                log.info(
                    f"WC: ?email= (role=all) returned 0 customers for {email} "
                    f"in {time.time()-t0:.1f}s — skipping second pass"
                )
                return None

        log.info(f"WC: no customer found for {email} in {time.time()-t0:.1f}s")
        return None

    # ================================================================== #
    #  READ — subscription lookup (fast paths only)                      #
    # ================================================================== #

    def _find_sub_ids_from_orders(self, customer_id: int) -> list[int]:
        """
        Find subscription IDs by looking at customer's orders.
        /orders?customer={id} — usually 1-2s, but can take 15-30s on slow server.
        Each WC Subscription order has a 'subscription_ids' in meta_data
        or '_subscription_id' linking back to the subscription.
        Returns list of unique subscription IDs found.
        """
        try:
            resp = requests.get(
                f"{self.base}/orders",
                params={
                    "customer": customer_id,
                    "per_page": 3,
                    "orderby": "date",
                    "order": "desc",
                },
                auth=self.auth,
                timeout=8,
            )
        except requests.exceptions.RequestException as e:
            log.warning(f"WC: orders lookup error for customer {customer_id}: {e}")
            return []

        if not resp.ok:
            log.warning(
                f"WC: orders lookup failed for customer {customer_id}: "
                f"{resp.status_code}"
            )
            return []

        orders = resp.json()
        if not isinstance(orders, list):
            return []

        sub_ids = set()
        for order in orders:
            # Check meta_data for subscription references
            for meta in order.get("meta_data", []):
                key = meta.get("key", "")
                if key in ("_subscription_id", "subscription_id", "_subscription_ids"):
                    val = meta.get("value")
                    if isinstance(val, (int, str)):
                        try:
                            sub_ids.add(int(val))
                        except (ValueError, TypeError):
                            pass
                    elif isinstance(val, list):
                        for v in val:
                            try:
                                sub_ids.add(int(v))
                            except (ValueError, TypeError):
                                pass

            # Also check line_items for subscription product references
            for item in order.get("line_items", []):
                for meta in item.get("meta_data", []):
                    if meta.get("key") in ("_subscription_id", "subscription_id"):
                        try:
                            sub_ids.add(int(meta["value"]))
                        except (ValueError, TypeError):
                            pass

        if sub_ids:
            log.info(
                f"WC: found subscription IDs {sub_ids} from orders "
                f"for customer {customer_id}"
            )
        else:
            log.info(
                f"WC: no subscription IDs in orders for customer {customer_id}"
            )

        return sorted(sub_ids)

    @staticmethod
    def _sub_id_from_customer_meta(customer: dict) -> int | None:
        """
        Extract subscription_id from customer meta_data if present.
        Most customers have this set at signup time.
        """
        for meta in customer.get("meta_data", []):
            if meta.get("key") == "subscription_id":
                try:
                    return int(meta["value"])
                except (ValueError, TypeError):
                    pass
        return None

    def _get_subscription_by_id(self, subscription_id: int) -> dict | None:
        """Direct single-subscription lookup by ID. Usually fast (~0.3s), 20s timeout + retry."""
        try:
            resp = _request_with_retry(
                "GET",
                f"{self.base}/subscriptions/{subscription_id}",
                auth=self.auth,
                timeout=20,
                max_retries=1,
            )
        except requests.exceptions.RequestException as e:
            log.warning(f"WC: direct sub lookup error for #{subscription_id}: {e}")
            return None
        if not resp.ok:
            log.warning(f"WC: direct sub lookup failed #{subscription_id}: {resp.status_code}")
            return None
        return resp.json()

    def _find_subs_by_billing_email(self, email: str, _errors: list | None = None) -> list[dict]:
        """
        Search subscriptions by ?billing_email= with pagination.

        The billing_email filter is broken on some servers (returns ALL subs).
        We paginate through up to MAX_PAGES pages to find an exact match,
        because the correct subscription could be on any page.
        """
        email_lower = email.lower().strip()
        MAX_PAGES = 5       # scan up to 500 subs (5 × 100)
        PER_PAGE  = 100
        TIMEOUT   = 60      # generous — server is slow

        exact = []
        trusted_empty = []
        wrong_count = 0
        filter_looks_broken = False

        for page in range(1, MAX_PAGES + 1):
            try:
                resp = _request_with_retry(
                    "GET",
                    f"{self.base}/subscriptions",
                    params={
                        "billing_email": email,
                        "per_page": PER_PAGE,
                        "page": page,
                    },
                    auth=self.auth,
                    timeout=TIMEOUT,
                    max_retries=1,
                )
            except requests.exceptions.Timeout as e:
                log.warning(
                    f"WC: billing_email page {page} timed out ({TIMEOUT}s) "
                    f"for {email}"
                )
                if _errors is not None:
                    kind, detail = _error_kind_from_exception(e)
                    _errors.append({"step": "billing_email", "kind": kind, "detail": detail})
                break
            except requests.exceptions.RequestException as e:
                log.warning(f"WC: billing_email lookup error for {email}: {e}")
                if _errors is not None:
                    kind, detail = _error_kind_from_exception(e)
                    _errors.append({"step": "billing_email", "kind": kind, "detail": detail})
                break

            if not resp.ok:
                log.warning(
                    f"WC: billing_email lookup failed for {email}: "
                    f"{resp.status_code}"
                )
                if _errors is not None:
                    err = _error_kind_from_response(resp)
                    if err:
                        kind, detail = err
                        _errors.append({"step": "billing_email", "kind": kind, "detail": detail})
                break

            data = resp.json()
            if not isinstance(data, list) or not data:
                break  # no more results

            for s in data:
                if self._subscription_matches_email(s, email_lower):
                    exact.append(s)
                else:
                    be = s.get("billing", {}).get("email", "").strip()
                    if be:
                        wrong_count += 1
                    else:
                        trusted_empty.append(s)

            # If we already found exact matches, no need to paginate further
            if exact:
                break

            # If page 1 has wrong-email results → filter is broken on this server.
            # Keep paginating to find the right one, but cap at MAX_PAGES.
            if wrong_count > 0 and page == 1:
                filter_looks_broken = True
                log.info(
                    f"WC: billing_email filter looks broken for {email} "
                    f"({wrong_count} wrong emails on page 1) — paginating "
                    f"up to {MAX_PAGES} pages"
                )

            # If filter is NOT broken (all results are empty-email or exact),
            # one page is enough.
            if not filter_looks_broken:
                break

            # Stop paginating if this page was less than full (last page)
            if len(data) < PER_PAGE:
                break

        # Log one summary warning for all mismatches
        if wrong_count:
            log.warning(
                f"WC: billing_email query for {email} returned {wrong_count} "
                f"sub(s) with wrong billing emails — server filter unreliable"
            )

        if exact:
            log.info(
                f"WC: billing_email found {len(exact)} exact match(es) for "
                f"{email}"
            )
            return exact

        # If API returned ONLY trusted-empty subs (billing.email blank, server filtered) —
        # trust the server result as long as no wrong-email subs were mixed in.
        if trusted_empty and wrong_count == 0:
            log.info(
                f"WC: billing_email returned {len(trusted_empty)} sub(s) with empty "
                f"billing.email for {email} — trusting server filter"
            )
            return trusted_empty

        # Mixed or all-wrong results — discard trusted_empty too (can't trust
        # server filter when it also returned wrong-email subs).
        if wrong_count and trusted_empty:
            log.warning(
                f"WC: billing_email query for {email}: discarding "
                f"{len(trusted_empty)} empty-email sub(s) because {wrong_count} "
                "wrong-email sub(s) also present — server filter broken"
            )

        if wrong_count and not exact and not trusted_empty:
            log.warning(
                f"WC: billing_email query for {email}: all {wrong_count} "
                "results had wrong emails, no matches found"
            )

        return []

    @staticmethod
    def _subscription_matches_email(sub: dict, email_lower: str) -> bool:
        """
        Check if a subscription is associated with the given email.
        Checks billing.email, meta_data._billing_email, meta_data.billing_email.
        """
        if sub.get("billing", {}).get("email", "").lower().strip() == email_lower:
            return True
        for meta in sub.get("meta_data", []):
            if meta.get("key") in ("_billing_email", "billing_email"):
                if meta.get("value", "").lower().strip() == email_lower:
                    return True
        return False

    # ================================================================== #
    #  READ — order count (for trial vs subscription detection)          #
    # ================================================================== #

    def get_order_count(self, subscription_id: int) -> int | None:
        """
        Return number of SUCCESSFUL orders for a subscription.

        Only orders with status=completed/processing count — failed retries,
        cancelled orders, and refunded orders are excluded. The bot's "renewal
        vs trial" gate uses this number, and a failed-charge sub should NOT
        be treated as a paid renewal: customers were reporting tickets like
        "I only paid 2 times" while the bot's note said "orders=4" because
        the X-WP-Total header counted failed retry attempts.

        We fetch up to 50 orders per page (the user-reported case had 4 total
        orders so 50 is more than enough headroom) and count client-side.
        """
        try:
            resp = requests.get(
                f"{self.base}/subscriptions/{subscription_id}/orders",
                params={"per_page": 50},
                auth=self.auth,
                timeout=20,
            )
        except requests.exceptions.RequestException as e:
            log.warning(f"WC: order count error for sub #{subscription_id}: {e}")
            return None

        if not resp.ok:
            return None

        data = resp.json()
        if not isinstance(data, list):
            return None

        # Statuses that represent a successful (paid-through) order. Anything
        # else — failed, cancelled, refunded, pending, on-hold — is NOT a
        # paid renewal and must not count toward the bot's renewal threshold.
        _SUCCESSFUL_STATUSES = {"completed", "processing"}
        successful = sum(
            1 for o in data
            if isinstance(o, dict) and o.get("status") in _SUCCESSFUL_STATUSES
        )
        return successful

    # ================================================================== #
    #  Trial detection                                                   #
    # ================================================================== #

    @staticmethod
    def _get_sub_type(subscription: dict, order_count: int | None = None) -> str:
        """
        Determine whether subscription is a "trial" or "subscription".

        Rules:
        0. order_count > 1 → "subscription" (renewal happened)
        1. order_count ≤ 1 AND days_since_start ≤ 8 → "trial"
        2. order_count ≤ 1 AND days_since_start > 8 → "subscription"
        3. Fallback: trial_end in future + order_count ≤ 1 → "trial"
        4. Default → "subscription"
        """
        if order_count is not None and order_count > 1:
            return "subscription"

        start_raw = (
            subscription.get("start_date_gmt")
            or subscription.get("start_date")
            or ""
        )

        def _parse(s: str):
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

        now = datetime.now(timezone.utc)
        start_dt = None
        if start_raw and not start_raw.startswith("0000"):
            try:
                start_dt = _parse(start_raw)
            except (ValueError, AttributeError):
                pass

        if start_dt is not None:
            days_since_start = (now - start_dt).days
            is_trial = (order_count is None or order_count <= 1) and days_since_start <= 8
            return "trial" if is_trial else "subscription"

        trial_end_raw = (
            subscription.get("trial_end_date_gmt")
            or subscription.get("trial_end_date")
            or ""
        )
        if trial_end_raw and not trial_end_raw.startswith("0000") and (order_count is None or order_count <= 1):
            try:
                trial_end_dt = _parse(trial_end_raw)
                if trial_end_dt > now:
                    return "trial"
            except (ValueError, AttributeError):
                pass

        return "subscription"

    # ================================================================== #
    #  WRITE — cancel subscription                                       #
    # ================================================================== #

    def _cancel_sub_by_id(self, subscription_id: int) -> dict:
        """PUT status=cancelled for a single subscription.

        Uses _request_with_retry (1 retry on timeout, doubling timeout).
        Base timeout 60s — iqbooster.org PUTs have been observed taking
        30-70s during peak load; 20s was too aggressive and caused every
        slow-server moment to bubble up as api_error.

        On failure the returned dict includes typed error info so callers
        can distinguish timeout/auth/api errors and surface the real HTTP
        detail to operators (the previous code collapsed everything into
        a bare "error" status that hid the root cause).
        """
        if self.dry_run:
            log.info(f"[DRY] WC cancel subscription #{subscription_id}")
            return {
                "status": "dry_run",
                "subscription_id": subscription_id,
                "cancelled": True,
            }

        try:
            resp = _request_with_retry(
                "PUT",
                f"{self.base}/subscriptions/{subscription_id}",
                json={"status": "cancelled"},
                auth=self.auth,
                timeout=60,
                max_retries=1,
            )
        except requests.exceptions.Timeout as e:
            log.error(f"WC cancel TIMED OUT for #{subscription_id}: {e}")
            return {
                "status": "error",
                "subscription_id": subscription_id,
                "cancelled": False,
                "error": f"PUT timeout: {e}"[:300],
                "error_kind": "timeout_error",
                "error_detail": f"PUT /subscriptions/{subscription_id} timed out: {e}"[:300],
                "error_step": "put_cancel",
            }
        except requests.exceptions.RequestException as e:
            log.error(f"WC cancel network error for #{subscription_id}: {e}")
            return {
                "status": "error",
                "subscription_id": subscription_id,
                "cancelled": False,
                "error": str(e)[:300],
                "error_kind": "api_error",
                "error_detail": f"PUT /subscriptions/{subscription_id} network: {e}"[:300],
                "error_step": "put_cancel",
            }

        if not resp.ok:
            err = _error_kind_from_response(resp) or ("api_error", f"{resp.status_code}")
            err_kind, _ = err
            detail = f"PUT /subscriptions/{subscription_id} → {resp.status_code} {resp.reason or ''}: {(resp.text or '')[:250]}"
            log.error(f"WC cancel failed #{subscription_id}: {detail}")
            return {
                "status": "error",
                "subscription_id": subscription_id,
                "cancelled": False,
                "error": (resp.text or "")[:300],
                "error_kind": err_kind,
                "error_detail": detail[:300],
                "error_step": "put_cancel",
            }

        # Sanity check the response body says the subscription is now cancelled.
        try:
            body = resp.json()
            actual_status = body.get("status", "")
            if actual_status and actual_status != "cancelled":
                log.warning(
                    f"WC cancel #{subscription_id}: PUT 200 but returned status="
                    f"{actual_status!r} — WC did not actually cancel"
                )
                return {
                    "status": "error",
                    "subscription_id": subscription_id,
                    "cancelled": False,
                    "error": f"WC returned status={actual_status!r} after PUT",
                    "error_kind": "api_error",
                    "error_detail": (
                        f"PUT /subscriptions/{subscription_id} returned 200 but "
                        f"sub status is {actual_status!r}, not 'cancelled'"
                    )[:300],
                    "error_step": "put_cancel",
                }
        except Exception:
            pass  # body unparseable → trust 2xx as success

        log.info(f"WC: cancelled subscription #{subscription_id}")
        return {"status": "cancelled", "subscription_id": subscription_id,
                "cancelled": True}

    # ================================================================== #
    #  MAIN PUBLIC METHOD                                                #
    # ================================================================== #

    def cancel_subscription(
        self,
        email: str,
        *,
        max_auto_cancel_orders: int | None = None,
    ) -> dict:
        """
        Find customer → find subscription → cancel.

        If `max_auto_cancel_orders` is set and the matched subscription has
        `order_count >= max_auto_cancel_orders`, the PUT cancel is SKIPPED and
        the function returns status="renewal_too_many_orders" with the sub
        metadata so the caller can escalate to a human. This prevents the
        previous bug where a renewal would be silently cancelled in WC and
        then tagged "ai_bot_failed" without ever notifying the customer.

        Lookup chain (CF timeout = 3600s, generous timeouts):
          1.  /customers?email=           (15s timeout, indexed but server can spike)
          1b. /customers?search=          (30s timeout, PayPal fallback)
          2a. customer meta_data → /subscriptions/{id}  (15s timeout)
          2b. /subscriptions?customer=    (90s timeout, MOST RELIABLE)
          2c. /subscriptions?billing_email= (60s/page, up to 5 pages if filter broken)
          2d. /subscriptions?search=      (120s timeout, per_page=100, last resort)
          Worst case: 15s + 30s + 90s + 300s + 120s = 555s (CF=3600s — plenty of room)

        If subscription not found → returns "not_found" → bot asks customer
        for last 4 card digits → Stripe finds email → we try again.
        """
        wc_start = time.time()

        # Errors encountered during lookup — used at the end to distinguish
        # "truly not found" from "not found because WC was broken".
        errors: list[dict] = []

        base_result = {
            "email": email,
            "cancelled": False,
            "source": "woocommerce",
            "subscription_type": None,
            "subscription_id": None,
            "plan": "",
        }

        log.info(f"{'[DRY] ' if self.dry_run else ''}WC cancel_subscription START for email={email}")

        # ── Step 1: customer lookup (indexed, 15s timeout) ─────────────── #
        customer = self.get_customer_by_email(email, _errors=errors)
        step1_elapsed = time.time() - wc_start
        if customer:
            log.info(
                f"WC STEP1: found customer for {email} → id={customer['id']}, "
                f"wp_email={customer.get('email','')}, "
                f"billing_email={customer.get('billing',{}).get('email','')} "
                f"({step1_elapsed:.1f}s)"
            )
        else:
            log.info(f"WC STEP1: no customer found for {email} via ?email= ({step1_elapsed:.1f}s)")

        # Step 1b: if ?email= didn't find customer, try ?search= (slower but
        # catches PayPal users whose WP account email differs from billing email)
        if not customer:
            t1b = time.time()
            try:
                resp = _request_with_retry(
                    "GET",
                    f"{self.base}/customers",
                    params={"search": email, "per_page": 5},
                    auth=self.auth,
                    timeout=30,
                    max_retries=1,
                )
                if resp.ok:
                    data = resp.json()
                    if data:
                        # Verify the match — search is fuzzy
                        for c in data:
                            c_email = c.get("email", "").lower().strip()
                            c_billing = c.get("billing", {}).get("email", "").lower().strip()
                            if email.lower().strip() in (c_email, c_billing):
                                customer = c
                                log.info(
                                    f"WC: found customer via ?search= for {email} "
                                    f"(id={c['id']})"
                                )
                                break
                        if not customer and data:
                            if len(data) == 1:
                                customer = data[0]
                                log.info(
                                    f"WC: ?search= returned 1 customer for {email} "
                                    f"(id={data[0]['id']}, email={data[0].get('email','')})"
                                )
                            else:
                                log.info(
                                    f"WC: ?search= returned {len(data)} customers "
                                    f"for {email} but none matched exactly"
                                )
                else:
                    err = _error_kind_from_response(resp)
                    if err:
                        kind, detail = err
                        errors.append({"step": "customer_search", "kind": kind, "detail": detail})
            except requests.exceptions.Timeout as e:
                log.warning(f"WC: customer ?search= timed out (30s) for {email}")
                kind, detail = _error_kind_from_exception(e)
                errors.append({"step": "customer_search", "kind": kind, "detail": detail})
            except requests.exceptions.RequestException as e:
                log.warning(f"WC: customer ?search= error for {email}: {e}")
                kind, detail = _error_kind_from_exception(e)
                errors.append({"step": "customer_search", "kind": kind, "detail": detail})
            log.info(f"WC TIMING: step1b customer?search= done in {time.time()-t1b:.1f}s")

        all_subs: list[dict] | None = None

        # ── Step 2a: direct subscription from meta_data (~0.3s) ───────── #
        if customer:
            meta_sub_id = self._sub_id_from_customer_meta(customer)
            if meta_sub_id:
                sub = self._get_subscription_by_id(meta_sub_id)
                if sub:
                    log.info(
                        f"WC: found sub #{meta_sub_id} via customer meta_data "
                        f"(status={sub.get('status')})"
                    )
                    all_subs = [sub]
                else:
                    log.info(
                        f"WC: meta_data had subscription_id={meta_sub_id} "
                        f"but direct lookup failed"
                    )
            else:
                log.info(f"WC: customer found but no subscription_id in meta_data")

        # ── Step 2b: /subscriptions?customer= (MOST RELIABLE, 90s) ───── #
        # CF timeout is 3600s — give this endpoint plenty of time.
        # iqbooster.org regularly takes 30-70s for this query.
        if not all_subs and customer:
            customer_id = customer.get("id")
            if customer_id:
                t2b = time.time()
                try:
                    log.info(
                        f"WC: trying ?customer={customer_id} "
                        "(3 attempts: 90→120→150s + 3s/5s backoff)"
                    )
                    resp = _request_with_retry(
                        "GET",
                        f"{self.base}/subscriptions",
                        params={"customer": customer_id, "per_page": 10, "status": "any"},
                        auth=self.auth,
                        timeout=90,
                        timeout_schedule=(90, 120, 150),
                        backoff_sleeps=(3, 5),
                    )
                    if resp.ok:
                        customer_subs = resp.json()
                        if isinstance(customer_subs, list) and customer_subs:
                            log.info(
                                f"WC: found {len(customer_subs)} sub(s) via "
                                f"?customer={customer_id} for {email} "
                                f"in {time.time()-t2b:.1f}s"
                            )
                            all_subs = customer_subs
                        else:
                            log.info(
                                f"WC: ?customer={customer_id} returned no subs "
                                f"in {time.time()-t2b:.1f}s"
                            )
                    else:
                        log.warning(
                            f"WC: ?customer={customer_id} failed: {resp.status_code} "
                            f"in {time.time()-t2b:.1f}s"
                        )
                        err = _error_kind_from_response(resp)
                        if err:
                            kind, detail = err
                            errors.append({"step": "subs_by_customer", "kind": kind, "detail": detail})
                except requests.exceptions.Timeout as e:
                    log.warning(
                        f"WC: ?customer={customer_id} TIMED OUT (90s) for {email}"
                    )
                    kind, detail = _error_kind_from_exception(e)
                    errors.append({"step": "subs_by_customer", "kind": kind, "detail": detail})
                except requests.exceptions.RequestException as e:
                    log.warning(f"WC: ?customer={customer_id} error: {e}")
                    kind, detail = _error_kind_from_exception(e)
                    errors.append({"step": "subs_by_customer", "kind": kind, "detail": detail})
                log.info(f"WC TIMING: step2b ?customer= done in {time.time()-t2b:.1f}s (total {time.time()-wc_start:.1f}s)")

        # ── Step 2c: /subscriptions?billing_email= (20s timeout) ─────── #
        # Previously only ran when no customer was found, but Stripe Multi
        # Sync can create subscriptions that are NOT linked to the WC
        # customer_id. In that case ?customer= returns 0 results, yet
        # ?billing_email= finds the subscription by its billing email.
        if not all_subs:
            t2c = time.time()
            billing_subs = self._find_subs_by_billing_email(email, _errors=errors)
            if billing_subs:
                log.info(
                    f"WC: found {len(billing_subs)} sub(s) via billing_email "
                    f"for {email}"
                )
                all_subs = billing_subs
            log.info(f"WC TIMING: step2c billing_email done in {time.time()-t2c:.1f}s (total {time.time()-wc_start:.1f}s)")

        # ── Step 2d: /subscriptions?search= last resort (120s timeout) ──── #
        # Increased per_page (100) and timeout (120s) — the server is slow
        # (200K+ subs, full-text search not indexed) and 45s/20 was not enough.
        if not all_subs:
            t2d = time.time()
            _SEARCH_TIMEOUT = 120
            _SEARCH_PER_PAGE = 100
            try:
                log.info(
                    f"WC: all lookups failed for {email}, "
                    f"trying ?search= last resort ({_SEARCH_TIMEOUT}s timeout, "
                    f"per_page={_SEARCH_PER_PAGE})"
                )
                resp = _request_with_retry(
                    "GET",
                    f"{self.base}/subscriptions",
                    params={
                        "search": email,
                        "per_page": _SEARCH_PER_PAGE,
                        "status": "any",
                    },
                    auth=self.auth,
                    timeout=_SEARCH_TIMEOUT,
                    max_retries=1,
                )
                if resp.ok:
                    search_subs = resp.json()
                    if isinstance(search_subs, list) and search_subs:
                        verified = [
                            s for s in search_subs
                            if self._subscription_matches_email(
                                s, email.lower().strip()
                            )
                        ]
                        if not verified:
                            verified = [
                                s for s in search_subs
                                if not s.get("billing", {}).get("email", "").strip()
                            ]
                        if verified:
                            log.info(
                                f"WC: ?search= found {len(verified)} verified "
                                f"sub(s) for {email}"
                            )
                            all_subs = verified
                        else:
                            log.info(
                                f"WC: ?search= returned {len(search_subs)} sub(s) "
                                f"but none matched {email}"
                            )
                    else:
                        log.info(f"WC: ?search= returned 0 results for {email}")
                else:
                    log.warning(f"WC: ?search= failed: {resp.status_code}")
                    err = _error_kind_from_response(resp)
                    if err:
                        kind, detail = err
                        errors.append({"step": "subs_search", "kind": kind, "detail": detail})
            except requests.exceptions.Timeout as e:
                log.warning(
                    f"WC: ?search= TIMED OUT ({_SEARCH_TIMEOUT}s) for {email} "
                    "— falling through to Stripe fallback"
                )
                kind, detail = _error_kind_from_exception(e)
                errors.append({"step": "subs_search", "kind": kind, "detail": detail})
            except requests.exceptions.RequestException as e:
                log.warning(f"WC: ?search= error: {e}")
                kind, detail = _error_kind_from_exception(e)
                errors.append({"step": "subs_search", "kind": kind, "detail": detail})
            log.info(f"WC TIMING: step2d ?search= done in {time.time()-t2d:.1f}s (total {time.time()-wc_start:.1f}s)")

        if not all_subs:
            total = time.time() - wc_start

            # If WC errored on any lookup step, distinguish between
            # "truly not found" and "lookup failed — we don't actually know".
            # This prevents the bot from escalating a real customer as if their
            # email didn't exist when in fact WC was just broken.
            worst = _worst_error_kind(errors)
            if worst is not None:
                first = next((e for e in errors if e.get("kind") == worst), errors[0])
                log.error(
                    f"WC RESULT: ERROR — {worst} during lookup for {email} "
                    f"({len(errors)} error(s), first: {first}). total={total:.1f}s"
                )
                return {
                    **base_result,
                    "status": worst,  # auth_error | timeout_error | api_error
                    "error_detail": first.get("detail", ""),
                    "error_step": first.get("step", ""),
                    "errors": errors,
                }

            if customer:
                log.warning(
                    f"WC RESULT: FAIL — customer found (id={customer['id']}) "
                    f"but NO subscription for email={email} (total: {total:.1f}s)"
                )
                return {**base_result, "status": "no_active_sub"}
            else:
                log.warning(
                    f"WC RESULT: FAIL — no customer AND no subscription "
                    f"for email={email} (total: {total:.1f}s). "
                    "All lookup steps exhausted."
                )
                return {**base_result, "status": "not_found"}

        # ── Step 3: filter active subscriptions ───────────────────────── #
        log.info(
            f"WC FOUND: {len(all_subs)} subscription(s) for email={email}: "
            + ", ".join(
                f"#{s.get('id')}(status={s.get('status')}, "
                f"billing={s.get('billing',{}).get('email','')})"
                for s in all_subs
            )
        )
        active_subs = [s for s in all_subs if s.get("status") in ACTIVE_STATUSES]

        if not active_subs:
            # Check for already-cancelled subscriptions
            cancelled_subs = [
                s for s in all_subs if s.get("status") == "cancelled"
            ]
            if cancelled_subs:
                cancelled_subs.sort(
                    key=lambda s: s.get("start_date_gmt") or "",
                    reverse=True,
                )
                target = cancelled_subs[0]

                # Stale-cancellation guard: if the most recent cancellation is
                # older than STALE_CANCELLATION_DAYS, the customer is almost
                # certainly NOT writing in to confirm a fresh cancellation —
                # they probably have a different sub on a different email, or
                # a Stripe sub that WC doesn't see, or they are confused. Do
                # not auto-reply "your sub is cancelled" with a year-old sub
                # — escalate as not_found so a human can investigate.
                _STALE_CANCELLATION_DAYS = 90
                end_raw = (
                    target.get("end_date_gmt")
                    or target.get("date_modified_gmt")
                    or target.get("date_modified")
                    or ""
                )
                stale = False
                if end_raw and not end_raw.startswith("0000"):
                    try:
                        end_dt = datetime.fromisoformat(
                            end_raw.replace("Z", "+00:00")
                        )
                        if end_dt.tzinfo is None:
                            end_dt = end_dt.replace(tzinfo=timezone.utc)
                        days_since_cancel = (
                            datetime.now(timezone.utc) - end_dt
                        ).days
                        if days_since_cancel > _STALE_CANCELLATION_DAYS:
                            stale = True
                            log.warning(
                                f"WC: latest cancelled sub #{target['id']} "
                                f"ended {days_since_cancel} days ago "
                                f"(> {_STALE_CANCELLATION_DAYS}) — treating "
                                "as not_found so a human can verify "
                                "(customer probably has a different sub)"
                            )
                    except (ValueError, AttributeError):
                        pass
                if stale:
                    return {
                        **base_result,
                        "status": "not_found",
                        "stale_cancelled_subscription_id": target.get("id"),
                    }

                order_count = self.get_order_count(target["id"])
                sub_type = self._get_sub_type(target, order_count=order_count)
                plan = ""
                li = target.get("line_items") or []
                if li:
                    plan = li[0].get("name", "")

                log.info(
                    f"WC: already-cancelled sub #{target['id']} "
                    f"(type={sub_type}, orders={order_count})"
                )
                return {
                    **base_result,
                    "status": "already_cancelled",
                    "cancelled": True,
                    "subscription_type": sub_type,
                    "subscription_id": target["id"],
                    "plan": plan or "IQ Test Subscription",
                    "order_count": order_count,  # FIX: include for order-count gate in _finish_cancellation
                }

            log.info(f"WC: subscriptions found but none active for {email}")
            return {**base_result, "status": "no_active_sub"}

        # ── Step 4: pick best subscription to cancel ──────────────────── #
        typed_subs = []
        for s in active_subs:
            oc = self.get_order_count(s["id"])
            typed_subs.append((s, self._get_sub_type(s, order_count=oc), oc))

        def _priority(entry: tuple) -> int:
            sub, stype, _oc = entry
            st = sub.get("status", "")
            if st == "pending-cancel" and stype == "subscription":
                return 0
            if st == "active" and stype == "subscription":
                return 1
            if st == "pending-cancel" and stype == "trial":
                return 2
            if stype == "subscription":
                return 3
            if stype == "trial":
                return 4
            return 5

        typed_subs.sort(key=_priority)
        target, sub_type, order_count = typed_subs[0]

        plan = ""
        li = target.get("line_items") or []
        if li:
            plan = li[0].get("name", "")

        # ── Step 4b: renewal gate (skip PUT for many-order subs) ──────── #
        # If the caller set a threshold, refuse to auto-cancel renewal
        # subscriptions and return metadata so they can escalate. This MUST
        # run before _cancel_sub_by_id — gating after the PUT was the cause
        # of "subscription cancelled in WC but ticket tagged ai_bot_failed
        # with no customer reply" reports.
        if (
            max_auto_cancel_orders is not None
            and order_count is not None
            and order_count >= max_auto_cancel_orders
        ):
            log.info(
                f"WC: sub #{target['id']} has {order_count} orders "
                f"(>= {max_auto_cancel_orders}) — SKIPPING PUT, returning "
                "renewal_too_many_orders for human review"
            )
            return {
                **base_result,
                "status": "renewal_too_many_orders",
                "cancelled": False,
                "subscription_type": sub_type,
                "subscription_id": target["id"],
                "plan": plan or "IQ Test Subscription",
                "order_count": order_count,
            }

        # ── Step 5: cancel ────────────────────────────────────────────── #
        cancel = self._cancel_sub_by_id(target["id"])

        total_wc = time.time() - wc_start
        log.info(
            f"WC TIMING: total cancel_subscription for {email} = {total_wc:.1f}s "
            f"(sub #{target['id']}, type={sub_type}, orders={order_count})"
        )

        if self.dry_run:
            status_label = "dry_run"
        elif cancel["cancelled"]:
            status_label = (
                "trial_cancelled" if sub_type == "trial"
                else "subscription_cancelled"
            )
        else:
            # Cancel failed — surface the typed error kind so callers can
            # distinguish "WC was down" from "lookup was clean but sub
            # genuinely does not exist". Falls back to legacy "error" only
            # when _cancel_sub_by_id did not produce a typed kind.
            status_label = cancel.get("error_kind") or cancel.get("status", "error")

        result = {
            **base_result,
            "status": status_label,
            "cancelled": cancel["cancelled"],
            "subscription_type": sub_type,
            "subscription_id": target["id"],
            "plan": plan or "IQ Test Subscription",
            "order_count": order_count,
            "error": cancel.get("error"),
        }

        # When the PUT itself failed, propagate the detailed error info
        # (real HTTP status + body) so main.py can render it in the
        # per-ticket Slack report instead of the opaque "legacy status: error".
        if not cancel["cancelled"] and not self.dry_run:
            if cancel.get("error_detail"):
                result["error_detail"] = cancel["error_detail"]
            if cancel.get("error_step"):
                result["error_step"] = cancel["error_step"]

        return result
