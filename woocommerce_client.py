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

  Lookup strategy (CF timeout = 3600s, generous timeouts):
    1.  /customers?email=              (15s timeout, indexed but server can be slow)
    1b. /customers?search=             (30s timeout, PayPal fallback)
    2a. customer meta_data → /subscriptions/{id}  (15s timeout)
    2b. /subscriptions?customer=       (90s timeout, MOST RELIABLE)
    2c. /subscriptions?billing_email=  (60s timeout per page, up to 5 pages if filter broken)
    2d. /subscriptions?search=         (120s timeout, per_page=100, last resort)
    Worst case: 15s + 30s + 90s + 300s + 120s = 555s (CF=3600s — plenty of room)
"""

import logging
import time
import requests
from datetime import datetime, timezone

log = logging.getLogger("woocommerce")

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
    #  READ — customer lookup                                            #
    # ================================================================== #

    def get_customer_by_email(self, email: str) -> dict | None:
        """
        Return the first WooCommerce customer matching *email* (exact), or None.

        Uses ?email= (fast, indexed). Tries ?role=all first; if the server
        rejects it (4xx), falls back to no role filter.
        If ?role=all returns 200 OK with empty results → email simply doesn't
        exist, no point retrying without role.  Timeout: 15s (server can be slow).
        """
        t0 = time.time()
        for i, params in enumerate([
            {"email": email, "per_page": 1, "role": "all"},
            {"email": email, "per_page": 1},
        ]):
            try:
                resp = requests.get(
                    f"{self.base}/customers",
                    params=params,
                    auth=self.auth,
                    timeout=15,
                )
            except requests.exceptions.RequestException as e:
                log.warning(f"WC customer lookup error for {email}: {e}")
                continue

            if resp.status_code == 401:
                log.error("WooCommerce 401 Unauthorized — check consumer key/secret")
                return None
            if resp.status_code == 404:
                continue
            if not resp.ok:
                log.warning(f"WC customer lookup {resp.status_code} for {email}")
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
        """Direct single-subscription lookup by ID. Usually fast (~0.3s), 15s timeout."""
        try:
            resp = requests.get(
                f"{self.base}/subscriptions/{subscription_id}",
                auth=self.auth,
                timeout=15,
            )
        except requests.exceptions.RequestException as e:
            log.warning(f"WC: direct sub lookup error for #{subscription_id}: {e}")
            return None
        if not resp.ok:
            log.warning(f"WC: direct sub lookup failed #{subscription_id}: {resp.status_code}")
            return None
        return resp.json()

    def _find_subs_by_billing_email(self, email: str) -> list[dict]:
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
                resp = requests.get(
                    f"{self.base}/subscriptions",
                    params={
                        "billing_email": email,
                        "per_page": PER_PAGE,
                        "page": page,
                    },
                    auth=self.auth,
                    timeout=TIMEOUT,
                )
            except requests.exceptions.Timeout:
                log.warning(
                    f"WC: billing_email page {page} timed out ({TIMEOUT}s) "
                    f"for {email}"
                )
                break
            except requests.exceptions.RequestException as e:
                log.warning(f"WC: billing_email lookup error for {email}: {e}")
                break

            if not resp.ok:
                log.warning(
                    f"WC: billing_email lookup failed for {email}: "
                    f"{resp.status_code}"
                )
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
        Return number of orders for a subscription.
        1 order = trial, 2+ orders = paid subscription (at least one renewal).
        """
        try:
            resp = requests.get(
                f"{self.base}/subscriptions/{subscription_id}/orders",
                params={"per_page": 2},
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

        total_header = resp.headers.get("X-WP-Total")
        if total_header is not None:
            try:
                return int(total_header)
            except ValueError:
                pass

        return len(data)

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
        """PUT status=cancelled for a single subscription."""
        if self.dry_run:
            log.info(f"[DRY] WC cancel subscription #{subscription_id}")
            return {
                "status": "dry_run",
                "subscription_id": subscription_id,
                "cancelled": True,
            }

        try:
            resp = requests.put(
                f"{self.base}/subscriptions/{subscription_id}",
                json={"status": "cancelled"},
                auth=self.auth,
                timeout=20,
            )
        except requests.exceptions.RequestException as e:
            log.error(f"WC cancel network error for #{subscription_id}: {e}")
            return {"status": "error", "subscription_id": subscription_id,
                    "cancelled": False, "error": str(e)}

        if not resp.ok:
            log.error(f"WC cancel failed #{subscription_id}: {resp.status_code}")
            return {"status": "error", "subscription_id": subscription_id,
                    "cancelled": False, "error": resp.text[:300]}

        log.info(f"WC: cancelled subscription #{subscription_id}")
        return {"status": "cancelled", "subscription_id": subscription_id,
                "cancelled": True}

    # ================================================================== #
    #  MAIN PUBLIC METHOD                                                #
    # ================================================================== #

    def cancel_subscription(self, email: str) -> dict:
        """
        Find customer → find subscription → cancel.

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
        customer = self.get_customer_by_email(email)
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
                resp = requests.get(
                    f"{self.base}/customers",
                    params={"search": email, "per_page": 5},
                    auth=self.auth,
                    timeout=30,
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
            except requests.exceptions.Timeout:
                log.warning(f"WC: customer ?search= timed out (30s) for {email}")
            except requests.exceptions.RequestException as e:
                log.warning(f"WC: customer ?search= error for {email}: {e}")
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
                        f"WC: trying ?customer={customer_id} (90s timeout)"
                    )
                    resp = requests.get(
                        f"{self.base}/subscriptions",
                        params={"customer": customer_id, "per_page": 10, "status": "any"},
                        auth=self.auth,
                        timeout=90,
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
                except requests.exceptions.Timeout:
                    log.warning(
                        f"WC: ?customer={customer_id} TIMED OUT (90s) for {email}"
                    )
                except requests.exceptions.RequestException as e:
                    log.warning(f"WC: ?customer={customer_id} error: {e}")
                log.info(f"WC TIMING: step2b ?customer= done in {time.time()-t2b:.1f}s (total {time.time()-wc_start:.1f}s)")

        # ── Step 2c: /subscriptions?billing_email= (20s timeout) ─────── #
        # Previously only ran when no customer was found, but Stripe Multi
        # Sync can create subscriptions that are NOT linked to the WC
        # customer_id. In that case ?customer= returns 0 results, yet
        # ?billing_email= finds the subscription by its billing email.
        if not all_subs:
            t2c = time.time()
            billing_subs = self._find_subs_by_billing_email(email)
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
                resp = requests.get(
                    f"{self.base}/subscriptions",
                    params={
                        "search": email,
                        "per_page": _SEARCH_PER_PAGE,
                        "status": "any",
                    },
                    auth=self.auth,
                    timeout=_SEARCH_TIMEOUT,
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
            except requests.exceptions.Timeout:
                log.warning(
                    f"WC: ?search= TIMED OUT ({_SEARCH_TIMEOUT}s) for {email} "
                    "— falling through to Stripe fallback"
                )
            except requests.exceptions.RequestException as e:
                log.warning(f"WC: ?search= error: {e}")
            log.info(f"WC TIMING: step2d ?search= done in {time.time()-t2d:.1f}s (total {time.time()-wc_start:.1f}s)")

        if not all_subs:
            total = time.time() - wc_start
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
            status_label = cancel.get("status", "error")

        return {
            **base_result,
            "status": status_label,
            "cancelled": cancel["cancelled"],
            "subscription_type": sub_type,
            "subscription_id": target["id"],
            "plan": plan or "IQ Test Subscription",
            "order_count": order_count,
            "error": cancel.get("error"),
        }
