"""
WooCommerce Subscriptions Client
=================================
Handles trial and subscription cancellations via WooCommerce REST API v3.
Requires the "WooCommerce Subscriptions" plugin to be active on the site.

IMPORTANT — performance notes for iqbooster.org:
  FAST endpoints (< 1s):
    GET /customers?email=             — exact email lookup, indexed
    GET /subscriptions/{id}           — direct single-row lookup
    GET /subscriptions?billing_email= — server-side billing email filter

  SLOW endpoints (8–30s):
    GET /subscriptions?search=        — full-text search, not indexed
    GET /customers?search=            — full-text search, not indexed
    GET /subscriptions?customer=      — customer_id filter, not indexed

  Lookup strategy (ordered by speed):
    1. /customers?email=         → meta_data subscription_id → /subscriptions/{id}
    2. /subscriptions?billing_email=
    3. /subscriptions?search=    (last resort, 20s timeout — better than asking card digits)
"""

import logging
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

        Uses only ?email= (fast, indexed). Two passes for role compatibility:
        1. ?role=all — finds users of ANY WordPress role (subscriber, customer,
           administrator, etc.).
        2. Default (no role filter) — fallback for WC versions that reject role=all.

        Does NOT use ?search= — it's a full-text scan that times out on this server.
        """
        for params in [
            {"email": email, "per_page": 1, "role": "all"},
            {"email": email, "per_page": 1},
        ]:
            try:
                resp = requests.get(
                    f"{self.base}/customers",
                    params=params,
                    auth=self.auth,
                    timeout=10,
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
                log.info(f"WC: found customer for {email} (id={data[0]['id']})")
                return data[0]

        return None

    # ================================================================== #
    #  READ — subscription lookup (fast paths only)                      #
    # ================================================================== #

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
        """Direct single-subscription lookup by ID. Always fast (~0.3s)."""
        try:
            resp = requests.get(
                f"{self.base}/subscriptions/{subscription_id}",
                auth=self.auth,
                timeout=10,
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
        Search subscriptions by ?billing_email= ONLY (no ?search= fallback).

        ?billing_email= is fast (~1s) because WC Subscriptions plugin filters
        server-side. ?search= is a full-text scan that times out — not used.

        Accepts:
        - Subscriptions with exact billing.email match
        - Subscriptions with empty billing.email (WC stores email in _billing_email
          post meta which isn't in the REST response — trust server filter)
        """
        email_lower = email.lower().strip()
        try:
            resp = requests.get(
                f"{self.base}/subscriptions",
                params={"billing_email": email, "per_page": 50},
                auth=self.auth,
                timeout=15,
            )
        except requests.exceptions.RequestException as e:
            log.warning(f"WC: billing_email lookup error for {email}: {e}")
            return []

        if not resp.ok:
            log.warning(f"WC: billing_email lookup failed for {email}: {resp.status_code}")
            return []

        data = resp.json()
        if not isinstance(data, list) or not data:
            return []

        exact = []
        trusted_empty = []
        has_wrong_email = False

        for s in data:
            if self._subscription_matches_email(s, email_lower):
                exact.append(s)
            else:
                be = s.get("billing", {}).get("email", "").strip()
                if be:
                    has_wrong_email = True
                else:
                    trusted_empty.append(s)

        if exact:
            log.info(
                f"WC: billing_email found {len(exact)} exact match(es) for {email}"
            )
            return exact

        if trusted_empty and not has_wrong_email:
            log.info(
                f"WC: billing_email returned {len(trusted_empty)} sub(s) with empty "
                f"billing.email for {email} — trusting server filter"
            )
            return trusted_empty

        if has_wrong_email:
            log.info(
                f"WC: billing_email returned subs with wrong emails for {email} "
                "— server filter broken, discarding results"
            )

        return []

    def _find_subs_by_search(self, email: str) -> list[dict]:
        """
        Last-resort subscription search using ?search= (full-text).

        This endpoint is SLOW (8-30s) because WC does a full database scan.
        We use it only when faster lookups (?email=, ?billing_email=) failed.

        To reduce false positives, we verify that each returned subscription
        actually matches the target email (via billing.email or meta_data).
        If billing.email is empty (common with post-meta-only storage),
        we fetch the individual subscription detail to check _billing_email.
        """
        email_lower = email.lower().strip()
        # Use email username part for search to increase match chances
        # (WC search matches against multiple fields including name, email, etc.)
        try:
            resp = requests.get(
                f"{self.base}/subscriptions",
                params={"search": email, "per_page": 10},
                auth=self.auth,
                timeout=20,  # generous timeout — this endpoint is slow
            )
        except requests.exceptions.Timeout:
            log.warning(f"WC: ?search= timed out for {email} (expected on this server)")
            return []
        except requests.exceptions.RequestException as e:
            log.warning(f"WC: ?search= error for {email}: {e}")
            return []

        if not resp.ok:
            log.warning(f"WC: ?search= failed for {email}: {resp.status_code}")
            return []

        data = resp.json()
        if not isinstance(data, list) or not data:
            return []

        log.info(f"WC: ?search= returned {len(data)} result(s) for {email}")

        # Verify email match — ?search= can return false positives
        matched = []
        for s in data:
            if self._subscription_matches_email(s, email_lower):
                matched.append(s)
                continue

            # billing.email might be empty — check detail endpoint for _billing_email
            be = s.get("billing", {}).get("email", "").strip()
            if not be:
                detail = self._get_subscription_by_id(s["id"])
                if detail and self._subscription_matches_email(detail, email_lower):
                    matched.append(detail)
                    continue

        if matched:
            log.info(
                f"WC: ?search= found {len(matched)} verified match(es) for {email}"
            )
        else:
            log.info(
                f"WC: ?search= returned results but none matched {email} — discarding"
            )

        return matched

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
                timeout=10,
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
                timeout=10,
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

        Uses ONLY fast WC API endpoints:
          1. /customers?email=         (~0.3s)
          2. /subscriptions/{id}       (~0.3s) — via meta_data subscription_id
          3. /subscriptions?billing_email= (~1s)

        Does NOT use slow endpoints that timeout on this server:
          ✗ /customers?search=         (10s+ timeout)
          ✗ /subscriptions?search=     (8s+ timeout)
          ✗ /subscriptions?customer=   (15s+ timeout)

        If subscription not found → returns "not_found" → bot asks customer
        for last 4 card digits → Stripe finds email → we try again.
        """
        base_result = {
            "email": email,
            "cancelled": False,
            "source": "woocommerce",
            "subscription_type": None,
            "subscription_id": None,
            "plan": "",
        }

        log.info(f"{'[DRY] ' if self.dry_run else ''}WC cancel for {email}")

        # ── Step 1: customer lookup (fast, ~0.3s) ─────────────────────── #
        customer = self.get_customer_by_email(email)
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

        # ── Step 2b: billing_email query (~1s) ────────────────────────── #
        if not all_subs:
            billing_subs = self._find_subs_by_billing_email(email)
            if billing_subs:
                log.info(
                    f"WC: found {len(billing_subs)} sub(s) via billing_email"
                )
                all_subs = billing_subs

        # ── Step 2c: ?search= last resort (slow, 8–30s) ────────────── #
        # WC admin finds subscriptions via full-text ?search= which checks
        # billing name, email stored in _billing_email post meta, order
        # notes, etc. — fields that ?billing_email= and ?email= miss.
        # This endpoint is slow (8-30s on this server), so we only try it
        # as a last resort with a generous timeout. Better than asking the
        # customer for card digits and adding days of delay.
        if not all_subs:
            search_subs = self._find_subs_by_search(email)
            if search_subs:
                log.info(
                    f"WC: found {len(search_subs)} sub(s) via ?search= fallback"
                )
                all_subs = search_subs

        if not all_subs:
            if customer:
                log.info(f"WC: customer found for {email} but no subscription")
                return {**base_result, "status": "no_active_sub"}
            else:
                log.info(f"WC: no customer and no subscription for {email}")
                return {**base_result, "status": "not_found"}

        # ── Step 3: filter active subscriptions ───────────────────────── #
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
