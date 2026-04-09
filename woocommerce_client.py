"""
WooCommerce Subscriptions Client
=================================
Handles trial and subscription cancellations via WooCommerce REST API v3.
Requires the "WooCommerce Subscriptions" plugin to be active on the site.

Logic:
 1. Find customer by email
 2. Fetch their subscriptions
 3. If any subscription has an active trial period → cancel trial
 4. Otherwise → cancel active subscription
 5. Falls back gracefully if customer or subscription not found
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

    # ------------------------------------------------------------------ #
    # Read operations (always real, even in dry_run) #
    # ------------------------------------------------------------------ #

    def get_customer_by_email(self, email: str) -> dict | None:
        """
        Return the first WooCommerce customer matching *email* (exact), or None.

        Tries two passes:
        1. ?role=all — finds users of ANY WordPress role (subscriber, customer,
           administrator, etc.). PayPal subscribers often get the 'subscriber'
           role rather than 'customer', so the default endpoint misses them.
        2. Default (no role filter) — fallback for WC versions that reject role=all.
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
                log.error("WooCommerce 401 Unauthorized — check WOO_CONSUMER_KEY / WOO_CONSUMER_SECRET in Secret Manager")
                return None
            if resp.status_code == 404:
                continue
            if not resp.ok:
                log.warning(f"WC customer lookup failed for {email} (params={params}): {resp.status_code}")
                continue

            data = resp.json()
            if data:
                log.info(f"WC: found customer for {email} via params={list(params.keys())} (id={data[0]['id']})")
                return data[0]

        return None

    def search_customer_by_email(self, email: str) -> dict | None:
        """
        Broader customer search via ?search= — handles edge cases where
        ?email= fails (different casing, partial WP account, etc.).
        Validates the returned customer's email matches.
        """
        try:
            resp = requests.get(
                f"{self.base}/customers",
                params={"search": email, "per_page": 10},
                auth=self.auth,
                timeout=10,
            )
        except requests.exceptions.RequestException as e:
            log.warning(f"WC customer search error for {email}: {e}")
            return None

        if not resp.ok:
            return None

        data = resp.json()
        if not isinstance(data, list):
            return None

        email_lower = email.lower().strip()
        for customer in data:
            if customer.get("email", "").lower().strip() == email_lower:
                log.info(f"WC: found customer via ?search= for {email} (id={customer['id']})")
                return customer

        return None

    def get_subscriptions(self, customer_id: int) -> list[dict] | None:
        """
        Return WooCommerce Subscriptions for a given customer ID.

        Return values:
        list[dict] — subscriptions found (may include non-active ones)
        [] — confirmed empty: server responded OK with zero results
             on BOTH passes (status-filtered + unfiltered)
        None — uncertain: at least one pass timed out or errored.
               Caller will ask for card digits rather than assuming no subscription exists.

        Tries two passes:
        1. With status filter (active,pending-cancel,on-hold,pending) — fast path.
           If the server returns empty (filter may be ignored), falls through to pass 2.
           On timeout → returns None immediately (no retry — fail fast to card digits).
        2. Without status filter — catches all statuses when pass 1 returned empty OK.
           On timeout → returns None.

        TIMEOUT POLICY: 15 seconds, no retry.
        The WC server is slow; waiting 30s × 2 = 60s per ticket before falling back
        to card digits makes the bot unacceptably slow and creates timeouts in tests.
        15s is generous enough for a healthy WC server; if it times out, we ask for
        card digits immediately rather than waiting further.
        """
        # Pass 1: status-filtered query (lighter server load)
        try:
            resp = requests.get(
                f"{self.base}/subscriptions",
                params={
                    "customer": customer_id,
                    "per_page": 10,
                    "status": "active,pending-cancel,on-hold,pending",
                },
                auth=self.auth,
                timeout=15,
            )
            if resp.ok:
                data = resp.json()
                if data:
                    return data
                # Empty result — status filter may be unsupported. Fall through to pass 2.
                log.info(
                    f"WC: status-filtered query returned 0 subs for customer {customer_id} "
                    "— retrying without status filter"
                )
            else:
                log.warning(
                    f"WC subscriptions lookup failed for customer {customer_id}: {resp.status_code}"
                )
                # Fall through to pass 2 anyway — different params might succeed

        except requests.exceptions.Timeout:
            log.warning(
                f"WC subscriptions lookup TIMED OUT for customer {customer_id} (pass 1, 15s) "
                "— returning timeout so caller can ask for card digits"
            )
            return None  # signal: uncertain — ask card digits

        except requests.exceptions.RequestException as e:
            log.warning(
                f"WC subscriptions lookup error for customer {customer_id} (pass 1): {e}"
                " — retrying without status filter"
            )
            # Fall through to pass 2

        # Pass 2: unfiltered query — returns all statuses, Python will filter
        try:
            resp = requests.get(
                f"{self.base}/subscriptions",
                params={"customer": customer_id, "per_page": 20},
                auth=self.auth,
                timeout=15,
            )
        except requests.exceptions.Timeout:
            log.warning(
                f"WC subscriptions lookup TIMED OUT for customer {customer_id} (pass 2, 15s)"
            )
            return None  # uncertain — don't treat as confirmed empty
        except requests.exceptions.RequestException as e:
            log.warning(f"WC subscriptions lookup error for customer {customer_id} (pass 2): {e}")
            return None

        if not resp.ok:
            log.warning(
                f"WC subscriptions lookup failed for customer {customer_id}: {resp.status_code}"
            )
            return None
        return resp.json()

    def get_subscriptions_by_billing_email(self, email: str) -> list[dict]:
        """
        Search subscriptions directly by billing email.

        Fallback for cases where the customer's WooCommerce account email differs
        from the billing email on the subscription (e.g. guest checkout, or the
        customer changed their account email after subscribing).

        Strategy:
        1. ?billing_email= with pagination (up to MAX_PAGES × 50 results).
           WooCommerce Subscriptions plugin should filter server-side.
           We trust this filter partially: subscriptions whose billing.email
           matches are kept (strict); subscriptions whose billing.email is
           EMPTY are also kept — WC often stores the email in WordPress post
           meta (_billing_email) which is NOT returned in list API responses,
           so billing.email is blank even for matching subscriptions.
           If we see any subscription with a *non-empty, non-matching* billing
           email we know the server-side filter is broken → reject whole page.
        2. ?search= as secondary fallback (strict email validation only).

        IMPORTANT: Always call cancel_subscription() via customer_id when possible
        (?customer= filter) — that path is reliable. This method is last-resort.

        Timeouts: billing_email pass = 15s, ?search= pass = 8s (fast-fail fallback).
        """
        email_lower = email.lower().strip()
        MAX_PAGES = 2  # scan up to 2 × 50 = 100 subscriptions per filter

        # ── Pass 1: ?billing_email= with pagination ───────────────────── #
        for page in range(1, MAX_PAGES + 1):
            try:
                resp = requests.get(
                    f"{self.base}/subscriptions",
                    params={"billing_email": email, "per_page": 50, "page": page},
                    auth=self.auth,
                    timeout=15,
                )
            except requests.exceptions.RequestException as e:
                log.warning(f"WC billing_email lookup error for {email} page={page}: {e}")
                break

            if not resp.ok:
                log.warning(f"WC billing_email lookup failed for {email}: {resp.status_code}")
                break

            data = resp.json()
            if not isinstance(data, list) or not data:
                break  # no more pages

            exact_match = []
            empty_email = []
            filter_broken = False

            for s in data:
                if self._subscription_matches_email(s, email_lower):
                    exact_match.append(s)
                else:
                    billing_email_in_response = s.get("billing", {}).get("email", "").strip()
                    if billing_email_in_response:
                        filter_broken = True
                    else:
                        empty_email.append(s)

            if exact_match:
                log.info(
                    f"WC: found {len(exact_match)} subscription(s) with exact billing email "
                    f"match for {email} (page {page}, total returned {len(data)})"
                )
                return exact_match

            if empty_email and not filter_broken:
                log.info(
                    f"WC: ?billing_email= returned {len(empty_email)} subscription(s) with "
                    f"empty billing.email for {email} page={page} — trusting server filter "
                    f"(PayPal/post-meta-only billing email pattern)"
                )
                return empty_email

            if filter_broken:
                log.info(
                    f"WC: ?billing_email= filter appears broken for {email} "
                    f"(page {page} returned subscriptions with different billing emails) — "
                    "not trusting results, skipping to ?search="
                )
                break

            log.info(
                f"WC: ?billing_email= page {page} returned {len(data)} sub(s), "
                f"none matched {email} — trying next page"
            )

        # ── Pass 2: ?search= with individual-fetch fallback ───────────── #
        try:
            resp = requests.get(
                f"{self.base}/subscriptions",
                params={"search": email, "per_page": 50},
                auth=self.auth,
                timeout=8,  # fast-fail: ?search= is last-resort fallback
            )
        except requests.exceptions.RequestException as e:
            log.warning(f"WC ?search= lookup error for {email}: {e}")
            return []

        if not resp.ok:
            return []

        data = resp.json()
        if not isinstance(data, list):
            return []

        matched = []
        for s in data:
            if self._subscription_matches_email(s, email_lower):
                matched.append(s)
            elif not s.get("billing", {}).get("email", "").strip():
                sub_id = s.get("id")
                if not sub_id:
                    continue

                try:
                    detail_resp = requests.get(
                        f"{self.base}/subscriptions/{sub_id}",
                        auth=self.auth,
                        timeout=10,
                    )
                    if detail_resp.ok:
                        detail = detail_resp.json()
                        if self._subscription_matches_email(detail, email_lower):
                            log.info(
                                f"WC: ?search= sub #{sub_id} matched {email} "
                                f"via individual detail fetch (meta_data._billing_email)"
                            )
                            matched.append(detail)
                except requests.exceptions.RequestException as e:
                    log.warning(f"WC: detail fetch error for sub #{sub_id}: {e}")

        if matched:
            log.info(f"WC: found {len(matched)} subscription(s) via ?search= for {email}")
        else:
            log.info(f"WC: ?search= returned {len(data)} sub(s) for {email}, none matched after detail-fetch check")
        return matched

    def get_order_count(self, subscription_id: int) -> int | None:
        """
        Return the number of orders (initial + renewals) attached to a subscription.

        We only need to know whether count == 1 or > 1, so per_page=2 is enough:
        - 1 result → orders == 1 (customer signed up but no renewal charged yet)
        - 2 results → orders >= 2 (at least one renewal → definitely a paid subscription)

        Returns None on timeout or API error (caller falls back to date-only logic).
        """
        try:
            resp = requests.get(
                f"{self.base}/subscriptions/{subscription_id}/orders",
                params={"per_page": 2},
                auth=self.auth,
                timeout=10,
            )
        except requests.exceptions.RequestException as e:
            log.warning(f"WC: order count lookup error for sub #{subscription_id}: {e}")
            return None

        if not resp.ok:
            log.warning(
                f"WC: order count lookup failed for sub #{subscription_id}: {resp.status_code}"
            )
            return None

        data = resp.json()
        if not isinstance(data, list):
            return None

        total_header = resp.headers.get("X-WP-Total")
        if total_header is not None:
            try:
                count = int(total_header)
                log.info(
                    f"WC: sub #{subscription_id} has {count} order(s) "
                    f"(via X-WP-Total header)"
                )
                return count
            except ValueError:
                pass

        count = len(data)
        log.info(
            f"WC: sub #{subscription_id} has {count} order(s) "
            f"(fetched up to 2, no X-WP-Total header)"
        )
        return count

    @staticmethod
    def _subscription_matches_email(sub: dict, email_lower: str) -> bool:
        """
        Check if a subscription is associated with the given email (lowercased).

        Checks in order:
        1. billing.email (REST API billing address field)
        2. meta_data._billing_email (WordPress post meta — the canonical store,
           sometimes differs from billing.email for PayPal subscriptions)
        3. meta_data.billing_email (alternate meta key used by some plugins)
        """
        if sub.get("billing", {}).get("email", "").lower().strip() == email_lower:
            return True

        for meta in sub.get("meta_data", []):
            if meta.get("key") in ("_billing_email", "billing_email"):
                if meta.get("value", "").lower().strip() == email_lower:
                    return True

        return False

    # ------------------------------------------------------------------ #
    # Trial detection #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _get_sub_type(subscription: dict, order_count: int | None = None) -> str:
        """
        Determine whether subscription is a "trial" or "subscription".

        Primary rules (order_count + days_since_start are the reliable signals):
        0. order_count > 1 → "subscription"
           Can't be a trial if more than one order has been charged.
        1. order_count ≤ 1 AND days_since_start ≤ 8 → "trial"
           Single order, signed up within 8 days → genuine free trial.
           NOTE: trial_end_date is intentionally NOT checked here because WC
           does not always populate it for Stripe Multi Sync subscriptions.
        2. order_count ≤ 1 AND days_since_start > 8 → "subscription"
           Been active too long to be a fresh trial.

        Fallback (no start_date available):
        3. trial_end_date set AND order_count == 1 → "trial"
        4. Everything else → "subscription" (safe default)
        """
        if order_count is not None and order_count > 1:
            log.info(f"WC sub_type: order_count={order_count} > 1 → subscription")
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
            except (ValueError, AttributeError) as e:
                log.warning(f"WC: could not parse start_date {start_raw!r}: {e}")

        if start_dt is not None:
            days_since_start = (now - start_dt).days
            is_trial = (order_count is None or order_count <= 1) and days_since_start <= 8
            log.info(
                f"WC sub_type: order_count={order_count}, "
                f"days_since_start={days_since_start} "
                f"→ {'trial' if is_trial else 'subscription'}"
            )
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
                    log.info(
                        f"WC sub_type: no start_date, trial_end in future "
                        f"({trial_end_raw}), order_count≤1 → trial"
                    )
                    return "trial"
                else:
                    log.info(
                        f"WC sub_type: no start_date, trial_end already past "
                        f"({trial_end_raw}) → subscription"
                    )
            except (ValueError, AttributeError) as e:
                log.warning(f"WC: could not parse trial_end in fallback {trial_end_raw!r}: {e}")

        log.info("WC sub_type: no usable start_date → subscription (safe default)")
        return "subscription"

    # ------------------------------------------------------------------ #
    # Write operation #
    # ------------------------------------------------------------------ #

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
            return {
                "status": "error",
                "subscription_id": subscription_id,
                "cancelled": False,
                "error": str(e),
            }

        if not resp.ok:
            log.error(
                f"WC cancel failed for #{subscription_id}: {resp.status_code} {resp.text[:200]}"
            )
            return {
                "status": "error",
                "subscription_id": subscription_id,
                "cancelled": False,
                "error": resp.text[:300],
            }

        log.info(f"WC: cancelled subscription #{subscription_id}")
        return {
            "status": "cancelled",
            "subscription_id": subscription_id,
            "cancelled": True,
        }

    # ------------------------------------------------------------------ #
    # Main public method #
    # ------------------------------------------------------------------ #

    def cancel_subscription(self, email: str) -> dict:
        """
        Find the customer by email, determine trial vs. paid subscription,
        and cancel appropriately.

        DRY_RUN: still performs real READ operations (lookup), but skips the
        actual cancel write. This way we always know the true subscription state.

        Return dict:
        status : "trial_cancelled" | "subscription_cancelled" |
                 "dry_run" | "not_found" | "no_active_sub" | "timeout" | "error"

        "timeout" — WC subscription lookup timed out; caller should try Stripe
        directly without assuming the customer has no subscription.
        """
        base_result = {
            "email": email,
            "cancelled": False,
            "source": "woocommerce",
            "subscription_type": None,
            "subscription_id": None,
            "plan": "",
        }

        log.info(f"[DRY] WC cancel for {email}" if self.dry_run else f"WC cancel for {email}")

        # ── Step 1: get customer account (fast, 0.3s) ─────────────────── #
        customer = self.get_customer_by_email(email)
        if not customer:
            customer = self.search_customer_by_email(email)

        customer_id = customer["id"] if customer else None

        # ── Step 2: subscription lookup ────────────────────────────────── #
        #
        # ORDER MATTERS — tested on production data:
        #   /subscriptions?customer=ID      → up to 21s, often returns 0 (WC
        #                                     stores subs by billing email, not
        #                                     customer account)
        #   /subscriptions?billing_email=X  → ~1s, returns correct results
        #
        # Strategy (fastest-first):
        # 2a. billing_email query   — fast & reliable for this WC setup
        # 2b. customer_id query     — fallback only if billing_email returned empty
        #     (covers edge case where sub is attached to account, not billing email)
        # 2c. If customer_id query times out → return "timeout"

        all_subs = self.get_subscriptions_by_billing_email(email)

        if not all_subs and customer_id is not None:
            log.info(
                f"WC: billing_email returned 0 subs for {email} — "
                f"trying customer_id={customer_id} as fallback"
            )
            id_subs = self.get_subscriptions(customer_id)

            if id_subs is None:
                log.warning(
                    f"WC: subscription lookup timed out for customer #{customer_id} ({email})"
                )
                return {**base_result, "status": "timeout"}

            if id_subs:
                log.info(
                    f"WC: found {len(id_subs)} subscription(s) via customer_id fallback "
                    f"for {email}"
                )
                all_subs = id_subs

        if not all_subs:
            log.info(f"WC: no subscriptions found for {email}")
            return {**base_result, "status": "not_found"}

        active_subs = [s for s in all_subs if s.get("status") in ACTIVE_STATUSES]

        if not active_subs:
            cancelled_subs = [s for s in all_subs if s.get("status") == "cancelled"]
            if cancelled_subs:
                cancelled_subs.sort(
                    key=lambda s: s.get("start_date_gmt") or s.get("start_date") or "",
                    reverse=True,
                )
                target = cancelled_subs[0]
                order_count = self.get_order_count(target["id"])
                sub_type = self._get_sub_type(target, order_count=order_count)
                plan = ""
                line_items = target.get("line_items") or []
                if line_items:
                    plan = line_items[0].get("name", "")

                log.info(
                    f"WC: no active subs for {email} — "
                    f"found already-cancelled sub #{target['id']} "
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

            log.info(f"WC: no active subscriptions for {email}")
            return {**base_result, "status": "no_active_sub"}

        typed_subs = []
        for s in active_subs:
            order_count = self.get_order_count(s["id"])
            typed_subs.append((s, self._get_sub_type(s, order_count=order_count)))

        def _select_priority(entry: tuple) -> tuple:
            sub, sub_type = entry
            status = sub.get("status", "")
            if status == "pending-cancel" and sub_type == "subscription":
                return (0,)
            elif status == "active" and sub_type == "subscription":
                return (1,)
            elif status == "pending-cancel" and sub_type == "trial":
                return (2,)
            elif sub_type == "subscription":
                return (3,)
            elif sub_type == "trial":
                return (4,)
            else:
                return (5,)

        typed_subs.sort(key=_select_priority)
        target, sub_type = typed_subs[0]

        plan = ""
        line_items = target.get("line_items") or []
        if line_items:
            plan = line_items[0].get("name", "")

        cancel = self._cancel_sub_by_id(target["id"])

        if self.dry_run:
            status_label = "dry_run"
        elif cancel["cancelled"]:
            status_label = "trial_cancelled" if sub_type == "trial" else "subscription_cancelled"
        else:
            status_label = cancel.get("status", "error")

        return {
            **base_result,
            "status": status_label,
            "cancelled": cancel["cancelled"],
            "subscription_type": sub_type,
            "subscription_id": target["id"],
            "plan": plan or "IQ Test Subscription",
            "error": cancel.get("error"),
        }
