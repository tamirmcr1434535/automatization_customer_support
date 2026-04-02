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
    # Read operations (always real, even in dry_run)                       #
    # ------------------------------------------------------------------ #

    def get_customer_by_email(self, email: str) -> dict | None:
        """
        Return the first WooCommerce customer matching *email* (exact), or None.

        Tries two passes:
          1. ?role=all  — finds users of ANY WordPress role (subscriber, customer,
             administrator, etc.).  PayPal subscribers often get the 'subscriber'
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

    def get_subscriptions(self, customer_id: int) -> list[dict]:
        """
        Return active WooCommerce Subscriptions for a given customer ID.

        Filters by status=active,pending-cancel,on-hold,pending to reduce server-side
        query cost (WooCommerce has 179K+ subscriptions — unfiltered queries time out).
        Timeout is 25s because the WooCommerce server is slow under load.
        """
        try:
            resp = requests.get(
                f"{self.base}/subscriptions",
                params={
                    "customer": customer_id,
                    "per_page": 10,
                    "status": "active,pending-cancel,on-hold,pending",
                },
                auth=self.auth,
                timeout=25,
            )
        except requests.exceptions.RequestException as e:
            log.warning(f"WC subscriptions lookup error for customer {customer_id}: {e}")
            return []

        if not resp.ok:
            log.warning(
                f"WC subscriptions lookup failed for customer {customer_id}: {resp.status_code}"
            )
            return []
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
        """
        email_lower = email.lower().strip()
        MAX_PAGES   = 5   # scan up to 5 × 50 = 250 subscriptions per filter

        # ── Pass 1: ?billing_email= with pagination ───────────────────── #
        for page in range(1, MAX_PAGES + 1):
            try:
                resp = requests.get(
                    f"{self.base}/subscriptions",
                    params={"billing_email": email, "per_page": 50, "page": page},
                    auth=self.auth,
                    timeout=10,
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

            # Classify results:
            #  - exact_match  : billing.email == our email  (or meta_data._billing_email)
            #  - empty_email  : billing.email is "" or missing  (server may have filtered correctly
            #                   but doesn't serialize email in list responses — trust it)
            #  - wrong_email  : billing.email is a *different* email  (filter is broken/ignored)
            exact_match  = []
            empty_email  = []
            filter_broken = False

            for s in data:
                if self._subscription_matches_email(s, email_lower):
                    exact_match.append(s)
                else:
                    billing_email_in_response = s.get("billing", {}).get("email", "").strip()
                    if billing_email_in_response:
                        # Server returned a sub with a DIFFERENT non-empty billing email
                        # → server-side filter is broken/ignored for this WC version
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
                # billing.email is empty for ALL results — common for PayPal subs where
                # email lives only in WP post meta _billing_email (not in REST list response).
                # The server-side ?billing_email= filter appears to be working (no wrong emails
                # in the results), so these subscriptions most likely belong to our customer.
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
                break  # server ignores the filter — pagination won't help

            # All 50 had empty email AND filter looks broken? Shouldn't reach here,
            # but break to be safe.
            log.info(
                f"WC: ?billing_email= page {page} returned {len(data)} sub(s), "
                f"none matched {email} — trying next page"
            )

        # ── Pass 2: ?search= (strict validation only) ─────────────────── #
        try:
            resp = requests.get(
                f"{self.base}/subscriptions",
                params={"search": email, "per_page": 50},
                auth=self.auth,
                timeout=15,
            )
        except requests.exceptions.RequestException as e:
            log.warning(f"WC ?search= lookup error for {email}: {e}")
            return []

        if not resp.ok:
            return []

        data = resp.json()
        if not isinstance(data, list):
            return []

        matched = [s for s in data if self._subscription_matches_email(s, email_lower)]
        if matched:
            log.info(f"WC: found {len(matched)} subscription(s) via ?search= for {email}")
        else:
            log.info(f"WC: ?search= returned {len(data)} sub(s) for {email}, none matched")
        return matched

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
        # 1. REST API billing object
        if sub.get("billing", {}).get("email", "").lower().strip() == email_lower:
            return True
        # 2 & 3. WordPress post meta
        for meta in sub.get("meta_data", []):
            if meta.get("key") in ("_billing_email", "billing_email"):
                if meta.get("value", "").lower().strip() == email_lower:
                    return True
        return False

    # ------------------------------------------------------------------ #
    # Trial detection                                                       #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _get_sub_type(subscription: dict) -> str:
        """
        Determine whether subscription is a "trial" or "subscription".

        Priority order:
          0. trial_end_date NOT set (empty / "0000-...") → "subscription"
             A trial ALWAYS has a trial_end_date. If it's missing, the subscription
             never had a free trial — it's a paid subscription (possibly a renewal
             cancellation). This is the fastest and most reliable check.
          1. end_date - start_date > 7 days → "subscription"
             Paid billing cycle exists beyond the trial window.
          2. trial_end_date <= NOW → "subscription" (trial already expired)
          3. trial_end_date in future AND (trial_end - start) <= 7 days → "trial"
          4. trial_end_date in future AND (trial_end - start) > 7 days → "subscription"
          5. No usable dates → "subscription" (safe default)

        Examples (product "IQ Booster 1 Week Trial Then 28 days"):
          - start=March 25, end=April 29 (35d), trial_end="-"
            → no trial_end → "subscription"  (pending-cancel renewal)
          - start=today,    end=0000,          trial_end=in 7 days (7d)
            → trial_end set, future, duration=7d → "trial"
          - start=March 20, end=April 24 (35d), trial_end=March 27 (future)
            → trial_end set → end_date check: 35d > 7 → "subscription"
          - start=30d ago,  end=0000,           trial_end=in 2d (32d since start)
            → trial_end set, future, duration=32d > 7 → "subscription"
        """
        trial_end_raw = (
            subscription.get("trial_end_date_gmt")
            or subscription.get("trial_end_date")
            or ""
        )

        # ── Path 0: no trial_end_date at all → subscription ──────────── #
        # A trial product always has a trial_end_date. If it's absent or zeroed,
        # there was never a free-trial period → this is a paid subscription.
        if not trial_end_raw or trial_end_raw.startswith("0000"):
            log.info("WC sub_type (no trial_end): trial_end_date not set → subscription")
            return "subscription"
        start_raw = (
            subscription.get("start_date_gmt")
            or subscription.get("start_date")
            or ""
        )
        end_raw = (
            subscription.get("end_date_gmt")
            or subscription.get("end_date")
            or ""
        )

        def _parse(s: str):
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

        now = datetime.now(timezone.utc)

        # Parse start_date once (needed by multiple paths below)
        start_dt = None
        if start_raw and not start_raw.startswith("0000"):
            try:
                start_dt = _parse(start_raw)
            except (ValueError, AttributeError) as e:
                log.warning(f"WC: could not parse start_date {start_raw!r}: {e}")

        # ── Path 1: end_date - start_date > 7 days → already a paid sub ─ #
        # Fires when the subscription has an end_date set that is more than
        # 7 days from start, meaning at least one paid billing cycle has occurred.
        if (
            end_raw and not end_raw.startswith("0000")
            and start_dt is not None
        ):
            try:
                end_dt     = _parse(end_raw)
                total_days = (end_dt - start_dt).days
                log.info(
                    f"WC sub_type (end_date path): start={start_dt.date()}, "
                    f"end={end_dt.date()}, total={total_days}d "
                    f"→ {'subscription' if total_days > 7 else 'checking trial_end...'}"
                )
                if total_days > 7:
                    return "subscription"
                # total_days <= 7: still within trial window — fall through
            except (ValueError, AttributeError) as e:
                log.warning(f"WC: could not parse end_date {end_raw!r}: {e}")

        # ── Path 2: trial_end_date check ─────────────────────────────── #
        # trial_end_raw is guaranteed non-empty here (Path 0 already handled missing case)
        if trial_end_raw and not trial_end_raw.startswith("0000"):
            try:
                trial_end_dt = _parse(trial_end_raw)

                # Trial already expired → subscription
                if trial_end_dt <= now:
                    log.info(
                        f"WC sub_type (trial_end expired): "
                        f"trial_end={trial_end_dt.date()} ≤ now → subscription"
                    )
                    return "subscription"

                # Trial still active: check duration
                if start_dt is not None:
                    trial_duration_days = (trial_end_dt - start_dt).days
                    log.info(
                        f"WC sub_type (trial_end future): start={start_dt.date()}, "
                        f"trial_end={trial_end_dt.date()}, duration={trial_duration_days}d "
                        f"→ {'trial' if trial_duration_days <= 7 else 'subscription'}"
                    )
                    return "trial" if trial_duration_days <= 7 else "subscription"
                else:
                    # No start_date but future trial_end → assume trial
                    return "trial"

            except (ValueError, AttributeError) as e:
                log.warning(f"WC: could not parse trial_end {trial_end_raw!r}: {e}")

        # ── Default: no usable date info ─────────────────────────────── #
        return "subscription"

    # ------------------------------------------------------------------ #
    # Write operation                                                       #
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
    # Main public method                                                    #
    # ------------------------------------------------------------------ #

    def cancel_subscription(self, email: str) -> dict:
        """
        Find the customer by email, determine trial vs. paid subscription,
        and cancel appropriately.

        DRY_RUN: still performs real READ operations (lookup), but skips the
        actual cancel write. This way we always know the true subscription state.

        Return dict:
            status : "trial_cancelled" | "subscription_cancelled" |
                     "dry_run" | "not_found" | "no_active_sub" | "error"
        """
        base_result = {
            "email": email,
            "cancelled": False,
            "source": "woocommerce",
            "subscription_type": None,
            "subscription_id": None,
            "plan": "",
        }

        # 1. Customer lookup (always real, even in dry_run)
        log.info(f"[DRY] WC cancel for {email}" if self.dry_run else f"WC cancel for {email}")
        customer = self.get_customer_by_email(email)

        if not customer:
            # Exact email lookup failed — try broader ?search= before giving up on
            # the customer-ID path (covers slight email variations, WP account quirks).
            customer = self.search_customer_by_email(email)

        # 2. Subscriptions (always real)
        if customer:
            all_subs = self.get_subscriptions(customer["id"])
        else:
            # No WP customer account found at all — fall back to searching
            # subscriptions directly by billing email.
            # Covers: guest checkouts, PayPal-only accounts, account email ≠ billing email.
            log.info(
                f"WC: no customer account for {email} — "
                "falling back to billing-email subscription search"
            )
            all_subs = self.get_subscriptions_by_billing_email(email)
            if not all_subs:
                log.info(f"WC: no subscriptions found by billing email for {email}")
                return {**base_result, "status": "not_found"}
        active_subs = [s for s in all_subs if s.get("status") in ACTIVE_STATUSES]

        if not active_subs:
            log.info(f"WC: no active subscriptions for {email}")
            return {**base_result, "status": "no_active_sub"}

        # 3. Determine type for each active sub and select by priority:
        #    pending-cancel (paid) → active (paid) → pending-cancel (trial) → active (trial)
        #
        # Rationale: a customer asking to cancel almost certainly means their CURRENT
        # paid subscription. If they also have a fresh trial (e.g. signed up again
        # on the same email), we should NOT cancel the trial instead of the paid sub.
        typed_subs = [
            (s, self._get_sub_type(s)) for s in active_subs
        ]

        def _select_priority(entry: tuple) -> tuple:
            sub, sub_type = entry
            status = sub.get("status", "")
            # Lower tuple = higher priority (sort ascending)
            if status == "pending-cancel" and sub_type == "subscription":
                return (0,)   # highest: already-requested paid cancellation
            elif status == "active" and sub_type == "subscription":
                return (1,)   # active paid subscription
            elif status == "pending-cancel" and sub_type == "trial":
                return (2,)   # pending-cancel trial
            elif sub_type == "subscription":
                return (3,)   # other status, paid
            elif sub_type == "trial":
                return (4,)   # trial — lowest priority
            else:
                return (5,)

        typed_subs.sort(key=_select_priority)
        target, sub_type = typed_subs[0]

        plan = ""
        line_items = target.get("line_items") or []
        if line_items:
            plan = line_items[0].get("name", "")

        # 4. Cancel (skipped in dry_run, but we still return real sub info)
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
