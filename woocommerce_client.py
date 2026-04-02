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
        """Return the first WooCommerce customer matching *email*, or None."""
        try:
            resp = requests.get(
                f"{self.base}/customers",
                params={"email": email, "per_page": 1},
                auth=self.auth,
                timeout=10,
            )
        except requests.exceptions.RequestException as e:
            log.warning(f"WC customer lookup error for {email}: {e}")
            return None

        if resp.status_code == 401:
            log.error("WooCommerce 401 Unauthorized — check WOO_CONSUMER_KEY / WOO_CONSUMER_SECRET in Secret Manager")
            return None
        if resp.status_code == 404:
            return None
        if not resp.ok:
            log.warning(f"WC customer lookup failed for {email}: {resp.status_code}")
            return None
        data = resp.json()
        return data[0] if data else None

    def get_subscriptions(self, customer_id: int) -> list[dict]:
        """Return all WooCommerce Subscriptions for a given customer ID."""
        try:
            resp = requests.get(
                f"{self.base}/subscriptions",
                params={"customer": customer_id, "per_page": 20},
                auth=self.auth,
                timeout=10,
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

        Tries ?billing_email= first (WooCommerce Subscriptions plugin filter),
        then ?search= as a secondary fallback.
        """
        for params in [
            {"billing_email": email, "per_page": 10},
            {"search": email,        "per_page": 10},
        ]:
            try:
                resp = requests.get(
                    f"{self.base}/subscriptions",
                    params=params,
                    auth=self.auth,
                    timeout=10,
                )
            except requests.exceptions.RequestException as e:
                log.warning(f"WC billing-email lookup error for {email} (params={params}): {e}")
                continue

            if resp.ok:
                data = resp.json()
                if isinstance(data, list) and data:
                    log.info(
                        f"WC: found {len(data)} subscription(s) by billing email "
                        f"({list(params.keys())[0]}={email})"
                    )
                    return data

        return []

    # ------------------------------------------------------------------ #
    # Trial detection                                                       #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _get_sub_type(subscription: dict) -> str:
        """
        Determine whether subscription is a "trial" or "subscription".

        Priority order:
          1. end_date - start_date > 7 days → "subscription"
             The subscription has a paid billing cycle beyond the trial window,
             meaning the customer was already charged. This is the most reliable
             signal regardless of what trial_end_date says.
          2. trial_end_date <= NOW → "subscription" (trial already expired)
          3. trial_end_date in future AND (trial_end - start) <= 7 days → "trial"
          4. No usable dates → "subscription" (safe default)

        Examples (product "IQ Booster 1 Week Trial Then 28 days"):
          - start=March 20, end=April 24 (35d), trial_end=March 27 future
            → 35 > 7 → "subscription"   ← was wrong before (returned "trial")
          - start=today,    end=0000,     trial_end=in 7 days (7d)
            → no end_date → trial_end in future, duration=7d → "trial"
          - start=30d ago,  end=0000,     trial_end=in 2d (32d)
            → no end_date → trial_end in future, but duration=32d > 7 → "subscription"
          - start=2d ago,   end=0000,     trial_end="-"
            → no end_date, no trial_end → "subscription" (default)
        """
        trial_end_raw = (
            subscription.get("trial_end_date_gmt")
            or subscription.get("trial_end_date")
            or ""
        )
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

        # ── Path 1.5: days since start > 7 AND no trial_end → subscription ─ #
        # Safety net for cases where the API returns end_date as "0000-00-00"
        # (e.g. some WooCommerce Subscriptions plugin versions don't populate
        # end_date_gmt for pending-cancel subscriptions).
        # If start_date was more than 7 days ago AND there is no trial_end_date,
        # the trial window has definitely closed → paid subscription.
        if start_dt is not None:
            if not (trial_end_raw and not trial_end_raw.startswith("0000")):
                # no trial_end info available at all
                days_since_start = (now - start_dt).days
                log.info(
                    f"WC sub_type (days-since-start path): {days_since_start}d since start, "
                    f"no trial_end → {'subscription' if days_since_start > 7 else 'trial'}"
                )
                if days_since_start > 7:
                    return "subscription"

        # ── Path 2: trial_end_date check ─────────────────────────────── #
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

        # 2. Subscriptions (always real)
        if customer:
            all_subs = self.get_subscriptions(customer["id"])
        else:
            # Customer account not found by email — try searching subscriptions by billing email.
            # This covers: guest checkouts, account email ≠ billing email,
            # or customers whose WordPress account was created with a different address.
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

        # 3. Determine type for each active sub; prefer trial over paid sub
        typed_subs = [
            (s, self._get_sub_type(s)) for s in active_subs
        ]
        # Pick a trial sub first; if none — pick the first active sub
        trial_entry = next(((s, t) for s, t in typed_subs if t == "trial"), None)
        target, sub_type = trial_entry if trial_entry else typed_subs[0]

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
