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

    # ------------------------------------------------------------------ #
    # Trial detection                                                       #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _get_sub_type(subscription: dict) -> str:
        """
        Determine whether subscription is a "trial" or "subscription".

        Logic:
          1. trial_end_date must exist and be in the future (still in trial).
          2. trial_end_date - start_date <= 7 days  → "trial"  (1-week trial period)
             trial_end_date - start_date  > 7 days  → "subscription" (already a paying sub)
          3. No trial_end_date, or trial already ended → "subscription"

        Examples (product "WW IQ Test 1 Week Trial Then 28 days"):
          - start=2 days ago, trial_end=in 5 days  →  delta=7d  → "trial"
          - start=30 days ago, trial_end=in 2 days →  delta=32d → "subscription"
          - start=1 day ago,   trial_end="-"       →             → "subscription"
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

        def _parse(s: str):
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

        # ── Path A: trial_end_date is set ─────────────────────────────── #
        if trial_end_raw and not trial_end_raw.startswith("0000"):
            try:
                trial_end_dt = _parse(trial_end_raw)
                now = datetime.now(timezone.utc)

                # Trial already expired → subscription
                if trial_end_dt <= now:
                    return "subscription"

                if not start_raw or start_raw.startswith("0000"):
                    return "trial"  # no start date but future trial_end → trial

                start_dt = _parse(start_raw)
                trial_duration_days = (trial_end_dt - start_dt).days
                log.info(
                    f"WC trial check (via trial_end): start={start_dt.date()}, "
                    f"trial_end={trial_end_dt.date()}, duration={trial_duration_days}d"
                )
                return "trial" if trial_duration_days <= 7 else "subscription"

            except (ValueError, AttributeError) as e:
                log.warning(f"Could not parse trial_end: {trial_end_raw!r} — {e}")

        # ── Path B: trial_end_date is empty (e.g. pending-cancel clears it) ─ #
        # Fall back to: end_date - start_date <= 7 days → trial
        end_raw = (
            subscription.get("end_date_gmt")
            or subscription.get("end_date")
            or ""
        )
        if (
            end_raw and not end_raw.startswith("0000")
            and start_raw and not start_raw.startswith("0000")
        ):
            try:
                end_dt   = _parse(end_raw)
                start_dt = _parse(start_raw)
                total_days = (end_dt - start_dt).days
                log.info(
                    f"WC trial check (via end_date fallback): start={start_dt.date()}, "
                    f"end={end_dt.date()}, total={total_days}d"
                )
                # <= 7 days total window → 1-week trial product
                return "trial" if total_days <= 7 else "subscription"

            except (ValueError, AttributeError) as e:
                log.warning(f"Could not parse end/start dates: {e}")

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
            log.info(f"WC: no customer found for {email}")
            return {**base_result, "status": "not_found"}

        # 2. Subscriptions (always real)
        all_subs = self.get_subscriptions(customer["id"])
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
