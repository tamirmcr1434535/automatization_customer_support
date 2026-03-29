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
    #  Read operations (always real, even in dry_run)                      #
    # ------------------------------------------------------------------ #

    def get_customer_by_email(self, email: str) -> dict | None:
        """Return the first WooCommerce customer matching *email*, or None."""
        resp = requests.get(
            f"{self.base}/customers",
            params={"email": email, "per_page": 1},
            auth=self.auth,
            timeout=10,
        )
        if resp.status_code == 404:
            return None
        if not resp.ok:
            log.warning(f"WC customer lookup failed for {email}: {resp.status_code}")
            return None
        data = resp.json()
        return data[0] if data else None

    def get_subscriptions(self, customer_id: int) -> list[dict]:
        """Return all WooCommerce Subscriptions for a given customer ID."""
        resp = requests.get(
            f"{self.base}/subscriptions",
            params={"customer": customer_id, "per_page": 20},
            auth=self.auth,
            timeout=10,
        )
        if not resp.ok:
            log.warning(
                f"WC subscriptions lookup failed for customer {customer_id}: {resp.status_code}"
            )
            return []
        return resp.json()

    # ------------------------------------------------------------------ #
    #  Trial detection                                                      #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _is_trial_active(subscription: dict) -> bool:
        """
        Return True if the subscription is currently inside its trial period.

        WooCommerce Subscriptions stores trial_end_date in site local time
        (field "trial_end_date") and UTC (field "trial_end_date_gmt").
        We prefer the GMT field; fall back to the local one.
        A value of "0000-00-00 00:00:00" or "" means no trial.
        """
        trial_end = (
            subscription.get("trial_end_date_gmt")
            or subscription.get("trial_end_date")
            or ""
        )
        if not trial_end or trial_end.startswith("0000"):
            return False
        try:
            # Normalise ISO string — WC may omit the +00:00 suffix
            trial_end_dt = datetime.fromisoformat(
                trial_end.replace("Z", "+00:00")
            )
            if trial_end_dt.tzinfo is None:
                trial_end_dt = trial_end_dt.replace(tzinfo=timezone.utc)
            return trial_end_dt > datetime.now(timezone.utc)
        except (ValueError, AttributeError):
            log.warning(f"Could not parse trial_end_date: {trial_end!r}")
            return False

    # ------------------------------------------------------------------ #
    #  Write operation                                                      #
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
        resp = requests.put(
            f"{self.base}/subscriptions/{subscription_id}",
            json={"status": "cancelled"},
            auth=self.auth,
            timeout=10,
        )
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
    #  Main public method                                                   #
    # ------------------------------------------------------------------ #

    def cancel_subscription(self, email: str) -> dict:
        """
        Find the customer by email, determine trial vs. paid subscription,
        and cancel appropriately.

        Return dict:
          status            : "trial_cancelled" | "subscription_cancelled" |
                              "not_found" | "no_active_sub" | "dry_run" | "error"
          email             : str
          cancelled         : bool
          subscription_type : "trial" | "subscription" | None
          subscription_id   : int | None
          plan              : str
          source            : "woocommerce"
        """
        base = {"email": email, "cancelled": False, "source": "woocommerce",
                "subscription_type": None, "subscription_id": None, "plan": ""}

        # --- DRY RUN shortcut (still real for read ops in integration tests) ---
        if self.dry_run:
            log.info(f"[DRY] WC cancel for {email}")
            return {
                **base,
                "status": "dry_run",
                "cancelled": True,
                "subscription_type": "trial",
                "plan": "IQ Test Subscription",
            }

        # 1. Customer lookup
        customer = self.get_customer_by_email(email)
        if not customer:
            log.info(f"WC: no customer found for {email}")
            return {**base, "status": "not_found"}

        # 2. Subscriptions
        all_subs = self.get_subscriptions(customer["id"])
        active_subs = [s for s in all_subs if s.get("status") in ACTIVE_STATUSES]

        if not active_subs:
            log.info(f"WC: no active subscriptions for {email}")
            return {**base, "status": "no_active_sub"}

        # 3. Prefer trial if active trial exists
        trial_sub = next((s for s in active_subs if self._is_trial_active(s)), None)
        target = trial_sub or active_subs[0]
        sub_type = "trial" if trial_sub else "subscription"

        plan = ""
        line_items = target.get("line_items") or []
        if line_items:
            plan = line_items[0].get("name", "")

        # 4. Cancel
        cancel = self._cancel_sub_by_id(target["id"])
        status_label = "trial_cancelled" if sub_type == "trial" else "subscription_cancelled"

        return {
            **base,
            "status": cancel.get("status") if not cancel["cancelled"] else status_label,
            "cancelled": cancel["cancelled"],
            "subscription_type": sub_type,
            "subscription_id": target["id"],
            "plan": plan or "IQ Test Subscription",
            "error": cancel.get("error"),
        }
