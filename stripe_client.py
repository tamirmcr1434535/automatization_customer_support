import logging
import stripe as stripe_lib

log = logging.getLogger("stripe")


class StripeClient:
    def __init__(self, api_key: str, dry_run: bool = True):
        stripe_lib.api_key = api_key
        self.dry_run = dry_run
        if dry_run:
            log.info("StripeClient: DRY_RUN — no writes")

    def cancel_subscription(self, email: str) -> dict:
        """Find customer by email and cancel their active subscription.
        Checks 'trialing' BEFORE 'active' — trial takes priority."""
        log.info(f"[DRY] Stripe lookup for {email}" if self.dry_run else f"Stripe cancel for {email}")

        try:
            customers = stripe_lib.Customer.list(email=email, limit=1)
        except stripe_lib.error.AuthenticationError as e:
            log.error(f"Stripe auth error — check STRIPE_SECRET_KEY in Secret Manager: {e}")
            return {"status": "error", "email": email, "error": str(e), "cancelled": False}
        except stripe_lib.error.StripeError as e:
            log.error(f"Stripe error for {email}: {e}")
            return {"status": "error", "email": email, "error": str(e), "cancelled": False}

        if not customers.data:
            return {"status": "not_found", "email": email, "cancelled": False}

        customer = customers.data[0]
        return self._cancel_customer_sub(customer.id, email)

    def find_active_subscription(self, email: str) -> dict:
        """READ-ONLY: is this customer still on the hook for future charges?

        Distinct from cancel_subscription() in that it cancels nothing AND it
        reports `cancel_at_period_end`. That flag is the whole point: a Stripe
        subscription that has been cancelled gracefully stays status="active"
        until the period actually ends, so "status == active" alone does NOT
        mean billing will continue — only `cancel_at_period_end is False` does.

        Returns:
          {"status": "billing", subscription_id, subscription_status, plan}
              — a live sub that WILL renew (no cancellation scheduled)
          {"status": "cancel_scheduled", ...}  — live but already set to end
          {"status": "no_active_sub"}          — nothing active/trialing
          {"status": "not_found"}              — no such Stripe customer
          {"status": "error", "error": ...}    — lookup failed
        """
        try:
            customers = stripe_lib.Customer.list(email=email, limit=1)
        except stripe_lib.error.StripeError as e:
            log.warning(f"Stripe find_active_subscription({email}) failed: {e}")
            return {"status": "error", "email": email, "error": str(e)}

        if not customers.data:
            return {"status": "not_found", "email": email}

        customer_id = customers.data[0].id
        for status in ("trialing", "active"):
            try:
                subs = stripe_lib.Subscription.list(
                    customer=customer_id, status=status, limit=5
                )
            except stripe_lib.error.StripeError as e:
                log.warning(f"Stripe subscription list failed for {customer_id}: {e}")
                return {"status": "error", "email": email, "error": str(e)}

            for sub in subs.data:
                if sub.get("cancel_at_period_end"):
                    continue  # already winding down — gateway agrees with WC
                plan = ""
                if sub.items and sub.items.data:
                    plan = sub.items.data[0].price.nickname or ""
                return {
                    "status": "billing",
                    "email": email,
                    "customer_id": customer_id,
                    "subscription_id": sub.id,
                    "subscription_status": status,
                    "plan": plan,
                }

        # Everything live is already scheduled to end (or there is nothing live).
        return {"status": "no_active_sub", "email": email, "customer_id": customer_id}

    def cancel_subscription_by_id(self, subscription_id: str) -> dict:
        """Cancel one specific Stripe subscription (graceful — at period end).

        Targeted counterpart to cancel_subscription(email): the caller has
        already identified WHICH subscription is still billing, so we do not
        re-run an email lookup that could pick a different one."""
        if self.dry_run:
            log.info(f"[DRY] Would cancel Stripe subscription {subscription_id}")
            return {"status": "dry_run", "subscription_id": subscription_id,
                    "cancelled": True}
        try:
            stripe_lib.Subscription.modify(subscription_id, cancel_at_period_end=True)
        except stripe_lib.error.StripeError as e:
            log.error(f"Stripe cancel_subscription_by_id({subscription_id}) failed: {e}")
            return {"status": "error", "subscription_id": subscription_id,
                    "cancelled": False, "error": str(e)}
        log.info(f"Stripe: cancelled subscription {subscription_id} (at period end)")
        return {"status": "cancelled", "subscription_id": subscription_id,
                "cancelled": True}

    def find_email_by_last4(self, last4: str) -> str | None:
        """
        Look up the customer email associated with a card ending in last4.

        Used when WooCommerce lookup by email failed/timed out — the customer
        provides their last 4 card digits so we can find their email in Stripe,
        then cancel in WooCommerce using that email.

        Returns the email string, or None if not found.
        Does NOT cancel anything in Stripe.
        """
        log.info(f"Stripe: looking up email for card last4={last4}")

        try:
            charges = stripe_lib.Charge.search(
                query=f'payment_method_details.card.last4:"{last4}"',
                limit=5,
            )
        except stripe_lib.error.InvalidRequestError:
            log.warning("Stripe Search API not available, falling back to list")
            charges = self._list_charges_fallback(last4)
        except stripe_lib.error.StripeError as e:
            log.error(f"Stripe search by last4 error: {e}")
            return None

        if not charges.data:
            log.info(f"Stripe: no charges found for last4={last4}")
            return None

        seen = set()
        for charge in charges.data:
            customer_id = charge.customer
            if not customer_id or customer_id in seen:
                continue
            seen.add(customer_id)

            try:
                customer = stripe_lib.Customer.retrieve(customer_id)
                email = customer.get("email") or ""
                if email:
                    log.info(f"Stripe: found email {email!r} for card last4={last4}")
                    return email
            except stripe_lib.error.StripeError as e:
                log.warning(f"Stripe: error retrieving customer {customer_id}: {e}")
                continue

        log.info(f"Stripe: no customer email found for last4={last4}")
        return None

    def find_and_cancel_by_last4(self, last4: str) -> dict:
        """
        Search Stripe for a subscription tied to a card ending in last4.
        Checks 'trialing' BEFORE 'active'.
        """
        log.info(f"Stripe search by card last4={last4}")

        try:
            charges = stripe_lib.Charge.search(
                query=f'payment_method_details.card.last4:"{last4}"',
                limit=5,
            )
        except stripe_lib.error.InvalidRequestError:
            log.warning("Stripe Search API not available, falling back to list")
            charges = self._list_charges_fallback(last4)
        except stripe_lib.error.StripeError as e:
            log.error(f"Stripe search by last4 error: {e}")
            return {"found": False, "error": str(e)}

        if not charges.data:
            log.info(f"No Stripe charges found for last4={last4}")
            return {"found": False, "last4": last4}

        seen = set()
        for charge in charges.data:
            customer_id = charge.customer
            if not customer_id or customer_id in seen:
                continue
            seen.add(customer_id)

            result = self._cancel_customer_sub(customer_id, source="last4")
            if result.get("status") not in ("not_found", "no_active_sub", "error"):
                return {"found": True, **result}

        return {"found": False, "last4": last4}

    def _cancel_customer_sub(self, customer_id: str, email: str = "", source: str = "email") -> dict:
        """
        Find and cancel active/trialing sub for a known customer_id.
        Priority: trialing → active  (trial is cancelled first if exists)
        """
        # ── Check trialing FIRST, then active ──────────────────────────
        for status in ("trialing", "active"):
            try:
                subs = stripe_lib.Subscription.list(
                    customer=customer_id, status=status, limit=5
                )
            except stripe_lib.error.StripeError as e:
                log.error(f"Stripe subscriptions error for {customer_id}: {e}")
                return {"status": "error", "error": str(e), "cancelled": False}

            if subs.data:
                sub  = subs.data[0]
                plan = ""
                if sub.items and sub.items.data:
                    plan = sub.items.data[0].price.nickname or ""

                sub_type = "trial" if status == "trialing" else "subscription"

                if self.dry_run:
                    log.info(
                        f"[DRY] Would cancel Stripe {sub_type} {sub.id} "
                        f"(customer={customer_id}, status={status})"
                    )
                    return {
                        "status": "dry_run",
                        "email": email,
                        "customer_id": customer_id,
                        "subscription_id": sub.id,
                        "subscription_type": sub_type,
                        "plan": plan or "IQ Test Subscription",
                        "cancelled": True,
                    }

                stripe_lib.Subscription.modify(sub.id, cancel_at_period_end=True)
                log.info(f"Stripe: cancelled {sub_type} {sub.id} for customer {customer_id}")
                return {
                    "status": "cancelled",
                    "email": email,
                    "customer_id": customer_id,
                    "subscription_id": sub.id,
                    "subscription_type": sub_type,
                    "plan": plan or "IQ Test Subscription",
                    "cancelled": True,
                }

        return {"status": "no_active_sub", "email": email, "cancelled": False}

    def _list_charges_fallback(self, last4: str):
        """Fallback: list recent charges and filter by last4 manually."""
        class FakeResult:
            def __init__(self, data): self.data = data
        try:
            all_charges = stripe_lib.Charge.list(limit=100)
            matching = [
                c for c in all_charges.data
                if c.get("payment_method_details", {})
                   .get("card", {})
                   .get("last4") == last4
            ]
            return FakeResult(matching)
        except stripe_lib.error.StripeError:
            return FakeResult([])
