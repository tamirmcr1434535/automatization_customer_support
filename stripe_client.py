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
