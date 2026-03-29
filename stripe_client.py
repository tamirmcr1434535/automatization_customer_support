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
        # DRY_RUN: still do real lookup, just skip the actual cancel
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

        for status in ("active", "trialing"):
            try:
                subs = stripe_lib.Subscription.list(
                    customer=customer.id, status=status, limit=5
                )
            except stripe_lib.error.StripeError as e:
                log.error(f"Stripe subscriptions error for {email}: {e}")
                return {"status": "error", "email": email, "error": str(e), "cancelled": False}

            if subs.data:
                sub = subs.data[0]
                plan = ""
                if sub.items and sub.items.data:
                    plan = sub.items.data[0].price.nickname or ""

                if self.dry_run:
                    log.info(f"[DRY] Would cancel Stripe sub {sub.id} for {email} (status={status})")
                    return {
                        "status": "dry_run",
                        "email": email,
                        "subscription_id": sub.id,
                        "subscription_type": "trial" if status == "trialing" else "subscription",
                        "plan": plan or "IQ Test Subscription",
                        "cancelled": True,
                    }

                stripe_lib.Subscription.modify(sub.id, cancel_at_period_end=True)
                log.info(f"Stripe: cancelled {sub.id} for {email}")
                return {
                    "status": "cancelled",
                    "email": email,
                    "subscription_id": sub.id,
                    "subscription_type": "trial" if status == "trialing" else "subscription",
                    "plan": plan or "IQ Test Subscription",
                    "cancelled": True,
                }

        return {"status": "no_active_sub", "email": email, "cancelled": False}
