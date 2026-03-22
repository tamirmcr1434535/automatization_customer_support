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
        if self.dry_run:
            log.info(f"[DRY] cancel Stripe sub for {email}")
            return {"status": "dry_run", "email": email, "cancelled": True,
                    "subscription_id": "sub_dry_xxx", "plan": "IQ Test Subscription"}

        try:
            customers = stripe_lib.Customer.list(email=email, limit=1)
            if not customers.data:
                return {"status": "not_found", "email": email, "cancelled": False}

            customer = customers.data[0]

            for status in ("active", "trialing"):
                subs = stripe_lib.Subscription.list(
                    customer=customer.id, status=status, limit=5
                )
                if subs.data:
                    sub = subs.data[0]
                    stripe_lib.Subscription.modify(sub.id, cancel_at_period_end=True)
                    plan = ""
                    if sub.items and sub.items.data:
                        plan = sub.items.data[0].price.nickname or ""
                    log.info(f"Stripe: cancelled {sub.id} for {email}")
                    return {"status": "cancelled", "email": email,
                            "subscription_id": sub.id, "plan": plan, "cancelled": True}

            return {"status": "no_active_sub", "email": email, "cancelled": False}

        except stripe_lib.error.StripeError as e:
            log.error(f"Stripe error for {email}: {e}")
            return {"status": "error", "email": email, "error": str(e), "cancelled": False}
