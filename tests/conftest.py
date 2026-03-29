"""
Shared pytest fixtures for unit and integration tests.
"""
import pytest
from unittest.mock import MagicMock
from datetime import datetime, timezone, timedelta


# ── WooCommerce fixtures ───────────────────────────────────────────────────── #

def make_wc_subscription(
    sub_id: int = 101,
    status: str = "active",
    trial_days_from_now: int = 0,
    plan_name: str = "IQ Test Monthly",
) -> dict:
    """Build a fake WooCommerce subscription dict."""
    if trial_days_from_now > 0:
        trial_end = (datetime.now(timezone.utc) + timedelta(days=trial_days_from_now)).isoformat()
    else:
        trial_end = "0000-00-00 00:00:00"

    return {
        "id": sub_id,
        "status": status,
        "trial_end_date_gmt": trial_end,
        "line_items": [{"name": plan_name}],
    }


def make_wc_customer(customer_id: int = 42, email: str = "test@example.com") -> dict:
    return {"id": customer_id, "email": email}


@pytest.fixture
def wc_customer():
    return make_wc_customer()


@pytest.fixture
def active_trial_sub():
    return make_wc_subscription(trial_days_from_now=5)


@pytest.fixture
def active_paid_sub():
    return make_wc_subscription(trial_days_from_now=0)


@pytest.fixture
def expired_trial_sub():
    """Trial ended yesterday → treated as regular paid subscription."""
    sub = make_wc_subscription()
    past = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    sub["trial_end_date_gmt"] = past
    return sub


# ── Stripe fixtures ────────────────────────────────────────────────────────── #

@pytest.fixture
def stripe_cancelled_result():
    return {
        "status": "cancelled",
        "email": "test@example.com",
        "subscription_id": "sub_abc123",
        "plan": "IQ Test Subscription",
        "cancelled": True,
        "source": "stripe",
        "subscription_type": "subscription",
    }


# ── Zendesk ticket fixtures ────────────────────────────────────────────────── #

def make_zendesk_ticket(
    ticket_id: str = "1001",
    subject: str = "Cancel my trial",
    body: str = "Please cancel my free trial. I don't want to be charged.",
    email: str = "user@example.com",
    name: str = "Test User",
    tags: list = None,
) -> dict:
    return {
        "id": ticket_id,
        "subject": subject,
        "description": body,
        "tags": tags if tags is not None else ["automation_test"],
        "requester": {"email": email, "name": name},
    }


@pytest.fixture
def zendesk_trial_ticket():
    return make_zendesk_ticket()


@pytest.fixture
def zendesk_sub_ticket():
    return make_zendesk_ticket(
        subject="Cancel my subscription",
        body="I want to cancel my paid subscription immediately.",
    )
