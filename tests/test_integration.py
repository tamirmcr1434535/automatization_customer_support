"""
Integration tests — hit real Zendesk + WooCommerce in DRY_RUN mode
===================================================================
These tests require real env vars to be set. They are SKIPPED automatically
when vars are missing so CI doesn't break.

What they validate:
  1. Can reach Zendesk and fetch a ticket tagged 'automation_test'
  2. Can reach WooCommerce and look up a customer by email
  3. Full end-to-end flow for a test ticket (DRY_RUN=true, writes are logged only)

How to run locally:
  export ZENDESK_SUBDOMAIN=wwiqtest
  export ZENDESK_EMAIL=bot@wwiqtest.com
  export ZENDESK_API_TOKEN=<token>
  export WOO_SITE_URL=https://wwiqtest.com
  export WOO_CONSUMER_KEY=ck_...
  export WOO_CONSUMER_SECRET=cs_...
  export ANTHROPIC_API_KEY=sk-ant-...
  export TEST_TICKET_ID=<zendesk_ticket_id_with_automation_test_tag>
  export TEST_EMAIL=<woocommerce_customer_email>

  pytest tests/test_integration.py -v -s
"""

import os
import pytest

# Skip all integration tests if core credentials are absent
CREDS_PRESENT = all([
    os.getenv("ZENDESK_SUBDOMAIN"),
    os.getenv("ZENDESK_EMAIL"),
    os.getenv("ZENDESK_API_TOKEN"),
    os.getenv("ANTHROPIC_API_KEY"),
])

pytestmark = pytest.mark.skipif(
    not CREDS_PRESENT,
    reason="Integration creds not set — skipping (set ZENDESK_SUBDOMAIN, ZENDESK_EMAIL, etc.)"
)


# ── Fixtures ──────────────────────────────────────────────────────────────── #

@pytest.fixture(scope="module")
def zendesk_client():
    from zendesk_client import ZendeskClient
    return ZendeskClient(
        subdomain=os.getenv("ZENDESK_SUBDOMAIN"),
        email=os.getenv("ZENDESK_EMAIL"),
        api_token=os.getenv("ZENDESK_API_TOKEN"),
        dry_run=True,   # always True for integration tests
    )


@pytest.fixture(scope="module")
def woo_client():
    woo_url = os.getenv("WOO_SITE_URL")
    woo_key = os.getenv("WOO_CONSUMER_KEY")
    woo_secret = os.getenv("WOO_CONSUMER_SECRET")

    if not all([woo_url, woo_key, woo_secret]):
        pytest.skip("WooCommerce credentials not set")

    from woocommerce_client import WooCommerceClient
    return WooCommerceClient(
        site_url=woo_url,
        consumer_key=woo_key,
        consumer_secret=woo_secret,
        dry_run=True,
    )


# ── Tests ─────────────────────────────────────────────────────────────────── #

class TestZendeskConnectivity:
    def test_fetch_test_ticket(self, zendesk_client):
        """Fetch a real ticket tagged automation_test and verify shape."""
        ticket_id = os.getenv("TEST_TICKET_ID")
        if not ticket_id:
            pytest.skip("TEST_TICKET_ID env var not set")

        ticket = zendesk_client.get_ticket(ticket_id)
        assert ticket is not None, f"Ticket {ticket_id} not found in Zendesk"
        assert "subject" in ticket
        assert "requester" in ticket
        assert "email" in ticket["requester"], "Requester email missing"
        print(f"\n  ✅ Ticket {ticket_id}: {ticket['subject'][:60]}")
        print(f"     Email: {ticket['requester']['email']}")
        print(f"     Tags:  {ticket.get('tags', [])}")

    def test_dry_run_write_does_not_raise(self, zendesk_client):
        """DRY_RUN post_reply, add_tag, solve_ticket must not raise."""
        ticket_id = os.getenv("TEST_TICKET_ID", "0")
        zendesk_client.post_reply(ticket_id, "DRY RUN — no actual reply sent")
        zendesk_client.add_tag(ticket_id, "bot_test_tag")
        zendesk_client.solve_ticket(ticket_id)
        # All above are no-ops in DRY_RUN — just verifying they don't crash


class TestWooCommerceConnectivity:
    def test_lookup_test_customer(self, woo_client):
        """Look up a real customer email in WooCommerce (read-only)."""
        email = os.getenv("TEST_EMAIL")
        if not email:
            pytest.skip("TEST_EMAIL env var not set")

        customer = woo_client.get_customer_by_email(email)
        if customer is None:
            pytest.skip(f"No WooCommerce customer found for {email} — skipping")

        assert "id" in customer
        print(f"\n  ✅ WooCommerce customer found: id={customer['id']}, email={email}")

    def test_list_subscriptions(self, woo_client):
        """Fetch subscriptions for the test customer (read-only)."""
        email = os.getenv("TEST_EMAIL")
        if not email:
            pytest.skip("TEST_EMAIL env var not set")

        customer = woo_client.get_customer_by_email(email)
        if not customer:
            pytest.skip("No customer found")

        subs = woo_client.get_subscriptions(customer["id"])
        print(f"\n  ✅ Subscriptions for {email}: {len(subs)} found")
        for s in subs:
            trial_active = woo_client._is_trial_active(s)
            print(
                f"     #{s['id']} status={s.get('status')} "
                f"trial_active={trial_active} "
                f"trial_end={s.get('trial_end_date_gmt', 'n/a')}"
            )

    def test_dry_run_cancel_does_not_call_api(self, woo_client):
        """DRY_RUN cancel must return dry_run status without any PUT call."""
        result = woo_client.cancel_subscription("dryrun@example.com")
        assert result["status"] == "dry_run"
        assert result["cancelled"] is True
        print(f"\n  ✅ DRY_RUN cancel returned: {result}")


class TestEndToEndDryRun:
    """Full pipeline with DRY_RUN=true — no writes to any external system."""

    def test_full_flow_trial_cancellation(self, zendesk_client, woo_client):
        """
        Simulate a TRIAL_CANCELLATION ticket from end to end:
        Zendesk read → classify → WooCommerce dry-run cancel → generate reply.
        """
        from classifier import classify_ticket
        from reply_generator import generate_reply

        ticket_id = os.getenv("TEST_TICKET_ID")
        if not ticket_id:
            pytest.skip("TEST_TICKET_ID not set")

        # 1. Fetch ticket
        ticket = zendesk_client.get_ticket(ticket_id)
        assert ticket, "Test ticket not found"

        subject = ticket.get("subject", "")
        body = ticket.get("description", "")
        email = ticket.get("requester", {}).get("email", "")
        name = ticket.get("requester", {}).get("name", "")

        # 2. Classify
        classification = classify_ticket(subject, body)
        print(f"\n  Classification: {classification}")
        assert "intent" in classification
        assert "language" in classification
        assert 0.0 <= classification["confidence"] <= 1.0

        # 3. WooCommerce dry-run cancel
        woo_result = woo_client.cancel_subscription(email)
        assert woo_result["status"] in (
            "dry_run", "not_found", "no_active_sub",
            "trial_cancelled", "subscription_cancelled",
        )
        print(f"  WooCommerce result: {woo_result['status']}")

        # 4. Generate reply
        reply = generate_reply(
            intent=classification["intent"],
            language=classification["language"],
            customer_name=name,
            cancel_result={**woo_result, "source": "woocommerce"},
        )
        assert isinstance(reply, str)
        assert len(reply) > 20
        print(f"  Generated reply ({classification['language']}):\n  {reply[:200]}")

        # 5. Dry-run Zendesk writes
        zendesk_client.post_reply(ticket_id, reply)
        zendesk_client.add_tag(ticket_id, "bot_integration_test")
        zendesk_client.solve_ticket(ticket_id)
        print(f"\n  ✅ Full dry-run flow completed for ticket #{ticket_id}")
