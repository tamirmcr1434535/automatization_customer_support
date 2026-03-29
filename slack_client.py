"""
Slack Notifier
==============
Sends alerts to a Slack channel via Incoming Webhook when a customer
cannot be found in WooCommerce or Stripe and requires manual review.
"""

import logging
import requests

log = logging.getLogger("slack")


class SlackClient:
    def __init__(self, webhook_url: str, dry_run: bool = True):
        self.webhook_url = webhook_url
        self.dry_run = dry_run
        if dry_run:
            log.info("SlackClient: DRY_RUN — no messages will be sent")

    def notify_manual_review(
        self,
        ticket_id: str,
        email: str,
        intent: str,
        zendesk_subdomain: str,
    ) -> bool:
        """
        Post a manual review alert to Slack.

        Returns True if the message was sent (or dry-run), False on error.
        """
        ticket_url = f"https://{zendesk_subdomain}.zendesk.com/agent/tickets/{ticket_id}"

        message = {
            "text": "⚠️ *Manual Review Required*",
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "⚠️ Manual Review Required",
                    },
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*Ticket:*\n<{ticket_url}|#{ticket_id}>",
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Email:*\n`{email}`",
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Intent:*\n{intent.replace('_', ' ').title()}",
                        },
                        {
                            "type": "mrkdwn",
                            "text": "*Reason:*\nCustomer not found in WooCommerce or Stripe",
                        },
                    ],
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            "The customer may have used a different email address. "
                            "Please verify manually (e.g. by last 4 digits of card)."
                        ),
                    },
                },
                {"type": "divider"},
            ],
        }

        if self.dry_run:
            log.info(
                f"[DRY] Slack alert for ticket #{ticket_id} | email={email} | intent={intent}"
            )
            return True

        try:
            resp = requests.post(self.webhook_url, json=message, timeout=10)
            resp.raise_for_status()
            log.info(f"Slack: alert sent for ticket #{ticket_id}")
            return True
        except Exception as e:
            log.error(f"Slack: failed to send alert for ticket #{ticket_id}: {e}")
            return False
