"""
Slack Notifier
==============
Sends DM alerts via Slack Bot Token when a customer requires manual review
or the bot fails to find them after all lookup attempts.

Uses users.lookupByEmail to resolve the target user, then chat.postMessage.
Set SLACK_BOT_TOKEN + SLACK_TARGET_EMAIL env vars to enable.
"""

import logging
import requests

log = logging.getLogger("slack")

_SLACK_API = "https://slack.com/api"


def _get_user_id(token: str, email: str) -> str | None:
    """Resolve Slack user ID from email address."""
    try:
        resp = requests.get(
            f"{_SLACK_API}/users.lookupByEmail",
            headers={"Authorization": f"Bearer {token}"},
            params={"email": email},
            timeout=10,
        )
        data = resp.json()
        if data.get("ok"):
            return data["user"]["id"]
        log.warning(f"Slack: could not find user for {email}: {data.get('error')}")
        return None
    except Exception as e:
        log.error(f"Slack: user lookup failed for {email}: {e}")
        return None


class SlackClient:
    def __init__(
        self,
        bot_token: str,
        target_email: str,
        dry_run: bool = True,
        # legacy webhook_url kept for backwards compat — ignored when bot_token is set
        webhook_url: str = "",
    ):
        self.bot_token     = bot_token
        self.target_email  = target_email
        self.dry_run       = dry_run
        self._user_id_cache: str | None = None

        if dry_run:
            log.info("SlackClient: DRY_RUN — no messages will be sent")
        if not bot_token:
            log.warning("SlackClient: SLACK_BOT_TOKEN not set — Slack alerts disabled")

    # ── internal ──────────────────────────────────────────────────────────

    def _resolve_channel(self) -> str | None:
        """Return cached Slack user ID for target_email."""
        if self._user_id_cache is None:
            self._user_id_cache = _get_user_id(self.bot_token, self.target_email)
        return self._user_id_cache

    def _open_dm_channel(self, user_id: str) -> str | None:
        """Open a DM channel with the user. Required for bot tokens to send DMs."""
        try:
            resp = requests.post(
                f"{_SLACK_API}/conversations.open",
                headers={
                    "Authorization": f"Bearer {self.bot_token}",
                    "Content-Type": "application/json",
                },
                json={"users": user_id},
                timeout=10,
            )
            data = resp.json()
            if data.get("ok"):
                channel_id = data["channel"]["id"]
                log.info(f"Slack: opened DM channel {channel_id} for user {user_id}")
                return channel_id
            log.error(f"Slack: conversations.open failed — {data.get('error')}")
            return None
        except Exception as e:
            log.error(f"Slack: conversations.open request failed — {e}")
            return None

    def _post(self, text: str, blocks: list | None = None) -> bool:
        """Core DM send method. Returns True on success."""
        if self.dry_run:
            log.info(f"[DRY] Slack DM to {self.target_email}: {text[:120]}")
            return True

        if not self.bot_token:
            log.error("Slack: bot token missing — cannot send alert")
            return False

        user_id = self._resolve_channel()
        if not user_id:
            log.error(f"Slack: could not resolve user ID for {self.target_email} — alert NOT sent")
            return False

        # Open DM channel first — required for bot tokens to send DMs
        dm_channel = self._open_dm_channel(user_id)
        if not dm_channel:
            log.error(f"Slack: could not open DM channel for {self.target_email} — alert NOT sent")
            return False

        payload: dict = {"channel": dm_channel, "text": text}
        if blocks:
            payload["blocks"] = blocks

        try:
            resp = requests.post(
                f"{_SLACK_API}/chat.postMessage",
                headers={
                    "Authorization": f"Bearer {self.bot_token}",
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=10,
            )
            data = resp.json()
            if data.get("ok"):
                log.info(f"Slack: ✅ DM successfully delivered to {self.target_email}")
                return True
            log.error(
                f"Slack: ❌ chat.postMessage failed — error={data.get('error')}, "
                f"channel={dm_channel}, user={self.target_email}"
            )
            return False
        except Exception as e:
            log.error(f"Slack: ❌ request failed — {e}")
            return False

    # ── public API ────────────────────────────────────────────────────────

    def notify_manual_review(
        self,
        ticket_id: str,
        email: str,
        intent: str,
        zendesk_subdomain: str,
    ) -> bool:
        """Alert: customer found but no active subscription — needs manual review."""
        ticket_url = (
            f"https://{zendesk_subdomain}.zendesk.com/agent/tickets/{ticket_id}"
        )
        text = (
            f"⚠️ *Manual Review Required* | Ticket <{ticket_url}|#{ticket_id}> "
            f"| `{email}` | {intent.replace('_', ' ').title()}"
        )
        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": "⚠️ Manual Review Required"},
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Ticket:*\n<{ticket_url}|#{ticket_id}>"},
                    {"type": "mrkdwn", "text": f"*Email:*\n`{email}`"},
                    {"type": "mrkdwn", "text": f"*Intent:*\n{intent.replace('_', ' ').title()}"},
                    {"type": "mrkdwn", "text": "*Reason:*\nCustomer found but has no active subscription"},
                ],
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        "The subscription may already be cancelled, or registered under "
                        "a different email / payment method. Please verify manually."
                    ),
                },
            },
            {"type": "divider"},
        ]
        sent = self._post(text, blocks)
        if sent:
            log.info(f"Slack: manual_review alert SENT for ticket #{ticket_id}")
        else:
            log.error(f"Slack: manual_review alert FAILED for ticket #{ticket_id}")
        return sent

    def notify_not_found(
        self,
        ticket_id: str,
        email: str,
        zendesk_subdomain: str,
    ) -> bool:
        """Alert: customer not found after all lookup attempts — ticket closed."""
        ticket_url = (
            f"https://{zendesk_subdomain}.zendesk.com/agent/tickets/{ticket_id}"
        )
        text = (
            f"❌ *Customer Not Found — Ticket Closed* | "
            f"<{ticket_url}|#{ticket_id}> | `{email}`"
        )
        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": "❌ Customer Not Found — Ticket Closed"},
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Ticket:*\n<{ticket_url}|#{ticket_id}>"},
                    {"type": "mrkdwn", "text": f"*Email:*\n`{email}`"},
                ],
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        "Bot exhausted all lookup options (email → alt emails → card digits). "
                        "Ticket closed automatically. May need manual follow-up."
                    ),
                },
            },
            {"type": "divider"},
        ]
        sent = self._post(text, blocks)
        if sent:
            log.info(f"Slack: not_found alert SENT for ticket #{ticket_id}")
        else:
            log.error(f"Slack: not_found alert FAILED for ticket #{ticket_id}")
        return sent
