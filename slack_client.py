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
        self.bot_token = bot_token
        # Support comma-separated list of emails: "a@x.com,b@x.com"
        self.target_emails: list[str] = [
            e.strip() for e in target_email.split(",") if e.strip()
        ]
        self.dry_run = dry_run
        self._user_id_cache: dict[str, str] = {}  # email → user_id

        if dry_run:
            log.info("SlackClient: DRY_RUN — no messages will be sent")
        if not bot_token:
            log.warning("SlackClient: SLACK_BOT_TOKEN not set — Slack alerts disabled")
        if self.target_emails:
            log.info(f"SlackClient: will notify {len(self.target_emails)} recipient(s): {self.target_emails}")

    # ── internal ──────────────────────────────────────────────────────────

    def _resolve_user_id(self, email: str) -> str | None:
        """Return cached Slack user ID for a single email."""
        if email not in self._user_id_cache:
            uid = _get_user_id(self.bot_token, email)
            if uid:
                self._user_id_cache[email] = uid
        return self._user_id_cache.get(email)

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

    def _post_to_one(self, email: str, text: str, blocks: list | None) -> bool:
        """Send a message to a single recipient. Returns True on success."""
        user_id = self._resolve_user_id(email)
        if not user_id:
            log.error(f"Slack: could not resolve user ID for {email} — alert NOT sent")
            return False

        dm_channel = self._open_dm_channel(user_id)
        if not dm_channel:
            log.error(f"Slack: could not open DM channel for {email} — alert NOT sent")
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
                log.info(f"Slack: ✅ DM delivered to {email}")
                return True
            log.error(
                f"Slack: ❌ chat.postMessage failed — error={data.get('error')}, "
                f"channel={dm_channel}, user={email}"
            )
            return False
        except Exception as e:
            log.error(f"Slack: ❌ request failed for {email} — {e}")
            return False

    def _post(self, text: str, blocks: list | None = None) -> bool:
        """Send to all target emails. Returns True if at least one succeeded."""
        if self.dry_run:
            log.info(f"[DRY] Slack DM to {self.target_emails}: {text[:120]}")
            return True

        if not self.bot_token:
            log.error("Slack: bot token missing — cannot send alert")
            return False

        if not self.target_emails:
            log.error("Slack: no target emails configured — cannot send alert")
            return False

        any_sent = False
        for email in self.target_emails:
            ok = self._post_to_one(email, text, blocks)
            if ok:
                any_sent = True
        return any_sent

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

    def notify_refund_skip(
        self,
        ticket_id: str,
        email: str,
        intent: str,
        zendesk_subdomain: str,
    ) -> bool:
        """Alert: ticket skipped because refund keywords detected — human must review."""
        ticket_url = (
            f"https://{zendesk_subdomain}.zendesk.com/agent/tickets/{ticket_id}"
        )
        text = (
            f"💰 *Refund Request — Skipped* | Ticket <{ticket_url}|#{ticket_id}> "
            f"| `{email}` | {intent.replace('_', ' ').title()}"
        )
        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": "💰 Refund Request — Needs Human Review"},
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Ticket:*\n<{ticket_url}|#{ticket_id}>"},
                    {"type": "mrkdwn", "text": f"*Email:*\n`{email}`"},
                    {"type": "mrkdwn", "text": f"*Detected Intent:*\n{intent.replace('_', ' ').title()}"},
                ],
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        "Customer message contains refund/payment-related keywords. "
                        "Bot skipped this ticket — please handle manually."
                    ),
                },
            },
            {"type": "divider"},
        ]
        sent = self._post(text, blocks)
        if sent:
            log.info(f"Slack: refund_skip alert SENT for ticket #{ticket_id}")
        else:
            log.error(f"Slack: refund_skip alert FAILED for ticket #{ticket_id}")
        return sent

    def notify_error(
        self,
        ticket_id: str,
        error_msg: str,
        zendesk_subdomain: str,
    ) -> bool:
        """Alert: bot crashed while processing a ticket."""
        ticket_url = (
            f"https://{zendesk_subdomain}.zendesk.com/agent/tickets/{ticket_id}"
        )
        text = (
            f"🔴 *Bot Error* | Ticket <{ticket_url}|#{ticket_id}> "
            f"| `{error_msg[:200]}`"
        )
        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": "🔴 Bot Error — Processing Failed"},
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Ticket:*\n<{ticket_url}|#{ticket_id}>"},
                    {"type": "mrkdwn", "text": f"*Error:*\n```{error_msg[:300]}```"},
                ],
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "Bot encountered an unhandled error. Ticket may need manual review.",
                },
            },
            {"type": "divider"},
        ]
        sent = self._post(text, blocks)
        if sent:
            log.info(f"Slack: error alert SENT for ticket #{ticket_id}")
        else:
            log.error(f"Slack: error alert FAILED for ticket #{ticket_id}")
        return sent

    def notify_spam_detected(
        self,
        ticket_id: str,
        email: str,
        reply_count: int,
        zendesk_subdomain: str,
    ) -> bool:
        """Alert: bot has already replied 2+ times to this ticket — possible spam loop."""
        ticket_url = (
            f"https://{zendesk_subdomain}.zendesk.com/agent/tickets/{ticket_id}"
        )
        text = (
            f"🔁 *Spam Alert — {reply_count} Bot Replies* | "
            f"Ticket <{ticket_url}|#{ticket_id}> | `{email}`"
        )
        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"🔁 Spam Alert — Bot Replied {reply_count}x"},
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Ticket:*\n<{ticket_url}|#{ticket_id}>"},
                    {"type": "mrkdwn", "text": f"*Email:*\n`{email}`"},
                    {"type": "mrkdwn", "text": f"*Bot Replies:*\n{reply_count}"},
                ],
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        "Bot has sent multiple replies to this ticket. "
                        "Possible webhook loop or re-trigger. Please investigate."
                    ),
                },
            },
            {"type": "divider"},
        ]
        sent = self._post(text, blocks)
        if sent:
            log.info(f"Slack: spam alert SENT for ticket #{ticket_id} ({reply_count} replies)")
        else:
            log.error(f"Slack: spam alert FAILED for ticket #{ticket_id}")
        return sent
