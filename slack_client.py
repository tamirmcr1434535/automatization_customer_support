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

    def notify_ticket_result(
        self,
        ticket_id: str,
        result: dict,
        zendesk_subdomain: str,
        shadow: bool = False,
    ) -> bool:
        """Per-ticket post-process report, emitted once per ticket in
        BOTH shadow and live (prod) modes.

        `shadow=True`  → labels the report as SHADOW and uses "Would do"
                         phrasing (the bot did not actually act).
        `shadow=False` → labels the report as LIVE and uses "Did" phrasing
                         (the bot's action already happened).
        """
        ticket_url = (
            f"https://{zendesk_subdomain}.zendesk.com/agent/tickets/{ticket_id}"
        )
        status = result.get("status", "unknown")
        intent = result.get("intent", "—")
        email  = result.get("email", "—")
        lang   = result.get("language", "—")
        conf   = result.get("confidence")
        conf_s = f"{conf:.0%}" if conf else "—"
        source = result.get("cancel_source", "—")
        action = result.get("action", "—")
        order_count = result.get("order_count", "—")
        parent_count = result.get("parent_count")
        renewal_count = result.get("renewal_count")

        # Choose emoji by status
        emoji_map = {
            "success": "✅",
            "manual_review_required": "⚠️",
            "escalated_delete_account": "🗑️",
            "escalated_explanation_question": "❓",
            "escalated_no_results_received": "📭",
            "escalated_legacy_card_digits": "🗂️",
            "wc_lookup_error": "🟠",
            "escalated_not_found": "⛔",
            "skipped_refund_request": "💰",
            "skipped_not_handled": "⏭️",
            "escalated_low_confidence": "🔻",
            "skipped_followup": "↩️",
            "skipped_agent_already_replied": "🧑‍💼",
            "skipped_merge_candidate": "🔗",
            "skipped_merged": "🔀",
            "skipped_spam_detected": "🔁",
            "skipped_closed": "🔒",
            "error": "🔴",
        }
        emoji = emoji_map.get(status, "👁️")

        # Short reply preview (if any)
        reply = result.get("reply_text", "")
        reply_preview = (reply[:120] + "…") if len(reply) > 120 else reply

        mode_label = "SHADOW" if shadow else "LIVE"
        action_label = "Would do" if shadow else "Did"
        header_title = (
            f"{emoji} Shadow Mode — Ticket #{ticket_id}"
            if shadow
            else f"{emoji} Live — Ticket #{ticket_id}"
        )

        text = (
            f"{emoji} *{mode_label}* | <{ticket_url}|#{ticket_id}> "
            f"| `{email}` | {intent} → {status}"
        )
        fields = [
            {"type": "mrkdwn", "text": f"*Ticket:*\n<{ticket_url}|#{ticket_id}>"},
            {"type": "mrkdwn", "text": f"*Email:*\n`{email}`"},
            {"type": "mrkdwn", "text": f"*Intent:*\n{intent} ({conf_s})"},
            {"type": "mrkdwn", "text": f"*{action_label}:*\n{status}"},
            {"type": "mrkdwn", "text": f"*Language:*\n{lang}"},
            {"type": "mrkdwn", "text": f"*Source:*\n{source}"},
        ]
        # Show the WC Related Orders breakdown when we have it (parent /
        # renewal split). Mirrors the "Relationship" column in the WC
        # admin so the support team can read the bot's classification at
        # a glance. Falls back to a plain order total if the bot only
        # had the legacy count (older logs / Stripe-only path).
        if parent_count is not None and renewal_count is not None:
            if renewal_count == 0:
                breakdown_text = f"Parent: {parent_count} (trial)"
            elif renewal_count == 1:
                breakdown_text = (
                    f"Parent: {parent_count} + Renewal: {renewal_count} "
                    "(subscription)"
                )
            else:
                breakdown_text = (
                    f"Parent: {parent_count} + Renewals: {renewal_count} "
                    "(renewal subscription)"
                )
            fields.append({"type": "mrkdwn", "text": f"*Orders:*\n{breakdown_text}"})
        elif order_count not in ("—", None):
            fields.append({"type": "mrkdwn", "text": f"*Orders:*\n{order_count}"})
        reply_count = result.get("reply_count")
        if reply_count is not None:
            fields.append({"type": "mrkdwn", "text": f"*Bot replies so far:*\n{reply_count}"})

        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": header_title},
            },
            {"type": "section", "fields": fields[:10]},  # Slack max 10 fields per section
        ]

        # Status-specific detail block — renders whichever of the optional
        # context fields the caller populated on `result`. This replaces the
        # per-decision Slack alerts (notify_manual_review / notify_wc_lookup_failed /
        # etc.) — each ticket gets exactly ONE Slack message carrying the same
        # information that used to be split across two.
        detail_lines: list[str] = []
        if result.get("error_kind"):
            err_k = result.get("error_kind")
            err_d = (result.get("error_detail") or "").strip()
            err_s = (result.get("error_step") or "").strip() or "—"
            detail_lines.append(f"*WC error:* `{err_k}` at step `{err_s}`")
            if err_d:
                detail_lines.append(f"```{err_d[:400]}```")
        if result.get("reason"):
            detail_lines.append(f"*Reason:* {str(result['reason'])[:400]}")
        if result.get("validation_fail_reason"):
            detail_lines.append(f"*Reply validation failed:* {str(result['validation_fail_reason'])[:400]}")
        if status == "error" and result.get("error"):
            detail_lines.append(f"*Exception:*\n```{str(result['error'])[:400]}```")
        if detail_lines:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": "\n".join(detail_lines)},
            })

        if reply_preview:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Reply preview:*\n> {reply_preview}",
                },
            })

        blocks.append({"type": "divider"})

        sent = self._post(text, blocks)
        if sent:
            log.info(f"Slack: {mode_label.lower()} report SENT for ticket #{ticket_id} → {status}")
        else:
            log.error(f"Slack: {mode_label.lower()} report FAILED for ticket #{ticket_id}")
        return sent

    def notify_startup_failure(
        self,
        service: str,
        error_kind: str,
        error_detail: str,
    ) -> bool:
        """Alert: bot failed startup health check — deploy is broken, no tickets
        will be processed until ops fixes credentials / connectivity.
        """
        text = (
            f"🚨 *Startup health check FAILED* — `{service}` `{error_kind}`. "
            "Bot is NOT processing tickets."
        )
        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"🚨 Startup health check FAILED — {service}"},
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Service:*\n`{service}`"},
                    {"type": "mrkdwn", "text": f"*Error:*\n`{error_kind}`"},
                ],
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Detail:*\n```{(error_detail or '—')[:400]}```",
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        "Deploy is broken. Bot will exit and no tickets will be "
                        "processed. Check credentials and redeploy."
                    ),
                },
            },
            {"type": "divider"},
        ]
        sent = self._post(text, blocks)
        if sent:
            log.info(f"Slack: startup failure alert SENT ({service}/{error_kind})")
        else:
            log.error(f"Slack: startup failure alert FAILED ({service}/{error_kind})")
        return sent
