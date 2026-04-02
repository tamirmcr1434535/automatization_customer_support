import logging
import requests

log = logging.getLogger("zendesk")


class ZendeskClient:
    def __init__(self, subdomain, email, api_token, dry_run=True):
        self.base = f"https://{subdomain}.zendesk.com/api/v2"
        self.auth = (f"{email}/token", api_token)
        self.dry_run = dry_run
        if dry_run:
            log.info("ZendeskClient: DRY_RUN — no writes")

    def get_ticket(self, ticket_id: str) -> dict | None:
        """Read-only — always real even in dry_run."""
        resp = requests.get(
            f"{self.base}/tickets/{ticket_id}.json", auth=self.auth, timeout=10
        )
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        ticket = resp.json()["ticket"]

        rid = ticket.get("requester_id")
        if rid:
            u = requests.get(f"{self.base}/users/{rid}.json", auth=self.auth, timeout=10)
            if u.ok:
                ticket["requester"] = u.json()["user"]
        return ticket

    def _fetch_comments_with_agent_ids(self, ticket_id: str) -> tuple[list, set]:
        """
        Fetch all comments for a ticket, returning (comments, agent_ids).

        Uses ?include=users so we get user roles inline — the base comments endpoint
        only returns author_id (no embedded author object), so we must include users
        to reliably distinguish end-user comments from agent/bot comments.

        agent_ids: set of user IDs whose role is "agent" or "admin".
        Always real, even in dry_run.
        """
        resp = requests.get(
            f"{self.base}/tickets/{ticket_id}/comments.json",
            params={"include": "users"},
            auth=self.auth,
            timeout=10,
        )
        if not resp.ok:
            log.warning(f"Could not fetch comments for #{ticket_id}: {resp.status_code}")
            return [], set()

        data = resp.json()
        agent_ids = {
            u["id"]
            for u in data.get("users", [])
            if u.get("role") in ("agent", "admin")
        }
        return data.get("comments", []), agent_ids

    def get_first_customer_comment(self, ticket_id: str) -> str | None:
        """
        Return the plain body of the FIRST public comment from the end-user (non-agent).
        Used for messaging/chat tickets where description is empty or just a header.
        Always real, even in dry_run.
        """
        comments, agent_ids = self._fetch_comments_with_agent_ids(ticket_id)
        for comment in comments:  # oldest first
            if (
                comment.get("public")
                and comment.get("author_id") not in agent_ids
            ):
                return comment.get("plain_body") or comment.get("body", "")
        return None

    def get_all_customer_comments_text(self, ticket_id: str) -> str:
        """
        Return concatenated text of ALL public customer (non-agent) comments, oldest first.
        Used to search for alternative email addresses when primary lookup fails.
        Always real, even in dry_run.
        """
        comments, agent_ids = self._fetch_comments_with_agent_ids(ticket_id)
        return "\n".join(
            comment.get("plain_body") or comment.get("body", "")
            for comment in comments
            if comment.get("public") and comment.get("author_id") not in agent_ids
        )

    def last_public_comment_is_from_agent(self, ticket_id: str) -> bool:
        """
        Return True if the most recent PUBLIC comment was posted by an agent or admin.

        Used to detect tickets where a human agent already replied — in that case
        the bot should step aside and not escalate or interfere.
        Always real, even in dry_run.
        """
        comments, agent_ids = self._fetch_comments_with_agent_ids(ticket_id)
        for comment in reversed(comments):
            if comment.get("public"):
                return comment.get("author_id") in agent_ids
        return False  # no public comments yet

    def get_last_customer_comment(self, ticket_id: str) -> str | None:
        """
        Return the plain body of the most recent public comment from the end-user
        (non-agent). Always real, even in dry_run.
        """
        comments, agent_ids = self._fetch_comments_with_agent_ids(ticket_id)
        for comment in reversed(comments):
            if (
                comment.get("public")
                and comment.get("author_id") not in agent_ids
            ):
                return comment.get("plain_body") or comment.get("body", "")
        return None

    def post_reply(self, ticket_id: str, body: str):
        if self.dry_run:
            log.info(f"[DRY] reply → #{ticket_id}: {body[:120]}...")
            return
        requests.put(
            f"{self.base}/tickets/{ticket_id}.json",
            json={"ticket": {"comment": {"body": body, "public": True}}},
            auth=self.auth, timeout=10,
        ).raise_for_status()

    def post_reply_and_set_pending(self, ticket_id: str, body: str):
        """
        Post a public reply AND set ticket status to Pending in one API call.
        Zendesk Pending automation will handle:
          - 24h: send reminder email to customer
          - 72h after that: close ticket with no-response message
        When customer replies, Zendesk auto-moves ticket back to Open,
        which fires our trigger again.
        """
        if self.dry_run:
            log.info(f"[DRY] reply+pending → #{ticket_id}: {body[:120]}...")
            return
        requests.put(
            f"{self.base}/tickets/{ticket_id}.json",
            json={"ticket": {
                "status": "pending",
                "comment": {"body": body, "public": True},
            }},
            auth=self.auth, timeout=10,
        ).raise_for_status()

    def add_tag(self, ticket_id: str, tag: str):
        if self.dry_run:
            log.info(f"[DRY] tag '{tag}' → #{ticket_id}")
            return
        requests.post(
            f"{self.base}/tickets/{ticket_id}/tags.json",
            json={"tags": [tag]}, auth=self.auth, timeout=10,
        ).raise_for_status()

    def remove_tag(self, ticket_id: str, tag: str):
        if self.dry_run:
            log.info(f"[DRY] remove tag '{tag}' from #{ticket_id}")
            return
        requests.delete(
            f"{self.base}/tickets/{ticket_id}/tags.json",
            json={"tags": [tag]}, auth=self.auth, timeout=10,
        ).raise_for_status()

    def set_open(self, ticket_id: str):
        """Set ticket status to Open so it appears in agent queues for manual handling."""
        if self.dry_run:
            log.info(f"[DRY] set open → #{ticket_id}")
            return
        requests.put(
            f"{self.base}/tickets/{ticket_id}.json",
            json={"ticket": {"status": "open"}},
            auth=self.auth, timeout=10,
        ).raise_for_status()

    def solve_ticket(self, ticket_id: str):
        if self.dry_run:
            log.info(f"[DRY] solve → #{ticket_id}")
            return
        requests.put(
            f"{self.base}/tickets/{ticket_id}.json",
            json={"ticket": {"status": "solved"}},
            auth=self.auth, timeout=10,
        ).raise_for_status()

    def add_internal_note(self, ticket_id: str, note: str):
        if self.dry_run:
            log.info(f"[DRY] note → #{ticket_id}: {note[:80]}")
            return
        requests.put(
            f"{self.base}/tickets/{ticket_id}.json",
            json={"ticket": {"comment": {"body": note, "public": False}}},
            auth=self.auth, timeout=10,
        ).raise_for_status()
