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

    def get_first_customer_comment(self, ticket_id: str) -> str | None:
        """
        Return the plain body of the FIRST public comment from the end-user.
        Used for messaging/chat tickets where description is empty or just a header.
        Always real, even in dry_run.
        """
        resp = requests.get(
            f"{self.base}/tickets/{ticket_id}/comments.json",
            auth=self.auth,
            timeout=10,
        )
        if not resp.ok:
            log.warning(f"Could not fetch comments for #{ticket_id}: {resp.status_code}")
            return None

        comments = resp.json().get("comments", [])
        for comment in comments:  # oldest first
            if comment.get("public") and not comment.get("author", {}).get("agent", False):
                return comment.get("plain_body") or comment.get("body", "")
        return None

    def get_all_customer_comments_text(self, ticket_id: str) -> str:
        """
        Return concatenated text of ALL public customer comments (oldest first).
        Used to search for alternative email addresses when primary lookup fails —
        covers cases where a customer mentioned a different email in a follow-up reply.
        Always real, even in dry_run.
        """
        resp = requests.get(
            f"{self.base}/tickets/{ticket_id}/comments.json",
            auth=self.auth,
            timeout=10,
        )
        if not resp.ok:
            log.warning(f"Could not fetch comments for #{ticket_id}: {resp.status_code}")
            return ""

        comments = resp.json().get("comments", [])
        return "\n".join(
            comment.get("plain_body") or comment.get("body", "")
            for comment in comments
            if comment.get("public") and not comment.get("author", {}).get("agent", False)
        )

    def get_last_customer_comment(self, ticket_id: str) -> str | None:
        """
        Return the plain body of the most recent public comment from the end-user
        (non-agent). Always real, even in dry_run.
        """
        resp = requests.get(
            f"{self.base}/tickets/{ticket_id}/comments.json",
            auth=self.auth,
            timeout=10,
        )
        if not resp.ok:
            log.warning(f"Could not fetch comments for #{ticket_id}: {resp.status_code}")
            return None

        comments = resp.json().get("comments", [])
        for comment in reversed(comments):
            if comment.get("public") and not comment.get("author", {}).get("agent", False):
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
