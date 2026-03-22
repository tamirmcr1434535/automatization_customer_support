import logging
import requests

log = logging.getLogger("zendesk")


class ZendeskClient:
    def __init__(self, subdomain, email, api_token, dry_run=True):
        self.base    = f"https://{subdomain}.zendesk.com/api/v2"
        self.auth    = (f"{email}/token", api_token)
        self.dry_run = dry_run
        if dry_run:
            log.info("ZendeskClient: DRY_RUN — no writes")

    def get_ticket(self, ticket_id: str) -> dict | None:
        """Read-only — always real even in dry_run."""
        resp = requests.get(f"{self.base}/tickets/{ticket_id}.json", auth=self.auth, timeout=10)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        ticket = resp.json()["ticket"]

        # Fetch requester email
        rid = ticket.get("requester_id")
        if rid:
            u = requests.get(f"{self.base}/users/{rid}.json", auth=self.auth, timeout=10)
            if u.ok:
                ticket["requester"] = u.json()["user"]
        return ticket

    def post_reply(self, ticket_id: str, body: str):
        if self.dry_run:
            log.info(f"[DRY] reply → #{ticket_id}: {body[:120]}...")
            return
        requests.put(
            f"{self.base}/tickets/{ticket_id}.json",
            json={"ticket": {"comment": {"body": body, "public": True}}},
            auth=self.auth, timeout=10
        ).raise_for_status()

    def add_tag(self, ticket_id: str, tag: str):
        if self.dry_run:
            log.info(f"[DRY] tag '{tag}' → #{ticket_id}")
            return
        requests.post(
            f"{self.base}/tickets/{ticket_id}/tags.json",
            json={"tags": [tag]}, auth=self.auth, timeout=10
        ).raise_for_status()

    def solve_ticket(self, ticket_id: str):
        if self.dry_run:
            log.info(f"[DRY] solve → #{ticket_id}")
            return
        requests.put(
            f"{self.base}/tickets/{ticket_id}.json",
            json={"ticket": {"status": "solved"}},
            auth=self.auth, timeout=10
        ).raise_for_status()

    def add_internal_note(self, ticket_id: str, note: str):
        if self.dry_run:
            log.info(f"[DRY] note → #{ticket_id}: {note[:80]}")
            return
        requests.put(
            f"{self.base}/tickets/{ticket_id}.json",
            json={"ticket": {"comment": {"body": note, "public": False}}},
            auth=self.auth, timeout=10
        ).raise_for_status()
