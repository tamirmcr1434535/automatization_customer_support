import logging
import time
import requests

log = logging.getLogger("zendesk")

# Retry config for 429 rate-limit responses
_MAX_RETRIES = 3
_DEFAULT_RETRY_AFTER = 1  # seconds, if Retry-After header is missing


class ZendeskClient:
    def __init__(self, subdomain, email, api_token, dry_run=True, shadow_mode=False):
        self.base = f"https://{subdomain}.zendesk.com/api/v2"
        self.auth = (f"{email}/token", api_token)
        self.dry_run = dry_run
        self.shadow_mode = shadow_mode
        if shadow_mode:
            log.info("ZendeskClient: SHADOW_MODE — tags allowed, replies/status blocked")
        elif dry_run:
            log.info("ZendeskClient: DRY_RUN — no writes")

    def _request_with_retry(
        self, method: str, url: str, accept_statuses: set[int] | None = None, **kwargs
    ) -> requests.Response:
        """
        Send an HTTP request with automatic retry on 429 (Too Many Requests).

        Zendesk returns Retry-After header with the number of seconds to wait.
        Retries up to _MAX_RETRIES times with exponential backoff fallback.

        accept_statuses: additional HTTP codes that should NOT raise (e.g. {404}).
        """
        if accept_statuses is None:
            accept_statuses = set()

        for attempt in range(_MAX_RETRIES + 1):
            resp = requests.request(method, url, auth=self.auth, timeout=10, **kwargs)

            if resp.status_code != 429:
                if resp.status_code not in accept_statuses:
                    resp.raise_for_status()
                return resp

            if attempt == _MAX_RETRIES:
                log.error(
                    f"Zendesk 429: exhausted {_MAX_RETRIES} retries for {method} {url}"
                )
                resp.raise_for_status()  # will raise HTTPError

            retry_after = int(resp.headers.get("Retry-After", _DEFAULT_RETRY_AFTER * (attempt + 1)))
            log.warning(
                f"Zendesk 429: rate limited on {method} {url} — "
                f"retry {attempt + 1}/{_MAX_RETRIES} in {retry_after}s"
            )
            time.sleep(retry_after)

        return resp  # unreachable, but keeps type checker happy

    def get_ticket(self, ticket_id: str) -> dict | None:
        """Read-only — always real even in dry_run."""
        resp = self._request_with_retry(
            "GET", f"{self.base}/tickets/{ticket_id}.json",
            accept_statuses={404},
        )
        if resp.status_code == 404:
            return None
        ticket = resp.json()["ticket"]

        rid = ticket.get("requester_id")
        if rid:
            u = self._request_with_retry("GET", f"{self.base}/users/{rid}.json")
            if u.ok:
                ticket["requester"] = u.json()["user"]
        return ticket

    def get_ticket_tags(self, ticket_id: str) -> list[str]:
        """
        Lightweight re-fetch of current ticket tags only.

        Used as a race-condition guard: before sending a reply that sets a new
        state tag (e.g. awaiting_card_digits), re-fetch to check whether a
        concurrent webhook call already added the tag. Cheaper than a full
        get_ticket() because it fetches only the ticket object (no user lookup).

        Always real, even in dry_run.
        """
        try:
            resp = self._request_with_retry(
                "GET", f"{self.base}/tickets/{ticket_id}.json",
            )
        except requests.exceptions.HTTPError as e:
            log.warning(f"get_ticket_tags: could not fetch #{ticket_id}: {e}")
            return []
        return resp.json().get("ticket", {}).get("tags", [])

    def _fetch_comments_with_agent_ids(self, ticket_id: str) -> tuple[list, set]:
        """
        Fetch all comments for a ticket, returning (comments, agent_ids).

        Uses ?include=users so we get user roles inline — the base comments endpoint
        only returns author_id (no embedded author object), so we must include users
        to reliably distinguish end-user comments from agent/bot comments.

        agent_ids: set of user IDs whose role is "agent" or "admin".
        Always real, even in dry_run.
        """
        try:
            resp = self._request_with_retry(
                "GET", f"{self.base}/tickets/{ticket_id}/comments.json",
                params={"include": "users"},
            )
        except requests.exceptions.HTTPError as e:
            log.warning(f"Could not fetch comments for #{ticket_id}: {e}")
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

    def count_bot_replies(self, ticket_id: str) -> int:
        """
        Count how many public comments were posted by agents (bot) on this ticket.
        Used for spam detection — if >= 2, the bot might be looping.
        Always real, even in dry_run.
        """
        comments, agent_ids = self._fetch_comments_with_agent_ids(ticket_id)
        return sum(
            1 for c in comments
            if c.get("public") and c.get("author_id") in agent_ids
        )

    def post_reply(self, ticket_id: str, body: str):
        if self.dry_run:
            log.info(f"[DRY] reply → #{ticket_id}: {body[:120]}...")
            return
        self._request_with_retry(
            "PUT", f"{self.base}/tickets/{ticket_id}.json",
            json={"ticket": {"comment": {"body": body, "public": True}}},
        )

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
        self._request_with_retry(
            "PUT", f"{self.base}/tickets/{ticket_id}.json",
            json={"ticket": {
                "status": "pending",
                "comment": {"body": body, "public": True},
            }},
        )

    def set_custom_field(self, ticket_id: str, field_id: int, value: str):
        """
        Set a single ticket custom field (e.g. the "Topic" dropdown).

        Zendesk merges custom_fields by id, so passing a single entry does
        NOT wipe other fields on the ticket.
        """
        if self.dry_run:
            log.info(
                f"[DRY] custom_field {field_id}={value!r} → #{ticket_id}"
            )
            return
        self._request_with_retry(
            "PUT", f"{self.base}/tickets/{ticket_id}.json",
            json={"ticket": {"custom_fields": [
                {"id": int(field_id), "value": value},
            ]}},
        )

    def add_tag(self, ticket_id: str, tag: str):
        if self.dry_run and not self.shadow_mode:
            log.info(f"[DRY] tag '{tag}' → #{ticket_id}")
            return
        self._request_with_retry(
            "POST", f"{self.base}/tickets/{ticket_id}/tags.json",
            json={"tags": [tag]},
        )

    def remove_tag(self, ticket_id: str, tag: str):
        if self.dry_run and not self.shadow_mode:
            log.info(f"[DRY] remove tag '{tag}' from #{ticket_id}")
            return
        self._request_with_retry(
            "DELETE", f"{self.base}/tickets/{ticket_id}/tags.json",
            json={"tags": [tag]},
        )

    def set_open(self, ticket_id: str):
        """Set ticket status to Open so it appears in agent queues for manual handling."""
        if self.dry_run:
            log.info(f"[DRY] set open → #{ticket_id}")
            return
        self._request_with_retry(
            "PUT", f"{self.base}/tickets/{ticket_id}.json",
            json={"ticket": {"status": "open"}},
        )

    def solve_ticket(self, ticket_id: str):
        if self.dry_run:
            log.info(f"[DRY] solve → #{ticket_id}")
            return
        self._request_with_retry(
            "PUT", f"{self.base}/tickets/{ticket_id}.json",
            json={"ticket": {"status": "solved"}},
        )

    def was_recently_handled(
        self, email: str, hours: int = 24, exclude_ticket_id: str = ""
    ) -> bool:
        """
        Return True if another ticket from the same requester email was already
        handled by the bot (has 'bot_handled' tag) in the last `hours` hours.

        Used to prevent sending multiple replies when a customer submits the
        same form several times in quick succession (e.g. 21 identical Help Form
        submissions in one minute).

        Always performs a real API call even in dry_run — read-only, safe.
        """
        from datetime import datetime, timezone, timedelta
        cutoff = (
            datetime.now(timezone.utc) - timedelta(hours=hours)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")

        query = (
            f"type:ticket requester:{email} "
            f"tags:bot_handled created>{cutoff}"
        )
        resp = requests.get(
            f"{self.base}/search.json",
            params={"query": query, "per_page": 5},
            auth=self.auth,
            timeout=10,
        )
        if not resp.ok:
            log.warning(
                f"Email dedup search failed ({resp.status_code}) — "
                "skipping dedup check (safe default: continue processing)"
            )
            return False

        results = resp.json().get("results", [])
        # Exclude the current ticket so we don't skip it on re-delivery
        return any(str(r.get("id")) != str(exclude_ticket_id) for r in results)

    def add_internal_note(self, ticket_id: str, note: str):
        if self.dry_run:
            log.info(f"[DRY] note → #{ticket_id}: {note[:80]}")
            return
        self._request_with_retry(
            "PUT", f"{self.base}/tickets/{ticket_id}.json",
            json={"ticket": {"comment": {"body": note, "public": False}}},
        )

    # ── Search (read-only, always real) ──────────────────────────────────

    def search_tickets(self, query: str, per_page: int = 100) -> list[dict]:
        """
        Zendesk Search API. Always real, even in dry_run.
        Returns list of ticket dicts matching the query.
        """
        all_results: list[dict] = []
        url = f"{self.base}/search.json"
        params = {"query": query, "per_page": per_page, "sort_by": "created_at", "sort_order": "desc"}

        try:
            resp = self._request_with_retry("GET", url, params=params)
            data = resp.json()
            all_results.extend(data.get("results", []))
        except Exception as e:
            log.error(f"Zendesk search failed: {e}")

        return all_results
