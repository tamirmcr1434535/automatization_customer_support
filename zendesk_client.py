import logging
import time
import requests

log = logging.getLogger("zendesk")

# Retry config for 429 rate-limit responses
_MAX_RETRIES = 3
_DEFAULT_RETRY_AFTER = 1  # seconds, if Retry-After header is missing

# Write methods that mutate ticket state. A 422 on any of these almost
# always means the ticket was merged / closed between our read and our
# write — Zendesk rejects further mutations on a merged-away ticket with
# 422 Unprocessable Entity.
_WRITE_METHODS = {"POST", "PUT", "DELETE"}


class TicketNotWritableError(Exception):
    """
    Raised when a write request (tag/note/reply/status) is rejected with
    422 Unprocessable Entity. Almost always caused by a race: the ticket
    was merged or closed by an agent (or a parallel workflow) between the
    bot's initial fetch and the write. Caller should treat this as
    "ticket disappeared under us" and skip cleanly rather than
    surfacing it as a generic error.
    """

    def __init__(self, ticket_id: str, method: str, url: str, detail: str):
        self.ticket_id = ticket_id
        self.method = method
        self.url = url
        self.detail = detail
        super().__init__(
            f"Zendesk 422 on {method} {url} (ticket #{ticket_id}): {detail}"
        )


def _extract_ticket_id_from_url(url: str) -> str:
    """Best-effort: pull the ticket id out of a /tickets/{id}/... URL."""
    import re
    m = re.search(r"/tickets/(\d+)", url)
    return m.group(1) if m else ""


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
                if resp.status_code == 422 and method.upper() in _WRITE_METHODS:
                    # Ticket was merged/closed mid-flight — surface as a
                    # typed exception so main.py can skip cleanly instead
                    # of reporting a raw HTTPError to Slack.
                    detail = (resp.text or "")[:300]
                    raise TicketNotWritableError(
                        ticket_id=_extract_ticket_id_from_url(url),
                        method=method.upper(),
                        url=url,
                        detail=detail,
                    )
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

        agent_ids: set of user IDs whose role is NOT "end-user". This covers
        "agent", "admin", and Zendesk's newer roles ("light-agent",
        "contributor", custom role names). Previously the set only included
        the two literal strings "agent" and "admin" — when Vova replied
        under a non-default role, last_public_comment_is_from_agent
        returned False and the bot wrote on top of his work.
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
            if u.get("role") and u.get("role") != "end-user"
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

    def find_active_tickets_for_email(
        self,
        email: str,
        exclude_ticket_id: str = "",
        days: int = 14,
    ) -> list[dict]:
        """
        Return tickets from the same requester that are still MERGEABLE
        (status < closed, i.e. new / open / pending / hold / solved)
        within the last `days` days, excluding `exclude_ticket_id`.

        Includes SOLVED tickets on purpose: a Solved sibling means the bot
        already replied to an earlier message from this customer, and the
        new ticket is a follow-up (cf. shiho12210 case where #116700 was
        Solved before #116783 / #116801 arrived). The merger needs to see
        those Solved tickets so it can fold the follow-up into the
        original thread and re-open it for human review. Only truly
        Closed (archived) tickets are excluded.

        Used by `_process` to decide whether to invoke ticket_merger.

        Always performs a real API call even in dry_run — read-only, safe.
        Returns [] on any API error (fail-open: bot continues as normal).
        """
        if not email:
            return []
        from datetime import datetime, timezone, timedelta
        cutoff = (
            datetime.now(timezone.utc) - timedelta(days=days)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")

        query = (
            f"type:ticket requester:{email} "
            f"status<closed created>{cutoff}"
        )
        try:
            resp = requests.get(
                f"{self.base}/search.json",
                params={"query": query, "per_page": 30},
                auth=self.auth,
                timeout=10,
            )
        except requests.exceptions.RequestException as e:
            log.warning(f"Merge-candidate search error for {email}: {e}")
            return []

        if not resp.ok:
            log.warning(
                f"Merge-candidate search failed for {email} "
                f"({resp.status_code}) — fail-open"
            )
            return []

        exclude = str(exclude_ticket_id)
        results = resp.json().get("results", [])
        return [t for t in results if str(t.get("id")) != exclude]

    def add_internal_note(self, ticket_id: str, note: str):
        if self.dry_run:
            log.info(f"[DRY] note → #{ticket_id}: {note[:80]}")
            return
        self._request_with_retry(
            "PUT", f"{self.base}/tickets/{ticket_id}.json",
            json={"ticket": {"comment": {"body": note, "public": False}}},
        )

    # ── Merger support (search user tickets, merge, update status) ──────

    def search_user_tickets(
        self,
        requester_id: str,
        sort_by: str = "created_at",
        sort_order: str = "asc",
    ) -> dict[int, dict]:
        """
        Return all tickets for `requester_id`, keyed by ticket id.
        Paginates through the Zendesk Search API. Used by ticket_merger
        to discover sibling tickets before folding them into the oldest.

        Always real, even in dry_run. Returns {} on any API error
        (fail-open: caller will see no siblings → no merge).
        """
        if not requester_id:
            return {}

        tickets: dict[int, dict] = {}
        url = f"{self.base}/search.json"
        params = {
            "query": f"type:ticket requester_id:{requester_id}",
            "sort_by": sort_by,
            "sort_order": sort_order,
            "per_page": 100,
        }

        try:
            resp = requests.get(url, params=params, auth=self.auth, timeout=10)
            if not resp.ok:
                log.warning(
                    f"search_user_tickets failed for {requester_id} "
                    f"({resp.status_code}) — fail-open"
                )
                return {}
            data = resp.json()
            for t in data.get("results", []):
                if t.get("id"):
                    tickets[t["id"]] = t
            next_page = data.get("next_page")
            while next_page:
                resp = requests.get(next_page, auth=self.auth, timeout=10)
                if not resp.ok:
                    log.warning(
                        f"search_user_tickets pagination failed for "
                        f"{requester_id} ({resp.status_code})"
                    )
                    break
                data = resp.json()
                for t in data.get("results", []):
                    if t.get("id"):
                        tickets[t["id"]] = t
                next_page = data.get("next_page")
        except requests.exceptions.RequestException as e:
            log.warning(f"search_user_tickets error for {requester_id}: {e}")
            return {}

        return tickets

    def merge_tickets(
        self,
        target_id: str,
        source_ids: list,
        target_comment: str = "",
        source_comment: str = "",
        target_comment_is_public: bool = False,
        source_comment_is_public: bool = False,
    ) -> dict | None:
        """
        Merge `source_ids` into `target_id` via Zendesk's native merge API.
        Source tickets become closed and their contents land as comments
        on the target. Returns the Zendesk job_status response on success,
        or None in dry_run.
        """
        if self.dry_run:
            log.info(
                f"[DRY] merge {source_ids} → #{target_id} "
                f"(target_comment={target_comment[:80]!r})"
            )
            return None
        resp = self._request_with_retry(
            "POST", f"{self.base}/tickets/{target_id}/merge",
            json={
                "ids": source_ids,
                "target_comment": target_comment,
                "source_comment": source_comment,
                "target_comment_is_public": target_comment_is_public,
                "source_comment_is_public": source_comment_is_public,
            },
        )
        return resp.json()

    def update_ticket_status(self, ticket_id: str, status: str) -> dict | None:
        """
        Update ticket status (e.g. "new", "open", "pending"). Used after a
        merge to reset the survivor to an active state — Zendesk's merge
        API can flip the target to "solved" otherwise. Returns the Zendesk
        ticket response on success, or None in dry_run.
        """
        if self.dry_run:
            log.info(f"[DRY] update_status '{status}' → #{ticket_id}")
            return None
        resp = self._request_with_retry(
            "PUT", f"{self.base}/tickets/{ticket_id}.json",
            json={"ticket": {"status": status}},
        )
        return resp.json()

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
