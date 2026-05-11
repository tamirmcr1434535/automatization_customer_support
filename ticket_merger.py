"""
In-process merger for duplicate Zendesk tickets.

When a customer creates a new ticket while they already have other ACTIVE
tickets, this module folds the newer tickets into the OLDEST active one
via Zendesk's native merge API. The oldest ticket survives; newer ones
are closed and their contents land as private comments on the survivor.

Ported from the external `ticket_merger` FastAPI service. Differences:
- Synchronous (no asyncio / no FastAPI / no keep-alive loop).
- Reuses zendesk_client.py for auth, retry, and dry_run handling.
- No PRE_DELAY: the caller already detected siblings via
  find_active_tickets_for_email, so we have proof there's work to do
  and don't need to wait for Zendesk search indexing.
- Returns a status dict instead of fire-and-forget; the caller in main.py
  re-fetches the current ticket afterwards to decide skipped_merged vs.
  continue-processing.
"""
import logging

log = logging.getLogger("ticket_merger")

# Tickets from these requester emails are NEVER merged. Regulatory /
# government addresses where folding tickets together would break audit
# trails on the human side.
EMAIL_BLACKLIST: set[str] = {
    "crossborder@kca.go.kr",
    "cukbusan@cuk.or.kr",
}


def _extract_requester_email(ticket: dict) -> str:
    """Best-effort: pull the requester's email from the via.source.from chain."""
    via = ticket.get("via") or {}
    src = via.get("source") or {}
    frm = src.get("from") or {}
    return (frm.get("address") or "").strip().lower()


def merge_user_tickets(
    ticket_id: str,
    requester_id: str,
    zendesk,
) -> dict:
    """
    Fold all active tickets from `requester_id` (except the OLDEST) into
    that oldest active ticket. Returns a status dict describing what
    happened. Never raises.

    Status values:
      - "merged":            at least one merge call succeeded. Includes
                             target_id, merged_ids, current_was_target.
      - "no_action":         <= 1 active tickets — nothing to merge.
      - "skipped_blacklist": requester email is in EMAIL_BLACKLIST.
      - "ticket_not_found":  current ticket disappeared mid-flight.
      - "no_requester_id":   defensive guard for missing requester.
      - "error":             every merge attempt raised. Includes errors.
    """
    if not requester_id:
        return {"status": "no_requester_id"}

    current = zendesk.get_ticket(ticket_id)
    if not current:
        return {"status": "ticket_not_found"}

    email = _extract_requester_email(current)
    if email in EMAIL_BLACKLIST:
        log.info(
            f"[{ticket_id}] requester {email!r} is in EMAIL_BLACKLIST — "
            "skipping merge"
        )
        return {"status": "skipped_blacklist", "email": email}

    all_tickets = zendesk.search_user_tickets(requester_id)
    # Search may not have indexed the just-created current ticket yet —
    # ensure it's in the candidate set so we don't accidentally exclude it
    # from sibling resolution.
    if current.get("id") is not None:
        all_tickets[current["id"]] = current

    active = {
        tid: t for tid, t in all_tickets.items()
        if t.get("status") != "closed"
    }
    log.info(
        f"[{ticket_id}] found {len(active)}/{len(all_tickets)} active "
        f"ticket(s) for requester {requester_id}"
    )

    if len(active) <= 1:
        return {"status": "no_action", "active_count": len(active)}

    sorted_tickets = sorted(
        active.values(), key=lambda t: t.get("created_at") or ""
    )
    oldest = sorted_tickets[0]
    target_id = oldest["id"]
    # Preserve "new" if the survivor is still untouched; otherwise mark as
    # "open" so it surfaces in agent queues. Zendesk merge can otherwise
    # leave the target in a stale state.
    target_status = "new" if oldest.get("status") == "new" else "open"

    merged_ids: list = []
    errors: list[str] = []
    for src in sorted_tickets[1:]:
        src_id = src["id"]
        subject = src.get("subject") or ""
        description = src.get("description") or ""
        target_comment = (
            f"Ticket #{src_id} ({subject}: {description}) merged "
            f"into this ticket"
        )
        source_comment = f"Merged into ticket #{target_id}"
        try:
            log.info(f"[{ticket_id}] merging #{src_id} → #{target_id}")
            zendesk.merge_tickets(
                target_id=str(target_id),
                source_ids=[src_id],
                target_comment=target_comment,
                source_comment=source_comment,
            )
            zendesk.update_ticket_status(str(target_id), target_status)
            merged_ids.append(src_id)
        except Exception as e:
            log.warning(
                f"[{ticket_id}] merge #{src_id} → #{target_id} failed: {e}",
                exc_info=True,
            )
            errors.append(f"{src_id}: {e}")

    return {
        "status": "merged" if merged_ids else "error",
        "target_id": target_id,
        "merged_ids": merged_ids,
        "current_was_target": str(target_id) == str(ticket_id),
        "errors": errors,
    }
