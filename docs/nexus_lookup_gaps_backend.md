# Backend ticket — Nexus `search-subscription` lookup coverage gaps (AN-192)

**Type:** Backend / Nexus data
**Owner:** Backend (Nexus / `iqtest-backend-nexus` loader + search API)
**Reporter:** AN-192 refund would-be audit
**Priority:** Medium — blocks the refund would-be signal for a real, refundable subset
**Money movement:** none (this only feeds the read-only would-be eval)

## Problem

The AN-192 refund would-be engine looks a customer up in Nexus via
`search_subscription(email)` and reads back `charges[]`. When the lookup returns
nothing the engine emits `NOT_FOUND_IN_NEXUS` (would_be = NO) — correctly, since
it has no data to reason over.

An accuracy audit against Zendesk ground truth (human `refund_approved` /
`refund_denied` tags) on 2026-07-24 found that **a real, refundable charge often
existed even though the lookup returned nothing** — the CS agent found it
manually and refunded. These are **data / lookup-coverage gaps in Nexus, not
logic bugs in the would-be engine**: the engine cannot help a customer whose
charge it can't see.

## Evidence — 4 audited tickets (post-release window, engine wb-flow12-v9)

All four: bot said `NOT_FOUND_IN_NEXUS` (NO); human tagged `refund_approved`.

| Ticket | Email (writing addr) | Charge customer cites | Likely cause |
|---|---|---|---|
| 167012 | yabiku370@gmail.com (JP) | ¥5,490, via wwpersonalitytest.com form | **Email mismatch** — paid under a different email than the contact-form address |
| 167050 | tobias.herp@gmx.de (DE) | **PayPal** €29.99 | **PayPal charge not indexed** by the writing email / absent from `search-subscription` |
| 167156 | sk_suipen-drop@yahoo.co.jp (JP) | **PayPal** ¥5,490 | Same — PayPal charge not found by contact email |
| 166997 | jarnoschneider1@gmail.com (NL) | €2.99 one-time | **One-time-only / email mismatch** — customer with no subscription record, or paid under a different email |

Broader signal: `NOT_FOUND_IN_NEXUS` was ~6 in the last 14h and 4/4 audited were
genuinely refundable — so this is a recurring, not one-off, gap.

> **NOTE (corrected):** `search-subscription` already **searches by email across
> every brand** Nexus has migrated (`nexus_client.py` docstring) — so *cross-brand*
> is NOT the cause. The real causes are **email mismatch** (customer writes from a
> different address than they paid with) and **PayPal charges not being in the
> index**. The bot-side mitigation for the email-mismatch slice (retry the lookup
> with alternate emails found in the ticket text) ships separately as
> `an-192-refund-alt-email`; the PayPal-indexing and one-time-only gaps below
> remain backend-only.

## Root causes & proposed fixes

1. **PayPal charges not searchable by the contact email.**
   Stripe charges resolve by email but PayPal ones don't (the PayPal payer email
   often differs from the email the customer writes from, and/or PayPal charges
   aren't in the `search-subscription` index).
   → **Index PayPal charges in `search-subscription`**, resolvable by the
   customer/account email (not only the PayPal payer email). Return them in
   `charges[]` with `provider="paypal"` (already a field per the v2 spec).

2. **Email mismatch (writing address ≠ paying address).**
   The customer writes from one email (contact form, or the address on file) but
   paid under another. `search-subscription` resolves by the single email the bot
   passes, so the charge is invisible. (Cross-brand is NOT the cause — the endpoint
   already searches across all migrated brands.)
   → Bot-side mitigation shipped (`an-192-refund-alt-email`): retry the lookup with
   alternate emails found in the ticket text. Backend can help further by matching
   on normalized email / linked-account emails, and by returning charges keyed to a
   customer, not just an exact email string.

3. **One-time-only customers / email mismatch.**
   `search-subscription` appears to return nothing when the customer has only a
   one-time charge and no subscription record.
   → **Return one-time-only customers too** (a `charges[]` with only
   `first_sale`/`cross_sale` and no `subscription`), so the engine can at least
   evaluate/decline explicitly instead of NOT_FOUND. Confirm email normalization
   (case/alias) on the lookup key.

## Acceptance criteria

- `search_subscription(email)` returns the customer's charges when the charge was
  paid via **PayPal**, resolvable by the account/contact email.
- A ticket originating from a sibling brand resolves the customer's charges
  (cross-brand), or the bot is given a brand-agnostic lookup.
- A one-time-only customer returns a non-empty `charges[]` instead of nothing.
- Re-running the 4 audited tickets (167012, 167050, 167156, 166997) through
  `search_subscription` returns the cited charge.

## How to verify (QA recipe)

Ground truth = Zendesk human tags `refund_approved` / `refund_denied` (see the
would-be audit). After the backend change, re-run the refund audit: the
`NOT_FOUND_IN_NEXUS`→`refund_approved` mismatch count should drop toward zero
(those tickets should instead produce a real would-be decision — YES within
window, or a specific NO reason).

## Out of scope

- The would-be engine logic (unchanged; this is purely lookup coverage).
- The separate `AMBIGUOUS_FLOW` routing improvement (tracked separately as a
  would-be engine v10 "dispute-target = surprise recurring charge" heuristic).

---

## UPDATE 2026-07-25 — confirmed by replay: renewal-type + loader freshness

Replayed `search_subscription` live for 4 audited "bot NO → human approved"
tickets and compared to what the bot saw at eval time (`refund_candidate_charges`)
and what the agent refunded. Two distinct root causes surfaced:

### (a) Recurring payments are `type="renewal"` — FIXED bot-side (engine v11)
Nexus returns the first paid period as `type="subscription"` and **every recurring
payment as `type="renewal"`**. The engine only recognized `"subscription"`, so
renewals were invisible to flow #1. Fixed in engine **v11** (`_is_subscription`
now accepts `renewal`). **Not a backend item** — recorded here for context.

### (b) Loader freshness lag — BACKEND, still open
Even with v11, the disputed renewal must be **present in Nexus at eval time**. In
these tickets the recent renewal the customer disputes was NOT yet loaded when the
bot evaluated (the bot's `candidate_charges` showed only an older/one-time charge),
though `search_subscription` returns it now.

| Ticket | Bot saw at eval | Nexus returns now | Agent refunded |
|---|---|---|---|
| 167485 | only `4990 first_sale @07-07` | + `39990 renewal @07-22` | 39990 renewal |
| 167518 | only `199 first_sale @07-16` | + `5490 subscription @07-23` | 5490 subscription |
| 167505 | only `299990 subscription @05-14` (72d) | 7 charges incl. `renewal @07-25` | latest renewal @07-25 |
| 167508 | only `29.99 @05-30` (56d) | + `renewal @06-27`, `@07-25` | fresh renewal |

Replay through engine v11 (eval-time simulation, disputed charge marked refundable):
167505 → **YES now on live data**; 167485 & 167508 → **YES** once the fresh renewal
is present; 167518 → still mis-routes (separate engine track: dispute-vs-accept).

**Ask:** `search-subscription` must return the customer's **recent** subscription
`renewal` charges (same-day / T-1 / T-2) at the time of the support ticket — the
loader lag currently hides the very charge the customer is disputing.

**Acceptance:** for a ticket filed the same day as a renewal charge,
`search_subscription(email)` includes that renewal within minutes; re-running the
refund audit, the `ONE_TIME`/`OUTSIDE_WINDOW`→`refund_approved` mismatches driven by
missing recent renewals drop toward zero.
