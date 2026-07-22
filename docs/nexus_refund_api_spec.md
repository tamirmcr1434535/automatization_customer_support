# Nexus refund — two-API design (IQTEST-1431 / AN-192)

**Decided in huddle (Jar Halst + Harshad Patel, 2026-07-21).** Refund is split into
**two separate APIs** on purpose — validation is read-only and separate from execution,
so a mistake or retry can never move money wrongly.

## Flow

1. **Validate (read-only):** bot calls the enhanced **search-subscription** API by email,
   gets the list of charges, and compares them against what the customer wrote in the
   ticket (amount, date, card last4) to detect fraud / discrepancies and pick the correct
   charge. No money moves.
2. **Execute (gated):** only after a clean match, bot calls the **refund** API with the
   `charge_id`. Full-amount refund only. One refund request may trigger these two calls.

---

## API 1 — Search subscription (enhanced, read-only)

`POST /api/v1/customer/search-subscription` — add a new **`charges`** key: an array of
charge objects with all available metadata from Stripe and PayPal, each with:

- `charge_id`
- `amount` (minor units) + `currency`
- `date` — transaction / DB record date (a stable date, **not** a continuously-updated
  charge-creation timestamp)
- `provider` (`stripe` / `paypal`)
- `card_last4` — **last 4 digits only** (never the full card number — PCI/privacy)
- `status` (`paid` / `already_refunded`)

Purpose: the bot matches the amount + date (+ card last4 if the customer gave it) from the
ticket against these charges. Today the lookup returns only `subscription_id` (no charge,
no amount), which makes validation impossible.

**Matching logic (bot side):**
- Primary key = **amount + currency** (+ **date** to disambiguate — a customer may cite
  several charges, e.g. "5490 on 7/21", "199 + 1990 on 7/14").
- `card_last4` = optional secondary key when the customer sends it from a receipt.
- Amounts may differ slightly (bank / FX fees) → a mismatch means **"send to a human"**,
  not fraud.
- One clean unique match → candidate charge. No match or several → human. Fail-closed.

## API 2 — Refund (execute)

`POST /api/v1/customer/refund` · Bearer auth.

**Request:** `charge_id` (only) + `reason`. No amount/currency — system always refunds the
**full** charge (no partial). Idempotency handled server-side by charge.

**Response:** `status` (`refunded` / `already_refunded` / `rejected` / `failed`),
`refunded_amount`, `currency`, `provider`.

**Server-side safety:** idempotency + already-refunded (no double refund), atomicity on
retry/timeout, audit (reason + who).

---

## STILL OPEN — hard prerequisite before enabling execution

**Dispute guard — for Stripe AND PayPal.** An open dispute/chargeback must make the refund
API **refuse** (e.g. `rejected` / `DISPUTE_OPEN`). Refunding a disputed charge = paying
twice (going negative). Nexus can't see disputes (especially PayPal), so this must be
enforced server-side in the refund API. Not covered in the huddle — needs confirmation.
Refunds stay disabled (`REFUNDS_ENABLED=false`) until this is implemented and confirmed.

Also confirm: what the refund API returns when it does **not** refund (distinct
`rejected`/`failed` status + reason), so the bot can tell success from refusal.
