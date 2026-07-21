# Nexus API — refund endpoint (spec for backend)

**Type:** Story · **Component:** Nexus API (`apinexus.cellon.ai`) · **Related:** AN-192

## Context
The support-automation bot is adding a refund flow. Refund **execution must go
through Nexus** (the way operators do it manually) so state and reporting stay
authoritative. The current Nexus API is read-only (`/api/v1/customer/search-subscription`);
there is no refund capability. A new endpoint is required. **Critical:** the
endpoint must be safe on its own — even if the client (bot) makes a mistake or
retries, money must never go negative or be refunded twice.

## Endpoint
`POST /api/v1/customer/refund` · auth: `Authorization: Bearer <token>` (dedicated refund scope).

## Request

**Mandatory**
| Field | Type | Purpose |
|-------|------|---------|
| `idempotency_key` | string | Unique per refund attempt. Server dedups: a repeat with the same key must **not** issue a second refund — it returns the original result. |
| `order_id` | string/int | The **specific** charge/order to refund (NOT `subscription_id` — a sub has many renewal charges). |
| `amount` | integer (minor units) | Amount to refund. Server validates against what was actually charged. |
| `currency` | string (ISO-4217) | Must match the charge currency. |
| `reason` | string | Reason (for audit). |

**Optional (recommended)**
| Field | Type | Purpose |
|-------|------|---------|
| `refund_type` | enum `full`\|`partial` | Default `full`. For `partial`, `amount` required. |
| `dry_run` | boolean | Server-side validation with **no money movement** (returns whether it would pass + amount). Lets the bot test end-to-end safely. |
| `brand` | string | If the server can't derive it from `order_id`. |
| `ticket_id` | string | Zendesk ticket, for traceability. |
| `requested_by` | string | Who initiated (bot/agent id) — for audit. |

## Response

**Mandatory**
| Field | Type | Purpose |
|-------|------|---------|
| `status` | enum | `refunded` \| `already_refunded` \| `rejected` \| `failed` |
| `refund_id` | string | Refund id (Nexus and/or provider). |
| `refunded_amount` | integer (minor units) | The **actual** amount refunded. |
| `currency` | string | Refund currency. |
| `provider` | enum `stripe`\|`paypal` | Which provider processed the charge. |
| `charge_id` / `transaction_id` | string | Original charge id — for cross-check against Stripe. |
| `idempotency_key` | string | Echo. |
| `reason_code` | string | On `rejected`/`failed` (e.g. `DISPUTE_OPEN`, `AMOUNT_EXCEEDS_CHARGE`, `ALREADY_REFUNDED`). |

## Mandatory server-side logic (safety)
1. **Idempotency** — atomic dedup on `idempotency_key`; repeat returns original result, never a second refund.
2. **Amount validation** — `refunded_amount ≤ (charged − already_refunded)`; else `rejected / AMOUNT_EXCEEDS_CHARGE`. Never refund more than charged.
3. **Already-refunded / partial state** — account for prior refunds correctly.
4. **Dispute guard** — open Stripe/PayPal dispute/chargeback → **reject** with `DISPUTE_OPEN` (second line of defense; Nexus is closer to the provider than the bot).
5. **Atomicity** — on timeout/retry, do not double-refund; safely repeatable.
6. **Audit** — log who/when/how much/idempotency_key/result.

## Acceptance criteria
- Two calls with the same `idempotency_key` → exactly one refund.
- `amount` > refundable → `rejected / AMOUNT_EXCEEDS_CHARGE`, no money moves.
- Open dispute → `rejected / DISPUTE_OPEN`.
- `dry_run=true` → validation with no money movement, returns predicted amount/status.
- Response always includes `provider` and `charge_id` for cross-verification.
