# Refund abuse / fraud protections (AN-192)

How a bad actor could profit from the refund bot, and what stops them. Refunds
always go back to the **original payment method** (Stripe/PayPal full refund to
source), so funds cannot be redirected — the real exposure is **volume abuse**
(mass in-window refund farming), **double-dip** (bot refund + chargeback), and
**runaway execution** (bug/flood draining a brand).

## Per-decision guards (already in place)

These stop a single *wrong* refund. All fail-closed → human.

| Guard | Blocks |
|---|---|
| Country refund **window** (charge date → ticket date) | out-of-window renewals |
| **Amount** guard (±20% of standard renewal price, non-zero, known currency) | anomalous / manipulated amounts |
| **Dispute** guard (charge-detail `disputed=true`) | refunding a disputed charge (double-pay) |
| **refundable=false / refunded_at** guard | double refund of the same charge |
| **x-host** guard | executing against an unknown/unmapped brand scope |
| **LLM-disambiguated** gate | low-confidence routing (0/2 in backtest) auto-executing |
| **Confidence** ≥ 0.90 + **brand allowlist** (`REFUNDS_ENABLED_BRANDS`) | low-confidence / non-canary brands |
| Amount is **rule-selected** (latest renewal), never customer-stated | amount injection via ticket text |

## Volume guards (NEW — this branch: `refund_abuse.py`)

Evaluated against the BigQuery refund log immediately before execution.
**FAIL-CLOSED**: any error → escalate, never execute. Env-tunable; kill with
`REFUND_ABUSE_GUARD_ENABLED=false`.

| Guard | Env | Default | Blocks |
|---|---|---|---|
| Per-brand **daily circuit breaker** | `REFUND_MAX_PER_DAY_PER_BRAND` | 30 | mass abuse / runaway bug draining a brand in one day |
| Per-customer **velocity** | `REFUND_MAX_PER_EMAIL` / `REFUND_EMAIL_WINDOW_DAYS` | 1 / 30d | same customer farming refunds across renewals |

Counts `refund_execution_status='refunded'` rows. On trip → `refund_execution_status =
skipped_abuse_guard:<reason>`, refund NOT executed, ticket → human.

**Recommended canary thresholds:** keep defaults (brand ≤ 30/day, 1 refund per
email per 30 days). Tighten `REFUND_MAX_PER_DAY_PER_BRAND` to ~10 for the very
first day if desired.

## Proposed — not yet implemented (prioritised)

1. **Daily SUM cap per brand+currency** (`REFUND_MAX_SUM_PER_DAY_*`) — cap total
   money, not just count. Stronger circuit breaker; needs per-currency handling.
2. **`chargeback_risk` gate** — the classifier already sets `chargeback_risk`;
   when true, do not auto-execute even if in-window (pre-empts double-dip).
3. **Fresh-charge cooldown** — if the charge is < N hours old + high value, hold
   for a human (blocks buy-then-instant-refund farming).
4. **Send `idempotency_key`** in `create_refund` body (currently accepted but not
   sent) — belt-and-suspenders vs double-execution, in addition to server-side
   `already_refunded`.
5. **Velocity spike alert** — Slack alert when refunds/hour exceeds a baseline
   (pairs with the `charges[] outage` monitor).
6. **Approve-path human-in-the-loop toggle** — a mode that drafts approvals for a
   human to confirm while auto-sending declines (removes the residual in-window
   "human judged legitimate" false-positive class during early canary).

## Not exploitable (verified)

- Redirecting funds — refund goes to original source only.
- Amount inflation via ticket text — amount is rule-selected from Nexus.
- Prompt injection into the decision — engine is deterministic on the window;
  the LLM only *selects among real refundable charges*, and LLM-routed approvals
  are not auto-executed.
