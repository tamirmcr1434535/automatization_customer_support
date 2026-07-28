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
| Per-brand **hourly rate** (burst breaker) | `REFUND_MAX_PER_HOUR_PER_BRAND` | 5 | a sudden spike (attack / runaway bug) within the hour — flexible: lets steady legit volume through, slams the brakes on a burst |
| Per-brand **daily count** backstop | `REFUND_MAX_PER_DAY_PER_BRAND` | 30 | hard daily ceiling |
| Per-customer **velocity** | `REFUND_MAX_PER_EMAIL` / `REFUND_EMAIL_WINDOW_HOURS` | 2 / 24h | same customer farming refunds |

Counts `refund_execution_status='refunded'` rows. On trip → `refund_execution_status =
skipped_abuse_guard:<reason>`, refund NOT executed, ticket → human, **and an internal
note is posted to the agent** explaining the spike: *"Auto-refund PAUSED — refund
volume/velocity above normal … auto-refunds stay routed to a human until manually
re-enabled. Please review the spike and handle this refund manually."*

**Pause-until-toggle:** while the burst is active the rate guard keeps every refund
escalating to a human on its own. To hold the pause deliberately after a spike, the
operator flips the manual kill-switch — `REFUNDS_ENABLED=false` (or drop the brand
from `REFUNDS_ENABLED_BRANDS`) — and flips it back after reviewing. (Pairs with the
recommended Slack spike alert so the operator is notified to toggle.)

The per-brand control is **two-tier on purpose**: a fixed daily count alone is too
crude (a small brand needs a small cap, a big one a large one, and it reacts a full
day late). The **rolling-hour rate** adapts to that — it doesn't care about brand
size, it only trips on an abnormal *burst*, which is exactly what an attack or a
runaway bug looks like — while the daily count stays as a hard backstop.

**Recommended canary thresholds:** brand ≤ 5/hour + ≤ 30/day; email ≤ 2 / 24h.
Tighten the hourly rate to ~3 for the very first day if desired.

**Next upgrade (recommended):** a per-brand+currency **daily SUM cap** (money, not
count) — 10 large refunds are worse than 30 small ones; and an **adaptive baseline**
(trip when today's bot-refund rate exceeds the brand's trailing-7-day mean + k·σ)
for a fully self-tuning breaker.

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
