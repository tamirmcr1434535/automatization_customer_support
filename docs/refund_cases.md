# Refund cases → would-be decision (AN-192)

This is the case map for the **would-be refund** evaluation. The bot computes
`would_be_refunded: YES/NO + reason_code` on real refund tickets, logs it to
BigQuery (`zendesk_bot.cancellation_logs`, `refund_*` columns) and to Slack, and
**still escalates the refund to a human exactly as before**. It never moves money.

> ⚠️ **Nexus-only, disputes NOT checked.** A `YES` here is a *draft signal for
> review*, **not** permission to refund. Nexus cannot see Stripe/PayPal disputes
> or non-Nexus payments (Slack #2/#3/#4). Dispute + PayPal checks are a hard
> prerequisite before any real execution (future iteration).

## Decision pipeline (fail-closed; YES only if every level passes)

| Level | Check | Fail → reason_code | Source |
|------|-------|--------------------|--------|
| 0 | Nexus available? | `UNABLE_TO_EVAL` | infra (R4) |
| 1 | Intent ∈ {REFUND_REQUEST, SUB_RENEWAL_REFUND} | `OUT_OF_SCOPE` | classifier |
| 2 | Classifier confidence ≥ `REFUND_MIN_CONFIDENCE` (0.90) | `LOW_CONFIDENCE` | classifier |
| 3 | Subscription/order found in Nexus | `NOT_FOUND_IN_NEXUS` | Nexus |
| 4 | Exactly one candidate charge | `CHARGE_AMBIGUOUS` | Nexus (Slack #8/#9/#10) |
| 5 | Verifiable amount present (A/B "150+150" summed) | `AMOUNT_UNAVAILABLE` | Nexus (Slack #1/#5) |
| 6 | All pass | **`WOULD_BE_REFUNDED`** (YES) | — |
| — | Unexpected error anywhere | `EVAL_ERROR` (fail-closed → NO) | engine |

## Case → expected outcome

| Case (from Jira AN-192 Slack notes) | would_be | reason_code |
|-------------------------------------|----------|-------------|
| #1 A/B price shown as "150+150" | (amount is **summed** to 300; not blocking by itself) | via level 5 → YES if other levels pass |
| #2 Payment not in Nexus | NO | `NOT_FOUND_IN_NEXUS` / `AMOUNT_UNAVAILABLE` |
| #3 Open PayPal dispute | NOT visible to Nexus → **known limitation**; hard-blocked pre-execution (future) | — (today may false-YES; do NOT act) |
| #4 Open Stripe dispute | NOT visible to Nexus → **known limitation** (future dispute-guard) | — |
| #5 Customer-quoted amount differs (Korea bank fees) | customer amount is **never trusted**; recorded audit-only | payout basis = Nexus amount |
| #6 "Cancel my contract" (implicit refund) | NO | `LOW_CONFIDENCE` (weak/implicit signal) |
| #7 Emailed info@… to cancel | NO | `LOW_CONFIDENCE` |
| #8 Registered on multiple sites | NO | `CHARGE_AMBIGUOUS` |
| #9 Multiple emails per card | NO | `CHARGE_AMBIGUOUS` |
| #10 Legacy cross-sell (IQ Booster sub vs report) | NO | `CHARGE_AMBIGUOUS` |

## Known limitation (must fix before execution)

Because this iteration reads **only** Nexus, cases #3/#4 (disputes) and #2
(non-Nexus payments) can produce a misleading `YES`. That is acceptable **only**
because the bot does not execute refunds. Before enabling execution, add:
Stripe read-only dispute check, PayPal dispute check, two-source amount verify.

## Learning / ground-truth

Each ticket's `refund_decision` + `refund_reason_code` + `refund_guard_trail` is a
labeled record. With Zendesk access, join `would_be_refunded` to what the human
actually did (refunded / not) to get real ground-truth labels — that validation
is required before trusting the signal for automation.
