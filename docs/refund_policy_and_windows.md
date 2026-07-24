# Refund policy & country timeline windows (AN-192)

Source: business rules from Anna Ilyk (2026-07-23) + the "Subscription Refund Policies
by Country" sheet:
https://docs.google.com/spreadsheets/d/12bJLRNIDBvPOiDPAJxC5-gHiz0YF2nfGWTl7xMxqQMg/edit?gid=0

## Policy rules

1. **Subscriptions — refund ONLY the LAST (most recent) payment**, and only if it is
   **within the country's refund timeline window** (see table). All earlier payments are
   **NOT** refunded — even if the customer asks to refund them too.
   - This deterministically resolves the "customer has several same-amount renewal
     charges" case (e.g. #166788/#166827: three ¥4,990 renewals on 24.05 / 21.06 / 19.07):
     the target is the **latest** renewal, refundable only if within the window.
2. **One-time charges are NOT refunded at all**: IQ Test fee (`first_sale`) and
   IQ Test Report (`cross_sale`). Only recurring `subscription` charges are refundable.
3. If the country is **not** in the table below → default window = **14 days**.

## Country → refund window (days)

| Region | Country | Window |
|--------|---------|--------|
| Asia | Japan | 8 |
| Asia | Korea | 7 |
| Asia | Vietnam | 7 |
| Asia | Hong Kong | 7 |
| Asia | Indonesia | 7 |
| Asia | Taiwan | 7 |
| Middle East | Saudi Arabia | 7 |
| Middle East | Turkey | 14 |
| LATAM | Brazil | 10 |
| LATAM | Mexico | 10 |
| LATAM | Argentina | 10 |
| LATAM | Chile | 10 |
| LATAM | Colombia | 10 |
| LATAM | Peru | 10 |
| LATAM | Uruguay | 10 |
| LATAM | Ecuador | 10 |
| LATAM | Guatemala | 10 |
| Europe | European Countries | 14 |
| — | USA | 14 |
| — | Canada | 14 |
| — | United Kingdom | 14 |
| — | Australia | 14 |
| — | New Zealand | 14 |
| — | (any country not listed) | **14 (default)** |

## Implication for the would-be engine (to implement, pending confirmation)

For a refund intent:
- `subscription` charges → candidate = the **latest** refundable subscription charge;
  refundable only if `today − charge_date ≤ window(country)`. Otherwise `OUTSIDE_REFUND_WINDOW`.
- `first_sale` / `cross_sale` (one-time) → `ONE_TIME_NOT_REFUNDABLE` (never refunded).
- Country comes from the ticket/Nexus; unknown → 14-day default.

Open questions to confirm before coding:
- Window measured from charge date to **ticket-created date** (recommended) or to "now"?
- Country source of truth: Zendesk `country` field / Nexus? (bot has a country field at main.py:406.)
- Does "latest payment" mean latest of the SUBSCRIPTION only (ignoring one-time charges)? (assumed yes)
