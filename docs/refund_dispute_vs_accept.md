# Engine track — dispute-vs-accept routing (AN-192, would-be engine)

**Type:** Bot-side (refund_engine) — our repo, our deploy cycle
**Reporter:** AN-192 refund would-be audit (2026-07-25)
**Priority:** Medium — a recurring AMBIGUOUS/ONE_TIME mis-route on real refund tickets
**Money movement:** none (would-be only; window guard preserves 0 false YES)

## Problem

Customers routinely **list the charges they ACCEPT and the one they DISPUTE** in
the same message. The engine treats every stated amount equally and routes by the
first amount that maps to a single charge type — so when the customer names the
*accepted* one-time amount (the IQ Test fee / Report), amount-matching routes to
that non-refundable one-time charge and the engine declines, even though the body
clearly disputes the recurring subscription.

## Evidence — ticket 167518 (bot NO, human refunded the subscription)

> 「先日199円で診断ができると表示され課金しました。しかしその後も**承諾した記憶がない請求**が続いており…
>  何かの**サブスクリプション**に登録してしまっているのでしたら解約をしたいです。
>  **最初にお支払いした金額以外はご返金**くださると幸いです。」
> (I paid ¥199 for the test — fine — but then charges I don't recall consenting to kept
>  coming. If I'm on some subscription, cancel it. Please **refund everything EXCEPT the
>  first payment**.)

- Charges: `subscription 5490`, `cross_sale 1990`, `first_sale 199`.
- Customer states **199** (the amount they explicitly accept) → `by_amount` maps to
  `first_sale` (a single type) → engine routes `first_sale` → `ONE_TIME_OUT_OF_SCOPE` (NO).
- The customer literally says "refund everything except the first payment" — i.e. refund the
  **subscription** — and the human did. Replaying through engine v11 (with the 5490 present
  and refundable) still returns `ONE_TIME` because the stated 199 short-circuits routing.

## Root cause

In `refund_engine.decide`, `by_amount` (stated-amount → charge-type) runs **before**
the date / type-keyword / dispute-target steps and short-circuits when it maps to a
single type. It has no notion of **accepted vs disputed** amounts, so a stated
*accepted* amount wins over an explicit *dispute* signal in the body.

## Proposed direction (not yet built)

Add an accept/dispute discrimination ahead of (or inside) amount routing:

1. **Detect accepted amounts** — phrases like "納得しております / 了承します / I accept / this one
   is fine / 最初の支払いは問題ない" attached to an amount → exclude that amount from
   `stated_set` used for routing (it's what they keep, not what they want back).
2. **Detect disputed amounts** — "覚えがない / 身に覚えのない / 承諾していない / unauthorized /
   didn't agree" attached to an amount → prefer that amount/charge as the target.
3. If the body disputes the recurring charge while accepting the one-time(s), route to the
   subscription (this is what `dispute_target_subscription` already does — but only when
   amount routing hasn't already short-circuited to a one-time type). Simplest fix: when a
   stated amount maps **only** to a non-subscription one-time type **and** the body carries an
   unauthorized-recurring signal **and** a refundable subscription exists → prefer the
   subscription over the accepted one-time.

Window guard stays in place → cannot create a within-window false YES.

## Acceptance

- 167518-pattern ("accept the 199, dispute the recurring 5490") → routes to the subscription
  (YES if the latest renewal is within the country window), not `ONE_TIME_OUT_OF_SCOPE`.
- No regression to existing amount-routing tests; 0 false YES preserved in the audit.

## Dependency note

167518 also needs the recurring `5490` charge to be **present in Nexus at eval time**
(loader freshness — see `nexus_lookup_gaps_backend.md`). This engine track fixes the
*routing*; freshness fixes the *data*. Both are required for 167518 to resolve on live data.
EOF
echo "created dispute-vs-accept doc"