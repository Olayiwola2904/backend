# Payment Processing

All payment state mutations run through `src/services/transaction.service.ts`,
which exposes named transactions with explicit isolation levels.

External payments (cards, wallets, crypto) are processed through a pluggable
`PaymentProvider`. The default implementation targets **Stripe** via its REST API
(no SDK dependency); tests and local development use a deterministic
`FakePaymentProvider`.

Raw card data is **never** accepted or stored. Only tokenized payment intent
references are persisted (PCI SAQ-A).

## Configuration

| Variable                             | Default                  | Description                                     |
| ------------------------------------ | ------------------------ | ----------------------------------------------- |
| `PAYMENTS_PROVIDER`                  | `none`                   | `stripe` to enable the provider                 |
| `STRIPE_SECRET_KEY`                  | –                        | Required when `PAYMENTS_PROVIDER=stripe`        |
| `STRIPE_WEBHOOK_SECRET`              | –                        | Required to accept webhooks                     |
| `STRIPE_API_BASE`                    | `https://api.stripe.com` | Override for proxies/sandboxes                  |
| `PAYMENTS_WEBHOOK_TOLERANCE_SECONDS` | `300`                    | Replay window for webhook timestamps            |

## Endpoints

| Method | Path                                | Auth | Description                                   |
| ------ | ----------------------------------- | ---- | --------------------------------------------- |
| POST   | `/api/v1/payments`                  | yes  | Create a payment intent                       |
| GET    | `/api/v1/payments`                  | yes  | List the caller's payments (paginated)        |
| GET    | `/api/v1/payments/:id`              | yes  | Fetch one payment (owner only)                |
| POST   | `/api/v1/payments/:id/refund`       | yes  | Refund in full or partially                   |
| POST   | `/api/v1/payments/webhooks/:provider` | no | Provider callback (signature verified)        |

## Idempotency

Create and refund requests are idempotent. Send an `Idempotency-Key` header (or a
body field of the same name); repeating the request returns the original result
instead of creating a second charge. Concurrent/duplicate webhook deliveries are
deduplicated by the unique provider event id.

```bash
curl -X POST http://localhost:3000/api/v1/payments \
  -H "Authorization: Bearer $TOKEN" \
  -H "Idempotency-Key: 6f1b6e2a-..." \
  -H "Content-Type: application/json" \
  -d '{"amount": 25, "currency": "USD", "method": "card", "creatorId": "creator_1"}'
```

## Status lifecycle

```
pending ─┬─▶ requires_action ─┬─▶ processing ─┬─▶ succeeded ─┬─▶ partially_refunded ─▶ refunded
         │                    │               │              └─▶ disputed
         ├─▶ processing ──────┘               └─▶ failed
         └─▶ canceled
```

Illegal transitions are rejected (`src/lib/payments/transitions.ts`), and every
state change appends a `PaymentEvent` audit record.

Status updates, tip creation, and payout calculations are wrapped in named
transactions from the transaction service so the payment row, its audit event,
and any related tip/payout records commit atomically.

Payout calculations in `src/domains/creators/payout.service.ts` delegate to
`calculatePayout` on the transaction service rather than opening their own
`prisma.$transaction()` blocks.

The transaction service exposes dedicated methods for each flow
(`createTip`, `updatePaymentStatus`, `calculatePayout`, `applyRefund`,
`confirmStellarPayment`), each declaring its isolation level explicitly.

## Webhooks

The provider signs `<timestamp>.<raw body>` with HMAC-SHA256. The webhook route
captures the raw body (via an encapsulated content-type parser), verifies the
signature and timestamp tolerance, then records the event and applies the
matching status transition:

| Provider event                    | Resulting status                        |
| --------------------------------- | --------------------------------------- |
| `payment_intent.succeeded`        | `succeeded`                             |
| `payment_intent.payment_failed`   | `failed`                                |
| `payment_intent.canceled`         | `canceled`                              |
| `charge.refunded`                 | `refunded` / `partially_refunded`       |
| `charge.dispute.created`          | `disputed` (chargeback)                 |

Invalid signatures are rejected with `401` and never mutate state.

Webhook-driven status transitions and Stellar confirmation updates in
`src/lib/stellar/listener.ts` use the transaction service rather than raw
Prisma calls, keeping confirmation handling atomic with the audit trail.

## Refunds

Refunds are validated against the refundable amount (sum of successful refunds),
so a payment can never be over-refunded. Partial refunds move the payment to
`partially_refunded`; a full refund moves it to `refunded`. Every refund writes
an audit event.

Refund state changes and their audit events are applied inside a single
transaction service operation; nested `prisma.$transaction()` calls are not
used across service layers.

## Resilience

Provider calls are retried with exponential backoff for transient failures
(5xx/timeouts/429) and are not retried for caller errors (4xx). See
`src/lib/payments/retry.ts`.

## Data model

- `Payment` — the payment record (provider ref, amount, currency, status, method).
- `PaymentEvent` — append-only audit trail; `providerEventId` is unique.
- `Refund` — refund records with their own idempotency key.

## Transactions

`src/services/transaction.service.ts` defines the named transactions used by
payment, tip, and payout flows. Each operation declares its isolation level and
executes related writes atomically; callers must not open nested transactions.

Isolation levels are chosen per operation: `Serializable` for payout
calculations and refunds (to prevent double-spend/over-refund races),
`ReadCommitted` for status updates and tip creation. Nested
`prisma.$transaction()` calls across service layers are prohibited; compose
operations by calling a single named transaction method instead.
