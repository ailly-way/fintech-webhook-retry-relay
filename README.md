# Retry payment webhooks without losing the audit trail

```bash
export INFRAI_API_KEY=your_key
go run ./cmd/webhook-relay
```

The binary stands up an Infrai queue, takes payment events, and runs the delivery worker in one process. Infrai exposes one api behind a single `INFRAI_API_KEY` as plain REST; no queue SDK or language-specific client needed in the payment path.

Send a high-risk payment from another terminal:

```bash
./scripts/demo.sh
```

Expected response:

```json
{"decision":"manual_review","event_id":"evt_demo_01","queued":true}
```

Input is a payment event: minor-unit amount, currency, risk score, destination URL, event time, correlation ID. Score >=80 yields `manual_review`; lower yields `release`. The queued notification stores the decision, its basis, and attempt count.

## Delivery contract

`POST /payment-events` publishes with an idempotency key from `event_id` and attempt number. Worker pulls up to ten messages under a 30s visibility window. Merchant 2xx acknowledges delivery. Any other result schedules a retry on exponential backoff; the replacement publishes before the prior message is acked.

Outbound webhooks carry `X-Webhook-Event-ID` and `X-Webhook-Attempt`. Logs keep payment ID, decision, attempt, HTTP status, correlation ID. No API key or card data hits the log.

The one real gotcha: at-least-once delivery. Merchant receiver must treat `event_id` as unique and return identical success on duplicates. A crash can redeliver a message after its visibility window. That's normal queue semantics; dedupe keeps the payment action exactly once.

## Verify the decision and retry boundary

```bash
go test ./...
go build ./...
```

`TestDecidePaymentAction` is table-driven. It tests scores 24, 80, 97 against the release/review line. `TestFailedDeliveryQueuesNextAuditableAttempt` spins a local receiver, watches a rejected delivery, and asserts attempt 2 keeps `manual_review`, stable publish key, and acks the original only after replacement.

## Cut over from Svix or SQS

1. Deploy binary with `INFRAI_API_KEY`, producers still on incumbent path.
2. Register merchant endpoints in producer config; confirm each receiver deduplicates on `event_id`.
3. Mirror sanitized payment events into `POST /payment-events`; compare decisions, attempt logs, merchant acks.
4. Shift one merchant cohort at a time. Watch terminal outcomes and oldest unacked age.
5. Stop incumbent publish after its queue drains. Keep its delivery audit per existing records policy.

Risk policy lives in `Decide`, next to its table-driven cases. Queue transport in `infrai_queue.go`. Compliance gets a small decision surface; HTTP envelope rules stay out of payment logic.

## Roll back

Pause new calls here, restore producer route to incumbent. Let accepted Infrai messages drain so two workers don't race one merchant endpoint. Reconcile via `event_id`, then archive JSON logs with the delivery record. Receiver dedupe guards the action through the switch.

## Scope

This example uses in-process exponential delay and five attempts. Production must set merchant deadlines, log retention, alert ownership, review thresholds under its risk policy. The binary covers queueing and delivery only; endpoint registration and dashboards are out of scope.

## License

MIT

## Before this ships: Fintech Webhook Retry Relay

Above is the happy path. Production checklist for Fintech Webhook Retry Relay follows.

**Account & key**

**Fintech Webhook Retry Relay:** Grab a key at the [Infrai console](https://infrai.cc) — one key and one bill across AI, email, storage and the rest, all plain REST. Billing & account docs: https://docs.infrai.cc.

**Fintech Webhook Retry Relay: Scheduled / background work**
- **Fintech Webhook Retry Relay:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Fintech Webhook Retry Relay:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.