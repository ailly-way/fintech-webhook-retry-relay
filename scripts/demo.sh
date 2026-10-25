#!/bin/sh
set -eu

curl --request POST http://localhost:8080/payment-events \
  --header 'Content-Type: application/json' \
  --data '{"event_id":"evt_demo_01","payment_id":"pay_demo_01","amount_minor":12500,"currency":"USD","risk_score":86,"occurred_at":"2026-09-06T10:30:00Z","webhook_url":"http://localhost:9090/payment-notifications","correlation_id":"checkout_demo_01"}'
