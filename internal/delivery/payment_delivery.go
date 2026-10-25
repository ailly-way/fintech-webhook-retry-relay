package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"
)

const (
	DecisionRelease      = "release"
	DecisionManualReview = "manual_review"
)

type PaymentEvent struct {
	EventID       string    `json:"event_id"`
	PaymentID     string    `json:"payment_id"`
	AmountMinor   int64     `json:"amount_minor"`
	Currency      string    `json:"currency"`
	RiskScore     int       `json:"risk_score"`
	OccurredAt    time.Time `json:"occurred_at"`
	WebhookURL    string    `json:"webhook_url"`
	CorrelationID string    `json:"correlation_id"`
}

type Notification struct {
	Event         PaymentEvent `json:"event"`
	Decision      string       `json:"decision"`
	DecisionBasis string       `json:"decision_basis"`
	Attempt       int          `json:"attempt"`
	MaxAttempts   int          `json:"max_attempts"`
}

func Decide(event PaymentEvent) Notification {
	decision := DecisionRelease
	basis := "risk_score_below_review_threshold"
	if event.RiskScore >= 80 {
		decision = DecisionManualReview
		basis = "risk_score_at_or_above_review_threshold"
	}
	return Notification{Event: event, Decision: decision, DecisionBasis: basis, Attempt: 1, MaxAttempts: 5}
}

type Queue interface {
	Publish(context.Context, any, string) error
	Consume(context.Context, int, int) ([]QueueMessage, error)
	Ack(context.Context, string) error
}

type Worker struct {
	Queue  Queue
	HTTP   *http.Client
	Logger *slog.Logger
	Sleep  func(context.Context, time.Duration) error
}

func (w *Worker) ProcessOne(ctx context.Context, message QueueMessage) error {
	var note Notification
	if err := json.Unmarshal(message.Payload, &note); err != nil {
		return fmt.Errorf("decode payment notification: %w", err)
	}
	delivered, status, err := w.deliver(ctx, note)
	if err != nil {
		return err
	}
	if delivered || note.Attempt >= note.MaxAttempts {
		w.Logger.Info("webhook outcome", "event_id", note.Event.EventID, "payment_id", note.Event.PaymentID, "decision", note.Decision, "attempt", note.Attempt, "http_status", status, "delivered", delivered)
		return w.Queue.Ack(ctx, message.MessageID)
	}

	delay := time.Second * time.Duration(1<<(note.Attempt-1))
	if err := w.Sleep(ctx, delay); err != nil {
		return err
	}
	note.Attempt++
	key := fmt.Sprintf("payment-%s-attempt-%d", note.Event.EventID, note.Attempt)
	if err := w.Queue.Publish(ctx, note, key); err != nil {
		return err
	}
	w.Logger.Info("webhook retry queued", "event_id", note.Event.EventID, "attempt", note.Attempt, "previous_http_status", status)
	return w.Queue.Ack(ctx, message.MessageID)
}

func (w *Worker) deliver(ctx context.Context, note Notification) (bool, int, error) {
	body, err := json.Marshal(note)
	if err != nil {
		return false, 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, note.Event.WebhookURL, bytes.NewReader(body))
	if err != nil {
		return false, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Webhook-Event-ID", note.Event.EventID)
	req.Header.Set("X-Webhook-Attempt", fmt.Sprint(note.Attempt))
	res, err := w.HTTP.Do(req)
	if err != nil {
		return false, 0, nil
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 64<<10))
	res.Body.Close()
	return res.StatusCode >= 200 && res.StatusCode < 300, res.StatusCode, nil
}

func (w *Worker) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		messages, err := w.Queue.Consume(ctx, 10, 30)
		if err != nil {
			return err
		}
		if len(messages) == 0 {
			if err := w.Sleep(ctx, time.Second); err != nil {
				return err
			}
			continue
		}
		for _, message := range messages {
			if err := w.ProcessOne(ctx, message); err != nil {
				w.Logger.Error("queue message retained", "message_id", message.MessageID, "error", err)
			}
		}
	}
	return ctx.Err()
}
