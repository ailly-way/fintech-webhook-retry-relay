package delivery

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestDecidePaymentAction(t *testing.T) {
	tests := []struct {
		name      string
		riskScore int
		want      string
	}{
		{name: "ordinary payment is released", riskScore: 24, want: DecisionRelease},
		{name: "threshold payment is reviewed", riskScore: 80, want: DecisionManualReview},
		{name: "high risk payment is reviewed", riskScore: 97, want: DecisionManualReview},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			note := Decide(PaymentEvent{EventID: "evt_101", RiskScore: tt.riskScore})
			if note.Decision != tt.want {
				t.Fatalf("Decide() = %q, want %q", note.Decision, tt.want)
			}
			if note.Attempt != 1 || note.MaxAttempts != 5 {
				t.Fatalf("retry audit fields = %d/%d, want 1/5", note.Attempt, note.MaxAttempts)
			}
		})
	}
}

type recordingQueue struct {
	published Notification
	key       string
	acked     string
}

func (q *recordingQueue) Publish(_ context.Context, payload any, key string) error {
	q.published = payload.(Notification)
	q.key = key
	return nil
}
func (q *recordingQueue) Consume(context.Context, int, int) ([]QueueMessage, error) { return nil, nil }
func (q *recordingQueue) Ack(_ context.Context, messageID string) error {
	q.acked = messageID
	return nil
}

func TestFailedDeliveryQueuesNextAuditableAttempt(t *testing.T) {
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
	}))
	defer receiver.Close()

	note := Decide(PaymentEvent{EventID: "evt_202", PaymentID: "pay_9", RiskScore: 91, WebhookURL: receiver.URL})
	payload, err := json.Marshal(note)
	if err != nil {
		t.Fatal(err)
	}
	queue := &recordingQueue{}
	worker := Worker{
		Queue:  queue,
		HTTP:   receiver.Client(),
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Sleep:  func(context.Context, time.Duration) error { return nil },
	}
	if err := worker.ProcessOne(context.Background(), QueueMessage{MessageID: "msg_7", Payload: payload}); err != nil {
		t.Fatal(err)
	}
	if queue.published.Attempt != 2 || queue.published.Decision != DecisionManualReview {
		t.Fatalf("republished attempt = %#v", queue.published)
	}
	if queue.key != "payment-evt_202-attempt-2" || queue.acked != "msg_7" {
		t.Fatalf("queue boundary key=%q ack=%q", queue.key, queue.acked)
	}
}
