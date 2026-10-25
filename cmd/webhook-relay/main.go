package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/example/fintech-webhook-relay/internal/delivery"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		logger.Error("INFRAI_API_KEY is required")
		os.Exit(2)
	}
	queue := delivery.NewQueueClient(apiKey)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := queue.CreateQueue(ctx); err != nil {
		logger.Error("queue setup failed", "error", err)
		os.Exit(1)
	}

	worker := &delivery.Worker{
		Queue:  queue,
		HTTP:   &http.Client{Timeout: 10 * time.Second},
		Logger: logger,
		Sleep:  sleepContext,
	}
	go func() {
		if err := worker.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("worker stopped", "error", err)
			stop()
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /payment-events", func(w http.ResponseWriter, r *http.Request) {
		var event delivery.PaymentEvent
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&event); err != nil || event.EventID == "" || event.PaymentID == "" || event.WebhookURL == "" {
			http.Error(w, "valid event_id, payment_id, and webhook_url are required", http.StatusBadRequest)
			return
		}
		note := delivery.Decide(event)
		if err := queue.Publish(r.Context(), note, "payment-"+event.EventID+"-attempt-1"); err != nil {
			var rejected *delivery.InfraiError
			if errors.As(err, &rejected) && rejected.Status >= 400 && rejected.Status < 500 {
				http.Error(w, rejected.Error(), rejected.Status)
				return
			}
			http.Error(w, "queue submission failed", http.StatusBadGateway)
			return
		}
		logger.Info("payment decision queued", "event_id", event.EventID, "payment_id", event.PaymentID, "decision", note.Decision, "correlation_id", event.CorrelationID)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{"event_id": event.EventID, "decision": note.Decision, "queued": true})
	})

	server := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	logger.Info("payment webhook relay listening", "address", server.Addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
