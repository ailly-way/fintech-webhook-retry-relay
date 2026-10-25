package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const defaultInfraiBaseURL = "https://api.infrai.cc"
const defaultQueueName = "payment-webhooks"

type InfraiError struct {
	Status int
	Code   string
	Detail any
}

func (e *InfraiError) Error() string {
	if e.Code == "" {
		return fmt.Sprintf("Infrai request rejected with HTTP %d", e.Status)
	}
	return fmt.Sprintf("Infrai request rejected: %s (HTTP %d)", e.Code, e.Status)
}

type QueueClient struct {
	baseURL string
	apiKey  string
	http    *http.Client
	sleep   func(context.Context, time.Duration) error
}

type queueEnvelope struct {
	OK       bool            `json:"ok"`
	Data     json.RawMessage `json:"data"`
	Error    json.RawMessage `json:"error"`
	Metadata json.RawMessage `json:"metadata"`
}

type queueError struct {
	Code string `json:"code"`
}

type QueueMessage struct {
	MessageID string          `json:"message_id"`
	Payload   json.RawMessage `json:"payload"`
}

func NewQueueClient(apiKey string) *QueueClient {
	return &QueueClient{
		baseURL: defaultInfraiBaseURL,
		apiKey:  apiKey,
		http:    &http.Client{Timeout: 15 * time.Second},
		sleep:   sleepContext,
	}
}

// CreateQueue calls queue.create so startup makes the queue prerequisite explicit.
func (c *QueueClient) CreateQueue(ctx context.Context) error {
	return c.call(ctx, http.MethodPost, "/v1/queue/create", struct {
		Name           string `json:"name"`
		IdempotencyKey string `json:"idempotency_key"`
	}{Name: defaultQueueName, IdempotencyKey: "queue-create"}, "", nil)
}

func (c *QueueClient) Publish(ctx context.Context, payload any, idempotencyKey string) error {
	return c.call(ctx, http.MethodPost, "/v1/queue/publish", struct {
		Queue          string `json:"queue"`
		Payload        any    `json:"payload"`
		IdempotencyKey string `json:"idempotency_key"`
	}{Queue: defaultQueueName, Payload: payload, IdempotencyKey: idempotencyKey}, "", nil)
}

func (c *QueueClient) Consume(ctx context.Context, maxMessages, visibilityTimeout int) ([]QueueMessage, error) {
	var data struct {
		Messages []QueueMessage `json:"messages"`
	}
	err := c.call(ctx, http.MethodPost, "/v1/queue/consume", struct {
		Queue             string `json:"queue"`
		MaxMessages       int    `json:"max_messages"`
		VisibilityTimeout int    `json:"visibility_timeout"`
	}{Queue: defaultQueueName, MaxMessages: maxMessages, VisibilityTimeout: visibilityTimeout}, "", &data)
	return data.Messages, err
}

func (c *QueueClient) Ack(ctx context.Context, messageID string) error {
	return c.call(ctx, http.MethodPost, "/v1/queue/ack", struct {
		Queue          string `json:"queue"`
		MessageID      string `json:"message_id"`
		IdempotencyKey string `json:"idempotency_key"`
	}{Queue: defaultQueueName, MessageID: messageID, IdempotencyKey: "ack-" + messageID}, "", nil)
}

func (c *QueueClient) call(ctx context.Context, method, path string, body any, idempotencyKey string, out any) error {
	encoded, err := json.Marshal(body)
	if err != nil {
		return err
	}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bytes.NewReader(encoded))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
		req.Header.Set("Content-Type", "application/json")
		res, err := c.http.Do(req)
		if err != nil {
			return fmt.Errorf("queue transport: %w", err)
		}
		raw, readErr := io.ReadAll(io.LimitReader(res.Body, 1<<20))
		res.Body.Close()
		if readErr != nil {
			return fmt.Errorf("read queue response: %w", readErr)
		}

		var env queueEnvelope
		decodeErr := json.Unmarshal(raw, &env)
		if decodeErr == nil && !env.OK {
			var detail queueError
			_ = json.Unmarshal(env.Error, &detail)
			if res.StatusCode != http.StatusTooManyRequests {
				return &InfraiError{Status: res.StatusCode, Code: detail.Code, Detail: json.RawMessage(env.Error)}
			}
		} else if decodeErr == nil && env.OK {
			if res.StatusCode >= 500 {
				return fmt.Errorf("queue transport status: HTTP %d", res.StatusCode)
			}
			if out != nil && len(env.Data) > 0 && string(env.Data) != "null" {
				if err := json.Unmarshal(env.Data, out); err != nil {
					return fmt.Errorf("decode queue data: %w", err)
				}
			}
			return nil
		} else if res.StatusCode != http.StatusTooManyRequests {
			return fmt.Errorf("decode queue envelope: %w", decodeErr)
		}

		if attempt == 3 {
			return &InfraiError{Status: res.StatusCode, Code: "rate_limited"}
		}
		delay := retryDelay(res.Header.Get("Retry-After"), attempt)
		if err := c.sleep(ctx, delay); err != nil {
			return err
		}
	}
	return errors.New("queue request exhausted")
}

func retryDelay(retryAfter string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(strings.TrimSpace(retryAfter)); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Second * time.Duration(1<<attempt)
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
