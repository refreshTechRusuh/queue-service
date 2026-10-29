// Package event (infrastructure) = adapter Kafka: membaca event dari BE-2 lalu
// memanggil usecase. Semantik at-least-once → semua handler usecase idempoten.
package event

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/refreshTechRusuh/queue-service/internal/queue/port/inbound"
	"github.com/refreshTechRusuh/queue-service/pkg/kafka"
)

// Payload (EVENT-CONTRACT.md; dicocokkan dengan pkg/kafka booking-service).
type stockChanged struct {
	EventID    string `json:"eventId"`
	CategoryID string `json:"categoryId"`
	Available  int    `json:"available"`
}

type slotReleased struct {
	BookingID  string `json:"bookingId"`
	EventID    string `json:"eventId"`
	QueueToken string `json:"queueToken,omitempty"` // opsional: bila ada, kursi dilepas lebih awal
	Reason     string `json:"reason"`
}

type eventSoldOut struct {
	EventID string `json:"eventId"`
}

const maxAttempts = 3

// Consumer menjalankan satu reader per topic.
type Consumer struct {
	uc      inbound.IQueueUsecase
	readers map[string]*kafkago.Reader
	log     *slog.Logger
}

func NewConsumer(uc inbound.IQueueUsecase, cfg kafka.Config, log *slog.Logger) *Consumer {
	readers := map[string]*kafkago.Reader{}
	for _, t := range []string{kafka.TopicStockChanged, kafka.TopicSlotReleased, kafka.TopicEventSoldOut} {
		readers[t] = kafka.NewReader(cfg, t)
	}
	return &Consumer{uc: uc, readers: readers, log: log}
}

// Start menjalankan goroutine pembaca tiap topic sampai ctx dibatalkan.
func (c *Consumer) Start(ctx context.Context) {
	for topic, r := range c.readers {
		go c.loop(ctx, topic, r)
	}
}

// Close menutup semua reader.
func (c *Consumer) Close() {
	for _, r := range c.readers {
		_ = r.Close()
	}
}

func (c *Consumer) loop(ctx context.Context, topic string, r *kafkago.Reader) {
	c.log.Info("kafka consumer start", slog.String("topic", topic))
	for {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return
			}
			c.log.Warn("kafka fetch gagal, coba lagi", slog.String("topic", topic), slog.Any("err", err))
			time.Sleep(2 * time.Second)
			continue
		}

		c.handleWithRetry(ctx, topic, msg)

		// Commit setelah diproses. Pesan yang tetap gagal setelah retry di-log lalu
		// dilewati (handler idempoten; sweeper & event berikutnya memulihkan state).
		if err := r.CommitMessages(ctx, msg); err != nil && ctx.Err() == nil {
			c.log.Warn("kafka commit gagal", slog.String("topic", topic), slog.Any("err", err))
		}
	}
}

func (c *Consumer) handleWithRetry(ctx context.Context, topic string, msg kafkago.Message) {
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := c.handle(ctx, topic, msg.Value)
		if err == nil {
			return
		}
		c.log.Warn("kafka handler gagal",
			slog.String("topic", topic), slog.Int("attempt", attempt), slog.Any("err", err))
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(attempt) * 500 * time.Millisecond):
		}
	}
	c.log.Error("kafka pesan dilewati setelah retry", slog.String("topic", topic), slog.String("value", string(msg.Value)))
}

func (c *Consumer) handle(ctx context.Context, topic string, value []byte) error {
	switch topic {
	case kafka.TopicStockChanged:
		var ev stockChanged
		if err := json.Unmarshal(value, &ev); err != nil {
			c.log.Warn("stock.changed payload rusak, dilewati", slog.Any("err", err))
			return nil // payload rusak tak akan sembuh dengan retry
		}
		return c.uc.OnStockChanged(ctx, ev.EventID, ev.CategoryID, ev.Available)

	case kafka.TopicSlotReleased:
		var ev slotReleased
		if err := json.Unmarshal(value, &ev); err != nil {
			c.log.Warn("slot.released payload rusak, dilewati", slog.Any("err", err))
			return nil
		}
		return c.uc.OnSlotReleased(ctx, ev.EventID, ev.QueueToken)

	case kafka.TopicEventSoldOut:
		var ev eventSoldOut
		if err := json.Unmarshal(value, &ev); err != nil {
			c.log.Warn("event.sold_out payload rusak, dilewati", slog.Any("err", err))
			return nil
		}
		return c.uc.OnSoldOut(ctx, ev.EventID)
	}
	return nil
}
