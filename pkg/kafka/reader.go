// Package kafka menyediakan Kafka reader (consumer group) via segmentio/kafka-go.
// Mendukung SASL/TLS untuk Confluent managed — pola sama dengan booking-service.
package kafka

import (
	"crypto/tls"
	"strings"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

// Config = koneksi broker.
type Config struct {
	Brokers  string // dipisah koma
	Username string // kosong = tanpa SASL (lokal)
	Password string `json:"-"`
	TLS      bool
	GroupID  string
}

// Topic yang dikonsumsi queue-service (EVENT-CONTRACT.md).
const (
	TopicStockChanged = "stock.changed"
	TopicSlotReleased = "slot.released"
	TopicEventSoldOut = "event.sold_out"
)

// NewReader membuat reader consumer-group untuk satu topic.
func NewReader(cfg Config, topic string) *kafkago.Reader {
	dialer := &kafkago.Dialer{Timeout: 10 * time.Second, DualStack: true}
	if cfg.TLS || cfg.Username != "" {
		dialer.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	if cfg.Username != "" {
		dialer.SASLMechanism = plain.Mechanism{Username: cfg.Username, Password: cfg.Password}
	}
	return kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:        strings.Split(cfg.Brokers, ","),
		GroupID:        cfg.GroupID,
		Topic:          topic,
		Dialer:         dialer,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        500 * time.Millisecond,
		CommitInterval: 0, // commit sinkron setelah pesan diproses
	})
}
