// Integration test consumer terhadap broker Kafka sungguhan (docker-compose tools,
// host port 29092). Di-skip bila broker tidak terjangkau.
package event

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/refreshTechRusuh/queue-service/internal/queue/domain/dto"
	"github.com/refreshTechRusuh/queue-service/pkg/kafka"
)

// safeUC = fake thread-safe (tiap topic dibaca goroutine sendiri).
type safeUC struct {
	mu       sync.Mutex
	released map[string]string // eventId → queueToken
}

func (s *safeUC) Join(context.Context, string) (dto.QueueStatusResponse, error) {
	return dto.QueueStatusResponse{}, nil
}
func (s *safeUC) Status(context.Context, string, string) (dto.QueueStatusResponse, error) {
	return dto.QueueStatusResponse{}, nil
}
func (s *safeUC) OnStockChanged(context.Context, string, string, int) error { return nil }
func (s *safeUC) OnSoldOut(context.Context, string) error                   { return nil }
func (s *safeUC) Sweep(context.Context) error                               { return nil }
func (s *safeUC) OnSlotReleased(_ context.Context, e, tok string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.released[e] = tok
	return nil
}
func (s *safeUC) got(e string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tok, ok := s.released[e]
	return tok, ok
}

func TestConsumer_ShouldConsumeSlotReleasedFromRealKafka(t *testing.T) {
	broker := os.Getenv("TEST_KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:29092"
	}
	conn, err := net.DialTimeout("tcp", broker, 2*time.Second)
	if err != nil {
		t.Skipf("kafka tidak tersedia di %s: %v", broker, err)
	}
	_ = conn.Close()

	uc := &safeUC{released: map[string]string{}}
	c := NewConsumer(uc, kafka.Config{Brokers: broker, GroupID: "queue-test-" + uuid.NewString()},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		c.Close()
	}()
	c.Start(ctx)

	eventID, tok := uuid.NewString(), "q_"+uuid.NewString()
	w := &kafkago.Writer{Addr: kafkago.TCP(broker), Topic: kafka.TopicSlotReleased, Balancer: &kafkago.Hash{}}
	defer w.Close()
	payload := []byte(`{"bookingId":"b-test","eventId":"` + eventID + `","queueToken":"` + tok + `","reason":"PAID"}`)
	if err := w.WriteMessages(context.Background(), kafkago.Message{Key: []byte(eventID), Value: payload}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	deadline := time.Now().Add(45 * time.Second) // join consumer group butuh beberapa detik
	for time.Now().Before(deadline) {
		if got, ok := uc.got(eventID); ok {
			if got != tok {
				t.Fatalf("queueToken = %q; mau %q", got, tok)
			}
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("pesan slot.released tidak dikonsumsi dalam 45 detik")
}
