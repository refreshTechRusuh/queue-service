package event

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/refreshTechRusuh/queue-service/internal/queue/domain/dto"
	"github.com/refreshTechRusuh/queue-service/pkg/kafka"
)

// fakeUC mencatat panggilan dari consumer.
type fakeUC struct {
	stock    []string
	released []string
	soldOut  []string
	failN    int // gagal sebanyak n kali dulu (uji retry)
	calls    int
}

func (f *fakeUC) Join(context.Context, string, dto.JoinRequest) (dto.QueueStatusResponse, error) {
	return dto.QueueStatusResponse{}, nil
}
func (f *fakeUC) Status(context.Context, string, string) (dto.QueueStatusResponse, error) {
	return dto.QueueStatusResponse{}, nil
}
func (f *fakeUC) OnStockChanged(_ context.Context, e, c string, a int) error {
	f.calls++
	if f.calls <= f.failN {
		return errors.New("redis sementara mati")
	}
	f.stock = append(f.stock, e+"|"+c+"|"+itoa(a))
	return nil
}
func (f *fakeUC) OnSlotReleased(_ context.Context, e, tok string) error {
	f.released = append(f.released, e+"|"+tok)
	return nil
}
func (f *fakeUC) OnSoldOut(_ context.Context, e string) error {
	f.soldOut = append(f.soldOut, e)
	return nil
}
func (f *fakeUC) Sweep(context.Context) error { return nil }

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	s := ""
	for n > 0 {
		s = string(rune('0'+n%10)) + s
		n /= 10
	}
	return s
}

func newTestConsumer(uc *fakeUC) *Consumer {
	return &Consumer{uc: uc, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func TestHandle_ShouldRouteEachTopicToUsecase(t *testing.T) {
	uc := &fakeUC{}
	c := newTestConsumer(uc)
	ctx := context.Background()

	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("handle error: %v", err)
		}
	}
	must(c.handle(ctx, kafka.TopicStockChanged,
		[]byte(`{"eventId":"e1","categoryId":"c1","delta":-2,"available":37,"reason":"HOLD"}`)))
	must(c.handle(ctx, kafka.TopicSlotReleased,
		[]byte(`{"bookingId":"b1","eventId":"e1","queueToken":"q_x","reason":"PAID"}`)))
	must(c.handle(ctx, kafka.TopicSlotReleased,
		[]byte(`{"bookingId":"b2","eventId":"e1","reason":"BOOKING_TTL"}`))) // payload BE-2 saat ini: tanpa queueToken
	must(c.handle(ctx, kafka.TopicEventSoldOut, []byte(`{"eventId":"e1"}`)))

	if len(uc.stock) != 1 || uc.stock[0] != "e1|c1|37" {
		t.Errorf("stock.changed → %v; mau [e1|c1|37]", uc.stock)
	}
	if len(uc.released) != 2 || uc.released[0] != "e1|q_x" || uc.released[1] != "e1|" {
		t.Errorf("slot.released → %v; mau [e1|q_x e1|]", uc.released)
	}
	if len(uc.soldOut) != 1 || uc.soldOut[0] != "e1" {
		t.Errorf("event.sold_out → %v; mau [e1]", uc.soldOut)
	}
}

// Payload rusak tak akan sembuh dengan retry → dilewati tanpa error, usecase tak dipanggil.
func TestHandle_ShouldSkipMalformedPayload(t *testing.T) {
	uc := &fakeUC{}
	c := newTestConsumer(uc)
	for _, topic := range []string{kafka.TopicStockChanged, kafka.TopicSlotReleased, kafka.TopicEventSoldOut} {
		if err := c.handle(context.Background(), topic, []byte("bukan json")); err != nil {
			t.Errorf("%s: err=%v; mau nil", topic, err)
		}
	}
	if len(uc.stock)+len(uc.released)+len(uc.soldOut) != 0 {
		t.Error("usecase terpanggil untuk payload rusak")
	}
}

func TestHandleWithRetry_ShouldRetryTransientError(t *testing.T) {
	uc := &fakeUC{failN: 2} // gagal 2x, sukses di percobaan ke-3
	c := newTestConsumer(uc)
	c.handleWithRetry(context.Background(), kafka.TopicStockChanged,
		kafkago.Message{Value: []byte(`{"eventId":"e1","categoryId":"c1","available":5}`)})
	if uc.calls != 3 || len(uc.stock) != 1 {
		t.Errorf("calls=%d tersimpan=%d; mau 3 percobaan dan 1 sukses", uc.calls, len(uc.stock))
	}
}
