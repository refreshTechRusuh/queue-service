// Package cache (infrastructure) mengimplementasikan outbound.IQueueStore di Redis.
// Operasi yang mengubah occupancy dijalankan lewat Lua script (lihat scripts.go).
package cache

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/refreshTechRusuh/queue-service/internal/queue/domain/entity"
	"github.com/refreshTechRusuh/queue-service/internal/queue/port/outbound"
)

// keyActiveEvents = SET event yang punya antrian (dibaca sweeper). Di luar hash tag.
const keyActiveEvents = "queue:events"

// StoreSettings = parameter kapasitas yang dipakai script.
type StoreSettings struct {
	GlobalBookingCap int           // plafon orang di booking room per event (0 = tanpa plafon)
	QueueCap         int           // panjang antrian maksimum (0 = tanpa batas)
	BookingTTL       time.Duration // umur Access Token / kursi booking room
	StateTTL         time.Duration // umur data state token di Redis
	ClosedTTL        time.Duration // umur flag sold-out
}

// QueueStore = implementasi IQueueStore berbasis Redis.
type QueueStore struct {
	rdb     *redis.Client
	cfg     StoreSettings
	join    *redis.Script
	promote *redis.Script
	release *redis.Script
	sweep   *redis.Script
	closeQ  *redis.Script
}

// NewQueueStore menyiapkan store + script (di-cache Redis lewat EVALSHA).
func NewQueueStore(rdb *redis.Client, cfg StoreSettings) *QueueStore {
	return &QueueStore{
		rdb:     rdb,
		cfg:     cfg,
		join:    redis.NewScript(luaJoin),
		promote: redis.NewScript(luaPromote),
		release: redis.NewScript(luaRelease),
		sweep:   redis.NewScript(luaSweep),
		closeQ:  redis.NewScript(luaClose),
	}
}

// ---- penamaan key ----

// tag = hash tag Redis Cluster: semua key satu event berada di slot yang sama.
func tag(eventID string) string { return "{" + eventID + "}" }

func keyQueue(e string) string     { return "queue:" + tag(e) }
func keyAdmitted(e string) string  { return "admitted:" + tag(e) }
func keyOccupancy(e string) string { return "admission:" + tag(e) }
func keyStock(e string) string     { return "stock:" + tag(e) }
func keyLog(e string) string       { return "admission:log:" + tag(e) }
func keyClosed(e string) string    { return "queue:closed:" + tag(e) }
func keySeq(e string) string       { return "queue:seq:" + tag(e) }
func prefixToken(e string) string  { return "queue:token:" + tag(e) + ":" }

func (s *QueueStore) keys(e string) []string {
	return []string{keyQueue(e), keyAdmitted(e), keyOccupancy(e), keyStock(e), keyLog(e), keyClosed(e), keySeq(e)}
}

func (s *QueueStore) baseArgs(e string) []any {
	return []any{
		prefixToken(e),
		s.cfg.GlobalBookingCap,
		s.cfg.BookingTTL.Milliseconds(),
		int64(s.cfg.StateTTL.Seconds()),
	}
}

// ---- stok ----

func (s *QueueStore) HasStock(ctx context.Context, eventID string) (bool, error) {
	n, err := s.rdb.Exists(ctx, keyStock(eventID)).Result()
	if err != nil {
		return false, fmt.Errorf("redis exists stock: %w", err)
	}
	return n > 0, nil
}

// WarmStock memakai HSETNX → nilai dari stock.changed yang lebih baru tak tertimpa.
func (s *QueueStore) WarmStock(ctx context.Context, eventID string, available map[string]int) error {
	pipe := s.rdb.Pipeline()
	for cat, n := range available {
		pipe.HSetNX(ctx, keyStock(eventID), cat, n)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis warm stock: %w", err)
	}
	return nil
}

func (s *QueueStore) SetStock(ctx context.Context, eventID, categoryID string, available int) error {
	if err := s.rdb.HSet(ctx, keyStock(eventID), categoryID, available).Err(); err != nil {
		return fmt.Errorf("redis set stock: %w", err)
	}
	return nil
}

// ---- antrian ----

func (s *QueueStore) Join(ctx context.Context, p outbound.JoinParams) (outbound.JoinResult, error) {
	args := append(s.baseArgs(p.EventID), p.Token, s.cfg.QueueCap)
	raw, err := s.join.Run(ctx, s.rdb, s.keys(p.EventID), args...).Slice()
	if err != nil {
		return outbound.JoinResult{}, fmt.Errorf("redis join script: %w", err)
	}
	if len(raw) == 0 {
		return outbound.JoinResult{}, errors.New("redis join script: hasil kosong")
	}

	res := outbound.JoinResult{Outcome: entity.JoinOutcome(asString(raw[0]))}
	switch res.Outcome {
	case entity.OutcomeAdmitted:
		res.ExpiresAt = time.UnixMilli(asInt(raw[1])).UTC()
		res.Promoted = asStrings(raw[2:])
	case entity.OutcomeQueued:
		res.Position = asInt(raw[1])
		res.Promoted = asStrings(raw[2:])
	}
	return res, nil
}

func (s *QueueStore) State(ctx context.Context, eventID, token string) (outbound.TokenState, error) {
	pipe := s.rdb.Pipeline()
	hget := pipe.HGetAll(ctx, prefixToken(eventID)+token)
	rank := pipe.ZRank(ctx, keyQueue(eventID), token)
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return outbound.TokenState{}, fmt.Errorf("redis state: %w", err)
	}

	fields := hget.Val()
	if len(fields) == 0 {
		return outbound.TokenState{Found: false}, nil
	}
	st := outbound.TokenState{
		Found:  true,
		Status: entity.Status(fields["status"]),
	}
	if v, err := strconv.ParseInt(fields["expiresAt"], 10, 64); err == nil {
		st.ExpiresAt = time.UnixMilli(v).UTC()
	}
	if r, err := rank.Result(); err == nil {
		st.Position = r + 1
	}
	return st, nil
}

func (s *QueueStore) Promote(ctx context.Context, eventID string) ([]string, error) {
	raw, err := s.promote.Run(ctx, s.rdb, s.keys(eventID), s.baseArgs(eventID)...).Slice()
	if err != nil {
		return nil, fmt.Errorf("redis promote script: %w", err)
	}
	return asStrings(raw), nil
}

func (s *QueueStore) Release(ctx context.Context, eventID, token string) (bool, []string, error) {
	args := append(s.baseArgs(eventID), token)
	raw, err := s.release.Run(ctx, s.rdb, s.keys(eventID), args...).Slice()
	if err != nil {
		return false, nil, fmt.Errorf("redis release script: %w", err)
	}
	if len(raw) == 0 {
		return false, nil, nil
	}
	return asInt(raw[0]) == 1, asStrings(raw[1:]), nil
}

func (s *QueueStore) SweepExpired(ctx context.Context, eventID string) ([]string, error) {
	raw, err := s.sweep.Run(ctx, s.rdb, s.keys(eventID), s.baseArgs(eventID)...).Slice()
	if err != nil {
		return nil, fmt.Errorf("redis sweep script: %w", err)
	}
	return asStrings(raw), nil
}

func (s *QueueStore) Close(ctx context.Context, eventID string) (int, error) {
	args := append(s.baseArgs(eventID), int64(s.cfg.ClosedTTL.Seconds()))
	n, err := s.closeQ.Run(ctx, s.rdb, s.keys(eventID), args...).Int()
	if err != nil {
		return 0, fmt.Errorf("redis close script: %w", err)
	}
	return n, nil
}

// ---- pendukung ----

func (s *QueueStore) RegisterEvent(ctx context.Context, eventID string) error {
	if err := s.rdb.SAdd(ctx, keyActiveEvents, eventID).Err(); err != nil {
		return fmt.Errorf("redis register event: %w", err)
	}
	return nil
}

func (s *QueueStore) ActiveEvents(ctx context.Context) ([]string, error) {
	ids, err := s.rdb.SMembers(ctx, keyActiveEvents).Result()
	if err != nil {
		return nil, fmt.Errorf("redis active events: %w", err)
	}
	return ids, nil
}

func (s *QueueStore) AdmissionRatePerSec(ctx context.Context, eventID string) (float64, error) {
	now := time.Now().UnixMilli()
	n, err := s.rdb.ZCount(ctx, keyLog(eventID),
		strconv.FormatInt(now-60_000, 10), strconv.FormatInt(now, 10)).Result()
	if err != nil {
		return 0, fmt.Errorf("redis admission rate: %w", err)
	}
	return float64(n) / 60.0, nil
}

// ---- konversi hasil Lua (string/int64 campur) ----

func asString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case int64:
		return strconv.FormatInt(x, 10)
	default:
		return fmt.Sprint(x)
	}
}

func asInt(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	default:
		return 0
	}
}

func asStrings(vs []any) []string {
	out := make([]string, 0, len(vs))
	for _, v := range vs {
		out = append(out, asString(v))
	}
	return out
}
