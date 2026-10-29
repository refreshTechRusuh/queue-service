// Package usecase berisi logika bisnis antrian. Hanya tahu domain + port,
// tidak tahu Redis/HTTP/Kafka (hexagonal).
package usecase

import (
	"context"
	"log/slog"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/singleflight"

	"github.com/refreshTechRusuh/queue-service/internal/queue/domain/dto"
	"github.com/refreshTechRusuh/queue-service/internal/queue/domain/entity"
	"github.com/refreshTechRusuh/queue-service/internal/queue/port/outbound"
	"github.com/refreshTechRusuh/queue-service/pkg/apperror"
)

var (
	tokenPattern = regexp.MustCompile(`^q_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	// eventIdPattern = UUID kanonik saja. Sengaja lebih ketat dari uuid.Parse (yang juga
	// menerima "{...}" / "urn:uuid:"), karena eventId dipakai di dalam key Redis {eventId}.
	eventIDPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
)

// Settings = parameter yang dibutuhkan usecase.
type Settings struct {
	BookingTTL time.Duration // dipakai menghitung iat Access Token (exp - TTL)
}

// warmupTimeout = batas waktu membaca stok awal dari DB.
const warmupTimeout = 5 * time.Second

// QueueUsecase mengimplementasikan inbound.IQueueUsecase.
type QueueUsecase struct {
	store    outbound.IQueueStore
	stock    outbound.IStockSource
	notifier outbound.INotifier
	signer   outbound.IAccessTokenSigner
	cfg      Settings
	log      *slog.Logger
	warmup   singleflight.Group // satu pembacaan DB per event per instance
}

func NewQueueUsecase(
	store outbound.IQueueStore,
	stock outbound.IStockSource,
	notifier outbound.INotifier,
	signer outbound.IAccessTokenSigner,
	cfg Settings,
	log *slog.Logger,
) *QueueUsecase {
	return &QueueUsecase{store: store, stock: stock, notifier: notifier, signer: signer, cfg: cfg, log: log}
}

// ---- HTTP ----

// Join: masuk antrian. Langsung ADMITTED bila ada kursi kosong & tak ada yang
// menunggu di depan; selain itu QUEUED dengan posisi.
// Join tidak menerima identitas apa pun (tanpa login, tanpa NIK): identitas pembeli
// baru diisi di Detail Ticket dan divalidasi booking-service.
func (u *QueueUsecase) Join(ctx context.Context, eventID string) (dto.QueueStatusResponse, error) {
	eventID, err := normEventID(eventID)
	if err != nil {
		return dto.QueueStatusResponse{}, err
	}
	if err := u.ensureStock(ctx, eventID); err != nil {
		return dto.QueueStatusResponse{}, err
	}
	if err := u.store.RegisterEvent(ctx, eventID); err != nil {
		return dto.QueueStatusResponse{}, apperror.Internal(err)
	}

	token := newQueueToken()
	res, err := u.store.Join(ctx, outbound.JoinParams{EventID: eventID, Token: token})
	if err != nil {
		return dto.QueueStatusResponse{}, apperror.Internal(err)
	}
	u.notify(ctx, eventID, res.Promoted)

	switch res.Outcome {
	case entity.OutcomeSoldOut:
		return dto.QueueStatusResponse{}, apperror.SoldOut()
	case entity.OutcomeFull:
		return dto.QueueStatusResponse{}, apperror.QueueFull()
	case entity.OutcomeAdmitted:
		return u.admitted(eventID, token, res.ExpiresAt)
	default: // QUEUED
		return u.queued(ctx, eventID, token, res.Position), nil
	}
}

// Status: cek posisi (dipakai saat refresh & oleh SSE). Token persisten → refresh
// mengembalikan posisi yang sama (Req 3.4).
func (u *QueueUsecase) Status(ctx context.Context, eventID, token string) (dto.QueueStatusResponse, error) {
	eventID, err := normEventID(eventID)
	if err != nil {
		return dto.QueueStatusResponse{}, err
	}
	if !tokenPattern.MatchString(token) {
		return dto.QueueStatusResponse{}, apperror.Validation("queueToken tidak valid")
	}
	st, err := u.store.State(ctx, eventID, token)
	if err != nil {
		return dto.QueueStatusResponse{}, apperror.Internal(err)
	}
	if !st.Found {
		return dto.QueueStatusResponse{}, apperror.NotFound("queueToken")
	}

	switch st.Status {
	case entity.StatusAdmitted:
		if time.Now().After(st.ExpiresAt) {
			// Sudah lewat TTL tapi sweeper belum sempat jalan → tampilkan EXPIRED.
			return dto.QueueStatusResponse{Status: string(entity.StatusExpired), QueueToken: token}, nil
		}
		return u.admitted(eventID, token, st.ExpiresAt)
	case entity.StatusQueued:
		return u.queued(ctx, eventID, token, st.Position), nil
	case entity.StatusSoldOut:
		return dto.QueueStatusResponse{Status: string(entity.StatusSoldOut), QueueToken: token}, nil
	default: // RELEASED / EXPIRED → kesempatan sudah habis
		return dto.QueueStatusResponse{Status: string(entity.StatusExpired), QueueToken: token}, nil
	}
}

// ---- Event dari BE-2 ----

// OnStockChanged: perbarui cache stok kategori lalu coba promosi (stok naik → cap naik).
func (u *QueueUsecase) OnStockChanged(ctx context.Context, eventID, categoryID string, available int) error {
	eventID, err := normEventID(eventID)
	if err != nil || categoryID == "" {
		u.log.WarnContext(ctx, "stock.changed diabaikan: payload tidak valid",
			slog.String("eventId", eventID), slog.String("categoryId", categoryID))
		return nil
	}
	// Warm-up dulu supaya kategori lain tidak hilang dari total (cap jadi terlalu kecil).
	if err := u.ensureStock(ctx, eventID); err != nil {
		u.log.WarnContext(ctx, "warm-up stok gagal saat stock.changed", slog.Any("err", err))
	}
	if err := u.store.SetStock(ctx, eventID, categoryID, available); err != nil {
		return err
	}
	promoted, err := u.store.Promote(ctx, eventID)
	if err != nil {
		return err
	}
	u.notify(ctx, eventID, promoted)
	return nil
}

// OnSlotReleased: bila payload membawa queueToken, kursi token itu dilepas
// (idempoten — hanya sekali walau event terkirim berulang). Tanpa queueToken,
// queue tak tahu kursi siapa, jadi cukup mencoba promosi; kursinya akan dilepas
// sweeper saat Access Token kedaluwarsa (aman: tak pernah over-admit).
func (u *QueueUsecase) OnSlotReleased(ctx context.Context, eventID, queueToken string) error {
	eventID, err := normEventID(eventID)
	if err != nil {
		u.log.WarnContext(ctx, "slot.released diabaikan: eventId tidak valid", slog.String("eventId", eventID))
		return nil
	}
	var promoted []string
	if tokenPattern.MatchString(queueToken) {
		_, promoted, err = u.store.Release(ctx, eventID, queueToken)
	} else {
		promoted, err = u.store.Promote(ctx, eventID)
	}
	if err != nil {
		return err
	}
	u.notify(ctx, eventID, promoted)
	return nil
}

// OnSoldOut: tutup antrian (idempoten), keluarkan semua yang menunggu, push SSE sold_out.
func (u *QueueUsecase) OnSoldOut(ctx context.Context, eventID string) error {
	eventID, err := normEventID(eventID)
	if err != nil {
		return nil
	}
	n, err := u.store.Close(ctx, eventID)
	if err != nil {
		return err
	}
	u.log.InfoContext(ctx, "antrian ditutup (sold out)", slog.String("eventId", eventID), slog.Int("dikeluarkan", n))
	if err := u.notifier.NotifyAll(ctx, eventID); err != nil {
		u.log.WarnContext(ctx, "notify sold_out gagal", slog.Any("err", err))
	}
	return nil
}

// Sweep: lepas kursi yang Access Token-nya kedaluwarsa di semua event, lalu promosi.
//
// Kenapa perlu: orang yang di-admit tapi TIDAK PERNAH submit tidak punya booking,
// jadi tak akan ada slot.released untuknya. Tanpa sweeper kursinya bocor dan
// antrian macet. Aman dari PHP: setelah token kedaluwarsa ia tak bisa submit lagi,
// dan bila ia sempat submit, tiketnya sudah di-hold (sudah keluar dari stok),
// jadi kursi cadangannya memang tidak dibutuhkan lagi.
func (u *QueueUsecase) Sweep(ctx context.Context) error {
	events, err := u.store.ActiveEvents(ctx)
	if err != nil {
		return err
	}
	for _, eventID := range events {
		promoted, err := u.store.SweepExpired(ctx, eventID)
		if err != nil {
			u.log.WarnContext(ctx, "sweep gagal", slog.String("eventId", eventID), slog.Any("err", err))
			continue
		}
		u.notify(ctx, eventID, promoted)
	}
	return nil
}

// ---- helper ----

// ensureStock mengisi cache stok dari sumber (DB) bila masih kosong.
//
// Saat gerbang dibuka, ribuan join datang hampir bersamaan dan semuanya melihat
// cache kosong. singleflight memastikan per instance hanya SATU yang membaca DB;
// sisanya menunggu hasil yang sama. Pembacaan memakai context terlepas (dengan
// timeout sendiri) supaya klien pertama yang memutus koneksi tidak menggagalkan
// semua yang ikut menunggu.
func (u *QueueUsecase) ensureStock(ctx context.Context, eventID string) error {
	has, err := u.store.HasStock(ctx, eventID)
	if err != nil {
		return apperror.Internal(err)
	}
	if has {
		return nil
	}
	_, err, _ = u.warmup.Do(eventID, func() (any, error) {
		wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), warmupTimeout)
		defer cancel()
		return nil, u.loadStock(wctx, eventID)
	})
	return err
}

// loadStock membaca stok dari DB lalu mengisi cache (dipanggil lewat singleflight).
func (u *QueueUsecase) loadStock(ctx context.Context, eventID string) error {
	if has, err := u.store.HasStock(ctx, eventID); err == nil && has {
		return nil // sudah diisi instance lain / stock.changed di sela-sela
	}
	stock, err := u.stock.LoadStock(ctx, eventID)
	if err != nil {
		return apperror.Internal(err)
	}
	if len(stock) == 0 {
		return apperror.NotFound("event")
	}
	if err := u.store.WarmStock(ctx, eventID, stock); err != nil {
		return apperror.Internal(err)
	}
	return nil
}

func (u *QueueUsecase) admitted(eventID, token string, expiresAt time.Time) (dto.QueueStatusResponse, error) {
	access, err := u.signer.Sign(entity.AccessClaims{
		EventID:    eventID,
		QueueToken: token,
		IssuedAt:   expiresAt.Add(-u.cfg.BookingTTL),
		ExpiresAt:  expiresAt,
	})
	if err != nil {
		return dto.QueueStatusResponse{}, apperror.Internal(err)
	}
	return dto.QueueStatusResponse{
		Status:           string(entity.StatusAdmitted),
		QueueToken:       token,
		AccessToken:      access,
		BookingExpiresAt: expiresAt.UTC().Format(time.RFC3339),
	}, nil
}

func (u *QueueUsecase) queued(ctx context.Context, eventID, token string, position int64) dto.QueueStatusResponse {
	return dto.QueueStatusResponse{
		Status:     string(entity.StatusQueued),
		QueueToken: token,
		Position:   position,
		EtaSeconds: u.eta(ctx, eventID, position),
	}
}

// eta = posisi / laju admit (rolling 60 detik). nil bila laju belum terukur (Req 19.3).
func (u *QueueUsecase) eta(ctx context.Context, eventID string, position int64) *int64 {
	rate, err := u.store.AdmissionRatePerSec(ctx, eventID)
	if err != nil || rate <= 0 {
		return nil
	}
	sec := int64(math.Ceil(float64(position) / rate))
	return &sec
}

func (u *QueueUsecase) notify(ctx context.Context, eventID string, tokens []string) {
	if len(tokens) == 0 {
		return
	}
	if err := u.notifier.Notify(ctx, eventID, tokens); err != nil {
		u.log.WarnContext(ctx, "notify gagal", slog.String("eventId", eventID), slog.Any("err", err))
	}
}

// newQueueToken = UUID v7 (urut waktu) berawalan "q_". Fallback v4 bila v7 gagal.
func newQueueToken() string {
	id, err := uuid.NewV7()
	if err != nil {
		id = uuid.New()
	}
	return "q_" + id.String()
}

// normEventID memvalidasi lalu menormalkan eventId ke huruf kecil, supaya UUID yang
// sama dengan beda kapitalisasi tidak memecah antrian ke key Redis berbeda.
func normEventID(eventID string) (string, error) {
	if !eventIDPattern.MatchString(eventID) {
		return "", apperror.Validation("eventId tidak valid")
	}
	return strings.ToLower(eventID), nil
}
