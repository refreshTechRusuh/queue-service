// Package http (infrastructure) = adapter HTTP antrian: join, cek posisi, SSE stream.
// Tidak ada logika bisnis di sini — hanya terjemahan HTTP ⇄ usecase.
package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/refreshTechRusuh/queue-service/internal/queue/domain/dto"
	"github.com/refreshTechRusuh/queue-service/internal/queue/domain/entity"
	"github.com/refreshTechRusuh/queue-service/internal/queue/port/inbound"
	"github.com/refreshTechRusuh/queue-service/pkg/apinizer"
)

// Waker = sumber sinyal "state token berubah" (diimplementasikan cache.Hub).
type Waker interface {
	Subscribe(eventID, token string) (<-chan struct{}, func())
}

// Handler memegang usecase + waker untuk SSE.
type Handler struct {
	uc      inbound.IQueueUsecase
	waker   Waker
	sseTick time.Duration // interval cek ulang posisi + heartbeat
}

func NewHandler(uc inbound.IQueueUsecase, waker Waker, sseTick time.Duration) *Handler {
	return &Handler{uc: uc, waker: waker, sseTick: sseTick}
}

// Join — POST /api/queue/v1/event/{eventId}/join   body opsional: {"nik":"..."}
func (h *Handler) Join(w http.ResponseWriter, r *http.Request) {
	var req dto.JoinRequest
	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			apinizer.FailedBadRequest(w, "body tidak valid")
			return
		}
	}
	res, err := h.uc.Join(r.Context(), r.PathValue("eventId"), req)
	if err != nil {
		apinizer.WriteError(w, err)
		return
	}
	apinizer.Success(w, messageFor(res.Status), res)
}

// Status — GET /api/queue/v1/event/{eventId}/{queueToken}
func (h *Handler) Status(w http.ResponseWriter, r *http.Request) {
	res, err := h.uc.Status(r.Context(), r.PathValue("eventId"), r.PathValue("queueToken"))
	if err != nil {
		apinizer.WriteError(w, err)
		return
	}
	apinizer.Success(w, messageFor(res.Status), res)
}

// Stream — GET /api/queue/v1/event/{eventId}/{queueToken}/stream  (SSE)
//
// Kirim event:
//
//	position  {position, etaSeconds?}      saat masih antre (hanya bila berubah)
//	admitted  {accessToken, bookingExpiresAt} lalu stream ditutup → klien redirect
//	sold_out  {message}                    lalu ditutup
//	expired   {message}                    lalu ditutup
//
// Selain dibangunkan oleh sinyal Pub/Sub, state dicek ulang tiap sseTick sebagai
// pengaman + heartbeat (": ping") supaya proxy tidak memutus koneksi idle.
func (h *Handler) Stream(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	eventID, token := r.PathValue("eventId"), r.PathValue("queueToken")

	// Validasi sebelum header SSE dikirim → error masih bisa berupa envelope biasa.
	st, err := h.uc.Status(ctx, eventID, token)
	if err != nil {
		apinizer.WriteError(w, err)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		apinizer.WriteError(w, errors.New("streaming tidak didukung"))
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // matikan buffering proxy
	w.WriteHeader(http.StatusOK)

	// Daftarkan ke hub memakai eventId hasil normalisasi usecase (huruf kecil).
	wake, unsubscribe := h.waker.Subscribe(normalizedEvent(eventID), token)
	defer unsubscribe()
	ticker := time.NewTicker(h.sseTick)
	defer ticker.Stop()

	lastPos := int64(-1)
	for {
		if done := writeState(w, st, &lastPos); done {
			flusher.Flush()
			return
		}
		flusher.Flush()

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-wake:
		}

		if st, err = h.uc.Status(ctx, eventID, token); err != nil {
			writeEvent(w, "error", dto.SSEMessage{Message: "gagal membaca status antrian"})
			flusher.Flush()
			return
		}
	}
}

// writeState menulis event sesuai state. Return true bila stream harus ditutup.
func writeState(w http.ResponseWriter, st dto.QueueStatusResponse, lastPos *int64) bool {
	switch entity.Status(st.Status) {
	case entity.StatusAdmitted:
		writeEvent(w, "admitted", dto.SSEAdmitted{AccessToken: st.AccessToken, BookingExpiresAt: st.BookingExpiresAt})
		return true
	case entity.StatusSoldOut:
		writeEvent(w, "sold_out", dto.SSEMessage{Message: "Tiket sudah habis. Antrian ditutup."})
		return true
	case entity.StatusExpired:
		writeEvent(w, "expired", dto.SSEMessage{Message: "Sesi antrian sudah berakhir."})
		return true
	default: // QUEUED
		if st.Position != *lastPos {
			writeEvent(w, "position", dto.SSEPosition{Position: st.Position, EtaSeconds: st.EtaSeconds})
			*lastPos = st.Position
		} else {
			_, _ = fmt.Fprint(w, ": ping\n\n")
		}
		return false
	}
}

func writeEvent(w http.ResponseWriter, name string, payload any) {
	b, err := json.Marshal(payload)
	if err != nil {
		return
	}
	_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", name, b)
}

func messageFor(status string) string {
	switch entity.Status(status) {
	case entity.StatusAdmitted:
		return "akses diberikan"
	case entity.StatusQueued:
		return "anda dalam antrian"
	case entity.StatusSoldOut:
		return "tiket sudah habis"
	default:
		return "sesi antrian berakhir"
	}
}
