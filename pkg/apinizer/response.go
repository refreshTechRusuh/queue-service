// Package apinizer menyediakan envelope response standar:
//
//	{ status, message, data }  — status "00000" sukses, string kode HTTP gagal.
package apinizer

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/refreshTechRusuh/queue-service/pkg/apperror"
)

// Envelope adalah bentuk response tunggal untuk setiap endpoint.
type Envelope struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	Data    any    `json:"data"`
	Partial bool   `json:"partial,omitempty"`
}

// Success menulis response 200 OK dengan payload data.
func Success(w http.ResponseWriter, message string, data any) {
	writeJSON(w, http.StatusOK, Envelope{Status: "00000", Message: message, Data: data})
}

// FailedBadRequest menulis response 400 (mis. body JSON rusak).
func FailedBadRequest(w http.ResponseWriter, message string) {
	writeJSON(w, http.StatusBadRequest, Envelope{Status: "400", Message: message, Data: struct{}{}})
}

// WriteError memetakan apperror.AppError ke HTTP status + envelope gagal.
// Error tak terduga (bukan AppError) -> 500 dengan pesan generik.
func WriteError(w http.ResponseWriter, err error) {
	var appErr *apperror.AppError
	if !errors.As(err, &appErr) {
		slog.Error("unhandled error", "err", err)
		writeJSON(w, http.StatusInternalServerError, Envelope{
			Status: "500", Message: "Terjadi kesalahan pada server", Data: struct{}{},
		})
		return
	}
	status := mapErrorCode(appErr.Code)
	if status == http.StatusInternalServerError {
		slog.Error("internal error", "err", err)
	}
	writeJSON(w, status, Envelope{Status: strconv.Itoa(status), Message: appErr.Message, Data: struct{}{}})
}

// mapErrorCode memetakan ErrorCode ke HTTP status.
func mapErrorCode(code apperror.ErrorCode) int {
	switch code {
	case apperror.ErrValidation:
		return http.StatusBadRequest
	case apperror.ErrNotFound:
		return http.StatusNotFound
	case apperror.ErrSoldOut:
		return http.StatusGone
	case apperror.ErrRateLimited:
		return http.StatusTooManyRequests
	case apperror.ErrQueueFull:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		slog.Error("apinizer: gagal encode response", "err", err)
	}
}
