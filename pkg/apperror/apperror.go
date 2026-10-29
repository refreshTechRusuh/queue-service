// Package apperror mendefinisikan tipe error domain queue-service.
// Pola: usecase menaikkan AppError{Code, Message}; apinizer memetakan Code -> HTTP status.
package apperror

import "fmt"

// ErrorCode adalah kode error domain yang dipetakan ke HTTP status di apinizer.
type ErrorCode string

const (
	ErrValidation  ErrorCode = "VALIDATION_ERROR" // 400
	ErrNotFound    ErrorCode = "NOT_FOUND"        // 404
	ErrSoldOut     ErrorCode = "SOLD_OUT"         // 410 — tiket habis, antrian ditutup
	ErrRateLimited ErrorCode = "RATE_LIMITED"     // 429
	ErrInternal    ErrorCode = "INTERNAL_ERROR"   // 500
	ErrQueueFull   ErrorCode = "QUEUE_FULL"       // 503 — ramai, coba lagi (≠ sold out)
)

// AppError adalah error domain dengan kode spesifik + pesan human-readable.
type AppError struct {
	Code    ErrorCode
	Message string
	cause   error
}

func New(code ErrorCode, message string) *AppError {
	return &AppError{Code: code, Message: message}
}

func Wrap(code ErrorCode, message string, cause error) *AppError {
	return &AppError{Code: code, Message: message, cause: cause}
}

func (e *AppError) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("[%s] %s: %v", e.Code, e.Message, e.cause)
	}
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

func (e *AppError) Unwrap() error { return e.cause }

// ---- Constructor helpers ----

func Validation(msg string) *AppError    { return New(ErrValidation, msg) }
func NotFound(resource string) *AppError { return New(ErrNotFound, resource+" tidak ditemukan") }
func SoldOut() *AppError                 { return New(ErrSoldOut, "Tiket sudah habis") }
func RateLimited() *AppError {
	return New(ErrRateLimited, "Terlalu banyak permintaan, coba lagi sebentar")
}
func QueueFull() *AppError { return New(ErrQueueFull, "Antrian sedang penuh, coba lagi sebentar") }
func Internal(cause error) *AppError {
	return Wrap(ErrInternal, "Terjadi kesalahan pada server", cause)
}
