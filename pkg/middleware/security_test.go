package middleware

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSecurityHeaders_ShouldSetAllHeaders_WithoutFramingHeaders(t *testing.T) {
	rr := httptest.NewRecorder()

	SecurityHeaders(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/x", nil))

	h := rr.Header()
	assert.Equal(t, "nosniff", h.Get("X-Content-Type-Options"))
	assert.Contains(t, h.Get("Strict-Transport-Security"), "max-age=31536000")
	assert.Equal(t, "default-src 'none'", h.Get("Content-Security-Policy"))
	assert.Equal(t, "no-referrer", h.Get("Referrer-Policy"))
	assert.Equal(t, "no-store", h.Get("Cache-Control"))
	assert.Empty(t, h.Get("X-Frame-Options"), "sengaja tidak dipasang (iframe pembayaran FE)")
	assert.NotContains(t, h.Get("Content-Security-Policy"), "frame-ancestors")
}

func TestSecurityHeaders_ShouldAlsoApplyToErrorResponses(t *testing.T) {
	rr := httptest.NewRecorder()

	SecurityHeaders(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/x", nil))

	assert.Equal(t, http.StatusInternalServerError, rr.Code)
	assert.Equal(t, "nosniff", rr.Header().Get("X-Content-Type-Options"))
	assert.Equal(t, "no-store", rr.Header().Get("Cache-Control"))
}

func TestSecurityHeaders_HandlerCanOverrideCacheControl(t *testing.T) {
	rr := httptest.NewRecorder()

	SecurityHeaders(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Cache-Control", "no-cache") // mis. SSE
	})).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/x", nil))

	assert.Equal(t, "no-cache", rr.Header().Get("Cache-Control"), "default no-store hanya default")
}

func TestMaxBody_ShouldReject413_WhenContentLengthExceedsLimit(t *testing.T) {
	called := false
	h := MaxBody(10)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
	req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(strings.Repeat("a", 11)))
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusRequestEntityTooLarge, rr.Code)
	assert.False(t, called, "handler tidak boleh dijalankan untuk body kebesaran")
	assert.Equal(t, "application/json", rr.Header().Get("Content-Type"))
	assert.Contains(t, rr.Body.String(), `"status":"413"`)
}

func TestMaxBody_ShouldPass_WhenBodyAtOrBelowLimit(t *testing.T) {
	for _, n := range []int{0, 1, 10} {
		var got string
		h := MaxBody(10)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			got = string(b)
		}))
		rr := httptest.NewRecorder()

		h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(strings.Repeat("a", n))))

		assert.Equal(t, http.StatusOK, rr.Code, "n=%d", n)
		assert.Len(t, got, n)
	}
}

func TestMaxBody_ShouldCutReading_WhenLengthUnknownOrUnderstated(t *testing.T) {
	var readErr error
	h := MaxBody(10)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, readErr = io.ReadAll(r.Body)
	}))
	req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(strings.Repeat("a", 1000)))
	req.ContentLength = -1 // chunked: panjang tidak diketahui di muka

	h.ServeHTTP(httptest.NewRecorder(), req)

	var mbe *http.MaxBytesError
	require.Error(t, readErr)
	assert.True(t, errors.As(readErr, &mbe), "pembacaan harus berhenti dengan MaxBytesError, bukan menelan 1000 byte")
}

func TestMaxBody_ShouldFallBackToDefault_WhenLimitNotPositive(t *testing.T) {
	for _, limit := range []int64{0, -5} {
		h := MaxBody(limit)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))

		small := httptest.NewRecorder()
		h.ServeHTTP(small, httptest.NewRequest(http.MethodPost, "/x", strings.NewReader("ok")))
		big := httptest.NewRecorder()
		h.ServeHTTP(big, httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(strings.Repeat("a", int(DefaultMaxBodyBytes)+1))))

		assert.Equal(t, http.StatusOK, small.Code)
		assert.Equal(t, http.StatusRequestEntityTooLarge, big.Code, "limit=%d tidak boleh berarti tanpa batas", limit)
	}
}

func TestMaxBody_ShouldHandleRequestsWithoutBody(t *testing.T) {
	h := MaxBody(10)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Body = nil

	assert.NotPanics(t, func() { h.ServeHTTP(httptest.NewRecorder(), req) })
}
