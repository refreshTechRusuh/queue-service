package apinizer

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/refreshTechRusuh/queue-service/pkg/apperror"
)

func decode(t *testing.T, rr *httptest.ResponseRecorder) Envelope {
	t.Helper()
	var env Envelope
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &env))
	return env
}

func TestSuccess_ShouldWriteEnvelope200(t *testing.T) {
	rr := httptest.NewRecorder()

	Success(rr, "OK", map[string]int{"n": 1})

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "application/json", rr.Header().Get("Content-Type"))
	env := decode(t, rr)
	assert.Equal(t, "00000", env.Status)
	assert.Equal(t, "OK", env.Message)
}

func TestFailedBadRequest_ShouldWrite400(t *testing.T) {
	rr := httptest.NewRecorder()

	FailedBadRequest(rr, "body rusak")

	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Equal(t, "400", decode(t, rr).Status)
}

func TestWriteError_ShouldMapErrorCodeToHTTPStatus(t *testing.T) {
	cases := []struct {
		err  *apperror.AppError
		want int
	}{
		{apperror.Validation("x"), http.StatusBadRequest},
		{apperror.NotFound("x"), http.StatusNotFound},
		{apperror.SoldOut(), http.StatusGone},
		{apperror.RateLimited(), http.StatusTooManyRequests},
		{apperror.QueueFull(), http.StatusServiceUnavailable},
		{apperror.Internal(errors.New("x")), http.StatusInternalServerError},
	}
	for _, tc := range cases {
		t.Run(string(tc.err.Code), func(t *testing.T) {
			rr := httptest.NewRecorder()

			WriteError(rr, tc.err)

			assert.Equal(t, tc.want, rr.Code)
			assert.Equal(t, fmt.Sprint(tc.want), decode(t, rr).Status)
		})
	}
}

func TestWriteError_ShouldHideDetails_WhenNotAppError(t *testing.T) {
	rr := httptest.NewRecorder()

	WriteError(rr, errors.New("dial tcp 10.0.0.5:6379: refused"))

	assert.Equal(t, http.StatusInternalServerError, rr.Code)
	assert.NotContains(t, rr.Body.String(), "10.0.0.5")
}

func TestWriteError_ShouldUnwrapWrappedAppError(t *testing.T) {
	rr := httptest.NewRecorder()

	WriteError(rr, fmt.Errorf("konteks: %w", apperror.SoldOut()))

	assert.Equal(t, http.StatusGone, rr.Code)
}
