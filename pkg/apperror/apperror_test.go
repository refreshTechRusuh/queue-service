package apperror

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConstructors_ShouldSetCodeAndMessage(t *testing.T) {
	cause := errors.New("redis down")
	cases := map[string]struct {
		err  *AppError
		code ErrorCode
	}{
		"Validation":  {Validation("x"), ErrValidation},
		"NotFound":    {NotFound("queueToken"), ErrNotFound},
		"SoldOut":     {SoldOut(), ErrSoldOut},
		"RateLimited": {RateLimited(), ErrRateLimited},
		"QueueFull":   {QueueFull(), ErrQueueFull},
		"Internal":    {Internal(cause), ErrInternal},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.code, tc.err.Code)
			assert.NotEmpty(t, tc.err.Message)
		})
	}
	assert.Equal(t, "queueToken tidak ditemukan", NotFound("queueToken").Message)
}

func TestError_ShouldIncludeCause_WhenWrapped(t *testing.T) {
	cause := errors.New("boom")

	wrapped := Wrap(ErrInternal, "gagal", cause)

	assert.Contains(t, wrapped.Error(), "boom")
	assert.ErrorIs(t, wrapped, cause)
}

func TestError_ShouldOmitCause_WhenNotWrapped(t *testing.T) {
	assert.Equal(t, "[VALIDATION_ERROR] salah", New(ErrValidation, "salah").Error())
}

func TestInternal_ShouldNotLeakCauseToClient(t *testing.T) {
	assert.NotContains(t, Internal(errors.New("password=rahasia")).Message, "rahasia")
}
