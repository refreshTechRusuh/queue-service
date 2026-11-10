package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

var ok = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

func TestTransactionID_ShouldGenerate_WhenClientSendsNone(t *testing.T) {
	rr := httptest.NewRecorder()

	TransactionID(ok).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))

	assert.NotEmpty(t, rr.Header().Get("X-Transaction-Id"))
}

func TestTransactionID_ShouldReuseClientValue(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Transaction-Id", "tx-123")
	rr := httptest.NewRecorder()

	TransactionID(ok).ServeHTTP(rr, req)

	assert.Equal(t, "tx-123", rr.Header().Get("X-Transaction-Id"))
}

func TestChain_ShouldRunOutermostFirst(t *testing.T) {
	var order []string
	mk := func(name string) func(http.Handler) http.Handler {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				order = append(order, name)
				next.ServeHTTP(w, r)
			})
		}
	}

	Chain(ok, mk("luar"), mk("dalam")).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))

	assert.Equal(t, []string{"luar", "dalam"}, order)
}
