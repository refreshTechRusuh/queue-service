package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func runCORS(t *testing.T, origins []string, method, origin string) (*httptest.ResponseRecorder, bool) {
	t.Helper()
	reached := false
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	})
	req := httptest.NewRequest(method, "/x", nil)
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	rr := httptest.NewRecorder()
	CORSMiddleware(origins...)(next).ServeHTTP(rr, req)
	return rr, reached
}

func TestCORS_Wildcard_ShouldEchoAnyOrigin(t *testing.T) {
	rr, reached := runCORS(t, []string{"*"}, http.MethodGet, "https://apa-saja.example")

	assert.True(t, reached)
	assert.Equal(t, "https://apa-saja.example", rr.Header().Get("Access-Control-Allow-Origin"))
	assert.Equal(t, "Origin", rr.Header().Get("Vary"))
	assert.Contains(t, rr.Header().Get("Access-Control-Allow-Headers"), "Authorization")
	assert.Contains(t, rr.Header().Get("Access-Control-Allow-Methods"), "POST")
}

func TestCORS_Allowlist_ShouldAllowListedAndBlockOthers(t *testing.T) {
	list := []string{"https://app.example.com", "https://staging.example.com"}

	ok, _ := runCORS(t, list, http.MethodGet, "https://staging.example.com")
	bad, reached := runCORS(t, list, http.MethodGet, "https://jahat.example")

	assert.Equal(t, "https://staging.example.com", ok.Header().Get("Access-Control-Allow-Origin"))
	assert.Empty(t, bad.Header().Get("Access-Control-Allow-Origin"), "origin di luar daftar tidak diberi header")
	assert.True(t, reached, "request tetap diproses; yang memblokir adalah browser")
}

func TestCORS_ShouldNotSetHeaders_WhenNoOriginHeader(t *testing.T) {
	rr, reached := runCORS(t, []string{"*"}, http.MethodGet, "")

	assert.True(t, reached)
	assert.Empty(t, rr.Header().Get("Access-Control-Allow-Origin"), "request non-browser (curl, server-ke-server) tanpa Origin")
}

func TestCORS_Preflight_ShouldAnswer204WithoutReachingHandler(t *testing.T) {
	rr, reached := runCORS(t, []string{"*"}, http.MethodOptions, "https://fe.example")

	assert.Equal(t, http.StatusNoContent, rr.Code)
	assert.False(t, reached, "preflight tidak boleh diteruskan ke handler/ServeMux")
	assert.Equal(t, "86400", rr.Header().Get("Access-Control-Max-Age"))
}

func TestCORS_Preflight_FromDisallowedOrigin_ShouldGetNoCORSHeaders(t *testing.T) {
	rr, _ := runCORS(t, []string{"https://app.example.com"}, http.MethodOptions, "https://jahat.example")

	assert.Equal(t, http.StatusNoContent, rr.Code)
	assert.Empty(t, rr.Header().Get("Access-Control-Allow-Origin"), "browser akan memblokir request aslinya")
}

// Dokumentasi perilaku saat ini (bukan pembenaran): dengan "*", server memantulkan
// origin APA PUN dan menyertakan Allow-Credentials: true. Aman selama auth memakai header
// Authorization (bukan cookie), tapi harus diganti allowlist sebelum rilis akhir.
func TestCORS_Wildcard_CurrentlyReflectsOriginWithCredentials(t *testing.T) {
	rr, _ := runCORS(t, []string{"*"}, http.MethodGet, "https://situs-asing.example")

	assert.Equal(t, "https://situs-asing.example", rr.Header().Get("Access-Control-Allow-Origin"))
	assert.Equal(t, "true", rr.Header().Get("Access-Control-Allow-Credentials"))
}
