package middleware

import (
	"net/http"
	"strings"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// OtelMiddleware membungkus handler dengan OpenTelemetry HTTP instrumentation.
// Setiap request masuk mendapat span (method, route, status, url) dan header W3C
// traceparent dari pemanggil dijadikan parent, sehingga trace tersambung lintas service.
//
// Request yang path-nya berakhiran salah satu skipSuffixes TIDAK di-trace. Dipakai untuk:
//   - "/sys/ping": health check berulang hanya menambah noise & biaya
//   - "/stream"  : koneksi SSE berumur panjang (span baru selesai saat koneksi putus)
func OtelMiddleware(serviceName string, skipSuffixes ...string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return otelhttp.NewHandler(next, serviceName,
			otelhttp.WithFilter(func(r *http.Request) bool {
				for _, s := range skipSuffixes {
					if strings.HasSuffix(r.URL.Path, s) {
						return false
					}
				}
				return true
			}),
		)
	}
}
