package proxy

import (
	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/openwar/openwar/internal/middleware"
)

// Forward builds a ReverseProxy pointing at the upstream backend. A Rewrite
// hook injects X-Forwarded-* headers so the backend can derive client context.
func Forward(backend *url.URL) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(backend)
			pr.Out.URL.Path = "/checkout" // map any gateway route to the demo checkout
			pr.Out.Header.Set("X-Forwarded-Host", pr.In.Host)
			pr.Out.Header.Set("X-Forwarded-Proto", scheme(pr.In))
			pr.Out.Header.Set("X-Forwarded-For", pr.In.RemoteAddr)
			if user := middleware.UserIDFrom(pr.In); user != "" {
				pr.Out.Header.Set("X-User-ID", user)
			}
			if sid := middleware.SessionIDFrom(pr.In); sid != "" {
				pr.Out.Header.Set("X-Session-ID", sid)
			}
		},
	}
}

func scheme(r *http.Request) string {
	if r.TLS != nil {
		return "https"
	}
	if fw := r.Header.Get("X-Forwarded-Proto"); fw != "" {
		return fw
	}
	return "http"
}
