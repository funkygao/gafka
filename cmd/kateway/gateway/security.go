package gateway

import (
	"net"
	"net/http"
	"strconv"
	"time"
)

// SecurityConfig adds some HTTP header fields widely
// considered to improve safety of HTTP requests. These fields
// are documented as follows:
//
//   Strict Transport Security: https://tools.ietf.org/html/rfc6797
//   Frame Options:             https://tools.ietf.org/html/draft-ietf-websec-x-frame-options-00
//   Cross Site Scripting:      http://msdn.microsoft.com/en-us/library/dd565647%28v=vs.85%29.aspx
//   Content Type Options:      http://msdn.microsoft.com/en-us/library/ie/gg622941%28v=vs.85%29.aspx
type SecurityConfig struct {
	// If true, redirects any request with scheme http to the equivalent https URL.
	HTTPSRedirect          bool
	HTTPSUseForwardedProto bool

	// Allow cleartext (non-HTTPS) HTTP connections to a loopback
	// address, even if HTTPSRedirect is true.
	PermitClearLoopback bool

	// If true, sets X-Content-Type-Options to "nosniff".
	ContentTypeOptions bool

	// If true, sets the HTTP Strict Transport Security header
	// field, which instructs browsers to send future requests
	// over HTTPS, even if the URL uses the unencrypted http
	// scheme.
	HSTS                  bool
	HSTSMaxAge            time.Duration
	HSTSIncludeSubdomains bool

	// If true, sets X-Frame-Options, to control when the request
	// should be displayed inside an HTML frame.
	FrameOptions       bool
	FrameOptionsPolicy FramePolicy

	// If true, sets X-XSS-Protection to "1", optionally with
	// "mode=block". See the official documentation, linked above,
	// for the meaning of these values.
	XSSProtection      bool
	XSSProtectionBlock bool

	// Used by ServeHTTP, after setting any extra headers, to
	// reply to the request. Next is typically nil, in which case
	// http.DefaultServeMux is used instead.
	Next http.Handler
}

func (c *SecurityConfig) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if c.HTTPSRedirect && !c.isHTTPS(r) && !c.okLoopback(r) {
		url := *r.URL
		url.Scheme = "https"
		url.Host = r.Host
		http.Redirect(w, r, url.String(), http.StatusMovedPermanently)
		return
	}

	if c.ContentTypeOptions {
		w.Header().Set("X-Content-Type-Options", "nosniff")
	}

	if c.HSTS && c.isHTTPS(r) {
		v := "max-age=" + strconv.FormatInt(int64(c.HSTSMaxAge/time.Second), 10)
		if c.HSTSIncludeSubdomains {
			v += "; includeSubDomains"
		}
		w.Header().Set("Strict-Transport-Security", v)
	}

	if c.FrameOptions {
		w.Header().Set("X-Frame-Options", string(c.FrameOptionsPolicy))
	}

	if c.XSSProtection {
		v := "1"
		if c.XSSProtectionBlock {
			v += "; mode=block"
		}
		w.Header().Set("X-XSS-Protection", v)
	}

	next := c.Next
	if next == nil {
		next = http.DefaultServeMux
	}
	next.ServeHTTP(w, r)
}

// Given that r is cleartext (not HTTPS), okLoopback returns
// whether r is on a permitted loopback connection.
func (c *SecurityConfig) okLoopback(r *http.Request) bool {
	return c.PermitClearLoopback && isLoopback(r)
}

func (c *SecurityConfig) isHTTPS(r *http.Request) bool {
	if c.HTTPSUseForwardedProto {
		return r.Header.Get("X-Forwarded-Proto") == "https"
	}
	return r.TLS != nil
}

// FramePolicy tells the browser under what circumstances to allow
// the response to be displayed inside an HTML frame. There are
// three options:
//
//   Deny            do not permit display in a frame
//   SameOrigin      permit display in a frame from the same origin
//   AllowFrom(url)  permit display in a frame from the given url
type FramePolicy string

const (
	Deny       FramePolicy = "DENY"
	SameOrigin FramePolicy = "SAMEORIGIN"
)

// AllowFrom returns a FramePolicy specifying that the requested
// resource should be included in a frame from only the given url.
func AllowFrom(url string) FramePolicy {
	return FramePolicy("ALLOW-FROM: " + url)
}

// ShouldUseForwardedProto returns whether to trust the
// X-Forwarded-Proto header field.
// DefaultConfig.HTTPSUseForwardedProto is initialized to this
// value.
//
// This value depends on the particular environment where the
// package is built. It is currently true iff build constraint
// "heroku" is satisfied.
func ShouldUseForwardedProto() bool {
	return false
}

func isLoopback(r *http.Request) bool {
	a, err := net.ResolveTCPAddr("tcp", r.RemoteAddr)
	return err == nil && a.IP.IsLoopback()
}
