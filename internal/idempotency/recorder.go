package idempotency

import (
	"bytes"
	"net/http"
)

// recorder is an http.ResponseWriter that buffers the handler's response so it
// can be both cached and replayed. Buffering means responses guarded by this
// layer are not streamed to the client incrementally — an acceptable trade-off
// for the create/charge style endpoints idempotency keys protect.
type recorder struct {
	header      http.Header
	status      int
	body        bytes.Buffer
	wroteHeader bool
}

func newRecorder() *recorder {
	return &recorder{header: make(http.Header), status: http.StatusOK}
}

func (rec *recorder) Header() http.Header { return rec.header }

func (rec *recorder) WriteHeader(code int) {
	if rec.wroteHeader {
		return
	}
	rec.status = code
	rec.wroteHeader = true
}

func (rec *recorder) Write(b []byte) (int, error) {
	rec.wroteHeader = true
	return rec.body.Write(b)
}

// snapshot returns an immutable copy of what the handler produced.
func (rec *recorder) snapshot() *response {
	h := make(http.Header, len(rec.header))
	for k, vs := range rec.header {
		h[k] = append([]string(nil), vs...)
	}
	return &response{
		status: rec.status,
		header: h,
		body:   append([]byte(nil), rec.body.Bytes()...),
	}
}

// writeResponse copies a cached response onto w. When replayed is true it adds a
// header marking the response as served from the idempotency cache rather than
// freshly computed.
func writeResponse(w http.ResponseWriter, res *response, replayed bool) {
	dst := w.Header()
	for k, vs := range res.header {
		dst[k] = append([]string(nil), vs...)
	}
	if replayed {
		dst.Set("Idempotent-Replayed", "true")
	}
	w.WriteHeader(res.status)
	_, _ = w.Write(res.body)
}
