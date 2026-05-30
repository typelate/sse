// Package sse implements the server side of the WHATWG Server-Sent Events
// protocol.
//
// See https://html.spec.whatwg.org/multipage/server-sent-events.html for the
// wire format and the rules clients follow when interpreting an event stream.
package sse

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/textproto"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// LastEventIDRequestHeaderKey is the header a client sets when
	// reconnecting, carrying the id of the last event it received so the
	// server can resume the stream.
	LastEventIDRequestHeaderKey = "Last-Event-Id"

	ContentTypeResponseHeaderKey   = "Content-Type"
	ContentTypeResponseHeaderValue = "text/event-stream; charset=utf-8"

	ConnectionResponseHeaderKey   = "Connection"
	ConnectionResponseHeaderValue = "keep-alive"

	// CacheControlResponseHeaderKey is set to no-cache so intermediaries
	// do not buffer the stream.
	CacheControlResponseHeaderKey   = "Cache-Control"
	CacheControlResponseHeaderValue = "no-cache"
)

// ErrInvalidField is returned when an id, event, or comment value contains a
// character the SSE wire format forbids: CR or LF (reserved as line
// terminators) in any field, or a NUL in an id (which the client ignores,
// silently dropping the id).
const ErrInvalidField errorString = "sse: field contains a forbidden character"

type errorString string

func (se errorString) Error() string { return string(se) }

// Response is an open Server-Sent Events stream. Message and Comment are safe
// to call from multiple goroutines.
type Response struct {
	mut         sync.Mutex
	res         http.ResponseWriter
	flusher     http.Flusher
	lastEventID *string
}

// New writes the SSE response headers and the supplied status code, then
// returns a *Response for sending events. The boolean is false if the
// underlying ResponseWriter does not implement http.Flusher; in that case no
// headers are written and the caller should respond with an error.
func New(res http.ResponseWriter, req *http.Request, code int) (*Response, bool) {
	flusher, ok := res.(http.Flusher)
	if !ok {
		return nil, false
	}
	var lastEventID *string
	canonical := textproto.CanonicalMIMEHeaderKey(LastEventIDRequestHeaderKey)
	if values, isSet := req.Header[canonical]; isSet && len(values) > 0 && values[0] != "" {
		v := values[0]
		lastEventID = &v
	}
	h := res.Header()
	h.Set(ContentTypeResponseHeaderKey, ContentTypeResponseHeaderValue)
	h.Set(ConnectionResponseHeaderKey, ConnectionResponseHeaderValue)
	h.Set(CacheControlResponseHeaderKey, CacheControlResponseHeaderValue)
	res.WriteHeader(code)
	flusher.Flush()
	return &Response{res: res, flusher: flusher, lastEventID: lastEventID}, true
}

// LastEventID returns the value of the Last-Event-ID request header. The
// boolean is false if the header was absent or empty.
func (res *Response) LastEventID() (string, bool) {
	if res.lastEventID == nil {
		return "", false
	}
	return *res.lastEventID, true
}

// Message is an SSE event, configured via the With* options.
type Message struct {
	id                *string
	event             *string
	retryMilliseconds *int64
	buf, data         *bytes.Buffer
}

func (m *Message) WriteTo(w io.Writer) (int64, error) {
	if m.id != nil && strings.ContainsAny(*m.id, "\r\n\x00") {
		return 0, fmt.Errorf("%w: id %q", ErrInvalidField, *m.id)
	}
	if m.event != nil && strings.ContainsAny(*m.event, "\r\n") {
		return 0, fmt.Errorf("%w: event %q", ErrInvalidField, *m.event)
	}
	var bytesWritten int
	if m.id != nil {
		if n, err := io.WriteString(w, "id: "); err != nil {
			return int64(bytesWritten + n), err
		} else {
			bytesWritten += n
		}
		if n, err := io.WriteString(w, *m.id); err != nil {
			return int64(bytesWritten + n), err
		} else {
			bytesWritten += n
		}
		if n, err := w.Write([]byte{'\n'}); err != nil {
			return int64(bytesWritten + n), err
		} else {
			bytesWritten += n
		}
	}
	if m.event != nil && len(*m.event) > 0 {
		if n, err := io.WriteString(w, "event: "); err != nil {
			return int64(bytesWritten + n), err
		} else {
			bytesWritten += n
		}
		if n, err := io.WriteString(w, *m.event); err != nil {
			return int64(bytesWritten + n), err
		} else {
			bytesWritten += n
		}
		if n, err := w.Write([]byte{'\n'}); err != nil {
			return int64(bytesWritten + n), err
		} else {
			bytesWritten += n
		}
	}
	if m.retryMilliseconds != nil {
		if n, err := io.WriteString(w, "retry: "); err != nil {
			return int64(bytesWritten + n), err
		} else {
			bytesWritten += n
		}
		var retryBuf [20]byte
		if n, err := w.Write(strconv.AppendInt(retryBuf[:0], int64(*m.retryMilliseconds), 10)); err != nil {
			return int64(bytesWritten + n), err
		} else {
			bytesWritten += n
		}
		if n, err := w.Write([]byte{'\n'}); err != nil {
			return int64(bytesWritten + n), err
		} else {
			bytesWritten += n
		}
	}
	var data []byte
	if m.data != nil {
		data = m.data.Bytes()
	}
	if bytes.IndexByte(data, '\r') >= 0 {
		data = bytes.ReplaceAll(data, []byte("\r\n"), []byte("\n"))
		data = bytes.ReplaceAll(data, []byte("\r"), []byte("\n"))
	}
	data = bytes.TrimSuffix(data, []byte{'\n'})
	for line := range bytes.SplitSeq(data, []byte{'\n'}) {
		if n, err := io.WriteString(w, "data: "); err != nil {
			return int64(bytesWritten + n), err
		} else {
			bytesWritten += n
		}
		if len(line) > 0 {
			if n, err := w.Write(line); err != nil {
				return int64(bytesWritten + n), err
			} else {
				bytesWritten += n
			}
		}
		if n, err := w.Write([]byte{'\n'}); err != nil {
			return int64(bytesWritten + n), err
		} else {
			bytesWritten += n
		}
	}
	if n, err := w.Write([]byte{'\n'}); err != nil {
		return int64(bytesWritten + n), err
	} else {
		bytesWritten += n
	}
	return int64(bytesWritten), nil
}

func (m *Message) ID() (string, bool) {
	if m.id == nil {
		return "", false
	}
	return *m.id, true
}

func (m *Message) Event() (string, bool) {
	if m.event == nil {
		return "", false
	}
	return *m.event, true
}

func (m *Message) Retry() (time.Duration, bool) {
	if m.retryMilliseconds == nil {
		return 0, false
	}
	return time.Millisecond * time.Duration(*m.retryMilliseconds), true
}

func (m *Message) Data() string {
	if m.data == nil {
		return ""
	}
	return m.data.String()
}

// MessageOption configures a Message.
type MessageOption func(*Message)

// WithBytesBuffer reuses a caller-provided buffer for assembling the wire
// representation, useful for sync.Pool style allocation avoidance. The buffer
// is reset before use.
func WithBytesBuffer(buf *bytes.Buffer) MessageOption {
	return func(m *Message) { m.buf = buf }
}

// WithEvent sets the event type. The value must not contain CR or LF.
func WithEvent(event string) MessageOption {
	return func(m *Message) { m.event = &event }
}

// WithID sets the id field, which the client echoes back via Last-Event-ID
// after a disconnect. Pass an empty string to reset the client's stored id.
// The value must not contain CR, LF, or NUL: the client ignores an id
// containing NUL, so allowing it would silently drop the id.
func WithID(id string) MessageOption {
	return func(m *Message) { m.id = &id }
}

// WithIntID is a convenience for WithID(strconv.Itoa(id)).
func WithIntID(id int) MessageOption {
	return func(m *Message) {
		s := strconv.Itoa(id)
		m.id = &s
	}
}

// WithRetry sets the reconnection time the client should wait after the
// connection drops. Sub-millisecond precision is dropped; the wire field is
// an integer count of milliseconds. A negative duration is floored to 0,
// since the wire format cannot represent a negative reconnection time.
func WithRetry(d time.Duration) MessageOption {
	return func(m *Message) {
		if d < 0 {
			d = 0
		}
		ms := d.Milliseconds()
		m.retryMilliseconds = &ms
	}
}

var builderPool = sync.Pool{
	New: func() any { return bytes.NewBuffer(nil) },
}

// Message sends an event to the client and flushes the connection. Multi-line
// data is split into one "data:" line per line; \r\n and \r line terminators
// inside data are normalized to \n.
func (res *Response) Message(data []byte, opts ...MessageOption) error {
	m := Message{data: bytes.NewBuffer(data)}
	for _, opt := range opts {
		opt(&m)
	}
	if m.buf == nil {
		b := builderPool.Get().(*bytes.Buffer)
		defer builderPool.Put(b)
		m.buf = b
	}
	defer m.buf.Reset()
	if _, err := m.WriteTo(m.buf); err != nil {
		return err
	}

	res.mut.Lock()
	_, err := m.buf.WriteTo(res.res)
	res.flusher.Flush()
	res.mut.Unlock()

	return err
}

// Comment writes a colon-prefixed comment line. Clients ignore comments; they
// are useful as keep-alive pings to prevent proxies from closing idle
// connections. The text must not contain CR or LF.
func (res *Response) Comment(text string) error {
	if err := checkComment(text); err != nil {
		return err
	}

	b := builderPool.Get().(*bytes.Buffer)
	defer builderPool.Put(b)
	b.Reset()
	defer b.Reset()

	writeComment(b, text)

	res.mut.Lock()
	_, err := res.res.Write(b.Bytes())
	res.flusher.Flush()
	res.mut.Unlock()

	return err
}

func checkComment(text string) error {
	if strings.ContainsAny(text, "\r\n") {
		return fmt.Errorf("%w: comment %q", ErrInvalidField, text)
	}
	return nil
}

func writeComment(buf *bytes.Buffer, text string) {
	buf.WriteString(": ")
	if len(text) > 0 {
		buf.WriteString(text)
	}
	buf.WriteString("\n\n")
}
