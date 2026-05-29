// Package sse implements the server side of the WHATWG Server-Sent Events
// protocol.
//
// See https://html.spec.whatwg.org/multipage/server-sent-events.html for the
// wire format and the rules clients follow when interpreting an event stream.
package sse

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"net/textproto"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ErrInvalidField is returned when an id, event, or comment value contains a
// character the SSE wire format forbids: CR or LF (reserved as line
// terminators) in any field, or a NUL in an id (which the client ignores,
// silently dropping the id).
var ErrInvalidField = errors.New("sse: field contains a forbidden character")

type flushResponseWriter interface {
	http.Flusher
	http.ResponseWriter
}

// Response is an open Server-Sent Events stream. Message and Comment are safe
// to call from multiple goroutines.
type Response struct {
	mut         sync.Mutex
	res         flushResponseWriter
	lastEventID *string
}

// New writes the SSE response headers and the supplied status code, then
// returns a *Response for sending events. The boolean is false if the
// underlying ResponseWriter does not implement http.Flusher; in that case no
// headers are written and the caller should respond with an error.
func New(res http.ResponseWriter, req *http.Request, code int) (*Response, bool) {
	rw, ok := res.(flushResponseWriter)
	if !ok {
		return nil, false
	}
	var lastEventID *string
	canonical := textproto.CanonicalMIMEHeaderKey("Last-Event-Id")
	if values, isSet := req.Header[canonical]; isSet && len(values) > 0 && values[0] != "" {
		v := values[0]
		lastEventID = &v
	}
	h := res.Header()
	h.Set("Content-Type", "text/event-stream; charset=utf-8")
	h.Set("Connection", "keep-alive")
	h.Set("Cache-Control", "no-cache")
	rw.WriteHeader(code)
	rw.Flush()
	return &Response{res: rw, lastEventID: lastEventID}, true
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
	id    *string
	event *string
	retry *time.Duration
	data  []byte
	buf   *bytes.Buffer
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
	return func(m *Message) { m.retry = &d }
}

// builder holds a Message and its assembly buffer in one heap object so
// Response.Message can reuse both via sync.Pool and avoid per-call allocations.
// The Message field would otherwise escape (its address is taken when applying
// options), so co-locating it with the buffer hides that escape behind the
// pool.
type builder struct {
	msg Message
	buf bytes.Buffer
}

var builderPool = sync.Pool{
	New: func() any { return &builder{} },
}

// Message sends an event to the client and flushes the connection. Multi-line
// data is split into one "data:" line per line; \r\n and \r line terminators
// inside data are normalized to \n.
func (res *Response) Message(data []byte, opts ...MessageOption) error {
	b := builderPool.Get().(*builder)
	b.msg = Message{data: data}
	for _, opt := range opts {
		opt(&b.msg)
	}
	if b.msg.id != nil && strings.ContainsAny(*b.msg.id, "\r\n\x00") {
		err := fmt.Errorf("%w: id %q", ErrInvalidField, *b.msg.id)
		builderPool.Put(b)
		return err
	}
	if b.msg.event != nil && strings.ContainsAny(*b.msg.event, "\r\n") {
		err := fmt.Errorf("%w: event %q", ErrInvalidField, *b.msg.event)
		builderPool.Put(b)
		return err
	}

	buf := b.msg.buf
	if buf == nil {
		buf = &b.buf
	}
	buf.Reset()
	if b.msg.id != nil {
		buf.WriteString("id: ")
		if len(*b.msg.id) > 0 {
			buf.WriteString(*b.msg.id)
		}
		buf.WriteByte('\n')
	}
	if b.msg.event != nil {
		buf.WriteString("event: ")
		if len(*b.msg.event) > 0 {
			buf.WriteString(*b.msg.event)
		}
		buf.WriteByte('\n')
	}
	if b.msg.retry != nil {
		ms := b.msg.retry.Milliseconds()
		if ms < 0 {
			// A negative reconnection time can't be represented on the wire
			// (the client only parses an all-ASCII-digits value), so floor it.
			ms = 0
		}
		buf.WriteString("retry: ")
		var scratch [20]byte // enough for any int64
		if line := strconv.AppendInt(scratch[:0], ms, 10); len(line) > 0 {
			buf.Write(line)
		}
		buf.WriteByte('\n')
	}

	if bytes.IndexByte(b.msg.data, '\r') >= 0 {
		// Normalize CRLF and bare CR to LF so no stray line terminators
		// appear inside a data field on the wire.
		b.msg.data = bytes.ReplaceAll(b.msg.data, []byte("\r\n"), []byte("\n"))
		b.msg.data = bytes.ReplaceAll(b.msg.data, []byte{'\r'}, []byte{'\n'})
	}
	b.msg.data = bytes.TrimSuffix(b.msg.data, []byte{'\n'})

	for line := range bytes.SplitSeq(b.msg.data, []byte{'\n'}) {
		buf.WriteString("data: ")
		if len(line) > 0 {
			buf.Write(line)
		}
		buf.WriteByte('\n')
	}
	buf.WriteByte('\n')

	res.mut.Lock()
	_, err := res.res.Write(buf.Bytes())
	res.res.Flush()
	res.mut.Unlock()

	builderPool.Put(b)
	return err
}

// Comment writes a colon-prefixed comment line. Clients ignore comments; they
// are useful as keep-alive pings to prevent proxies from closing idle
// connections. The text must not contain CR or LF.
func (res *Response) Comment(text string) error {
	if strings.ContainsAny(text, "\r\n") {
		return fmt.Errorf("%w: comment %q", ErrInvalidField, text)
	}
	b := builderPool.Get().(*builder)
	b.buf.Reset()
	b.buf.WriteString(": ")
	if len(text) > 0 {
		b.buf.WriteString(text)
	}
	b.buf.WriteString("\n\n")

	res.mut.Lock()
	_, err := res.res.Write(b.buf.Bytes())
	res.res.Flush()
	res.mut.Unlock()

	builderPool.Put(b)

	return err
}
