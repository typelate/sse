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

	// dataPrefix is the prefix the most recent writer left in effect. A
	// write from a different writer terminates the current line first.
	dataPrefix string

	// pendingCR records that the last byte written was a CR, whose line
	// break has already been emitted. It lets a CRLF straddling two writes
	// count as one break rather than two.
	pendingCR bool

	// err is the first failure met while building the data, held until the
	// message is sent because the writers have no useful moment to return
	// it.
	err error
}

func (m *Message) WriteTo(w io.Writer) (int64, error) {
	if m.err != nil {
		return 0, m.err
	}
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

// NewMessage returns a Message configured by opts, ready to have its data
// written in with Write. Send dispatches it.
//
// A Message under construction is not safe for concurrent use.
func NewMessage(opts ...MessageOption) *Message {
	var m Message
	for _, opt := range opts {
		opt(&m)
	}
	return &m
}

// Write appends p to the message data, implementing io.Writer. The data is
// split into "data:" lines when the message is sent, so a line break in p
// starts a new data line.
//
// LF, CRLF, and a bare CR all count as line breaks and are normalized to LF,
// matching how the SSE format treats them. A CRLF split across two writes
// counts as one break.
func (m *Message) Write(p []byte) (int, error) {
	return m.writeData("", p)
}

// Prefix returns an io.Writer that begins every data line it writes with
// prefix, for wire formats that key their data lines. Datastar, for example,
// writes an element patch as:
//
//	io.WriteString(m.Prefix("selector "), "#foo")
//	io.WriteString(m.Prefix("mode "), "inner")
//	err := tmpl.Execute(m.Prefix("elements "), page)
//
//	event: datastar-patch-elements
//	data: selector #foo
//	data: mode inner
//	data: elements <div>hi</div>
//
// Prefix terminates any partially written line, so each call starts a new
// data line. Writers stay usable after a later call: writing to an earlier
// one terminates the current line and resumes that writer's prefix. A writer
// that is never written to contributes nothing.
//
// Within a value, line breaks are recognized as in [Message.Write], and each
// resulting line carries the prefix.
//
// The prefix is written verbatim, so include any separator the format wants
// ("elements ", not "elements"). A prefix containing CR or LF is rejected:
// every Write on the returned writer fails with ErrInvalidField.
func (m *Message) Prefix(prefix string) io.Writer {
	if err := checkDataPrefix(prefix); err != nil {
		return dataWriter{m: m, err: err}
	}
	m.endDataLine()
	m.dataPrefix = prefix
	return dataWriter{m: m, prefix: prefix}
}

// StringPrefix writes value as data lines beginning with prefix, the common
// case of a keyed field whose value is already a string:
//
//	m.StringPrefix("selector ", "#foo")   // data: selector #foo
//
// It is [Message.Prefix] plus the write, without the io.Writer in between,
// so it neither boxes a writer nor copies value. Line breaks in value are
// recognized as in [Message.Write], and every resulting line carries the
// prefix.
//
// There is nothing to report at the call site: writing to a buffer cannot
// fail, and an unusable prefix is a mistake in the calling code rather than
// in the data. Send reports it, as it already does for an invalid id or
// event.
func (m *Message) StringPrefix(prefix, value string) {
	if err := checkDataPrefix(prefix); err != nil {
		m.setErr(err)
		return
	}
	if m.data == nil {
		m.data = bytes.NewBuffer(nil)
	}
	m.endDataLine()
	m.dataPrefix = prefix
	// Open the line before writing, so an empty value still produces one.
	// Calling this method is itself the request for the line, where asking
	// Prefix for a writer and never using it is not.
	m.startDataLine()
	m.writeDataString(prefix, value)
}

func checkDataPrefix(prefix string) error {
	if strings.ContainsAny(prefix, "\r\n") {
		return fmt.Errorf("%w: data line prefix %q", ErrInvalidField, prefix)
	}
	return nil
}

// setErr keeps the first failure. It is reported when the message is sent.
func (m *Message) setErr(err error) {
	if m.err == nil {
		m.err = err
	}
}

// dataWriter writes into its Message's data buffer, repeating prefix at the
// start of every line. A non-nil err rejects every write, so a prefix the
// wire format forbids fails at the point of use and again at Send, whether
// or not the caller checked.
type dataWriter struct {
	m      *Message
	prefix string
	err    error
}

func (w dataWriter) Write(p []byte) (int, error) {
	if w.err != nil {
		w.m.setErr(w.err)
		return 0, w.err
	}
	return w.m.writeData(w.prefix, p)
}

// WriteString implements io.StringWriter so io.WriteString reaches the string
// path directly instead of copying s into a []byte.
func (w dataWriter) WriteString(s string) (int, error) {
	if w.err != nil {
		w.m.setErr(w.err)
		return 0, w.err
	}
	return w.m.writeDataString(w.prefix, s)
}

// writeData appends p to the data buffer with prefix at the start of every
// line, taking over from whichever writer wrote last.
//
// CR and CRLF are normalized to LF here rather than left to WriteTo: the
// prefixes are inlined into the buffer, so a break WriteTo discovered later
// would open a line that no longer gets a prefix.
func (m *Message) writeData(prefix string, p []byte) (int, error) {
	if m.data == nil {
		m.data = bytes.NewBuffer(nil)
	}
	if m.dataPrefix != prefix {
		m.endDataLine()
		m.dataPrefix = prefix
	}
	consumed := len(p)
	for len(p) > 0 {
		if m.pendingCR {
			m.pendingCR = false
			if p[0] == '\n' {
				// The LF half of a CRLF split across two writes;
				// its break was emitted with the CR.
				p = p[1:]
				continue
			}
		}
		i := bytes.IndexAny(p, "\r\n")
		if i < 0 {
			m.startDataLine()
			m.data.Write(p)
			break
		}
		m.startDataLine()
		m.data.Write(p[:i])
		m.data.WriteByte('\n')
		if p[i] == '\r' {
			m.pendingCR = true
		}
		p = p[i+1:]
	}
	return consumed, nil
}

// writeDataString is writeData over a string. The two are kept separate, the
// way the stdlib splits bytes and strings, because converting between them is
// the copy this path exists to avoid: slicing a string is free, and
// bytes.Buffer.WriteString appends without an intermediate []byte.
func (m *Message) writeDataString(prefix, s string) (int, error) {
	if m.data == nil {
		m.data = bytes.NewBuffer(nil)
	}
	if m.dataPrefix != prefix {
		m.endDataLine()
		m.dataPrefix = prefix
	}
	consumed := len(s)
	for len(s) > 0 {
		if m.pendingCR {
			m.pendingCR = false
			if s[0] == '\n' {
				s = s[1:]
				continue
			}
		}
		i := strings.IndexAny(s, "\r\n")
		if i < 0 {
			m.startDataLine()
			m.data.WriteString(s)
			break
		}
		m.startDataLine()
		m.data.WriteString(s[:i])
		m.data.WriteByte('\n')
		if s[i] == '\r' {
			m.pendingCR = true
		}
		s = s[i+1:]
	}
	return consumed, nil
}

// startDataLine writes the current prefix if the buffer sits at the start of
// a line, so that even an empty line carries its prefix.
func (m *Message) startDataLine() {
	if m.atDataLineStart() {
		m.data.WriteString(m.dataPrefix)
	}
}

// endDataLine terminates a partially written line so the next write starts
// on a fresh one. A dangling CR belongs to the writer being left behind, so
// the next writer's leading LF is a break of its own.
func (m *Message) endDataLine() {
	if m.data != nil && !m.atDataLineStart() {
		m.data.WriteByte('\n')
	}
	m.pendingCR = false
}

func (m *Message) atDataLineStart() bool {
	b := m.data.Bytes()
	return len(b) == 0 || b[len(b)-1] == '\n'
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
	return res.Send(&m)
}

// Send writes m to the client and flushes the connection. It is safe to call
// from multiple goroutines; building m is not.
func (res *Response) Send(m *Message) error {
	buf := m.buf
	if buf == nil {
		b := builderPool.Get().(*bytes.Buffer)
		defer builderPool.Put(b)
		buf = b
	}
	buf.Reset()
	defer buf.Reset()
	if _, err := m.WriteTo(buf); err != nil {
		return err
	}

	res.mut.Lock()
	_, err := buf.WriteTo(res.res)
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
