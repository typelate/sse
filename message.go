package sse

import (
	"bytes"
	"io"
	"net/http"
	"sync"
)

const (
	LastEventIDRequestHeaderKey    = "Last-Event-ID"
	ContentTypeResponseHeaderKey   = "content-type"
	ContentTypeResponseHeaderValue = "text/event-stream; charset=utf-8"
	ConnectionResponseHeaderKey    = "Connection"
	ConnectionResponseHeaderValue  = "keep-alive"
)

type Response struct {
	mut         sync.Mutex
	flusher     http.Flusher
	lastEventID *string
}

func New(res http.ResponseWriter, req *http.Request, code int) (*Response, bool) {
	var lastEventID *string
	if _, isSet := req.Header[LastEventIDRequestHeaderKey]; isSet {
		val := req.Header.Get(LastEventIDRequestHeaderKey)
		lastEventID = &val
	}
	flusher, ok := res.(http.Flusher)
	if !ok {
		return nil, false
	}
	headers := res.Header()
	headers.Set(ContentTypeResponseHeaderKey, ContentTypeResponseHeaderValue)
	headers.Set(ConnectionResponseHeaderKey, ConnectionResponseHeaderValue)
	res.WriteHeader(code)
	return &Response{
		flusher:     flusher,
		lastEventID: lastEventID,
	}, true
}

func (res *Response) LastEventID() (string, bool) {
	if res.lastEventID != nil {
		return *res.lastEventID, true
	}
	return "", false
}

type Message struct {
	id    *string
	event *string
	data  []byte
	buf   *bytes.Buffer
}

type MessageOption func(*Message)

func WithBytesBuffer(buf *bytes.Buffer) MessageOption { return func(m *Message) { m.buf = buf } }
func WithEvent(event string) MessageOption            { return func(m *Message) { m.event = &event } }
func WithID(id string) MessageOption                  { return func(m *Message) { m.id = &id } }

func (res *Response) Message(data []byte, opts ...MessageOption) error {
	res.mut.Lock()
	defer func() {
		res.flusher.Flush()
		res.mut.Unlock()
	}()
	msg := Message{data: data}
	for _, opt := range opts {
		opt(&msg)
	}
	var buf = msg.buf
	if buf == nil {
		buf = bytes.NewBuffer(nil)
	}
	_, err := writeTo(&msg, buf)
	if err != nil {
		return err
	}
	return nil
}

func writeTo(msg *Message, w *bytes.Buffer) (int64, error) {
	var total int64
	if msg.id != nil {
		n, err := io.WriteString(w, "id: "+*msg.id+"\n")
		total += int64(n)
		if err != nil {
			return total, err
		}
	}
	if msg.event != nil {
		n, err := io.WriteString(w, "event: "+*msg.event+"\n")
		total += int64(n)
		if err != nil {
			return total, err
		}
	}

	for line := range bytes.SplitSeq(bytes.TrimSuffix(msg.data, []byte{'\n'}), []byte{'\n'}) {
		n1, err := io.WriteString(w, "data: ")
		total += int64(n1)
		if err != nil {
			return total, err
		}
		n2, err := w.Write(line)
		total += int64(n2)
		if err != nil {
			return total, err
		}
		n3, err := io.WriteString(w, "\n")
		total += int64(n3)
		if err != nil {
			return total, err
		}
	}
	n, err := io.WriteString(w, "\n")
	total += int64(n)
	return total, err
}
