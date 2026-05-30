package sse

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"iter"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// SourceOption configures Source.
type SourceOption func(*sourceConfig)

type sourceConfig struct {
	client *http.Client
	buf    *bytes.Buffer
}

// WithClient configures the HTTP client Source uses when reconnecting.
// Defaults to http.DefaultClient.
func WithClient(c *http.Client) SourceOption {
	return func(cfg *sourceConfig) { cfg.client = c }
}

// WithSourceBuffer provides a caller-owned bytes.Buffer for accumulating data
// fields, avoiding a pool round-trip per connection.
func WithSourceBuffer(buf *bytes.Buffer) SourceOption {
	return func(cfg *sourceConfig) { cfg.buf = buf }
}

// Source parses a Server-Sent Events stream from an HTTP response and returns
// an iterator that yields each dispatched event.
//
// If response.Request is set, Source automatically reconnects after the
// server-specified retry delay (default 3 s) when the connection closes,
// sending the Last-Event-ID header so the server can resume the stream.
// If response.Request is nil, the iterator ends when the response body is
// exhausted with no reconnection.
//
// The iterator stops when:
//   - the response body is exhausted and response.Request is nil
//   - the request context is cancelled
//   - the server responds with a non-text/event-stream Content-Type
//   - the caller breaks out of the range loop
//
// The caller is responsible for closing response.Body.
func Source(response *http.Response, opts ...SourceOption) (iter.Seq[*Message], error) {
	ct := response.Header.Get(ContentTypeResponseHeaderKey)
	if !strings.HasPrefix(ct, "text/event-stream") {
		return nil, fmt.Errorf("sse: response Content-Type %q is not text/event-stream", ct)
	}
	cfg := &sourceConfig{client: http.DefaultClient}
	for _, opt := range opts {
		opt(cfg)
	}
	return func(yield func(*Message) bool) {
		var (
			lastEventID string
			retryDelay  = 3 * time.Second
		)
		var dataBuf *bytes.Buffer
		if cfg.buf != nil {
			dataBuf = cfg.buf
		} else {
			dataBuf = builderPool.Get().(*bytes.Buffer)
			defer builderPool.Put(dataBuf)
			defer dataBuf.Reset()
		}

		if !parseSSEStream(response.Body, dataBuf, &lastEventID, &retryDelay, yield) {
			return
		}

		req := response.Request
		if req == nil {
			return
		}

		for {
			select {
			case <-req.Context().Done():
				return
			case <-time.After(retryDelay):
			}

			r := req.Clone(req.Context())
			if lastEventID != "" {
				r.Header.Set(LastEventIDRequestHeaderKey, lastEventID)
			}
			resp, err := cfg.client.Do(r)
			if err != nil {
				return
			}
			if !strings.HasPrefix(resp.Header.Get(ContentTypeResponseHeaderKey), "text/event-stream") {
				_ = resp.Body.Close()
				return
			}
			dataBuf.Reset()
			keepGoing := parseSSEStream(resp.Body, dataBuf, &lastEventID, &retryDelay, yield)
			_ = resp.Body.Close()
			if !keepGoing {
				return
			}
		}
	}, nil
}

const (
	// minRetryDelay is the floor applied to any server-supplied retry value,
	// preventing a retry: 0 from spinning the reconnect loop at CPU speed.
	minRetryDelay = time.Millisecond
	// maxRetryDelay caps the server-supplied retry value to prevent int64
	// overflow when converting milliseconds to time.Duration (nanoseconds).
	maxRetryDelay = 24 * time.Hour
	maxRetryMS    = int64(maxRetryDelay / time.Millisecond)
)

func parseSSEStream(r io.Reader, dataBuf *bytes.Buffer, lastEventID *string, retryDelay *time.Duration, yield func(*Message) bool) bool {
	scanner := bufio.NewScanner(r)
	var (
		id    *string
		event *string
		retry *int64
	)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if dataBuf.Len() == 0 {
				continue
			}
			if id != nil {
				*lastEventID = *id
			}
			if retry != nil {
				ms := min(*retry, maxRetryMS)
				*retryDelay = max(time.Duration(ms)*time.Millisecond, minRetryDelay)
			}
			m := &Message{
				id:                id,
				event:             event,
				retryMilliseconds: retry,
				data:              bytes.NewBuffer(bytes.Clone(dataBuf.Bytes())),
			}
			id = nil
			event = nil
			retry = nil
			dataBuf.Reset()
			if !yield(m) {
				return false
			}
			continue
		}
		line = strings.TrimPrefix(line, "\xef\xbb\xbf") // strip UTF-8 BOM if present
		fieldName, rest, _ := strings.Cut(line, ":")
		fieldValue := strings.TrimPrefix(rest, " ")
		switch fieldName {
		case "":
			// comment — ignore
		case "data":
			if dataBuf.Len() > 0 {
				dataBuf.WriteByte('\n')
			}
			dataBuf.WriteString(fieldValue)
		case "id":
			if !strings.ContainsRune(fieldValue, '\x00') {
				v := fieldValue
				id = &v
			}
		case "event":
			v := fieldValue
			event = &v
		case "retry":
			if ms, err := strconv.ParseInt(fieldValue, 10, 64); err == nil && ms >= 0 {
				retry = &ms
			}
		}
	}
	return true
}