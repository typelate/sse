package sse

import (
	"bufio"
	"bytes"
	"fmt"
	"iter"
	"net/http"
	"strconv"
	"strings"
)

// Source parses a Server-Sent Events stream from an HTTP response and returns
// an iterator that yields each dispatched event. The caller is responsible for
// closing response.Body.
//
// An error is returned immediately if the response Content-Type is not
// text/event-stream.
func Source(response *http.Response) (iter.Seq[*Message], error) {
	ct := response.Header.Get(ContentTypeResponseHeaderKey)
	if !strings.HasPrefix(ct, "text/event-stream") {
		return nil, fmt.Errorf("sse: response Content-Type %q is not text/event-stream", ct)
	}
	return func(yield func(*Message) bool) {
		scanner := bufio.NewScanner(response.Body)
		var (
			id    *string
			event *string
			retry *int64
			data  bytes.Buffer
		)
		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				if data.Len() == 0 {
					continue
				}
				m := &Message{
					id:                id,
					event:             event,
					retryMilliseconds: retry,
					data:              bytes.NewBuffer(bytes.Clone(data.Bytes())),
				}
				id = nil
				event = nil
				retry = nil
				data.Reset()
				if !yield(m) {
					return
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
				if data.Len() > 0 {
					data.WriteByte('\n')
				}
				data.WriteString(fieldValue)
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
	}, nil
}