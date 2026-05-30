package sse_test

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/typelate/sse"
)

func TestNew_setsHeadersAndStatus(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)

	if _, ok := sse.New(rec, req, http.StatusOK); !ok {
		t.Fatal("New returned false; httptest.ResponseRecorder should support Flush")
	}
	res := rec.Result()
	defer res.Body.Close()

	for k, want := range map[string]string{
		"Content-Type":  "text/event-stream; charset=utf-8",
		"Connection":    "keep-alive",
		"Cache-Control": "no-cache",
	} {
		if got := res.Header.Get(k); got != want {
			t.Errorf("header %s = %q, want %q", k, got, want)
		}
	}
	if res.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want %d", res.StatusCode, http.StatusOK)
	}
}

func TestNew_flushesHeaders(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)

	if _, ok := sse.New(rec, req, http.StatusOK); !ok {
		t.Fatal("New returned false")
	}
	if !rec.Flushed {
		t.Error("New did not flush after WriteHeader; clients would not see onopen until first event")
	}
}

func TestNew_returnsFalseWithoutFlusher(t *testing.T) {
	var rec nonFlushingResponseWriter
	req := httptest.NewRequest(http.MethodGet, "/", nil)

	if _, ok := sse.New(&rec, req, http.StatusOK); ok {
		t.Fatal("New returned true for non-flushing ResponseWriter")
	}
	if rec.statusWritten {
		t.Error("WriteHeader called when New should have bailed out")
	}
}

type nonFlushingResponseWriter struct {
	h             http.Header
	body          bytes.Buffer
	statusWritten bool
}

func (r *nonFlushingResponseWriter) Header() http.Header {
	if r.h == nil {
		r.h = http.Header{}
	}
	return r.h
}
func (r *nonFlushingResponseWriter) Write(p []byte) (int, error) { return r.body.Write(p) }
func (r *nonFlushingResponseWriter) WriteHeader(int)             { r.statusWritten = true }

func TestLastEventID(t *testing.T) {
	for _, tc := range []struct {
		name    string
		headers map[string]string
		want    string
		wantOK  bool
	}{
		{name: "absent", wantOK: false},
		{name: "present", headers: map[string]string{"Last-Event-ID": "42"}, want: "42", wantOK: true},
		{name: "empty value treated as absent", headers: map[string]string{"Last-Event-ID": ""}, wantOK: false},
		{name: "lowercase header canonicalized", headers: map[string]string{"last-event-id": "abc"}, want: "abc", wantOK: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}
			r, ok := sse.New(httptest.NewRecorder(), req, http.StatusOK)
			if !ok {
				t.Fatal("New failed")
			}
			got, gotOK := r.LastEventID()
			if got != tc.want || gotOK != tc.wantOK {
				t.Errorf("LastEventID() = (%q, %v), want (%q, %v)", got, gotOK, tc.want, tc.wantOK)
			}
		})
	}
}

func TestMessage(t *testing.T) {
	for _, tc := range []struct {
		name string
		data []byte
		opts []sse.MessageOption
		want string
	}{
		{name: "data only", data: []byte("hello"), want: "data: hello\n\n"},
		{name: "id", data: []byte("hello"), opts: []sse.MessageOption{sse.WithID("1")}, want: "id: 1\ndata: hello\n\n"},
		{name: "int id", data: []byte("hello"), opts: []sse.MessageOption{sse.WithIntID(42)}, want: "id: 42\ndata: hello\n\n"},
		{name: "event", data: []byte("hello"), opts: []sse.MessageOption{sse.WithEvent("greet")}, want: "event: greet\ndata: hello\n\n"},
		{name: "retry", data: []byte("hello"), opts: []sse.MessageOption{sse.WithRetry(2 * time.Second)}, want: "retry: 2000\ndata: hello\n\n"},
		{name: "all fields", data: []byte("hello"), opts: []sse.MessageOption{sse.WithID("9"), sse.WithEvent("ping"), sse.WithRetry(500 * time.Millisecond)}, want: "id: 9\nevent: ping\nretry: 500\ndata: hello\n\n"},
		{name: "negative retry clamped to zero", data: []byte("hello"), opts: []sse.MessageOption{sse.WithRetry(-2 * time.Second)}, want: "retry: 0\ndata: hello\n\n"},
		{name: "multi-line LF", data: []byte("a\nb\nc"), want: "data: a\ndata: b\ndata: c\n\n"},
		{name: "multi-line CRLF normalized", data: []byte("a\r\nb\r\nc"), want: "data: a\ndata: b\ndata: c\n\n"},
		{name: "multi-line bare CR normalized", data: []byte("a\rb\rc"), want: "data: a\ndata: b\ndata: c\n\n"},
		{name: "single trailing newline trimmed", data: []byte("hello\n"), want: "data: hello\n\n"},
		{name: "double trailing newline keeps one as empty data line", data: []byte("hello\n\n"), want: "data: hello\ndata: \n\n"},
		{name: "nil data still dispatches", data: nil, want: "data: \n\n"},
		{name: "empty id resets client state", data: []byte("x"), opts: []sse.MessageOption{sse.WithID("")}, want: "id: \ndata: x\n\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			r, ok := sse.New(rec, req, http.StatusOK)
			if !ok {
				t.Fatal("New failed")
			}
			if err := r.Message(tc.data, tc.opts...); err != nil {
				t.Fatalf("Message: %v", err)
			}
			if got := rec.Body.String(); got != tc.want {
				t.Errorf("body = %q, want %q", got, tc.want)
			}
			if !rec.Flushed {
				t.Error("response was not flushed")
			}
		})
	}
}

func TestMessage_invalidFields(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []sse.MessageOption
	}{
		{"id with LF", []sse.MessageOption{sse.WithID("a\nb")}},
		{"id with CR", []sse.MessageOption{sse.WithID("a\rb")}},
		{"id with NUL", []sse.MessageOption{sse.WithID("a\x00b")}},
		{"event with LF", []sse.MessageOption{sse.WithEvent("a\nb")}},
		{"event with CR", []sse.MessageOption{sse.WithEvent("a\rb")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			r, _ := sse.New(rec, req, http.StatusOK)
			err := r.Message([]byte("x"), tc.opts...)
			if !errors.Is(err, sse.ErrInvalidField) {
				t.Errorf("err = %v, want errors.Is(_, ErrInvalidField)", err)
			}
			if rec.Body.Len() != 0 {
				t.Errorf("body should stay empty when validation fails, got %q", rec.Body.String())
			}
		})
	}
}

func TestComment(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	r, _ := sse.New(rec, req, http.StatusOK)
	if err := r.Comment("ping"); err != nil {
		t.Fatalf("Comment: %v", err)
	}
	if got, want := rec.Body.String(), ": ping\n\n"; got != want {
		t.Errorf("body = %q, want %q", got, want)
	}
	if !rec.Flushed {
		t.Error("response was not flushed")
	}
}

func TestComment_invalid(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	r, _ := sse.New(rec, req, http.StatusOK)
	if err := r.Comment("a\nb"); !errors.Is(err, sse.ErrInvalidField) {
		t.Errorf("err = %v, want errors.Is(_, ErrInvalidField)", err)
	}
}

func TestMessage_withBytesBuffer(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	r, _ := sse.New(rec, req, http.StatusOK)

	if err := r.Message([]byte("x"), sse.WithBytesBuffer(buf)); err != nil {
		t.Fatalf("first Message: %v", err)
	}
	if got, want := rec.Body.String(), "data: x\n\n"; got != want {
		t.Errorf("body = %q, want %q", got, want)
	}
	if err := r.Message([]byte("y"), sse.WithBytesBuffer(buf)); err != nil {
		t.Fatalf("second Message: %v", err)
	}
	if got, want := rec.Body.String(), "data: x\n\ndata: y\n\n"; got != want {
		t.Errorf("body = %q, want %q (buffer not reset between calls)", got, want)
	}
}

func TestMessage_concurrentEventsAreFramed(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	r, _ := sse.New(rec, req, http.StatusOK)

	const goroutines, perGoroutine = 10, 50
	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perGoroutine {
				if err := r.Message([]byte("x")); err != nil {
					t.Errorf("Message: %v", err)
				}
			}
		}()
	}
	wg.Wait()

	body := rec.Body.String()
	if got, want := strings.Count(body, "data: x\n\n"), goroutines*perGoroutine; got != want {
		t.Errorf("event count = %d, want %d (events were interleaved)", got, want)
	}
}

func TestEndToEnd(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r, ok := sse.New(w, req, http.StatusOK)
		if !ok {
			http.Error(w, "no flush", http.StatusInternalServerError)
			return
		}
		if err := r.Message([]byte("hello"), sse.WithID("1"), sse.WithEvent("greet")); err != nil {
			t.Errorf("Message: %v", err)
		}
		if err := r.Comment("keepalive"); err != nil {
			t.Errorf("Comment: %v", err)
		}
		if err := r.Message([]byte("multi\nline")); err != nil {
			t.Errorf("Message: %v", err)
		}
	})
	srv := httptest.NewServer(handler)
	defer srv.Close()

	res, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	if got, want := res.Header.Get("Content-Type"), "text/event-stream; charset=utf-8"; got != want {
		t.Errorf("Content-Type = %q, want %q", got, want)
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	want := "id: 1\nevent: greet\ndata: hello\n\n: keepalive\n\ndata: multi\ndata: line\n\n"
	if string(body) != want {
		t.Errorf("body = %q\nwant %q", body, want)
	}
}

func TestMessage_WithEventEmpty_suppressesField(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	r, _ := sse.New(rec, req, http.StatusOK)
	if err := r.Message([]byte("x"), sse.WithEvent("")); err != nil {
		t.Fatal(err)
	}
	if got, want := rec.Body.String(), "data: x\n\n"; got != want {
		t.Errorf("body = %q, want %q (WithEvent(\"\") must not emit an event: field)", got, want)
	}
}

func TestMessage_WriteTo_zeroValue(t *testing.T) {
	var m sse.Message
	var buf bytes.Buffer
	if _, err := m.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo on zero-value Message: %v", err)
	}
	if got := m.Data(); got != "" {
		t.Errorf("Data() on zero-value Message = %q, want \"\"", got)
	}
}
