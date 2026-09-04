package sse_test

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
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

func TestNewMessage(t *testing.T) {
	for _, tc := range []struct {
		name  string
		opts  []sse.MessageOption
		write func(t *testing.T, m *sse.Message)
		want  string
	}{
		{
			name: "nothing written still dispatches",
			want: "data: \n\n",
		},
		{
			name:  "Message is an io.Writer",
			write: func(t *testing.T, m *sse.Message) { mustWriteString(t, m, "hello") },
			want:  "data: hello\n\n",
		},
		{
			name: "successive writes append to one data line",
			write: func(t *testing.T, m *sse.Message) {
				mustWriteString(t, m, "he")
				mustWriteString(t, m, "llo")
			},
			want: "data: hello\n\n",
		},
		{
			name:  "newline in written data starts a new data line",
			write: func(t *testing.T, m *sse.Message) { mustWriteString(t, m, "a\nb") },
			want:  "data: a\ndata: b\n\n",
		},
		{
			name: "newline split across writes starts a new data line",
			write: func(t *testing.T, m *sse.Message) {
				mustWriteString(t, m, "a\n")
				mustWriteString(t, m, "b")
			},
			want: "data: a\ndata: b\n\n",
		},
		{
			name:  "trailing newline is trimmed",
			write: func(t *testing.T, m *sse.Message) { mustWriteString(t, m, "hello\n") },
			want:  "data: hello\n\n",
		},
		{
			name:  "CRLF starts a new data line",
			write: func(t *testing.T, m *sse.Message) { mustWriteString(t, m, "a\r\nb") },
			want:  "data: a\ndata: b\n\n",
		},
		{
			name:  "bare CR starts a new data line",
			write: func(t *testing.T, m *sse.Message) { mustWriteString(t, m, "a\rb") },
			want:  "data: a\ndata: b\n\n",
		},
		{
			name: "CRLF split across writes counts as one line break",
			write: func(t *testing.T, m *sse.Message) {
				mustWriteString(t, m, "a\r")
				mustWriteString(t, m, "\nb")
			},
			want: "data: a\ndata: b\n\n",
		},
		{
			name:  "options configure the message",
			opts:  []sse.MessageOption{sse.WithID("1"), sse.WithEvent("greet")},
			write: func(t *testing.T, m *sse.Message) { mustWriteString(t, m, "hello") },
			want:  "id: 1\nevent: greet\ndata: hello\n\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			r, ok := sse.New(rec, req, http.StatusOK)
			if !ok {
				t.Fatal("New failed")
			}

			m := sse.NewMessage(tc.opts...)
			if tc.write != nil {
				tc.write(t, m)
			}
			if err := r.Send(m); err != nil {
				t.Fatalf("Send: %v", err)
			}

			if got := rec.Body.String(); got != tc.want {
				t.Errorf("body = %q, want %q", got, tc.want)
			}
			if !rec.Flushed {
				t.Error("Send did not flush the response")
			}
		})
	}
}

func TestMessage_Write_reportsBytesConsumed(t *testing.T) {
	m := sse.NewMessage()
	data := []byte("a\nb\nc")

	n, err := m.Write(data)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n != len(data) {
		t.Errorf("Write(%q) = %d, want %d (io.Writer must report bytes consumed from p)", data, n, len(data))
	}
	if got, want := m.Data(), "a\nb\nc"; got != want {
		t.Errorf("Data() = %q, want %q", got, want)
	}
}

func TestMessage_Prefix(t *testing.T) {
	for _, tc := range []struct {
		name  string
		opts  []sse.MessageOption
		write func(t *testing.T, m *sse.Message)
		want  string
	}{
		{
			name:  "prefix repeats on every line of the value",
			write: func(t *testing.T, m *sse.Message) { mustWriteString(t, m.Prefix("elements "), "<div>\n  hi\n</div>") },
			want:  "data: elements <div>\ndata: elements   hi\ndata: elements </div>\n\n",
		},
		{
			name: "each prefix gets its own data line",
			opts: []sse.MessageOption{sse.WithEvent("datastar-patch-elements")},
			write: func(t *testing.T, m *sse.Message) {
				mustWriteString(t, m.Prefix("selector "), "#foo")
				mustWriteString(t, m.Prefix("mode "), "inner")
				mustWriteString(t, m.Prefix("elements "), "<div>hi</div>")
			},
			want: "event: datastar-patch-elements\ndata: selector #foo\ndata: mode inner\ndata: elements <div>hi</div>\n\n",
		},
		{
			name: "a new writer terminates the partial line left by the previous one",
			write: func(t *testing.T, m *sse.Message) {
				mustWriteString(t, m.Prefix("a "), "x")
				mustWriteString(t, m.Prefix("b "), "y")
			},
			want: "data: a x\ndata: b y\n\n",
		},
		{
			name: "a writer that is never written to contributes nothing",
			write: func(t *testing.T, m *sse.Message) {
				m.Prefix("unused ")
				mustWriteString(t, m.Prefix("b "), "y")
			},
			want: "data: b y\n\n",
		},
		{
			name: "writing to an earlier writer again resumes its prefix on a new line",
			write: func(t *testing.T, m *sse.Message) {
				first, second := m.Prefix("a "), m.Prefix("b ")
				mustWriteString(t, first, "x")
				mustWriteString(t, second, "y")
				mustWriteString(t, first, "z")
			},
			want: "data: a x\ndata: b y\ndata: a z\n\n",
		},
		{
			name:  "an empty line still carries the prefix",
			write: func(t *testing.T, m *sse.Message) { mustWriteString(t, m.Prefix("e "), "a\n\nb") },
			want:  "data: e a\ndata: e \ndata: e b\n\n",
		},
		{
			name: "successive writes to one writer continue the same line",
			write: func(t *testing.T, m *sse.Message) {
				w := m.Prefix("e ")
				mustWriteString(t, w, "he")
				mustWriteString(t, w, "llo")
			},
			want: "data: e hello\n\n",
		},
		{
			name: "an unprefixed write after a prefixed one starts a new line",
			write: func(t *testing.T, m *sse.Message) {
				mustWriteString(t, m.Prefix("a "), "x")
				mustWriteString(t, m, "y")
			},
			want: "data: a x\ndata: y\n\n",
		},
		{
			name:  "an empty prefix behaves like a plain write",
			write: func(t *testing.T, m *sse.Message) { mustWriteString(t, m.Prefix(""), "a\nb") },
			want:  "data: a\ndata: b\n\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			r, ok := sse.New(rec, req, http.StatusOK)
			if !ok {
				t.Fatal("New failed")
			}

			m := sse.NewMessage(tc.opts...)
			tc.write(t, m)
			if err := r.Send(m); err != nil {
				t.Fatalf("Send: %v", err)
			}

			if got := rec.Body.String(); got != tc.want {
				t.Errorf("body = %q, want %q", got, tc.want)
			}
		})
	}
}

// A prefixed value carries its prefix on every line, so the writer has to
// recognise CR line breaks itself. Left to WriteTo's normalization they would
// split a line open after the prefixes were already inlined, emitting a bare
// "data:" line in the middle of a value.
func TestMessage_Prefix_carriageReturns(t *testing.T) {
	for _, tc := range []struct {
		name  string
		write func(t *testing.T, w io.Writer)
		want  string
	}{
		{
			name:  "CRLF starts a new prefixed line",
			write: func(t *testing.T, w io.Writer) { mustWriteString(t, w, "a\r\nb") },
			want:  "data: e a\ndata: e b\n\n",
		},
		{
			name:  "bare CR starts a new prefixed line",
			write: func(t *testing.T, w io.Writer) { mustWriteString(t, w, "a\rb") },
			want:  "data: e a\ndata: e b\n\n",
		},
		{
			name: "CRLF split across writes counts as one line break",
			write: func(t *testing.T, w io.Writer) {
				mustWriteString(t, w, "a\r")
				mustWriteString(t, w, "\nb")
			},
			want: "data: e a\ndata: e b\n\n",
		},
		{
			name: "bare CR ending a write starts a new line",
			write: func(t *testing.T, w io.Writer) {
				mustWriteString(t, w, "a\r")
				mustWriteString(t, w, "b")
			},
			want: "data: e a\ndata: e b\n\n",
		},
		{
			name:  "trailing CR is trimmed like a trailing newline",
			write: func(t *testing.T, w io.Writer) { mustWriteString(t, w, "a\r") },
			want:  "data: e a\n\n",
		},
		{
			name:  "CR before an empty line keeps the prefix on both",
			write: func(t *testing.T, w io.Writer) { mustWriteString(t, w, "a\r\n\r\nb") },
			want:  "data: e a\ndata: e \ndata: e b\n\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			r, ok := sse.New(rec, req, http.StatusOK)
			if !ok {
				t.Fatal("New failed")
			}

			m := sse.NewMessage()
			tc.write(t, m.Prefix("e "))
			if err := r.Send(m); err != nil {
				t.Fatalf("Send: %v", err)
			}

			if got := rec.Body.String(); got != tc.want {
				t.Errorf("body = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestMessage_Prefix_invalid(t *testing.T) {
	for _, prefix := range []string{"a\nb", "a\rb", "\n", "\r"} {
		t.Run(strconv.Quote(prefix), func(t *testing.T) {
			m := sse.NewMessage()
			mustWriteString(t, m.Prefix("ok "), "kept")

			n, err := m.Prefix(prefix).Write([]byte("dropped"))
			if !errors.Is(err, sse.ErrInvalidField) {
				t.Errorf("Prefix(%q).Write() error = %v, want errors.Is(_, ErrInvalidField)", prefix, err)
			}
			if n != 0 {
				t.Errorf("Prefix(%q).Write() = %d, want 0", prefix, n)
			}

			n, err = m.Prefix(prefix).(io.StringWriter).WriteString("dropped")
			if !errors.Is(err, sse.ErrInvalidField) {
				t.Errorf("Prefix(%q).WriteString() error = %v, want errors.Is(_, ErrInvalidField)", prefix, err)
			}
			if n != 0 {
				t.Errorf("Prefix(%q).WriteString() = %d, want 0", prefix, n)
			}
			if got, want := m.Data(), "ok kept"; got != want {
				t.Errorf("Data() = %q, want %q (a rejected prefix must not disturb the message)", got, want)
			}
		})
	}
}

func TestMessage_StringPrefix(t *testing.T) {
	for _, tc := range []struct {
		name  string
		opts  []sse.MessageOption
		write func(t *testing.T, m *sse.Message)
		want  string
	}{
		{
			name:  "writes a keyed data line",
			write: func(t *testing.T, m *sse.Message) { m.StringPrefix("mode ", "inner") },
			want:  "data: mode inner\n\n",
		},
		{
			name: "each call is its own data line",
			opts: []sse.MessageOption{sse.WithEvent("datastar-patch-elements")},
			write: func(t *testing.T, m *sse.Message) {
				m.StringPrefix("selector ", "#foo")
				m.StringPrefix("mode ", "inner")
			},
			want: "event: datastar-patch-elements\ndata: selector #foo\ndata: mode inner\n\n",
		},
		{
			name:  "a multi-line value repeats the prefix",
			write: func(t *testing.T, m *sse.Message) { m.StringPrefix("e ", "<div>\n  hi\n</div>") },
			want:  "data: e <div>\ndata: e   hi\ndata: e </div>\n\n",
		},
		{
			name:  "CRLF and bare CR are line breaks",
			write: func(t *testing.T, m *sse.Message) { m.StringPrefix("e ", "a\r\nb\rc") },
			want:  "data: e a\ndata: e b\ndata: e c\n\n",
		},
		{
			name:  "an empty value still writes its line",
			write: func(t *testing.T, m *sse.Message) { m.StringPrefix("onlyIfMissing ", "") },
			want:  "data: onlyIfMissing \n\n",
		},
		{
			name: "it terminates a line left open by a Prefix writer",
			write: func(t *testing.T, m *sse.Message) {
				mustWriteString(t, m.Prefix("a "), "x")
				m.StringPrefix("b ", "y")
			},
			want: "data: a x\ndata: b y\n\n",
		},
		{
			name: "a Prefix writer resumes after it",
			write: func(t *testing.T, m *sse.Message) {
				w := m.Prefix("a ")
				mustWriteString(t, w, "x")
				m.StringPrefix("b ", "y")
				mustWriteString(t, w, "z")
			},
			want: "data: a x\ndata: b y\ndata: a z\n\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			r, ok := sse.New(rec, req, http.StatusOK)
			if !ok {
				t.Fatal("New failed")
			}

			m := sse.NewMessage(tc.opts...)
			tc.write(t, m)
			if err := r.Send(m); err != nil {
				t.Fatalf("Send: %v", err)
			}

			if got := rec.Body.String(); got != tc.want {
				t.Errorf("body = %q, want %q", got, tc.want)
			}
		})
	}
}

// StringPrefix has no error to return, so an unusable prefix has to surface
// at Send, the way an invalid id or event already does.
func TestMessage_StringPrefix_invalidPrefixReportedBySend(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	r, _ := sse.New(rec, req, http.StatusOK)

	m := sse.NewMessage()
	m.StringPrefix("ok ", "kept")
	m.StringPrefix("bad\n", "dropped")

	if err := r.Send(m); !errors.Is(err, sse.ErrInvalidField) {
		t.Errorf("Send() error = %v, want errors.Is(_, ErrInvalidField)", err)
	}
	if rec.Body.Len() != 0 {
		t.Errorf("body should stay empty when validation fails, got %q", rec.Body.String())
	}
}

// A rejected Prefix writer records the failure too, so both paths report an
// unusable prefix the same way rather than one failing loudly and the other
// only when its Write error is checked.
func TestMessage_Prefix_invalidPrefixReportedBySend(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	r, _ := sse.New(rec, req, http.StatusOK)

	m := sse.NewMessage()
	mustWriteString(t, m.Prefix("ok "), "kept")
	_, _ = m.Prefix("bad\n").Write([]byte("dropped"))

	if err := r.Send(m); !errors.Is(err, sse.ErrInvalidField) {
		t.Errorf("Send() error = %v, want errors.Is(_, ErrInvalidField)", err)
	}
}

// The point of StringPrefix is to avoid the two allocations the io.Writer
// form costs: boxing dataWriter into an interface, and io.WriteString copying
// the string because dataWriter had no WriteString method.
func TestMessage_StringPrefix_doesNotAllocate(t *testing.T) {
	m := sse.NewMessage()
	m.StringPrefix("grow ", strings.Repeat("x", 64*1024)) // pre-grow the buffer

	if got := testing.AllocsPerRun(100, func() {
		m.StringPrefix("mode ", "inner")
	}); got != 0 {
		t.Errorf("StringPrefix allocated %v times per call, want 0", got)
	}
}

func TestMessage_Prefix_writerImplementsStringWriter(t *testing.T) {
	m := sse.NewMessage()

	w := m.Prefix("e ")
	sw, ok := w.(io.StringWriter)
	if !ok {
		t.Fatalf("Prefix returned %T, which does not implement io.StringWriter; io.WriteString would copy the string into a []byte", w)
	}
	if _, err := sw.WriteString("a\nb"); err != nil {
		t.Fatalf("WriteString: %v", err)
	}
	if got, want := m.Data(), "e a\ne b"; got != want {
		t.Errorf("Data() = %q, want %q", got, want)
	}
}

func TestMessage_Prefix_reportsBytesConsumed(t *testing.T) {
	m := sse.NewMessage()
	data := []byte("a\nb")

	n, err := m.Prefix("e ").Write(data)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n != len(data) {
		t.Errorf("Write(%q) = %d, want %d (io.Writer must report bytes consumed from p, not bytes buffered)", data, n, len(data))
	}
}

func mustWriteString(t *testing.T, w io.Writer, s string) {
	t.Helper()
	if _, err := io.WriteString(w, s); err != nil {
		t.Fatalf("WriteString(%q): %v", s, err)
	}
}
