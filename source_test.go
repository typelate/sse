package sse_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/typelate/sse"
)

func newSSEResponse(t *testing.T) (*httptest.ResponseRecorder, *sse.Response) {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	r, ok := sse.New(rec, req, http.StatusOK)
	if !ok {
		t.Fatal("New returned false")
	}
	return rec, r
}

func TestSource_wrongContentType(t *testing.T) {
	response := &http.Response{
		Header: http.Header{"Content-Type": []string{"application/json"}},
		Body:   http.NoBody,
	}
	if _, err := sse.Source(response); err == nil {
		t.Error("Source returned nil error for non-SSE Content-Type")
	}
}

func TestSource(t *testing.T) {
	for _, tc := range []struct {
		name      string
		write     func(*testing.T, *sse.Response)
		wantCount int
		check     func(*testing.T, []*sse.Message)
	}{
		{
			name: "data only",
			write: func(t *testing.T, r *sse.Response) {
				if err := r.Message([]byte("hello")); err != nil {
					t.Fatal(err)
				}
			},
			wantCount: 1,
			check: func(t *testing.T, msgs []*sse.Message) {
				if got := msgs[0].Data(); got != "hello" {
					t.Errorf("Data() = %q, want %q", got, "hello")
				}
			},
		},
		{
			name: "id",
			write: func(t *testing.T, r *sse.Response) {
				if err := r.Message([]byte("x"), sse.WithID("42")); err != nil {
					t.Fatal(err)
				}
			},
			wantCount: 1,
			check: func(t *testing.T, msgs []*sse.Message) {
				if id, ok := msgs[0].ID(); !ok || id != "42" {
					t.Errorf("ID() = (%q, %v), want (\"42\", true)", id, ok)
				}
			},
		},
		{
			name: "event type",
			write: func(t *testing.T, r *sse.Response) {
				if err := r.Message([]byte("x"), sse.WithEvent("greet")); err != nil {
					t.Fatal(err)
				}
			},
			wantCount: 1,
			check: func(t *testing.T, msgs []*sse.Message) {
				if ev, ok := msgs[0].Event(); !ok || ev != "greet" {
					t.Errorf("Event() = (%q, %v), want (\"greet\", true)", ev, ok)
				}
			},
		},
		{
			name: "retry",
			write: func(t *testing.T, r *sse.Response) {
				if err := r.Message([]byte("x"), sse.WithRetry(2*time.Second)); err != nil {
					t.Fatal(err)
				}
			},
			wantCount: 1,
			check: func(t *testing.T, msgs []*sse.Message) {
				if d, ok := msgs[0].Retry(); !ok || d != 2*time.Second {
					t.Errorf("Retry() = (%v, %v), want (2s, true)", d, ok)
				}
			},
		},
		{
			name: "all fields",
			write: func(t *testing.T, r *sse.Response) {
				if err := r.Message([]byte("hello"), sse.WithID("9"), sse.WithEvent("ping"), sse.WithRetry(500*time.Millisecond)); err != nil {
					t.Fatal(err)
				}
			},
			wantCount: 1,
			check: func(t *testing.T, msgs []*sse.Message) {
				m := msgs[0]
				if got := m.Data(); got != "hello" {
					t.Errorf("Data() = %q, want %q", got, "hello")
				}
				if id, ok := m.ID(); !ok || id != "9" {
					t.Errorf("ID() = (%q, %v), want (\"9\", true)", id, ok)
				}
				if ev, ok := m.Event(); !ok || ev != "ping" {
					t.Errorf("Event() = (%q, %v), want (\"ping\", true)", ev, ok)
				}
				if d, ok := m.Retry(); !ok || d != 500*time.Millisecond {
					t.Errorf("Retry() = (%v, %v), want (500ms, true)", d, ok)
				}
			},
		},
		{
			name: "multi-line data",
			write: func(t *testing.T, r *sse.Response) {
				if err := r.Message([]byte("a\nb\nc")); err != nil {
					t.Fatal(err)
				}
			},
			wantCount: 1,
			check: func(t *testing.T, msgs []*sse.Message) {
				if got := msgs[0].Data(); got != "a\nb\nc" {
					t.Errorf("Data() = %q, want %q", got, "a\nb\nc")
				}
			},
		},
		{
			name: "multiple events",
			write: func(t *testing.T, r *sse.Response) {
				for _, d := range []string{"first", "second", "third"} {
					if err := r.Message([]byte(d)); err != nil {
						t.Fatal(err)
					}
				}
			},
			wantCount: 3,
			check: func(t *testing.T, msgs []*sse.Message) {
				for i, want := range []string{"first", "second", "third"} {
					if got := msgs[i].Data(); got != want {
						t.Errorf("msgs[%d].Data() = %q, want %q", i, got, want)
					}
				}
			},
		},
		{
			name: "comments are skipped",
			write: func(t *testing.T, r *sse.Response) {
				if err := r.Comment("ping"); err != nil {
					t.Fatal(err)
				}
				if err := r.Message([]byte("hello")); err != nil {
					t.Fatal(err)
				}
				if err := r.Comment("keepalive"); err != nil {
					t.Fatal(err)
				}
			},
			wantCount: 1,
			check: func(t *testing.T, msgs []*sse.Message) {
				if got := msgs[0].Data(); got != "hello" {
					t.Errorf("Data() = %q, want %q", got, "hello")
				}
			},
		},
		{
			name: "empty id resets client state",
			write: func(t *testing.T, r *sse.Response) {
				if err := r.Message([]byte("x"), sse.WithID("")); err != nil {
					t.Fatal(err)
				}
			},
			wantCount: 1,
			check: func(t *testing.T, msgs []*sse.Message) {
				if id, ok := msgs[0].ID(); !ok || id != "" {
					t.Errorf("ID() = (%q, %v), want (\"\", true)", id, ok)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// rec.Result() has no Request field, so Source does not retry.
			rec, r := newSSEResponse(t)
			tc.write(t, r)

			seq, err := sse.Source(rec.Result())
			if err != nil {
				t.Fatalf("Source: %v", err)
			}
			var msgs []*sse.Message
			for m := range seq {
				msgs = append(msgs, m)
			}
			if len(msgs) != tc.wantCount {
				t.Fatalf("got %d messages, want %d", len(msgs), tc.wantCount)
			}
			tc.check(t, msgs)
		})
	}
}

func TestSource_earlyStop(t *testing.T) {
	rec, r := newSSEResponse(t)
	for range 5 {
		if err := r.Message([]byte("x")); err != nil {
			t.Fatal(err)
		}
	}

	seq, err := sse.Source(rec.Result())
	if err != nil {
		t.Fatalf("Source: %v", err)
	}
	count := 0
	for range seq {
		count++
		if count == 2 {
			break
		}
	}
	if count != 2 {
		t.Errorf("got %d messages before stop, want 2", count)
	}
}

func TestSource_endToEnd(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
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
	}))
	defer srv.Close()

	res, err := http.Get(srv.URL) //nolint:noctx
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	seq, err := sse.Source(res)
	if err != nil {
		t.Fatalf("Source: %v", err)
	}
	var msgs []*sse.Message
	for m := range seq {
		msgs = append(msgs, m)
		if len(msgs) == 2 {
			break // stop after expected count so we don't trigger reconnect
		}
	}

	if len(msgs) != 2 {
		t.Fatalf("got %d messages, want 2", len(msgs))
	}
	if got := msgs[0].Data(); got != "hello" {
		t.Errorf("msgs[0].Data() = %q, want %q", got, "hello")
	}
	if id, ok := msgs[0].ID(); !ok || id != "1" {
		t.Errorf("msgs[0].ID() = (%q, %v), want (\"1\", true)", id, ok)
	}
	if ev, ok := msgs[0].Event(); !ok || ev != "greet" {
		t.Errorf("msgs[0].Event() = (%q, %v), want (\"greet\", true)", ev, ok)
	}
	if got := msgs[1].Data(); got != "multi\nline" {
		t.Errorf("msgs[1].Data() = %q, want %q", got, "multi\nline")
	}
}

// TestSource_oversizedLine verifies that a data: line exceeding bufio's 64 KB
// default token limit causes Source to stop iterating (no reconnect) rather
// than silently discarding events and looping forever.
func TestSource_oversizedLine(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r, _ := sse.New(w, req, http.StatusOK)
		if err := r.Message([]byte("before")); err != nil {
			t.Error(err)
		}
		// Write a data: line exceeding bufio.MaxScanTokenSize (64 KB).
		fmt.Fprintf(w, "data: %s\n\ndata: after\n\n", strings.Repeat("x", 1<<17))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	}))
	defer srv.Close()

	res, err := http.Get(srv.URL) //nolint:noctx
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	seq, err := sse.Source(res)
	if err != nil {
		t.Fatalf("Source: %v", err)
	}
	var msgs []*sse.Message
	for m := range seq {
		msgs = append(msgs, m)
	}
	// "before" arrives before the oversized line; "after" must not appear.
	if len(msgs) != 1 || msgs[0].Data() != "before" {
		t.Errorf("got %d messages (%v), want exactly [before]", len(msgs), func() []string {
			var d []string
			for _, m := range msgs {
				d = append(d, m.Data())
			}
			return d
		}())
	}
}

// TestSource_reconnect verifies that Source reconnects after EOF, sends the
// Last-Event-ID header from the last received event, and yields messages from
// the new connection. The server uses retry:1 to keep the test fast.
func TestSource_reconnect(t *testing.T) {
	var connections atomic.Int32
	var lastEventIDOnReconnect string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch connections.Add(1) {
		case 1:
			r, _ := sse.New(w, req, http.StatusOK)
			// retry:1ms so the test doesn't wait the 3s default
			if err := r.Message([]byte("first"), sse.WithID("42"), sse.WithRetry(time.Millisecond)); err != nil {
				t.Errorf("Message: %v", err)
			}
		case 2:
			lastEventIDOnReconnect = req.Header.Get("Last-Event-Id")
			r, _ := sse.New(w, req, http.StatusOK)
			if err := r.Message([]byte("second")); err != nil {
				t.Errorf("Message: %v", err)
			}
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	seq, err := sse.Source(res, sse.WithClient(srv.Client()))
	if err != nil {
		t.Fatalf("Source: %v", err)
	}
	var msgs []*sse.Message
	for m := range seq {
		msgs = append(msgs, m)
		if len(msgs) == 2 {
			break
		}
	}

	if len(msgs) != 2 {
		t.Fatalf("got %d messages, want 2", len(msgs))
	}
	if got := msgs[0].Data(); got != "first" {
		t.Errorf("msgs[0].Data() = %q, want %q", got, "first")
	}
	if got := msgs[1].Data(); got != "second" {
		t.Errorf("msgs[1].Data() = %q, want %q", got, "second")
	}
	if lastEventIDOnReconnect != "42" {
		t.Errorf("Last-Event-Id on reconnect = %q, want %q", lastEventIDOnReconnect, "42")
	}
}
