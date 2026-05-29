package sse_test

import (
	"net/http"
	"net/http/httptest"
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

	res, err := http.Get(srv.URL)
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