package sse_test

import (
	"bytes"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"sse"
)

// discardResponseWriter is the smallest possible Flusher+ResponseWriter so
// benchmarks measure the package, not net/http machinery.
type discardResponseWriter struct {
	h http.Header
	n int
}

func (d *discardResponseWriter) Header() http.Header {
	if d.h == nil {
		d.h = http.Header{}
	}
	return d.h
}
func (d *discardResponseWriter) Write(p []byte) (int, error) { d.n += len(p); return len(p), nil }
func (d *discardResponseWriter) WriteHeader(int)             {}
func (d *discardResponseWriter) Flush()                      {}

func newBenchResponse(b *testing.B) *sse.Response {
	b.Helper()
	r, ok := sse.New(&discardResponseWriter{}, &http.Request{Header: http.Header{}}, http.StatusOK)
	if !ok {
		b.Fatal("New failed")
	}
	return r
}

func BenchmarkMessage_small(b *testing.B) {
	r := newBenchResponse(b)
	data := []byte("hello")
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := r.Message(data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMessage_allFields(b *testing.B) {
	r := newBenchResponse(b)
	data := []byte("hello world")
	opts := []sse.MessageOption{sse.WithID("123"), sse.WithEvent("update"), sse.WithRetry(time.Second)}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := r.Message(data, opts...); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMessage_intID(b *testing.B) {
	r := newBenchResponse(b)
	data := []byte("hello")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		if err := r.Message(data, sse.WithIntID(i)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMessage_multiline(b *testing.B) {
	r := newBenchResponse(b)
	data := []byte(strings.Repeat("a line of text\n", 16))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := r.Message(data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMessage_largePayload(b *testing.B) {
	r := newBenchResponse(b)
	data := bytes.Repeat([]byte("x"), 4096)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := r.Message(data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMessage_withCallerBuffer(b *testing.B) {
	r := newBenchResponse(b)
	data := []byte("hello")
	var pool = sync.Pool{New: func() any { return new(bytes.Buffer) }}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		buf := pool.Get().(*bytes.Buffer)
		if err := r.Message(data, sse.WithBytesBuffer(buf)); err != nil {
			b.Fatal(err)
		}
		pool.Put(buf)
	}
}

func BenchmarkMessage_parallel(b *testing.B) {
	r := newBenchResponse(b)
	data := []byte("hello")
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := r.Message(data); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkComment(b *testing.B) {
	r := newBenchResponse(b)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := r.Comment("ping"); err != nil {
			b.Fatal(err)
		}
	}
}