package sse

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestParseSSEStream_dataCapTruncatesEvent(t *testing.T) {
	linePayload := strings.Repeat("x", 1000)
	numLines := maxEventDataSize/len(linePayload) + 10 // clearly over the cap
	var sb strings.Builder
	for range numLines {
		sb.WriteString("data: ")
		sb.WriteString(linePayload)
		sb.WriteByte('\n')
	}
	sb.WriteString("\n") // dispatch

	var buf bytes.Buffer
	var lastEventID string
	delay := 3 * time.Second
	var msgs []*Message
	parseSSEStream(strings.NewReader(sb.String()), &buf, &lastEventID, &delay, func(m *Message) bool {
		msgs = append(msgs, m)
		return true
	})
	if len(msgs) != 1 {
		t.Fatalf("got %d messages, want 1", len(msgs))
	}
	if n := len(msgs[0].Data()); n > maxEventDataSize {
		t.Errorf("Data() len = %d, exceeds cap %d", n, maxEventDataSize)
	}
}

func TestParseSSEStream_scannerErrorReturnsFalse(t *testing.T) {
	// A line exceeding bufio's default 64 KB token limit causes ErrTooLong.
	longLine := "data: " + strings.Repeat("x", 1<<17)
	var buf bytes.Buffer
	var lastEventID string
	delay := 3 * time.Second
	if parseSSEStream(strings.NewReader(longLine), &buf, &lastEventID, &delay, func(*Message) bool { return true }) {
		t.Error("parseSSEStream returned true (keepGoing) on scanner error, want false")
	}
}

func TestParseSSEStream_retryDelayClamp(t *testing.T) {
	for _, tc := range []struct {
		name      string
		wire      string
		wantDelay time.Duration
	}{
		{"zero clamped to minimum", "retry: 0\ndata: x\n\n", minRetryDelay},
		{"normal value unchanged", "retry: 1000\ndata: x\n\n", time.Second},
		{"large value clamped to max", "retry: 9999999999999\ndata: x\n\n", maxRetryDelay},
	} {
		t.Run(tc.name, func(t *testing.T) {
			delay := 3 * time.Second
			var buf bytes.Buffer
			var lastEventID string
			parseSSEStream(strings.NewReader(tc.wire), &buf, &lastEventID, &delay, func(*Message) bool { return true })
			if delay != tc.wantDelay {
				t.Errorf("retryDelay = %v, want %v", delay, tc.wantDelay)
			}
		})
	}
}