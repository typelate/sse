package sse

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

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