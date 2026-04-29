# sse

[![Go Reference](https://pkg.go.dev/badge/github.com/typelate/sse.svg)](https://pkg.go.dev/github.com/typelate/sse)

A small, allocation-free server implementation of the [WHATWG Server-Sent Events](https://html.spec.whatwg.org/multipage/server-sent-events.html) protocol.

`Message` and `Comment` are safe to call from multiple goroutines and the hot path does no heap allocations.