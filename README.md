# sse

[![Go Reference](https://pkg.go.dev/badge/github.com/typelate/sse.svg)](https://pkg.go.dev/github.com/typelate/sse)

A small, allocation-light server implementation of the [WHATWG Server-Sent Events](https://html.spec.whatwg.org/multipage/server-sent-events.html) protocol.

`Message`, `Comment`, and `Send` are safe to call from multiple goroutines and the hot path does no heap allocations.

## Keyed data lines

Some formats layered on SSE key each data line and repeat the key across a multi-line value. [Datastar](https://data-star.dev/reference/sse_events) writes an element patch as:

```
event: datastar-patch-elements
data: selector #foo
data: mode inner
data: elements <div>
data: elements   Hello world!
data: elements </div>

```

Build one with `NewMessage` and a writer per key. `Prefix` repeats its argument on every line it writes, so a template can render straight into the stream:

```go
m := sse.NewMessage(sse.WithEvent("datastar-patch-elements"))
io.WriteString(m.Prefix("selector "), "#foo")
io.WriteString(m.Prefix("mode "), "inner")
if err := tmpl.Execute(m.Prefix("elements "), page); err != nil {
	return err
}
return stream.Send(m)
```

Each call to `Prefix` terminates the line the previous writer left open, so every value starts on a fresh data line. Writers stay usable afterwards — writing to an earlier one resumes its prefix on a new line — and a writer that is never written to contributes nothing.

`*Message` is itself an `io.Writer` for unkeyed data.

