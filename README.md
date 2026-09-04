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

Build one with `NewMessage`, writing each key's value with its prefix. `StringPrefix` takes a value you already have; `Prefix` hands back an `io.Writer` for one you want to stream, so a template can render straight into the message:

```go
m := sse.NewMessage(sse.WithEvent("datastar-patch-elements"))
m.StringPrefix("selector ", "#foo")
m.StringPrefix("mode ", "inner")
if err := tmpl.Execute(m.Prefix("elements "), page); err != nil {
	return err
}
return stream.Send(m)
```

Both repeat the prefix on every line of a multi-line value, and both terminate the line the previous one left open, so every value starts on a fresh data line. `Prefix` writers stay usable afterwards — writing to an earlier one resumes its prefix on a new line — and a `Prefix` writer that is never written to contributes nothing, where `StringPrefix` always writes its line.

`StringPrefix` returns nothing to check: writing to a buffer cannot fail, and a prefix containing CR or LF is a mistake in the calling code, reported by `Send` the way an invalid id or event already is.

`*Message` is itself an `io.Writer` for unkeyed data, and the writers from `Prefix` implement `io.StringWriter`.

