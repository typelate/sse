package sse_test

import (
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	"net/http/httptest"

	"github.com/typelate/sse"
)

func Example() {
	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		stream, ok := sse.New(w, req, http.StatusOK)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		_ = stream.Message([]byte("first"), sse.WithID("1"), sse.WithEvent("greet"))
		_ = stream.Comment("keep-alive")
		_ = stream.Message([]byte("multi\nline"))
	})
	srv := httptest.NewServer(handler)
	defer srv.Close()

	res, err := http.Get(srv.URL)
	if err != nil {
		log.Fatal(err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s", body)

	// Output:
	// id: 1
	// event: greet
	// data: first
	//
	// : keep-alive
	//
	// data: multi
	// data: line
	//
}

// ExampleMessage_Prefix builds a Datastar element patch. Datastar keys each
// data line, so every value is written through its own prefixed writer and a
// multi-line value repeats its key — which lets a template render straight
// into the stream.
func ExampleMessage_Prefix() {
	tmpl := template.Must(template.New("greeting").Parse("<div>\n  {{.}}\n</div>"))

	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		stream, ok := sse.New(w, req, http.StatusOK)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}

		m := sse.NewMessage(sse.WithEvent("datastar-patch-elements"))
		m.StringPrefix("selector ", "#foo")
		m.StringPrefix("mode ", "inner")
		if err := tmpl.Execute(m.Prefix("elements "), "Hello world!"); err != nil {
			log.Print(err)
			return
		}
		if err := stream.Send(m); err != nil {
			log.Print(err)
		}
	})
	srv := httptest.NewServer(handler)
	defer srv.Close()

	res, err := http.Get(srv.URL)
	if err != nil {
		log.Fatal(err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s", body)

	// Output:
	// event: datastar-patch-elements
	// data: selector #foo
	// data: mode inner
	// data: elements <div>
	// data: elements   Hello world!
	// data: elements </div>
	//
}
