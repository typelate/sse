package sse_test

import (
	"fmt"
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
