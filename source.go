package sse

import _ "embed"

//go:embed message.go
var sourceCode string

func SourceCode() string {
	return sourceCode
}
