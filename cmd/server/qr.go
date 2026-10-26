package main

import (
	"net/http"

	"github.com/sparshgupta001/scrapheap/internal/dashhttp"
)

// RegisterQR registers the QR code and join URL endpoints on the given ServeMux.
func RegisterQR(mux *http.ServeMux, port string) {
	dashhttp.RegisterQR(mux, port)
}
