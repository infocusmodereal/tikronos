package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:8000", "HTTP listen address")
	flag.Parse()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("POST /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "DynamoDB operations are not implemented yet", http.StatusNotImplemented)
	})

	server := &http.Server{
		Addr: *listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second,
	}
	log.Fatal(server.ListenAndServe())
}
