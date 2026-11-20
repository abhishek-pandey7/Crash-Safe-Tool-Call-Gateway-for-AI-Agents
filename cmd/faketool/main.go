// Command faketool runs the fake MCP tool server from the end-to-end tests
// on its own, for the Docker Compose demo.
//
// It serves the MCP endpoint at /mcp and the test controls: GET /ledger
// (every effect that really happened), DELETE /ledger, POST /faults and
// GET /requests. Never point real agents at it.
package main

import (
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/faketool"
)

func main() {
	addr := ":8080"
	if v := os.Getenv("FAKETOOL_LISTEN"); v != "" {
		addr = v
	}
	flag.StringVar(&addr, "listen", addr, "address to listen on")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	srv := &http.Server{Addr: addr, Handler: faketool.New().Handler(), ReadHeaderTimeout: 10 * time.Second}
	log.Info("fake tool server listening", "addr", addr)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		log.Error("fake tool server stopped", "err", err)
		os.Exit(1)
	}
}
