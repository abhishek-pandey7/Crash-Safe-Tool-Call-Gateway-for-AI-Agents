// Command gateway runs the crash-safe MCP tool-call gateway.
//
// Configuration comes from a YAML file (see gateway.example.yaml). The
// environment variables GATEWAY_DATABASE_URL, GATEWAY_LISTEN and
// GATEWAY_INSTANCE_ID override the matching settings.
//
// Besides the MCP endpoint, the gateway serves /-/healthz (the process is
// up) and /-/readyz (Postgres is reachable).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/config"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/gateway"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/killpoint"
	"github.com/abhishek-pandey7/Crash-Safe-Tool-Call-Gateway-for-AI-Agents/internal/store"
)

func main() {
	path := flag.String("config", envOr("GATEWAY_CONFIG", "gateway.yaml"), "path to the config file")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(*path, log); err != nil {
		log.Error("gateway stopped", "err", err)
		os.Exit(1)
	}
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func run(path string, log *slog.Logger) error {
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	cfg.DatabaseURL = envOr("GATEWAY_DATABASE_URL", cfg.DatabaseURL)
	cfg.Listen = envOr("GATEWAY_LISTEN", cfg.Listen)
	cfg.InstanceID = envOr("GATEWAY_INSTANCE_ID", cfg.InstanceID)
	if cfg.DatabaseURL == "" {
		return errors.New("no database: set database_url or GATEWAY_DATABASE_URL")
	}
	if killpoint.Enabled {
		log.Warn("built with kill points; for the crash suite only", "kill_at", os.Getenv("GATEWAY_KILL_AT"))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	openCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	st, err := store.Open(openCtx, cfg.DatabaseURL)
	cancel()
	if err != nil {
		return err
	}
	defer st.Close()

	gw, err := gateway.New(cfg, st, gateway.Options{Instance: cfg.InstanceID, Logger: log})
	if err != nil {
		return err
	}
	go gw.Run(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /-/healthz", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("GET /-/readyz", func(w http.ResponseWriter, r *http.Request) {
		c, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := st.Ping(c); err != nil {
			http.Error(w, "store unavailable", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "ready")
	})
	mux.Handle("/", gw)

	srv := &http.Server{Addr: cfg.Listen, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		// In-flight calls finish and commit before the process exits.
		c, cancel := context.WithTimeout(context.Background(), cfg.UpstreamTimeout+5*time.Second)
		defer cancel()
		_ = srv.Shutdown(c)
	}()

	log.Info("gateway listening", "addr", cfg.Listen, "upstream", cfg.Upstream, "instance", gw.Instance())
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
