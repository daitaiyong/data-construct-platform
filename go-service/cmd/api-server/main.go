package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/daitaiyong/data-construct-platform/go-service/api"
	"github.com/daitaiyong/data-construct-platform/go-service/internal/job"
	"github.com/daitaiyong/data-construct-platform/go-service/internal/template"
	"github.com/daitaiyong/data-construct-platform/go-service/internal/templateengine"
)

func main() {
	addr := envOr("API_ADDR", ":8080")
	engine, err := templateengine.Dial(envOr("TEMPLATE_ENGINE_ADDR", "localhost:50051"))
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()
	server := &http.Server{
		Addr:              addr,
		Handler:           api.NewHandler(template.NewMemoryRepository(), job.NewMemoryRepository(), engine).Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown server: %v", err)
		}
	}()

	log.Printf("API server listening on %s", addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
