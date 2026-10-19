package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/daitaiyong/data-construct-platform/go-service/internal/templateengine"
)

func main() {
	engine, err := templateengine.Dial(envOr("TEMPLATE_ENGINE_ADDR", "localhost:50051"))
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()
	log.Println("worker started with Python template-engine gRPC client")
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	<-signals
	log.Println("worker stopped")
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
