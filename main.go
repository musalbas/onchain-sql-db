package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/musalbas/onchain-sql-db/pkg/server"
)

func main() {
	var (
		httpPort        int
		dbPath          string
		celestiaURL     string
		celestiaToken   string
		celestiaNameHex string
	)

	flag.IntVar(&httpPort, "port", 8080, "HTTP server port")
	flag.StringVar(&dbPath, "db", "./onchain.db", "Path to SQLite database")
	flag.StringVar(&celestiaURL, "celestia-url", "http://localhost:26658", "Celestia node URL")
	flag.StringVar(&celestiaToken, "celestia-token", "", "Celestia node auth token")
	flag.StringVar(&celestiaNameHex, "namespace", "DEADBEEF", "Celestia namespace in hex (without 0x prefix)")
	flag.Parse()

	srv, err := server.NewServer(server.Config{
		Port:            httpPort,
		DBPath:          dbPath,
		CelestiaURL:     celestiaURL,
		CelestiaToken:   celestiaToken,
		CelestiaNameHex: celestiaNameHex,
	})
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// Start server in a goroutine
	go func() {
		if err := srv.Start(); err != nil {
			log.Printf("Server error: %v", err)
			os.Exit(1)
		}
	}()

	fmt.Printf("Server started on port %d\n", httpPort)
	fmt.Printf("Using Celestia namespace: %s\n", celestiaNameHex)
	fmt.Printf("Database path: %s\n", dbPath)

	// Wait for interrupt signal to gracefully shutdown the server
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	fmt.Println("Shutting down server...")
	srv.Stop()
}
