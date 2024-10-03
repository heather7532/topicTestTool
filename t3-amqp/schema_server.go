package main

import (
	"log"
	"net/http"
	"t3-amqp/db"
	"t3-amqp/rest"
)

func main() {
	// Load the database configuration
	config, err := db.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Connect to the database
	pool, err := db.ConnectDB(config)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	http.HandleFunc("/health", rest.HealthCheckHandler(pool).ServeHTTP)
	http.HandleFunc("/schema", rest.SchemaEndpointHandler(pool).ServeHTTP)
	http.HandleFunc("/schemas", rest.GetAllSchemasHandler(pool).ServeHTTP)

	// Start the HTTP server
	log.Println("Starting server on localhost:8080")
	if err := http.ListenAndServe("localhost:8080", nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
