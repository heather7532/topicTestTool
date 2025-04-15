package main

import (
	"net"
	"net/http"
	"os"
	"strconv"
	"t3-amqp/cfg"
	"t3-amqp/db"
	"t3-amqp/rest"

	log "github.com/sirupsen/logrus"
)

func main() {
	// Set up the logger
	log.SetFormatter(
		&log.TextFormatter{
			DisableColors: false,
			FullTimestamp: true,
		},
	)
	log.SetLevel(log.DebugLevel)
	log.SetOutput(os.Stdout)

	// Load the database configuration
	log.Info("Loading database configuration")
	config, err := cfg.LoadConfig()
	if err != nil {
		log.Fatal("Failed to load cfg: %v", err)
	}

	// Connect to the database
	log.Info("Connecting to the database")
	pool, err := db.ConnectDB(config)
	if err != nil {
		log.Fatal("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	http.HandleFunc("/health", rest.HealthCheckHandler(pool).ServeHTTP)
	http.HandleFunc("/schema", rest.SchemaEndpointHandler(pool).ServeHTTP)
	http.HandleFunc("/schemas", rest.GetAllSchemasHandler(pool).ServeHTTP)
	http.HandleFunc("/user", rest.UserEndpointHandler(pool).ServeHTTP)
	http.HandleFunc("/user/validate", rest.ValidateUserHandler(pool).ServeHTTP)
	http.HandleFunc("/artifacts/load", rest.LoadArtifactsHandler().ServeHTTP)
	http.HandleFunc("/artifacts/file", rest.ReadFileHandler().ServeHTTP)

	// Start the HTTP server
	address := net.JoinHostPort(config.Server.Host, strconv.Itoa(config.Server.Port))

	// Resolve the address to a net.Addr
	addr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		log.Fatalf("Failed to resolve address: %v", err)
	}
	log.Println("Starting server on ", addr.String())
	if err := http.ListenAndServe(addr.String(), nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
