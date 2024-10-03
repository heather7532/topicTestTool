package rest

import (
	"encoding/json"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/http"
	"t3-amqp/db"
)

// imlement a health check handler that will verify the datbase is avalable
// it will return a 200 status code if the database is available and a 500 status code if it is not
func HealthCheckHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		err := pool.Ping(r.Context())
		if err != nil {
			http.Error(w, "database not available", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	}
}

func SchemaEndpointHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Define the HTTP handlers
		switch r.Method {
		case http.MethodGet:
			GetSchemaFilterParamsHandler(pool).ServeHTTP(w, r)
		case http.MethodPost:
			PostSchemaHandler(pool).ServeHTTP(w, r)
		case http.MethodPut:
			UpdateSchemaHandler(pool).ServeHTTP(w, r)
		default:

			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

func PostSchemaHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req SchemaRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		params := db.QueryArgs{
			Name:       req.Name,
			Type:       req.Type,
			Version:    req.Version,
			SchemaData: req.SchemaData,
		}

		id, err := db.InsertSchema(pool, params)
		if err != nil {
			http.Error(w, "failed to insert schema", http.StatusInternalServerError)
			return
		}

		response := map[string]int64{"id": int64(id)}
		w.Header().Set("Content-Type", "application/json")
		err = json.NewEncoder(w).Encode(response)
		if err != nil {
			return
		}
	}
}

func UpdateSchemaHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req SchemaRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		params := db.QueryArgs{
			Name:       req.Name,
			Type:       req.Type,
			Version:    req.Version,
			SchemaData: req.SchemaData,
		}

		dbResponse, err := db.UpdateSchema(pool, params)
		if err != nil {
			if err.Error() == "schema not found" {
				http.Error(w, "schema not found", http.StatusNotFound)
			} else {
				http.Error(w, "failed to update schema", http.StatusInternalServerError)
			}
			return
		}

		response := dbResponse
		w.Header().Set("Content-Type", "application/json")
		err = json.NewEncoder(w).Encode(response)
		if err != nil {
			return
		}
	}
}

func GetAllSchemasHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		schemas, err := db.GetAllSchemas(pool)
		if err != nil {
			http.Error(w, "failed to retrieve schemas", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		err = json.NewEncoder(w).Encode(schemas)
		if err != nil {
			return
		}
	}
}

func GetSchemaFilterParamsHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Query().Get("name")
		typeStr := r.URL.Query().Get("type")
		versionStr := r.URL.Query().Get("version")

		var err error

		args := db.QueryArgs{
			Name:    name,
			Type:    typeStr,
			Version: versionStr,
		}

		schema, err := db.GetSchemaFilterParams(pool, args)
		if err != nil {
			http.Error(w, "schema not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		err = json.NewEncoder(w).Encode(schema)
		if err != nil {
			return
		}
	}
}
