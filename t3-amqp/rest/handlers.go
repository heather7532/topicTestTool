package rest

import (
	"encoding/json"
	"fmt"
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

func UserEndpointHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Define the HTTP handlers
		switch r.Method {
		case http.MethodGet:
			GetAllUsersHandler(pool).ServeHTTP(w, r)
		case http.MethodPost:
			PostUserHandler(pool).ServeHTTP(w, r)
		//case http.MethodPut: PutUserHandler(pool).ServeHTTP(w, r)
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

// Add a new handler function that will do a POST request to the /user endpoint
// The handler should accept a UserRequest struct and return a JSON response with a 200 status code
func PostUserHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req UserRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		params := db.User{
			Email:    req.Email,
			Username: req.Username,
			Password: req.Password,
		}

		err := db.AddUser(pool, params.Email, params.Username, params.Password)
		if err != nil {
			http.Error(w, "failed to insert user", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		response := "Created user"

		w.Header().Set("Content-Type", "application/json")
		err = json.NewEncoder(w).Encode(response)
		if err != nil {
			return
		}
	}
}

// Add a new handler function that will do a GET request to the /user endpoint and return all the users in the database
// The handler should return a JSON response with a 200 status code
func GetAllUsersHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		users, err := db.GetAllUsers(pool)
		if err != nil {
			http.Error(w, "failed to retrieve users", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		err = json.NewEncoder(w).Encode(users)
		if err != nil {
			return
		}
	}
}

// Validate the user request using the email address
// The handler should return a JSON response with a 200 status code if the email is valid and a 404 if not valid
func ValidateUserHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req UserRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			fmt.Println("ValidateUserHandler: Decode", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if req.Email == "" {
			http.Error(w, "email is required", http.StatusBadRequest)
			return
		}

		valid, err := db.ValidateUserByEmail(pool, req.Email, req.Password)
		if err != nil {
			fmt.Println("ValidateUserHandler: ", "Not Found ", req.Email)
			http.Error(w, "Access Denied", http.StatusNotFound)
			return
		}
		// if valid is true, return a 200 status code otherwise return a 404 status code
		if valid {
			fmt.Println("ValidateUserHandler: ", "Access Validated", req.Email)
			w.WriteHeader(http.StatusOK)
		} else {
			fmt.Println("ValidateUserHandler: ", "Access Denied", req.Email)
			http.Error(w, "Access Denied", http.StatusNotFound)
		}

		w.WriteHeader(http.StatusOK)
	}
}
