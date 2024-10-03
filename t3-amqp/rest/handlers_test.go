package rest_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"t3-amqp/db"
	"t3-amqp/rest"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
)

func setupTestDB(t *testing.T) *pgxpool.Pool {
	config, err := db.LoadConfig()
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	pool, err := db.ConnectDB(config)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}

	return pool
}

// implement a test for the HealthCheckHandler function
func TestHealthCheckHandler(t *testing.T) {
	pool := setupTestDB(t)
	defer pool.Close()

	handler := rest.HealthCheckHandler(pool)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
}

func TestCreateSchemaHandler(t *testing.T) {
	pool := setupTestDB(t)
	defer pool.Close()

	handler := rest.PostSchemaHandler(pool)

	reqBody := `{"name":"test_schema","type":"json","version":"1.0.1","schemaData":"{\"type\": \"object\", \"properties\": {\"example\": {\"type\": \"string\"}}}"}`

	req := httptest.NewRequest(http.MethodPost, "/schemas", bytes.NewBufferString(reqBody))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response map[string]int64
	err := json.NewDecoder(rr.Body).Decode(&response)
	assert.NoError(t, err)
	assert.NotZero(t, response["id"])
}

func TestGetSchemasHandler(t *testing.T) {
	pool := setupTestDB(t)
	defer pool.Close()

	handler := rest.GetAllSchemasHandler(pool)

	req := httptest.NewRequest(http.MethodGet, "/schemas", nil)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var schemas []db.QueryArgs
	err := json.NewDecoder(rr.Body).Decode(&schemas)
	assert.NoError(t, err)
	assert.NotEmpty(t, schemas)
}

func TestGetSchemaByNameHandler(t *testing.T) {
	pool := setupTestDB(t)
	defer pool.Close()

	handler := rest.GetSchemaFilterParamsHandler(pool)

	// Insert a schema for testing
	schema := db.QueryArgs{
		Name:       "test_schema",
		Type:       "json",
		Version:    "1.0.1",
		SchemaData: `{"type": "object", "properties": {"example": {"type": "string"}}}`,
	}
	_, err := db.InsertSchema(pool, schema)
	assert.NoError(t, err)

	req := httptest.NewRequest(
		http.MethodGet, "/schema?name=test_schema&type=json&version=1.0.1", nil,
	)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var retrievedSchema db.QueryArgs
	err = json.NewDecoder(rr.Body).Decode(&retrievedSchema)
	assert.NoError(t, err)
	assert.Equal(t, schema.Name, retrievedSchema.Name)
	assert.Equal(t, schema.Type, retrievedSchema.Type)
	assert.Equal(t, schema.Version, retrievedSchema.Version)
	assert.Equal(t, schema.SchemaData, retrievedSchema.SchemaData)
}
