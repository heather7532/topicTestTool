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
		t.Fatalf("Failed to load cfg: %v", err)
	}

	pool, err := db.ConnectDB(config)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}

	return pool
}

// Cleanup schema entry for any schema name starting with test_ prefix
func cleanupTestSchema(t *testing.T) {
	pool := setupTestDB(t)
	defer pool.Close()

	// delete using the db function
	err := db.DeleteSchemaByName(pool, "test\\_%")
	assert.NoError(t, err)
}

func createSchemaEntry(t *testing.T) (int64, error) {
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

	return response["id"], err
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
	defer cleanupTestSchema(t)

	handler := rest.SchemaEndpointHandler(pool)

	reqBody := `{"name":"test_schema","type":"json","version":"1.0.1","schemaData":"{\"type\": \"object\", \"properties\": {\"example\": {\"type\": \"string\"}}}"}`

	req := httptest.NewRequest(http.MethodPost, "/schema", bytes.NewBufferString(reqBody))
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
	defer cleanupTestSchema(t)

	_, err := createSchemaEntry(t)

	handler := rest.GetAllSchemasHandler(pool)

	req := httptest.NewRequest(http.MethodGet, "/schemas", nil)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var schemas []db.QueryArgs
	err = json.NewDecoder(rr.Body).Decode(&schemas)
	assert.NoError(t, err)
	assert.NotEmpty(t, schemas)
}

func TestGetSchemaByNameHandler(t *testing.T) {
	pool := setupTestDB(t)
	defer pool.Close()
	defer cleanupTestSchema(t)

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

	var retrievedSchema []db.Schema
	err = json.NewDecoder(rr.Body).Decode(&retrievedSchema)
	assert.NoError(t, err)
	assert.Equal(t, len(retrievedSchema), 1)
	assert.Equal(t, schema.Name, retrievedSchema[0].Name)
	assert.Equal(t, schema.Type, retrievedSchema[0].Type)
	assert.Equal(t, schema.Version, retrievedSchema[0].Version)
	assert.Equal(t, schema.SchemaData, retrievedSchema[0].SchemaData)

}

// Implement a test to add a new user
func TestCreateUserAndValidateUser(t *testing.T) {
	pool := setupTestDB(t)
	defer pool.Close()

	handler := rest.PostUserHandler(pool)

	reqBody := `{"username":"test_user","password":"password","email":"test@myemail.com"}`
	req := httptest.NewRequest(http.MethodPost, "/user", bytes.NewBufferString(reqBody))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response string
	err := json.NewDecoder(rr.Body).Decode(&response)
	assert.NoError(t, err)
	assert.Equal(t, response, "Created user")

	// Test user validation
	validationHandler := rest.ValidateUserHandler(pool)

	reqBody2 := `{"email":"test@myemail.com","password":"password"}`
	req2 := httptest.NewRequest(http.MethodPost, "/user/validate", bytes.NewBufferString(reqBody2))

	req2.Header.Set("Content-Type", "application/json")
	rr2 := httptest.NewRecorder()

	validationHandler.ServeHTTP(rr2, req2)

	assert.Equal(t, http.StatusOK, rr2.Code)

	// call the db function to delete the user
	db.DeleteUser(pool, "test_user")

}
