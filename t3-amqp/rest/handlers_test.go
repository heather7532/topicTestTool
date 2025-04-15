package rest_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"t3-amqp/cfg"
	"t3-amqp/db"
	"t3-amqp/rest"
	"t3-amqp/types"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
)

func setupTestDB(t *testing.T) *pgxpool.Pool {
	config, err := cfg.LoadConfig()
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
	if err != nil {
		t.Fatalf("Failed to delete user: %v", err)
	}
}

// implement a test for LoadArtifactsHandler
func TestLoadArtifactsHandler(t *testing.T) {
	os.Setenv("T3_ROOT", "/tmp/t3/test")
	root, exists := os.LookupEnv("T3_ROOT")
	if !exists {
		t.Fatalf("T3_ROOT environment variable not set")
	}
	os.MkdirAll(root, 0755)
	_, err := os.Create(root + "/test.txt")
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	os.MkdirAll(root+"/subdir", 0755)
	_, err = os.Create(root + "/subdir/test2.txt")
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	handler := rest.LoadArtifactsHandler()

	req := httptest.NewRequest(http.MethodPost, os.Getenv("T3_ROOT"), nil)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	t.Log("response body: ", rr.Body.String())

	os.RemoveAll(root)
}

// implement a test for ReadFileHandler
func TestReadFileHandler(t *testing.T) {
	root := t.TempDir()
	filePath := root + "/test.txt"
	err := os.WriteFile(filePath, []byte("test content"), 0644)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	handler := rest.ReadFileHandler()

	req := httptest.NewRequest(http.MethodGet, "/file?path="+filePath, nil)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var fileData types.FileData
	err = json.NewDecoder(rr.Body).Decode(&fileData)
	assert.NoError(t, err)
	assert.Equal(t, "test.txt", fileData.Name)
	assert.Equal(t, "text/plain", fileData.MimeType)
	assert.Equal(t, "test content", fileData.Contents)
}

// implement a test for WriteFileHandler
func TestWriteFileHandler(t *testing.T) {
	root := t.TempDir()
	filePath := root + "/test.txt"

	handler := rest.WriteFileHandler()

	fileData := types.FileData{
		Path:     filePath,
		Name:     "test.txt",
		Type:     ".txt",
		MimeType: "text/plain",
		Contents: "test content",
	}

	reqBody, err := json.Marshal(fileData)
	if err != nil {
		t.Fatalf("Failed to marshal request body: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/file/write", bytes.NewBuffer(reqBody))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}
	assert.Equal(t, "test content", string(content))
}

// implement a test for GetAllUsersHandler
func TestGetAllUsersHandler(t *testing.T) {
	pool := setupTestDB(t)
	defer pool.Close()

	handler := rest.GetAllUsersHandler(pool)

	req := httptest.NewRequest(http.MethodGet, "/users", nil)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var users []db.User
	err := json.NewDecoder(rr.Body).Decode(&users)
	assert.NoError(t, err)
	assert.NotEmpty(t, users)
}
