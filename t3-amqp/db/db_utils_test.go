package db

import (
	"context"
	"fmt"
	"t3-amqp/cfg"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
)

func setupTestDB(t *testing.T) *pgxpool.Pool {
	config, err := cfg.LoadConfig()
	if err != nil {
		t.Fatalf("Failed to load cfg: %v", err)
	}

	pool, err := ConnectDB(config)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}

	// Clear the table before each test
	_, err = pool.Exec(
		context.Background(),
		`DELETE FROM s1.schema WHERE name LIKE 'test_%'`,
	)
	if err != nil {
		t.Fatalf("Failed to clear table: %v", err)
	}

	return pool
}

func TestInsertSchema(t *testing.T) {
	pool := setupTestDB(t)
	defer pool.Close()

	// Insert a schema for testing
	newSchema := QueryArgs{
		Name:       "test_schema",
		Type:       "json",
		Version:    "1.0.1",
		SchemaData: `{"type": "object", "properties": {"example": {"type": "string"}}}`,
	}

	id, err := InsertSchema(pool, newSchema)
	assert.NoError(t, err, "InsertSchema should not return an error")

	insertedSchema, err := GetSchemaById(pool, id)
	assert.NoError(t, err, "Inserted schema should be retrievable from the database")

	assert.Equal(t, newSchema.Name, insertedSchema.Name, "Inserted schema name should match")
	assert.Equal(t, newSchema.Type, insertedSchema.Type, "Inserted schema type should match")
	assert.Equal(
		t, newSchema.Version, insertedSchema.Version, "Inserted schema version should match",
	)
	assert.Equal(
		t, newSchema.SchemaData, insertedSchema.SchemaData, "Inserted schema data should match",
	)
}

func TestGetSchema(t *testing.T) {
	pool := setupTestDB(t)
	defer pool.Close()

	// Insert a schema for testing
	newSchema := QueryArgs{
		Name:       "test_schema",
		Type:       "json",
		Version:    "1.0.1",
		SchemaData: `{"type": "object", "properties": {"example": {"type": "string"}}}`,
	}

	id, err := InsertSchema(pool, newSchema)
	assert.NoError(t, err)
	fmt.Printf("inserted id=%d\n", id)

	retrievedSchema, err := GetSchemaFilterParams(
		pool, QueryArgs{Name: "test_schema", Type: "json", Version: "1.0.1"},
	)
	assert.NoError(t, err, "GetSchemaFilterParams should not return an error")
	assert.NotNil(t, retrievedSchema, "GetSchemaFilterParams should return a valid schema")
	assert.Equal(t, newSchema.Name, retrievedSchema[0].Name, "Schema name should match")
	assert.Equal(t, newSchema.Type, retrievedSchema[0].Type, "Schema type should match")
	assert.Equal(t, newSchema.Version, retrievedSchema[0].Version, "Schema version should match")
	assert.Equal(t, newSchema.SchemaData, retrievedSchema[0].SchemaData, "Schema data should match")
}

func SchemaNotFoundById(t *testing.T) {
	pool := setupTestDB(t)
	defer pool.Close()

	_, err := GetSchemaById(pool, 9999) // Assuming 9999 is a non-existent ID
	assert.Error(t, err, "GetSchemaById should return an error for a non-existent schema")
}

func SchemaNotFoundByNameTypeVersion(t *testing.T) {
	pool := setupTestDB(t)
	defer pool.Close()

	_, err := GetSchemaFilterParams(
		pool, QueryArgs{Name: "non_existent", Type: "json", Version: "1.0.1"},
	)
	assert.Error(t, err, "GetSchemaFilterParams should return an error for a non-existent schema")
}

func InsertSchemaWithEmptyFields(t *testing.T) {
	pool := setupTestDB(t)
	defer pool.Close()

	newSchema := QueryArgs{
		Name:       "",
		Type:       "",
		Version:    "",
		SchemaData: "",
	}

	_, err := InsertSchema(pool, newSchema)
	assert.Error(t, err, "InsertSchema should return an error for empty fields")
}

func UpdateNonExistentSchema(t *testing.T) {
	pool := setupTestDB(t)
	defer pool.Close()

	newSchema := QueryArgs{
		Name:       "non_existent",
		Type:       "json",
		Version:    "1.0.1",
		SchemaData: `{"type": "object", "properties": {"example": {"type": "string"}}}`,
	}
	_, err := UpdateSchema(pool, newSchema)
	assert.Error(t, err, "UpdateSchema should return an error for a non-existent schema")
}

func DeleteNonExistentSchema(t *testing.T) {
	pool := setupTestDB(t)
	defer pool.Close()

	err := DeleteSchema(pool, 9999) // Assuming 9999 is a non-existent ID
	assert.Error(t, err, "DeleteSchema should return an error for a non-existent schema")
}

func TestUpdateSchema(t *testing.T) {
	pool := setupTestDB(t)
	defer pool.Close()

	// Insert a schema for testing
	newSchema := QueryArgs{
		Name:       "test_schema",
		Type:       "json",
		Version:    "1.0.1",
		SchemaData: `{"type": "object", "properties": {"example": {"type": "string"}}}`,
	}
	id, err := InsertSchema(pool, newSchema)
	assert.NoError(t, err)

	// Update the schema
	newSchema.Name = "test_schema"
	newSchema.SchemaData = `{"type": "avro", "properties": {"example": {"type": "number"}}}`
	_, err = UpdateSchema(pool, newSchema)
	assert.NoError(t, err, "UpdateSchema should not return an error")

	updatedSchema, err := GetSchemaById(pool, id)
	assert.NoError(t, err)
	assert.Equal(t, "test_schema", updatedSchema.Name, "Updated schema name should match")
	assert.Equal(
		t, `{"type": "avro", "properties": {"example": {"type": "number"}}}`,
		updatedSchema.SchemaData, "Updated schema data should match",
	)
}

func InsertInsteadOfUpdateWhenNameTypeOrVersionChanged(t *testing.T) {
	pool := setupTestDB(t)
	defer pool.Close()

	// Insert a schema for testing
	originalSchema := QueryArgs{
		Name:       "test_schema",
		Type:       "json",
		Version:    "1.0.1",
		SchemaData: `{"type": "object", "properties": {"example": {"type": "string"}}}`,
	}
	id, err := InsertSchema(pool, originalSchema)
	assert.NoError(t, err)

	// Change the name, type, and version
	updatedSchema := QueryArgs{
		Name:       "new_test_schema",
		Type:       "avro",
		Version:    "1.0.1",
		SchemaData: `{"type": "object", "properties": {"example": {"type": "number"}}}`,
	}
	_, err = UpdateSchema(pool, updatedSchema)
	assert.NoError(t, err, "UpdateSchema should not return an error")

	// Verify that the original schema still exists
	originalRetrievedSchema, err := GetSchemaById(pool, id)
	assert.NoError(t, err)
	assert.Equal(
		t, originalSchema.Name, originalRetrievedSchema.Name, "Original schema name should match",
	)
	assert.Equal(
		t, originalSchema.Type, originalRetrievedSchema.Type, "Original schema type should match",
	)
	assert.Equal(
		t, originalSchema.Version, originalRetrievedSchema.Version,
		"Original schema version should match",
	)
	assert.Equal(
		t, originalSchema.SchemaData, originalRetrievedSchema.SchemaData,
		"Original schema data should match",
	)

	// Verify that the new schema was inserted
	newRetrievedSchema, err := GetSchemaFilterParams(
		pool, QueryArgs{Name: "new_test_schema", Type: "avro", Version: "1.0.1"},
	)
	assert.NoError(t, err)
	assert.Equal(t, updatedSchema.Name, newRetrievedSchema[0].Name, "New schema name should match")
	assert.Equal(t, updatedSchema.Type, newRetrievedSchema[0].Type, "New schema type should match")
	assert.Equal(
		t, updatedSchema.Version, newRetrievedSchema[0].Version, "New schema version should match",
	)
	assert.Equal(
		t, updatedSchema.SchemaData, newRetrievedSchema[0].SchemaData,
		"New schema data should match",
	)
}

func TestDeleteSchema(t *testing.T) {
	pool := setupTestDB(t)
	defer pool.Close()

	// Insert a schema for testing
	newSchema := QueryArgs{
		Name:       "test_schema",
		Type:       "json",
		Version:    "1.0.1",
		SchemaData: `{"type": "object", "properties": {"example": {"type": "string"}}}`,
	}
	id, err := InsertSchema(pool, newSchema)
	assert.NoError(t, err)

	// Delete the schema
	err = DeleteSchema(pool, id)
	assert.NoError(t, err, "DeleteSchema should not return an error")

	deletedSchema, err := GetSchemaById(pool, id)
	assert.Error(t, err, "GetSchemaById should return an error for a deleted schema")
	assert.Nil(t, deletedSchema, "Deleted schema should be nil")
}

func TestGetAllSchemas(t *testing.T) {
	pool := setupTestDB(t)
	defer pool.Close()

	// Insert multiple schemas for testing
	schemas := []QueryArgs{
		{
			Name:       "test_schema_1",
			Type:       "json",
			Version:    "1.0.1",
			SchemaData: `{"type": "object", "properties": {"example": {"type": "string"}}}`,
		},
		{
			Name:       "test_schema_2",
			Type:       "avro",
			Version:    "1.0.1",
			SchemaData: `{"type": "object", "properties": {"example": {"type": "number"}}}`,
		},
		{
			Name:       "test_schema_3",
			Type:       "json",
			Version:    "1.0.3",
			SchemaData: `{"type": "object", "properties": {"example": {"type": "boolean"}}}`,
		},
	}

	for _, schema := range schemas {
		_, err := InsertSchema(pool, schema)
		assert.NoError(t, err, "InsertSchema should not return an error")
	}

	// Retrieve all schemas
	retrievedSchemas, err := GetAllSchemas(pool)
	assert.NoError(t, err, "GetAllSchemas should not return an error")
	assert.Len(
		t, retrievedSchemas, len(schemas),
		"The number of retrieved schemas should match the number of inserted schemas",
	)

	// Verify that each inserted schema is in the retrieved schemas
	for i, schema := range schemas {
		assert.Equal(t, schema.Name, retrievedSchemas[i].Name, "Schema name should match")
		assert.Equal(t, schema.Type, retrievedSchemas[i].Type, "Schema type should match")
		assert.Equal(t, schema.Version, retrievedSchemas[i].Version, "Schema version should match")
		assert.Equal(
			t, schema.SchemaData, retrievedSchemas[i].SchemaData, "Schema data should match",
		)
	}
}

// Test to add a new user
func TestInsertUser(t *testing.T) {
	pool := setupTestDB(t)
	defer pool.Close()

	email := "heather@mymail.com"
	username := "heather"
	password := "password"

	err := AddUser(pool, email, username, password)
	assert.NoError(t, err, "AddUser should not return an error")

	// delete the user
	err = DeleteUser(pool, username)
	assert.NoError(t, err, "DeleteUser should not return an error")

}

// Test to get all users
func TestGetUsers(t *testing.T) {
	pool := setupTestDB(t)
	defer pool.Close()

	//populate a User struct
	user1 := User{
		Email:    "heather@mymail.com",
		Username: "heather",
		Password: "password",
	}
	user2 := User{
		Email:    "denver@mymail.com",
		Username: "denver",
		Password: "password",
	}
	user3 := User{
		Email:    "nova@mymail.com",
		Username: "nova",
		Password: "password",
	}

	// Insert multiple users for testing
	users := []User{user1, user2, user3}
	for _, user := range users {
		err := AddUser(pool, user.Email, user.Username, user.Password)
		assert.NoError(t, err, "AddUser should not return an error")
	}

	// Retrieve all users
	retrievedUsers, err := GetAllUsers(pool)
	assert.NoError(t, err, "GetAllUsers should not return an error")
	assert.Len(
		t, retrievedUsers, len(users),
		"The number of retrieved users should match the number of inserted users",
	)

	// delete the users
	for _, user := range users {
		err := DeleteUser(pool, user.Username)
		assert.NoError(t, err, "DeleteUser should not return an error")
	}

}

// Test to validate user login credentials
func TestValidateUserByUsername(t *testing.T) {
	pool := setupTestDB(t)
	defer pool.Close()

	email := "heather@mymail.com"
	username := "heather"
	password := "password"

	// Insert a user for testing
	err := AddUser(pool, email, username, password)
	assert.NoError(t, err, "AddUser should not return an error")

	// Validate user credentials
	var valid bool
	valid, err = ValidateUserByUsername(pool, username, password)
	assert.NoError(t, err, "ValidateUserByUsername should not return an error")
	assert.True(t, valid, "User credentials should be valid")

	// delete the user
	err = DeleteUser(pool, username)
	assert.NoError(t, err, "DeleteUser should not return an error")

}

// Test to validate user by email
func TestValidateUserByEmail(t *testing.T) {
	pool := setupTestDB(t)
	defer pool.Close()

	email := "heather@mymail.com"
	username := "heather"
	password := "password"

	// Insert a user for testing
	err := AddUser(pool, email, username, password)
	assert.NoError(t, err, "AddUser should not return an error")

	// Validate user credentials
	var valid bool
	valid, err = ValidateUserByEmail(pool, email, password)
	assert.NoError(t, err, "ValidateUserByEmail should not return an error")
	assert.True(t, valid, "User credentials should be valid")

	// delete the user
	err = DeleteUser(pool, username)
	assert.NoError(t, err, "DeleteUser should not return an error")

}
