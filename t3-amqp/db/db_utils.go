package db

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
	"log"
	"strings"
	"t3-amqp/cfg"
	"time"
)

// ConnectDB creates a connection pool to the PostgreSQL database
func ConnectDB(config *cfg.Config) (*pgxpool.Pool, error) {
	connStr := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
		config.DB.User, config.DB.Password, config.DB.Host, config.DB.Port, config.DB.DBName,
		config.DB.SSLMode,
	)

	pool, err := pgxpool.New(context.Background(), connStr)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %w", err)
	}

	return pool, nil
}

// InsertSchema inserts a new schema into the s1.schema table
func InsertSchema(pool *pgxpool.Pool, params QueryArgs) (int64, error) {

	created := time.Now().UTC()
	modified := created

	args := pgx.NamedArgs{
		"name":        params.Name,
		"type":        params.Type,
		"version":     params.Version,
		"schema_data": params.SchemaData,
		"created":     created,
		"modified":    modified,
	}

	query := `INSERT INTO s1.schema (name, type, version, schema_data, created, modified) 
			VALUES (@name, @type, @version, @schema_data, @created, @modified) RETURNING id`
	var id int64
	err := pool.QueryRow(context.Background(), query, args).Scan(&id)

	if err != nil {
		return 0, fmt.Errorf("error inserting schema: %w", err)
	}
	return id, nil
}

// GetSchemaById retrieves a schema by its ID from the s1.schema table
func GetSchemaById(pool *pgxpool.Pool, id int64) (*Schema, error) {
	args := pgx.NamedArgs{
		"id": id,
	}

	query := `
		SELECT id, name, type, version, schema_data, created, modified 
		FROM s1.schema 
		WHERE id = @id`

	row := pool.QueryRow(context.Background(), query, args)

	var schema Schema
	err := row.Scan(&schema.ID, &schema.Name, &schema.Type, &schema.Version, &schema.SchemaData, &schema.Created, &schema.Modified)
	if err != nil {
		return nil, fmt.Errorf("error getting schema: %w", err)
	}

	return &schema, nil
}

// GetSchemaFilterParams retrieves schemas by optional name, type, and version from the s1.schema table
func GetSchemaFilterParams(pool *pgxpool.Pool, params QueryArgs) ([]Schema, error) {
	var conditions []string
	args := pgx.NamedArgs{}

	if params.Name != "" {
		conditions = append(conditions, "name = @name")
		args["name"] = params.Name
	}
	if params.Type != "" {
		conditions = append(conditions, "type = @type")
		args["type"] = params.Type
	}
	if params.Version != "" {
		conditions = append(conditions, "version = @version")
		args["version"] = params.Version
	}

	query := "SELECT id, name, type, version, schema_data, created, modified FROM s1.schema"
	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}

	rows, err := pool.Query(context.Background(), query, args)
	if err != nil {
		return nil, fmt.Errorf("error querying schemas: %w", err)
	}
	defer rows.Close()

	var schemas []Schema
	for rows.Next() {
		var schema Schema
		err := rows.Scan(
			&schema.ID, &schema.Name, &schema.Type, &schema.Version, &schema.SchemaData,
			&schema.Created, &schema.Modified,
		)
		if err != nil {
			return nil, fmt.Errorf("error scanning schema: %w", err)
		}
		schemas = append(schemas, schema)
	}

	return schemas, nil
}

// UpdateSchema updates an existing schema in the s1.schema table
func UpdateSchema(pool *pgxpool.Pool, params QueryArgs) ([]Schema, error) {
	// Retrieve the existing schema
	existingSchemas, err := GetSchemaFilterParams(
		pool, QueryArgs{Name: params.Name, Type: params.Type, Version: params.Version},
	)
	if err != nil {
		return nil, fmt.Errorf("error retrieving existing schema: %w", err)
	}

	// If the schema does not exist return an error
	if len(existingSchemas) == 0 {
		return []Schema{}, fmt.Errorf("schema not found")
	}

	// Check if any argument except schema_data has changed
	if existingSchemas[0].Name != params.Name || existingSchemas[0].Type != params.Type || existingSchemas[0].Version != params.Version {
		// Perform an insert instead of an update
		_, err := InsertSchema(pool, params)
		if err != nil {
			return nil, fmt.Errorf("error inserting schema: %w", err)
		}
		return GetSchemaFilterParams(pool, params)
	}

	// Update the modified timestamp
	modified := time.Now().UTC()

	// Proceed with the update for schema_data
	args := pgx.NamedArgs{
		"name":        params.Name,
		"type":        params.Type,
		"version":     params.Version,
		"schema_data": params.SchemaData,
		"modified":    modified,
	}

	query := `
		UPDATE s1.schema
		SET schema_data = @schema_data, modified = @modified
		WHERE name = @name AND type = @type AND version = @version`

	_, err = pool.Exec(context.Background(), query, args)
	if err != nil {
		return nil, fmt.Errorf("error updating schema: %w", err)
	}

	return GetSchemaFilterParams(pool, params)
}

// DeleteSchema deletes a schema from the s1.schema table
func DeleteSchema(pool *pgxpool.Pool, id int64) error {
	args := pgx.NamedArgs{
		"id": id,
	}

	query := `
		DELETE FROM s1.schema 
		WHERE id = @id`

	_, err := pool.Exec(context.Background(), query, args)
	if err != nil {
		return fmt.Errorf("error deleting schema: %w", err)
	}
	return nil
}

// Delete Schema by Name and support a wildcard
func DeleteSchemaByName(pool *pgxpool.Pool, name string) error {
	args := pgx.NamedArgs{
		"name": name,
	}

	query := `
		DELETE FROM s1.schema 
		WHERE name LIKE @name`

	_, err := pool.Exec(context.Background(), query, args)
	if err != nil {
		return fmt.Errorf("error deleting schema: %w", err)
	}
	return nil
}

func GetAllSchemas(pool *pgxpool.Pool) ([]Schema, error) {
	query := `SELECT id, name, type, version, schema_data, created, modified FROM s1.schema`
	rows, err := pool.Query(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("error querying schemas: %w", err)
	}
	defer rows.Close()

	var schemas []Schema
	for rows.Next() {
		var schema Schema
		err := rows.Scan(
			&schema.ID, &schema.Name, &schema.Type, &schema.Version, &schema.SchemaData,
			&schema.Created, &schema.Modified,
		)
		if err != nil {
			return nil, fmt.Errorf("error scanning schema: %w", err)
		}
		schemas = append(schemas, schema)
	}

	return schemas, nil
}

// ValidatePassword checks if the provided password matches the hashed password stored in the database.
func ValidateUserByUsername(pool *pgxpool.Pool, username string, password string) (bool, error) {
	var hashedPassword string

	query := `SELECT password FROM s1.user_account WHERE username = $1`
	err := pool.QueryRow(context.Background(), query, username).Scan(&hashedPassword)
	if err != nil {
		return false, fmt.Errorf("error querying user account: %w", err)
	}

	err = bcrypt.CompareHashAndPassword([]byte(hashedPassword), []byte(password))
	if err != nil {
		return false, fmt.Errorf("invalid password: %w", err)
	}

	return true, nil
}

// ValidatePassword by email
func ValidateUserByEmail(pool *pgxpool.Pool, email string, password string) (bool, error) {
	var hashedPassword string

	query := `SELECT password FROM s1.user_account WHERE email = $1`
	err := pool.QueryRow(context.Background(), query, email).Scan(&hashedPassword)
	if err != nil {
		return false, fmt.Errorf("error querying user account: %w", err)
	}

	err = bcrypt.CompareHashAndPassword([]byte(hashedPassword), []byte(password))
	if err != nil {
		return false, fmt.Errorf("invalid password: %w", err)
	}

	return true, nil
}

// add a user to the user account table using a post request including the email, username, and password
func AddUser(pool *pgxpool.Pool, email string, username string, password string) error {
	// using bcrypt to hash the password
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)

	query := `INSERT INTO s1.user_account (email, username, password) VALUES ($1, $2, $3)`
	_, err = pool.Exec(context.Background(), query, email, username, hashedPassword)
	if err != nil {
		return fmt.Errorf("error inserting user account: %w", err)
	}
	return nil
}

// Query a list of users excluding the password
func GetAllUsers(pool *pgxpool.Pool) ([]User, error) {
	query := `SELECT id, email, username FROM s1.user_account`
	rows, err := pool.Query(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("error querying users: %w", err)
	}
	defer rows.Close()

	var users []User
	for rows.Next() {
		var user User
		err := rows.Scan(&user.ID, &user.Email, &user.Username)
		if err != nil {
			return nil, fmt.Errorf("error scanning user: %w", err)
		}
		users = append(users, user)
	}

	return users, nil
}

// Delete a user from the user account table by username
func DeleteUser(pool *pgxpool.Pool, username string) error {
	query := `DELETE FROM s1.user_account WHERE username = $1`
	_, err := pool.Exec(context.Background(), query, username)
	if err != nil {
		return fmt.Errorf("error deleting user account: %w", err)
	}
	return nil
}

// Delete a user by username
func DeleteUserByEmail(pool *pgxpool.Pool, email string) error {
	query := `DELETE FROM s1.user_account WHERE email = $1`
	_, err := pool.Exec(context.Background(), query, email)
	if err != nil {
		return fmt.Errorf("error deleting user account: %w", err)
	}
	return nil
}

// ============== MAIN FUNCTION ==============

func main() {
	// Load configuration
	config, err := cfg.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load cfg: %v", err)
	}

	// Connect to the database
	pool, err := ConnectDB(config)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	// Example usage: Insert a new schema
	newSchema := QueryArgs{
		Name:       "example_schema",
		Type:       "json",
		Version:    "1.0.0",
		SchemaData: `{"type": "object", "properties": {"example": {"type": "string"}}}`,
	}

	id, err := InsertSchema(pool, newSchema)
	if err != nil {
		log.Fatalf("Failed to insert schema: %v", err)
	}
	fmt.Printf("Schema inserted successfully with ID: %d\n", id)
}
