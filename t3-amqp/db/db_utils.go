package db

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/viper"
	"log"
	"strings"
	"time"
)

// Config struct to hold database connection info
type Config struct {
	DB struct {
		Host     string `mapstructure:"host"`
		Port     int    `mapstructure:"port"`
		User     string `mapstructure:"user"`
		Password string `mapstructure:"password"`
		DBName   string `mapstructure:"dbname"`
		SSLMode  string `mapstructure:"sslmode"`
	} `mapstructure:"db"`
}

// LoadConfig loads configuration from the config.yaml file
func LoadConfig() (*Config, error) {
	var config Config

	err := viper.BindEnv("CONFIG_PATH")
	if err != nil {
		return nil, err
	}

	// Get the config path from the environment variable
	configPath := viper.GetString("CONFIG_PATH")
	if configPath == "" {
		return nil, fmt.Errorf("CONFIG_PATH environment variable is not set")
	}

	viper.SetConfigFile(configPath)
	err = viper.ReadInConfig()
	if err != nil {
		return nil, fmt.Errorf("error reading config file: %w", err)
	}

	err = viper.Unmarshal(&config)
	if err != nil {
		return nil, fmt.Errorf("unable to decode into struct: %w", err)
	}

	return &config, nil
}

// ConnectDB creates a connection pool to the PostgreSQL database
func ConnectDB(config *Config) (*pgxpool.Pool, error) {
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
func InsertSchema(pool *pgxpool.Pool, params QueryArgs) (int, error) {

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
	var id int
	err := pool.QueryRow(context.Background(), query, args).Scan(&id)

	if err != nil {
		return 0, fmt.Errorf("error inserting schema: %w", err)
	}
	return id, nil
}

// GetSchemaById retrieves a schema by its ID from the s1.schema table
func GetSchemaById(pool *pgxpool.Pool, id int) (*Schema, error) {
	args := pgx.NamedArgs{
		"id": id,
	}

	query := `
		SELECT id, name, type, version, schema_data, created, modified 
		FROM s1.schema 
		WHERE id = @id`

	row := pool.QueryRow(context.Background(), query, args)

	var schema Schema
	err := row.Scan(&schema.ID, &schema.Name, &schema.Type, &schema.Version, &schema.SchemaData)
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
func DeleteSchema(pool *pgxpool.Pool, id int) error {
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

func main() {
	// Load configuration
	config, err := LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
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
