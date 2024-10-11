package cfg

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoadConfig(t *testing.T) {
	// Set the T3_CONFIG environment variable to point to the test cfg file
	os.Setenv("T3_CONFIG", "config.yaml")

	// Load the configuration
	config, err := LoadConfig()
	assert.NoError(t, err, "LoadConfig should not return an error")

	// Validate the loaded configuration
	assert.Equal(t, "localhost", config.DB.Host, "DB host should be 'localhost'")
	assert.Equal(t, 5432, config.DB.Port, "DB port should be 5432")
	assert.Equal(t, "dba", config.DB.User, "DB user should be 'dba'")
	assert.Equal(t, "start123", config.DB.Password, "DB password should be 'start123'")
	assert.Equal(t, "t3", config.DB.DBName, "DB name should be 't3'")
	assert.Equal(t, "disable", config.DB.SSLMode, "DB SSL mode should be 'disable'")

	assert.Equal(t, "localhost", config.Server.Host, "Server host should be 'localhost'")
	assert.Equal(t, 8080, config.Server.Port, "Server port should be 8080")
	assert.False(t, false, config.Server.SSLEnabled, "Server SSL enabled should be false")
	assert.Equal(t, "cert.pem", config.Server.SSLCert, "Server SSL cert should be 'cert.pem'")
	assert.Equal(t, "key.pem", config.Server.SSLKey, "Server SSL key should be 'key.pem'")
}
