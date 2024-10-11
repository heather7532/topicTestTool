package cfg

import (
	"fmt"
	"github.com/spf13/viper"
)

// LoadConfig loads configuration from the cfg.yaml file
func LoadConfig() (*Config, error) {
	var config Config

	err := viper.BindEnv("CONFIG_PATH")
	if err != nil {
		return nil, err
	}

	// Get the cfg path from the environment variable
	configPath := viper.GetString("CONFIG_PATH")
	if configPath == "" {
		return nil, fmt.Errorf("CONFIG_PATH environment variable is not set")
	}

	viper.SetConfigFile(configPath)
	err = viper.ReadInConfig()
	if err != nil {
		return nil, fmt.Errorf("error reading cfg file: %w", err)
	}

	err = viper.Unmarshal(&config)
	if err != nil {
		return nil, fmt.Errorf("unable to decode into struct: %w", err)
	}

	return &config, nil
}
