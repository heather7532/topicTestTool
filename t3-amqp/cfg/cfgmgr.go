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
	err = viper.BindEnv("T3_ROOT")
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

	// get T3_ROOT from the environment
	t3Root := viper.GetString("T3_ROOT")
	if t3Root == "" {
		return nil, fmt.Errorf("T3_ROOT environment variable is not set")
	}

	err = viper.Unmarshal(&config)
	if err != nil {
		return nil, fmt.Errorf("unable to decode into struct: %w", err)
	}

	return &config, nil
}
