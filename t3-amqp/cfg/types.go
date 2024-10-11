package cfg

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
	Server struct {
		Host       string `mapstructure:"host"`
		Port       int    `mapstructure:"port"`
		SSLEnabled string `mapstructure:"ssl-enabled"`
		SSLCert    string `mapstructure:"ssl-cert"`
		SSLKey     string `mapstructure:"ssl-key"`
	} `mapstructure:"server"`
}
