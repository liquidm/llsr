package llsr

type DatabaseConfig struct {
	Database string
	User     string
	Password string
	Host     string
	Port     int
}

func NewDatabaseConfig(database string) *DatabaseConfig {
	return &DatabaseConfig{
		Database: database,
		User:     "postgres",
	}
}
