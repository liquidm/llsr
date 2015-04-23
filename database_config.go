package llsr

import (
  "fmt"
  "strings"
)

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

func (c *DatabaseConfig) ToConnectionString() string {
  options := make([]string, 0)
  if len(c.Database) > 0 {
    options = append(options, fmt.Sprintf("dbname=%s", c.Database))
  }
  if len(c.User) > 0 {
    options = append(options, fmt.Sprintf("user=%s", c.User))
  }
  if len(c.Password) > 0 {
    options = append(options, fmt.Sprintf("password=%s", c.Password))
  }
  if len(c.Host) > 0 {
    options = append(options, fmt.Sprintf("host=%s", c.Host))
  }
  if c.Port > 0 {
    options = append(options, fmt.Sprintf("port=%d", c.Port))
  }
  options = append(options, "sslmode=disable")
  return strings.Join(options, " ")
}
