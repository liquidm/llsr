package llsr

import (
	"testing"
)

func expectValidConnectionString(t *testing.T, dbConfig *DatabaseConfig, expectedValue string) {
	connectionString := dbConfig.ToConnectionString()
	if connectionString != expectedValue {
		t.Fatalf("Expected dbConfig.ToConnectionString() to equal:\n'%s'\n, but got:\n'%s'", expectedValue, connectionString)
	}
}

func TestNewDatabaseConfig(t *testing.T) {
	dbConfig := NewDatabaseConfig("database_name")

	if dbConfig.Database != "database_name" {
		t.Fatal("Expected NewDatabaseConfig to set Database attribute")
	}

	if dbConfig.User != "postgres" {
		t.Fatal("Expected NewDatabaseConfig to set User attribute")
	}

	if dbConfig.Password != "" {
		t.Fatal("Expected NewDatabaseConfig not to set Password attribute")
	}

	if dbConfig.Host != "" {
		t.Fatal("Expected NewDatabaseConfig not to set Host attribute")
	}

	if dbConfig.Port != 0 {
		t.Fatal("Expected NewDatabaseConfig not to set Port attribute")
	}
}

func TestConnectionStringCreation(t *testing.T) {
	dbConfig := NewDatabaseConfig("database_name")

	expectValidConnectionString(t, dbConfig, "dbname=database_name user=postgres sslmode=disable")

	dbConfig.Password = "pass"
	expectValidConnectionString(t, dbConfig, "dbname=database_name user=postgres password=pass sslmode=disable")

	dbConfig.Host = "localhost"
	expectValidConnectionString(t, dbConfig, "dbname=database_name user=postgres password=pass host=localhost sslmode=disable")

	dbConfig.Port = 1234
	expectValidConnectionString(t, dbConfig, "dbname=database_name user=postgres password=pass host=localhost port=1234 sslmode=disable")
}
