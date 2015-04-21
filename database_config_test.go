package llsr

import (
	"testing"
)

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
