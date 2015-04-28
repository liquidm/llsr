package llsr

import (
	"database/sql"
	"testing"

	_ "github.com/lib/pq"
	"github.com/liquidm/llsr/decoderbufs"
)

type testValueMapOidCallback func(*testing.T, int)

func withValueMapOid(t *testing.T, cb testValueMapOidCallback) {
	db, err := sql.Open("postgres", "sslmode=disable user="+dbUser()+" dbname="+dbName())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TYPE llsr_test_enum AS ENUM ('foo', 'bar', 'llsr_foobar')")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec("DROP TYPE llsr_test_enum")

	var oid int
	err = db.QueryRow("SELECT enumtypid FROM pg_enum WHERE enumlabel = 'llsr_foobar'").Scan(&oid)
	if err != nil {
		t.Fatal(err)
	}

	cb(t, oid)
}

func TestValueMapDiscovery(t *testing.T) {
	withValueMapOid(t, func(t *testing.T, oid int) {
		dbConfig := NewDatabaseConfig(dbName())
		dbConfig.User = dbUser()

		valuesMap, err := loadValuesMap(dbConfig)
		if err != nil {
			t.Fatal(err)
		}

		if !valuesMap[oid] {
			t.Fatal("Expected ValueMap.load() to discover enum types")
		}

		if valuesMap[1] {
			t.Fatal("Expected valuesMap to contain only enum oids")
		}
	})
}

func TestExtractValue(t *testing.T) {
	withValueMapOid(t, func(t *testing.T, oid int) {
		dbConfig := NewDatabaseConfig(dbName())
		dbConfig.User = dbUser()

		valuesMap, err := loadValuesMap(dbConfig)
		if err != nil {
			t.Fatal(err)
		}

		oid64 := int64(oid)

		datumMessage := &decoderbufs.DatumMessage{
			ColumnType: &oid64,
			DatumBytes: []byte("enum_label"),
		}

		extractedValue, err := valuesMap.Extract(datumMessage)
		if err != nil {
			t.Fatalf("Expected Extract not to return error. Got: %v", err)
		}

		if *(extractedValue.(*string)) != "enum_label" {
			t.Fatalf("Expected Extract to return enum label. Got: %v", extractedValue)
		}

		oid64 += 1

		datumMessage.ColumnType = &oid64

		extractedValue, err = valuesMap.Extract(datumMessage)
		if err != ErrUnknownOID {
			t.Fatalf("Expected ExtractValue to return ErrUnknownOID error. Got: %v", err)
		}
	})
}
