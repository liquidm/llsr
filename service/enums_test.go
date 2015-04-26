package service

import (
  "github.com/liquidm/llsr"
  "database/sql"
  _ "github.com/lib/pq"
  "testing"
)

type testEnumOidCallback func(*testing.T, int)

func withEnumOid(t *testing.T, cb testEnumOidCallback) {
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

func TestEnumDiscovery(t *testing.T) {
  withEnumOid(t, func(t *testing.T, oid int){
    dbConfig := llsr.NewDatabaseConfig(dbName())
    dbConfig.User = dbUser()

    enums, err := loadEnums(dbConfig)
    if err != nil {
      t.Fatal(err)
    }

    if !enums[oid] {
      t.Fatal("Expected enumMap.load() to discover enum types")
    }

    if enums[1] {
      t.Fatal("Expected enumMap to contain only enum oids")
    }
  })
}

func TestExtractValue(t *testing.T) {
  withEnumOid(t, func(t *testing.T, oid int){
    dbConfig := llsr.NewDatabaseConfig(dbName())
    dbConfig.User = dbUser()

    enums, err := loadEnums(dbConfig)
    if err != nil {
      t.Fatal(err)
    }

    oid64 := int64(oid)

    datumMessage := &llsr.DatumMessage{
      ColumnType: &oid64,
      DatumBytes: []byte("enum_label"),
    }

    extractedValue, err := enums.ExtractValue(datumMessage)
    if err != nil {
      t.Fatalf("Expected ExtractValue not to return error. Got: %v", err)
    }

    if *(extractedValue.(*string)) != "enum_label" {
      t.Fatalf("Expected ExtractValue to return enum label. Got: %v", extractedValue)
    }

    oid64 += 1

    datumMessage.ColumnType = &oid64

    extractedValue, err = enums.ExtractValue(datumMessage)
    if err != ErrUnknownOID {
      t.Fatalf("Expected ExtractValue to return ErrUnknownOID error. Got: %v", err)
    }
  })
}
