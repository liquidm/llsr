package service

import (
  "github.com/liquidm/llsr"
  "testing"
  "bytes"
  "time"
  "database/sql"
  _ "github.com/lib/pq"
)

type testConnCallback func(*testing.T, *sql.DB)
type testClientCallback func(*testing.T, *Client, *sql.DB)

type DummyConverter struct {}

func (*DummyConverter) Convert(change *llsr.RowMessage, enums EnumsMap) interface{}{
  var buf bytes.Buffer

  switch change.GetOp() {
  case llsr.Op_INSERT:
    buf.WriteString("INSERT ")
  case llsr.Op_UPDATE:
    buf.WriteString("UPDATE ")
  case llsr.Op_DELETE:
    buf.WriteString("DELETE ")
  }

  buf.WriteString(change.GetTable())

  return buf.String()
}

func testConfig() *llsr.DatabaseConfig {
  config := llsr.NewDatabaseConfig(dbName())
  config.User = dbUser()
  return config
}

func expectClientEvent(t *testing.T, c *Client, eventType EventType) {
  eventFound := make(chan bool)
  go func(){
    for {
      event := <-c.Events()
      if event.Type == eventType {
        eventFound<- true
        return
      }
    }
  }()
  select {
  case <-eventFound:
  case <-time.Tick(5 * time.Second):
    t.Fatal("Timeout")
  }
}

func expectClientUpdate(t *testing.T, c *Client, updateMsg string) {
  updateFound := make(chan bool)
  go func(){
    for {
      i := <-c.Updates()
      update := i.(string)
      if update == updateMsg {
        updateFound<- true
        return
      }
    }
  }()
  select {
  case <-updateFound:
  case <-time.Tick(5 * time.Second):
    t.Fatal("Timeout")
  }
}

func withTestConnection(t *testing.T, cb testConnCallback) {
  db, err := sql.Open("postgres", "sslmode=disable user="+dbUser()+" dbname="+dbName())
  if err != nil {
    t.Fatal(err)
  }
  defer db.Close()

  _, err = db.Exec("CREATE TABLE llsr_test_table (id int primary key, txt text NOT NULL);")
  if err != nil {
    t.Fatal(err)
  }
  defer db.Exec("DROP TABLE llsr_test_table")

  _, err = db.Exec("SELECT * FROM pg_create_logical_replication_slot('llsr_test_slot', 'decoderbufs')")
  if err != nil {
    t.Fatal(err)
  }
  defer db.Exec("SELECT * FROM pg_drop_replication_slot('llsr_test_slot')")

  cb(t, db)
}

func withTestClient(t *testing.T, cb testClientCallback) {
  withTestConnection(t, func(t *testing.T, db *sql.DB){
    client, err := NewClient(testConfig(), &DummyConverter{}, "llsr_test_slot", 0)
    if err != nil {
      t.Fatal(err)
    }

    err = client.Start()
    if err != nil {
      t.Fatal(err)
    }
    defer client.Stop()

    time.Sleep(1000000000)

    cb(t, client, db)
  })
}

func TestClientEvents(t *testing.T) {
  withTestClient(t, func(t *testing.T, client *Client, db *sql.DB){
    client.stream.Stop()
    expectClientEvent(t, client, EventReconnect)
  })
}

func TestClientUpdates(t *testing.T) {
  withTestClient(t, func(t *testing.T, client *Client, db *sql.DB){
    _, err := db.Exec("INSERT INTO llsr_test_table (id, txt) VALUES(1, 'foo')")
    if err != nil {
      t.Fatal(err)
    }

    expectClientUpdate(t, client, "INSERT llsr_test_table")
    
    _, err = db.Exec("UPDATE llsr_test_table SET txt = 'bar' WHERE id = 1")
    if err != nil {
      t.Fatal(err)
    }

    expectClientUpdate(t, client, "UPDATE llsr_test_table")

    _, err = db.Exec("DELETE FROM llsr_test_table WHERE id = 1")
    if err != nil {
      t.Fatal(err)
    }

    expectClientUpdate(t, client, "DELETE llsr_test_table")    
  })
}
