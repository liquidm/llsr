/*
Package llsr is a pg_recvlogical wraper for Postgres' Logical Log Streaming Replication.
*/
package llsr

import (
	"bytes"
	"database/sql"
	"strconv"

	"github.com/lib/pq/oid"
	"github.com/liquidm/llsr/decoderbufs"
)

//Converter is used to conver raw RowMessage structs into app specific data.
type Converter interface {
	//Converts RowMessage into app specific data.
	Convert(*decoderbufs.RowMessage, ValuesMap) interface{}
}

//Client is a generic postgres llsr client. It handles Updates and Events received from Postgres binlog. You must call Close() to make sure everything is cleaned up properly.
type Client interface {
	//Updates are database events such as adding, updating or deleting records. Updates return type is defined by Converter.
	Updates() <-chan interface{}
	//Events are internal messages received during communication with LLSR.
	Events() <-chan *Event
	//Close makes sure every resources are released succesfully
	Close()
}

type client struct {
	updates chan interface{}
	events  chan *Event

	db *sql.DB

	dbConfig      *DatabaseConfig
	slot          string
	startPosition LogPos
	converter     Converter

	stream *Stream

	stopped    bool
	closeChan  chan struct{}
	closedChan chan bool

	valuesMap ValuesMap
}

//Creates new Client struct
func NewClient(dbConfig *DatabaseConfig, converter Converter, slot string, startPosition LogPos) (Client, error) {
	valuesMap, err := loadValuesMap(dbConfig)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("postgres", dbConfig.ToConnectionString())
	if err != nil {
		return nil, err
	}

	client := &client{
		dbConfig:      dbConfig,
		db:            db,
		converter:     converter,
		slot:          slot,
		startPosition: startPosition,
		updates:       make(chan interface{}),
		events:        make(chan *Event),
		closeChan:     make(chan struct{}),
		closedChan:    make(chan bool),
		valuesMap:     valuesMap,
	}

	return client, client.start()
}

//Updates produces objects converted by Converter interface.
func (c *client) Updates() <-chan interface{} {
	return c.updates
}

//Events produces control events about underlying Stream object.
func (c *client) Events() <-chan *Event {
	return c.events
}

//Starts client. It does not block.
func (c *client) start() error {
	if c.stream != nil {
		return ErrStreamAlreadyRunning
	}

	c.stopped = false

	c.stream = NewStream(c.dbConfig, c.slot, c.startPosition)
	if err := c.stream.Start(); err != nil {
		return err
	}

	go c.recvData()
	go c.recvStdErr()
	go c.recvControl()

	return nil
}

//Stops client. It blocks untill pg_recvlogical closes.
func (c *client) Close() {
	c.stopped = true
	close(c.closeChan)
	<-c.closedChan
	c.db.Close()
}

func (c *client) recvData() {
	for {
		select {
		case data := <-c.stream.Data():
			c.setUnchangedValues(data.GetTable(), data.GetNewTuple())
			c.setUnchangedValues(data.GetTable(), data.GetOldTuple())
			c.updates <- c.converter.Convert(data, c.valuesMap)
			c.startPosition = LogPos(data.GetLogPosition())
		case <-c.closeChan:
			return
		}
	}
}

func (c *client) recvStdErr() {
	for {
		select {
		case stdErrStr := <-c.stream.ErrOut():
			value := stdErrStr.(string)
			c.events <- &Event{Type: EventBackendStdErr, Value: value[:len(value)-1]}
		case <-c.closeChan:
			return
		}
	}
}

func (c *client) recvControl() {
	for {
		select {
		case <-c.closeChan:
			c.stream.Close()
		case err := <-c.stream.Finished():
			if err != nil {
				go func() {
					c.events <- &Event{Type: EventBackendInvalidExitStatus, Value: err}
				}()
			}
			if !c.stopped {
				defer c.reconnect()
			} else {
				c.closedChan <- true
			}
			return
		}
	}
}

func (c *client) reconnect() {
	go func() {
		c.events <- &Event{Type: EventReconnect}
	}()
	c.stream = nil
	c.start()
}

func (c *client) setUnchangedValues(tableName string, msgs []*decoderbufs.DatumMessage) {
	var id int64
	var unchangedColumns map[string]int
	for i, msg := range msgs {
		if msg.GetColumnName() == "id" {
			id = int64(msg.GetDatumInt32())
			if id == 0 {
				id = int64(msg.GetDatumInt64())
			}
		}

		if msg.GetUnchangedNoValue() {
			if unchangedColumns == nil {
				unchangedColumns = make(map[string]int)
			}
			unchangedColumns[msg.GetColumnName()] = i
		}
	}

	if len(unchangedColumns) > 0 && id != 0 {
		var query bytes.Buffer
		columns := make([]interface{}, 0, len(unchangedColumns))
		first := true

		query.WriteString("SELECT ")
		for columnName, i := range unchangedColumns {
			var value string
			msgs[i].DatumString = &value
			textOid := int64(oid.T_text)
			msgs[i].ColumnType = &textOid
			columns = append(columns, &value)
			if !first {
				query.WriteString(", ")
			}
			query.WriteString(columnName)
			first = false
		}

		query.WriteString(" FROM ")
		query.WriteString(tableName)
		query.WriteString(" WHERE id = ")
		query.WriteString(strconv.Itoa(int(id)))

		err := c.db.QueryRow(query.String()).Scan(columns...)
		if err != nil && err != sql.ErrNoRows {
			panic(err)
		}
	}
}
