package service

import (
  "github.com/liquidm/llsr"
)

type Converter interface {
  Convert(*llsr.RowMessage, EnumsMap) interface{}
}

type Client struct {
  updates chan interface{}
  events chan *Event

  dbConfig         *llsr.DatabaseConfig
  slot             string
  startPosition    llsr.LogPos
  converter Converter

  stream *llsr.Stream

  stopped   bool
  closeChan chan struct{}
  closedChan chan bool

  enums EnumsMap
}

func NewClient(dbConfig *llsr.DatabaseConfig, converter Converter, slot string, startPosition llsr.LogPos) (*Client, error) {
  enums, err := loadEnums(dbConfig)
  if err != nil {
    return nil, err
  }

  client := &Client{
    dbConfig:         dbConfig,
    converter: converter,
    slot:             slot,
    startPosition:    startPosition,
    updates:          make(chan interface{}),
    events:           make(chan *Event),
    closeChan:        make(chan struct{}),
    closedChan:       make(chan bool),
    enums: enums,
  }
  return client, nil
}

func (c *Client) Updates() <-chan interface{} {
  return c.updates
}

func (c *Client) Events() <-chan *Event {
  return c.events
}

func (c *Client) Start() error {
  if c.stream != nil {
    return llsr.ErrStreamAlreadyRunning
  }

  c.stopped = false

  c.stream = llsr.NewStream(c.dbConfig, c.slot, c.startPosition)
  if err := c.stream.Start(); err != nil {
    return err
  }

  go c.recvData()
  go c.recvStdErr()
  go c.recvControl()

  return nil
}

func (c *Client) Stop() {
  c.stopped = true
  close(c.closeChan)
  <-c.closedChan
}

func (c *Client) recvData() {
  for {
    select {
    case data := <-c.stream.Data():
      c.updates <- c.converter.Convert(data, c.enums)
      c.startPosition = llsr.LogPos(data.GetLogPosition())
    case <-c.closeChan:
      return
    }
  }
}

func (c *Client) recvStdErr() {
  for {
    select {
      case stdErrStr := <-c.stream.ErrOut():
        c.events<- &Event{Type: EventBackendStdErr, Value: stdErrStr}
      case <-c.closeChan:
        return
    }
  }
}

func (c *Client) recvControl() {
  for {
    select {
    case <-c.closeChan:
      c.stream.Stop()
    case err := <-c.stream.Finished():
      if err != nil {
        go func() {
          c.events<- &Event{Type: EventBackendInvalidExitStatus, Value: err}
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

func (c *Client) reconnect() {
  go func(){
    c.events<- &Event{Type: EventReconnect}
  }()
  c.stream = nil
  c.Start()
}
