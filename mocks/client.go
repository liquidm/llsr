package mocks

import (
	"bytes"

	"github.com/liquidm/llsr"
)

type ErrorReporter interface {
	Errorf(string, ...interface{})
}

type DummyConverter struct{}

func (*DummyConverter) Convert(change *llsr.RowMessage, enums llsr.EnumsMap) interface{} {
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

type Client struct {
	t            ErrorReporter
	converter    llsr.Converter
	expectations chan *llsr.RowMessage
	updates      chan interface{}
}

func NewClient(t ErrorReporter, converter llsr.Converter) *Client {
	c := &Client{
		t:            t,
		converter:    converter,
		expectations: make(chan *llsr.RowMessage, 1000),
		updates:      make(chan interface{}, 1000),
	}

	return c
}

func (c *Client) Updates() <-chan interface{} {
	go c.handleExpectations()
	return c.updates
}

func (c *Client) Events() <-chan *llsr.Event {
	return nil
}

func (c *Client) ExpectUpdate() *Client {
	return c
}

func (c *Client) YieldMessage(msg *llsr.RowMessage) {
	c.expectations <- msg
}

func (c *Client) Close() {
	if len(c.updates) > 0 {
		c.t.Errorf("Not all messages were consumed")
	}
}

func (c *Client) handleExpectations() {
	for ex := range c.expectations {
		c.updates <- c.converter.Convert(ex, nil)
	}
}
