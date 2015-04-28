package mocks

import (
	"github.com/liquidm/llsr"
)

type Client struct {
	t            ErrorReporter
	converter    llsr.Converter
	expectations chan interface{}
	closeChan    chan int
	updates      chan interface{}
	events       chan *llsr.Event
}

func NewClient(t ErrorReporter, converter llsr.Converter) *Client {
	c := &Client{
		t:            t,
		converter:    converter,
		expectations: make(chan interface{}, 1000),
		updates:      make(chan interface{}, 1000),
		events:       make(chan *llsr.Event),
		closeChan:    make(chan int),
	}

	go c.handleExpectations()
	return c
}

func (c *Client) Updates() <-chan interface{} {
	return c.updates
}

func (c *Client) Events() <-chan *llsr.Event {
	return c.events
}

func (c *Client) ExpectYieldMessage(msg *llsr.RowMessage) {
	c.expectations <- msg
}

func (c *Client) ExpectYieldEvent(event *llsr.Event) {
	c.expectations <- event
}

func (c *Client) ExpectReconnectEvent() {
	c.expectations <- &llsr.Event{Type: llsr.EventReconnect}
}

func (c *Client) ExpectBackendStdErrEvent() {
	c.expectations <- &llsr.Event{Type: llsr.EventBackendStdErr}
}

func (c *Client) ExpectBackendInvalidExitStatusEvent() {
	c.expectations <- &llsr.Event{Type: llsr.EventBackendInvalidExitStatus}
}

func (c *Client) Close() {
	close(c.expectations)
	<-c.closeChan
	if len(c.updates) > 0 || len(c.events) > 0 {
		c.t.Errorf("Not all messages were consumed")
	}
}

func (c *Client) handleExpectations() {
	for ex := range c.expectations {
		switch t := ex.(type) {
		case *llsr.RowMessage:
			c.updates <- c.converter.Convert(t, nil)
		case *llsr.Event:
			c.events <- t
		}
	}

	c.closeChan <- 1
}
