package mocks

import (
	"go.build.ligatus.com/dsp/llsr"
	"go.build.ligatus.com/dsp/llsr/decoderbufs"
)

// Client implements llsr's Client interface for testing purposes.
// Before you start using it, you must define your expectations using Expect*
// commands
type Client struct {
	t            ErrorReporter
	converter    llsr.Converter
	expectations chan interface{}
	closeChan    chan int
	updates      chan interface{}
	events       chan *llsr.Event
}

// NewClient returns a new mock Client instance. The t argument should
// be the *testing.T instance of your test method
func NewClient(t ErrorReporter, converter llsr.Converter) *Client {
	c := &Client{
		t:            t,
		converter:    converter,
		expectations: make(chan interface{}, 1000),
		updates:      make(chan interface{}, 1000),
		events:       make(chan *llsr.Event, 1000),
		closeChan:    make(chan int),
	}

	go c.handleExpectations()
	return c
}

// Updates implements Updates method from llsr.Client interface.
// It returns update events converted by Converter.
func (c *Client) Updates() <-chan interface{} {
	return c.updates
}

// Events implements Events method from llsr.Client interface
// It returns system events
func (c *Client) Events() <-chan *llsr.Event {
	return c.events
}

// ExpectYieldMessage allows you to create message expectations
func (c *Client) ExpectYieldMessage(msg *decoderbufs.RowMessage) {
	c.expectations <- msg
}

// ExpectYieldMessage allows you to create event expectations
func (c *Client) ExpectYieldEvent(event *llsr.Event) {
	c.expectations <- event
}

// ExpectYieldMessage simulates Reconection event
func (c *Client) ExpectReconnectEvent() {
	c.expectations <- &llsr.Event{Type: llsr.EventReconnect}
}

// ExpectYieldMessage simulates Std error event
func (c *Client) ExpectBackendStdErrEvent(content string) {
	c.expectations <- &llsr.Event{Type: llsr.EventBackendStdErr, Value: content}
}

// ExpectYieldMessage simulates Backend error event
func (c *Client) ExpectBackendInvalidExitStatusEvent(err error) {
	c.expectations <- &llsr.Event{Type: llsr.EventBackendInvalidExitStatus, Value: err}
}

// Closes implements Close method from llsr.Client interface.
// It closes client and makes sure every message were consumed.
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
		case *decoderbufs.RowMessage:
			c.updates <- c.converter.Convert(t, nil)
		case *llsr.Event:
			c.events <- t
		}
	}

	c.closeChan <- 1
}
