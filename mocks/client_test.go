package mocks

import (
	"errors"
	"fmt"
	"testing"

	"github.com/golang/protobuf/proto"
	"go.build.ligatus.com/dsp/llsr"
	"go.build.ligatus.com/dsp/llsr/decoderbufs"
)

type testReporterMock struct {
	errors []string
}

func newTestReporterMock() *testReporterMock {
	return &testReporterMock{errors: make([]string, 0)}
}

func (trm *testReporterMock) Errorf(format string, args ...interface{}) {
	trm.errors = append(trm.errors, fmt.Sprintf(format, args...))
}

func TestMockClientImplementsClientInterface(t *testing.T) {
	var c interface{} = &Client{}
	if _, ok := c.(llsr.Client); !ok {
		t.Error("The mock client should implement llsr.Client interface")
	}
}

func TestClientHandlesUpdateExpectations(t *testing.T) {
	client := NewClient(t, &DummyConverter{})
	defer func() {
		client.Close()
	}()

	client.ExpectYieldMessage(&decoderbufs.RowMessage{Table: proto.String("users"), Op: decoderbufs.Op_INSERT.Enum()})
	client.ExpectYieldEvent(&llsr.Event{Type: llsr.EventReconnect})

	msg := <-client.Updates()

	if msg != "INSERT users" {
		t.Errorf("Expected to receive foo message got %s instead", msg)
	}

	event := <-client.Events()

	if event.Type != llsr.EventReconnect {
		t.Errorf("Expected to receive llsr.EventReconnect got %s instead", event.Type)
	}
}

func TestClientMeetsNotAllMessagesConsumedError(t *testing.T) {
	trm := newTestReporterMock()
	client := NewClient(trm, &DummyConverter{})

	client.ExpectYieldMessage(&decoderbufs.RowMessage{Table: proto.String("users"), Op: decoderbufs.Op_INSERT.Enum()})

	client.Close()

	if len(trm.errors) == 0 {
		t.Errorf("Expected to return error if not all messages consumed")
	}
}

func TestExpectReconnectEvent(t *testing.T) {
	client := NewClient(t, &DummyConverter{})
	client.ExpectReconnectEvent()

	event := <-client.Events()

	if event.Type != llsr.EventReconnect {
		t.Error("Expected to receive llsr.EventReconnect event")
	}
}

func TestExpectEventBackendStdErr(t *testing.T) {
	client := NewClient(t, &DummyConverter{})
	client.ExpectBackendStdErrEvent("stderr output")

	event := <-client.Events()

	if event.Type != llsr.EventBackendStdErr {
		t.Error("Expected to receive llsr.EventBackendStdErr event")
	}

	if event.Value.(string) != "stderr output" {
		t.Errorf("Expected event to contain '%s' value, got: %v", "stderr output", event.Value)
	}
}

func TestExpect(t *testing.T) {
	client := NewClient(t, &DummyConverter{})
	client.ExpectBackendInvalidExitStatusEvent(errors.New(""))

	event := <-client.Events()

	if event.Type != llsr.EventBackendInvalidExitStatus {
		t.Error("Expected to receive llsr.EventBackendInvalidExitStatus event")
	}
}
