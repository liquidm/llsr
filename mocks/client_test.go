package mocks

import (
	"fmt"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/liquidm/llsr"
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

	client.ExpectUpdate().YieldMessage(&llsr.RowMessage{Table: proto.String("users"), Op: llsr.Op_INSERT.Enum()})

	msg := <-client.Updates()

	if msg != "INSERT users" {
		t.Errorf("Expected to receive foo message got %s instead", msg)
	}
}

func TestClientMeetsNotAllMessagesConsumedError(t *testing.T) {
	trm := newTestReporterMock()
	client := NewClient(trm, &DummyConverter{})

	client.ExpectUpdate().YieldMessage(&llsr.RowMessage{Table: proto.String("users"), Op: llsr.Op_INSERT.Enum()})
	client.ExpectUpdate().YieldMessage(&llsr.RowMessage{Table: proto.String("users"), Op: llsr.Op_INSERT.Enum()})

	// consume one message to make sure handleExpectations already started
	<-client.Updates()

	client.Close()

	if len(trm.errors) == 0 {
		t.Errorf("Expected to return error if not all messages consumed")
	}
}
