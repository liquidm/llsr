package service

type EventType int

const (
  EventBackendStdErr EventType = iota
  EventReconnect
  EventBackendInvalidExitStatus
)

type Event struct {
  Type EventType
  Value interface{}
}

