package llsr

type EventType int

const (
	//Event dispatched when pg_recvlogical outputs to STDERR. Value interface is string with this output.
	EventBackendStdErr EventType = iota

	//Event dispatched when reconnecting to pg_recvlogical for some reason. Value is nil.
	EventReconnect

	//Event dispatched when pg_recvlogical exits with error. Value is set to error returned.
	EventBackendInvalidExitStatus
)

//Event represents event to Stream struct in Client
type Event struct {
	Type  EventType
	Value interface{}
}
