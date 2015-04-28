package mocks

// A simple interface that includes the testing.T methods we use to report
// expectation violations when using the mock objects.
type ErrorReporter interface {
	Errorf(string, ...interface{})
}
