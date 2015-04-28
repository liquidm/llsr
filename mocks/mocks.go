package mocks

type ErrorReporter interface {
	Errorf(string, ...interface{})
}
