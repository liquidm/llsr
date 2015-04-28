package mocks

import (
	"bytes"

	"github.com/liquidm/llsr"
)

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
