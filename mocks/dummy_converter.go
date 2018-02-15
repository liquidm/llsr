package mocks

import (
	"bytes"

	"go.build.ligatus.com/dsp/llsr"
	"go.build.ligatus.com/dsp/llsr/decoderbufs"
)

type DummyConverter struct{}

func (*DummyConverter) Convert(change *decoderbufs.RowMessage, enums llsr.ValuesMap) interface{} {
	var buf bytes.Buffer

	switch change.GetOp() {
	case decoderbufs.Op_INSERT:
		buf.WriteString("INSERT ")
	case decoderbufs.Op_UPDATE:
		buf.WriteString("UPDATE ")
	case decoderbufs.Op_DELETE:
		buf.WriteString("DELETE ")
	}

	buf.WriteString(change.GetTable())

	return buf.String()
}
