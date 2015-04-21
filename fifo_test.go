package llsr

import (
	"testing"
)

func TestFifoBehaviour(t *testing.T) {
	fifo := NewFifo()
	fifo.Open()
	defer fifo.Close()

	fifo.Input() <- int(1)
	fifo.Input() <- "aaa"
	fifo.Input() <- true

	v1 := (<-fifo.Output()).(int)
	v2 := (<-fifo.Output()).(string)
	v3 := (<-fifo.Output()).(bool)

	if v1 != 1 || v2 != "aaa" || v3 != true {
		t.Fatal("Expected Fifo to return values in FIFO order")
	}
}
