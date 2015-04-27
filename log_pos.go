package llsr

import (
	"fmt"
)

//LogPos represents position in PostgreSQL binlog
type LogPos uint64

//Converts position to its textual representation (e.g. 17/A4C41EC0)
func (v LogPos) ToString() string {
	high := uint32(v >> 32)
	low := uint32(v)
	return fmt.Sprintf("%X/%X", high, low)
}

//Converts textual representation (e.g. 17/A4C41EC0) to LogPos
func StrToLogPos(str string) LogPos {
	var high, low uint32
	fmt.Sscanf(str, "%X/%X", &high, &low)
	return LogPos((int64(high) << 32) | int64(low))
}
