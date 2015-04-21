package llsr

import (
  "fmt"
)

type LogPos uint64

func (v LogPos) ToString() string {
  high := uint32(v >> 32)
  low := uint32(v)
  return fmt.Sprintf("%X/%X", high, low)
}

func StrToLogPos(str string) LogPos {
  var high, low uint32
  fmt.Sscanf(str, "%X/%X", &high, &low)
  return LogPos((int64(high) << 32) | int64(low))
}
