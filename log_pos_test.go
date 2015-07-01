package llsr

import (
	"testing"
)

func TestXLogPosConversion(t *testing.T) {
	conversions := make(map[string]LogPos)
	conversions["0/243C4C60"] = LogPos(607931488)
	conversions["A1/243C4C60"] = LogPos(692097666144)

	for k, v := range conversions {
		rk := v.String()
		if rk != k {
			t.Fatalf("expected LogPos(%d).String() to equal %s; got %v", v, k, rk)
		}

		kr := StrToLogPos(k)
		if kr != v {
			t.Fatalf("expected StrToLogPos(\"%s\") to equal %d; got %v", k, v, kr)
		}
	}
}
