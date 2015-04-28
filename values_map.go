package llsr

import (
	"database/sql"
	"errors"

	_ "github.com/lib/pq"
	"github.com/lib/pq/oid"
	"github.com/liquidm/llsr/decoderbufs"
)

var (
	ErrUnknownOID = errors.New("Unknown OID")
)

//ValuesMap is used in Converter interface.
//It has true set to every oid which is value type in database.
type ValuesMap map[int]bool

func loadValuesMap(dbConfig *DatabaseConfig) (ValuesMap, error) {
	valuesMap := make(ValuesMap)
	if err := valuesMap.load(dbConfig); err != nil {
		return nil, err
	}
	return valuesMap, nil
}

//Extract value from DatumMessage. Returned value is always a pointer.
//Returns ErrUnknownOID if value is of unonkown OID. If returned with error, value is []byte or nil.
func (v ValuesMap) Extract(m *decoderbufs.DatumMessage) (interface{}, error) {
	var err error
	var value interface{}
	switch oid.Oid(*m.ColumnType) {
	case oid.T_bool:
		value = m.DatumBool
	case oid.T_int2, oid.T_int4:
		value = m.DatumInt32
	case oid.T_int8, oid.T_oid:
		value = m.DatumInt64
	case oid.T_float4:
		value = m.DatumFloat
	case oid.T_float8, oid.T_numeric:
		value = m.DatumDouble
	case oid.T_char, oid.T_varchar, oid.T_bpchar, oid.T_text, oid.T_json, oid.T_xml, oid.T_uuid, oid.T_timestamp, oid.T_timestamptz:
		value = m.DatumString
	case oid.T_point:
		value = m.DatumPoint
	case oid.T_bytea:
		value = m.DatumBytes
	default:
		if v[int(*m.ColumnType)] {
			valueStr := string(m.DatumBytes)
			value = &valueStr
		} else {
			err = ErrUnknownOID
			value = m.DatumBytes
		}
	}
	if value == nil {
		return nil, err
	}
	return value, err
}

func (v ValuesMap) load(dbConfig *DatabaseConfig) error {
	db, err := sql.Open("postgres", dbConfig.ToConnectionString())
	if err != nil {
		return err
	}
	defer db.Close()

	rows, err := db.Query("SELECT enumtypid FROM pg_enum;")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var oid int

		if err := rows.Scan(&oid); err != nil {
			return err
		}

		v[oid] = true
	}
	return nil
}
