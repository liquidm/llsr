package llsr

import (
	"database/sql"
)

//EnumsMap is used in Converter interface.
//It has true set to every oid which is enum type in database.
type EnumsMap map[int]bool

func loadEnums(dbConfig *DatabaseConfig) (EnumsMap, error) {
	enums := make(EnumsMap)
	if err := enums.load(dbConfig); err != nil {
		return nil, err
	}
	return enums, nil
}

func (e EnumsMap) load(dbConfig *DatabaseConfig) error {
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

		e[oid] = true
	}
	return nil
}
