package service

import (
	"os"
)

var user string
var dbname string

func dbUser() string {
	if len(user) == 0 {
		user = os.Getenv("PG_LLSR_TEST_USER")
		if len(user) == 0 {
			user = "postgres"
		}
	}
	return user
}

func dbName() string {
	if len(dbname) == 0 {
		dbname = os.Getenv("PG_LLSR_TEST_DB")
		if len(dbname) == 0 {
			dbname = "postgres"
		}
	}
	return dbname
}
