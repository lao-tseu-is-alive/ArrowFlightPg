package db

import (
	"github.com/lao-tseu-is-alive/go-cloud-k8s-common-libs/pkg/database"
	"github.com/lao-tseu-is-alive/go-cloud-k8s-common-libs/pkg/golog"
)

// Storage is an interface to different implementation of persistence for Tables
type Storage interface {
	// List returns the list of existing tables with the given offset and limit.
	List(params ListParams) ([]*TableList, error)
	// Get returns the table with the specified tables ID.
	Get(id int) (*Table, error)
	// Exist returns true only if a tables with the specified id exists in store.
	Exist(id int) bool
	// Count returns the total number of tables.
	Count(params CountParams) (int, error)
	// ListSchemas returns the list of existing schemas.
	ListSchemas() ([]string, error)
}

func GetStorageInstanceOrPanic(dbDriver string, db database.DB, l golog.MyLogger) Storage {
	var store Storage
	var err error
	switch dbDriver {
	case "pgx":
		store, err = NewPgxDB(db, l)
		if err != nil {
			l.Fatal("error doing NewPgxDB(pgConn : %w", err)
		}

	default:
		panic("unsupported DB driver type")
	}
	return store
}
