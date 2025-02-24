package db

import (
	"context"
	"errors"
	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lao-tseu-is-alive/go-cloud-k8s-common-libs/pkg/database"
	"github.com/lao-tseu-is-alive/go-cloud-k8s-common-libs/pkg/golog"
)

type PGX struct {
	Conn *pgxpool.Pool
	dbi  database.DB
	log  golog.MyLogger
}

// NewPgxDB will instantiate a new storage of type postgres and ensure schema exist
func NewPgxDB(db database.DB, log golog.MyLogger) (Storage, error) {
	var psql PGX
	pgConn, err := db.GetPGConn()
	if err != nil {
		return nil, err
	}
	psql.Conn = pgConn
	psql.dbi = db
	psql.log = log
	var numberOfTables int
	errCountTable := pgConn.QueryRow(context.Background(), tablesCount).Scan(&numberOfTables)
	if errCountTable != nil {
		log.Error("Unable to retrieve the number of tables error: %v", err)
		return nil, err
	}

	if numberOfTables > 0 {
		log.Info("'database contains %d tables'", numberOfTables)
	} else {
		log.Warn("database is empty ! it should contain at least one row to list anything")
		return nil, errors.New("problem with initial content of database it should not be empty ")
	}

	return &psql, err
}

// List returns the list of existing tables with the given offset and limit.
func (db *PGX) List(params ListParams) ([]*TableList, error) {
	db.log.Debug("trace : entering List(params : %+v)", params)
	listTables := tablesList
	var (
		res []*TableList
		err error
	)
	if params.SchemaName != nil {
		db.log.Info("param type : %v", *params.SchemaName)
		listTables += " AND n.nspname = $1"
		listTables += " ORDER BY schema_name, table_name, table_type;"
		err = pgxscan.Select(context.Background(), db.Conn, &res, listTables, &params.SchemaName)
	} else {
		listTables += " ORDER BY schema_name, table_name, table_type;"
		// db.log.Info("will run query %v", listTables)
		err = pgxscan.Select(context.Background(), db.Conn, &res, listTables)
	}

	if err != nil {
		db.log.Error(SelectFailedInNWithErrorE, "List", err)
		return nil, err
	}
	if res == nil {
		db.log.Info(FunctionNReturnedNoResults, "List")
		return nil, pgx.ErrNoRows
	}
	return res, nil
}

// Get will retrieve the table with given id
func (db *PGX) Get(id int) (*Table, error) {
	db.log.Debug("trace : entering Get(%v)", id)
	res := &Table{}
	err := pgxscan.Get(context.Background(), db.Conn, res, tableGet, id)
	if err != nil {
		db.log.Error(SelectFailedInNWithErrorE, "Get", err)
		return nil, err
	}
	if res == nil {
		db.log.Info(FunctionNReturnedNoResults, "Get")
		return nil, pgx.ErrNoRows
	}
	return res, nil
}

// GetTableSchema will retrieve the schema of the table for the given schema name and table name
func (db *PGX) GetTableSchema(schemaName string, tableName string) ([]ColumnInfo, error) {
	db.log.Debug("trace : entering GetTableSchema(%v, %v)", schemaName, tableName)
	var res []ColumnInfo
	err := pgxscan.Select(context.Background(), db.Conn, &res, tableSchema, schemaName, tableName)
	if err != nil {
		db.log.Error(SelectFailedInNWithErrorE, "GetTableSchema", err)
		return nil, err
	}
	if res == nil {
		db.log.Info(FunctionNReturnedNoResults, "GetTableSchema")
		return nil, pgx.ErrNoRows
	}
	return res, nil
}

// Exist returns true only if a table with the specified id exists in store.
func (db *PGX) Exist(id int) bool {
	db.log.Debug("trace : entering Exist(%v)", id)
	count, err := db.dbi.GetQueryInt(existTable, id)
	if err != nil {
		db.log.Error("Exist(%v) could not be retrieved from DB. failed db.Query err: %v", id, err)
		return false
	}
	if count > 0 {
		db.log.Info(" Exist(%v) id does exist  count:%v", id, count)
		return true
	} else {
		db.log.Info(" Exist(%v) id does not exist count:%v", id, count)
		return false
	}
}

// Count returns the number of table stored in DB
func (db *PGX) Count(params CountParams) (int, error) {
	db.log.Debug("trace : entering Count()")
	var (
		count int
		err   error
	)
	queryCount := tablesCount
	if params.SchemaName != nil {
		queryCount += "AND n.nspname = '$1'"
		count, err = db.dbi.GetQueryInt(queryCount, &params.SchemaName)

	} else {
		count, err = db.dbi.GetQueryInt(queryCount)
	}
	if err != nil {
		db.log.Error("Count() could not be retrieved from DB. failed db.Query err: %v", err)
		return 0, err
	}
	return count, nil
}

// ListSchemas returns the list of existing schemas.
func (db *PGX) ListSchemas() ([]string, error) {
	db.log.Debug("trace : entering ListSchema")
	var (
		res []string
		err error
	)
	err = pgxscan.Select(context.Background(), db.Conn, &res, schemasList)
	if err != nil {
		db.log.Error(SelectFailedInNWithErrorE, "ListSchema", err)
		return nil, err
	}
	if res == nil {
		db.log.Info(FunctionNReturnedNoResults, "ListSchema")
		return nil, pgx.ErrNoRows
	}
	return res, nil
}

// GetDb will return the database connection
func (db *PGX) GetDb() database.DB {
	return db.GetDb()
}
