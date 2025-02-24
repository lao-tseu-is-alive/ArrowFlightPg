package main

import (
	"fmt"
	"github.com/lao-tseu-is-alive/ArrowFlightPg/pkg/db"
	"github.com/lao-tseu-is-alive/ArrowFlightPg/pkg/version"
	"github.com/lao-tseu-is-alive/go-cloud-k8s-common-libs/pkg/config"
	"github.com/lao-tseu-is-alive/go-cloud-k8s-common-libs/pkg/database"
	"github.com/lao-tseu-is-alive/go-cloud-k8s-common-libs/pkg/golog"
	"github.com/lao-tseu-is-alive/go-cloud-k8s-common-libs/pkg/tools"
	"os"
	"runtime"
)

const (
	APP              = "getParquetFromPgDb"
	defaultDBPort    = 5432
	defaultDBIp      = "127.0.0.1"
	defaultDBSslMode = "prefer"
)

func main() {
	l, err := golog.NewLogger("zap", golog.TraceLevel, APP)
	if err != nil {
		panic(fmt.Sprintf("💥💥 error log.NewLogger error: %v'\n", err))
	}
	l.Info("🚀🚀 Starting App:'%s', ver:%s, from: %s", APP, version.VERSION, version.REPOSITORY)

	// read argument schema from command line
	if len(os.Args) < 2 {
		l.Fatal("💥💥 error missing argument schema name")
	}
	schemaName := os.Args[1]
	l.Info("using schema name : %s", schemaName)
	// read argument table from command line
	if len(os.Args) < 3 {
		l.Fatal("💥💥 error missing argument table name")
	}
	tableName := os.Args[2]
	l.Info("using table name : %s", tableName)

	dbDsn := config.GetPgDbDsnUrlFromEnvOrPanic(defaultDBIp, defaultDBPort, tools.ToSnakeCase(version.APP), version.AppSnake, defaultDBSslMode)
	dbInstance, err := database.GetInstance("pgx", dbDsn, runtime.NumCPU(), l)
	if err != nil {
		l.Fatal("💥💥 error doing database.GetInstance(pgx ...) error: %v", err)
	}
	defer dbInstance.Close()

	dbVersion, err := dbInstance.GetVersion()
	if err != nil {
		l.Fatal("💥💥 error doing dbConn.GetVersion() error: %v", err)
	}
	l.Info("connected to db version : %s", dbVersion)

	dbStore := db.GetStorageInstanceOrPanic("pgx", dbInstance, l)

	myTableColumns, err := dbStore.GetTableSchema(schemaName, tableName)
	if err != nil {
		l.Fatal("error doing dbStore.GetTableSchema() : %v", err)
	}
	// displaying all the schemas
	fmt.Printf("Found %d columns in schema: %s, table: %s\n", len(myTableColumns), schemaName, tableName)
	fmt.Printf("%20s\tType\tNullable \n", "Column")
	for _, column := range myTableColumns {
		fmt.Printf("%20s\t%s\t%v\n", column.Name, column.DataType, column.Nullable)
	}

}
