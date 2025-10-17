package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/lao-tseu-is-alive/ArrowFlightPg/pkg/version"
	"github.com/lao-tseu-is-alive/go-cloud-k8s-common-libs/pkg/golog"
)

const (
	APP = "parquetInfo"
)

func main() {
	l, err := golog.NewLogger("zap", golog.TraceLevel, APP)
	if err != nil {
		panic(fmt.Sprintf("💥💥 error log.NewLogger error: %v'\n", err))
	}
	l.Info("🚀🚀 Starting App:'%s', ver:%s, from: %s", APP, version.VERSION, version.REPOSITORY)

	if len(os.Args) < 2 {
		l.Fatal("💥💥 error missing argument parquet file path")
	}
	parquetFilePath := os.Args[1]
	fmt.Printf("using parquet file path : %s\n", parquetFilePath)

	// Open the Parquet file
	f, err := os.Open(parquetFilePath)
	if err != nil {
		l.Fatal("💥💥 error opening parquet file: %v", err)
	}
	defer f.Close()

	// Create a Parquet file reader
	reader, err := file.NewParquetReader(f)
	if err != nil {
		l.Fatal("💥💥 error creating parquet reader: %v", err)
	}

	meta := reader.MetaData()
	if meta.CreatedBy != nil {
		fmt.Printf("File creator  : %s\n", *meta.CreatedBy)
	}
	if meta.FileMetaData != nil {
		fileMeta := meta.FileMetaData
		fmt.Printf("Parquet version: %d\n", fileMeta.Version)
	}

	fmt.Printf("Number of rows: %d\n", meta.NumRows)
	fmt.Printf("Number of row groups: %d\n", reader.NumRowGroups())

	schema := meta.Schema
	fmt.Printf("Schema:\n%s\n\n", schema.String())

	for i := 0; i < reader.NumRowGroups(); i++ {
		rg := reader.RowGroup(i)
		rgMeta := rg.MetaData()
		fmt.Printf("##  --- Row Group %d ---\n", i)
		fmt.Printf("##  Number of rows: %d\n", rgMeta.NumRows())
		fmt.Printf("##  Total byte size: %d\n", rgMeta.TotalByteSize())
		fmt.Printf("##  Number of columns: %d\n", rgMeta.NumColumns())
		fmt.Println("  Columns info:")
		fmt.Printf("\t %5s | %20s | %10s | %8s | %12s | %15s | %8s | %8s\n", "Col", "Path", "Type", "Values", "Comp", "Encodings", "CSize", "USize")
		fmt.Printf("\t %s\n", strings.Repeat("-", 103))
		for j := 0; j < rgMeta.NumColumns(); j++ {
			cm, err := rgMeta.ColumnChunk(j)
			if err != nil {
				l.Fatal("error getting column metadata: %v", err)
			}
			path := strings.Join(cm.PathInSchema(), ".")
			typ := cm.Type().String()
			numValues := cm.NumValues()
			comp := cm.Compression().String()
			enc := fmt.Sprintf("%v", cm.Encodings())
			cSize := cm.TotalCompressedSize()
			uSize := cm.TotalUncompressedSize()
			fmt.Printf("\t %5d | %20s | %10s | %8d | %12s | %15s | %8d | %8d\n", j+1, path, typ, numValues, comp, enc, cSize, uSize)
		}
	}
}
