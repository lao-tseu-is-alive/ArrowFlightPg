package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
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
	fmt.Printf("parquet file path : %s\n", parquetFilePath)

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

	fileMetadata := reader.MetaData()
	fmt.Printf("Version : %d\n", fileMetadata.GetVersion())
	fmt.Printf("Created by : %s\n", fileMetadata.GetCreatedBy())

	fmt.Printf("Number of rows : %d\n", fileMetadata.GetNumRows())
	fmt.Printf("Number of row groups: %d\n", reader.NumRowGroups())

	schema := fileMetadata.Schema
	fmt.Printf("Schema:\n%s\n\n", schema.String())

	for i := 0; i < reader.NumRowGroups(); i++ {
		rgIdxRdr := reader.RowGroup(i)
		rgMeta := rgIdxRdr.MetaData()
		fmt.Printf("##  --- Row Group %d ---\n", i)
		fmt.Printf("##  Number of rows: %d\n", rgMeta.NumRows())
		fmt.Printf("##  Total byte size: %d\n", rgMeta.TotalByteSize())
		fmt.Printf("##  Number of columns: %d\n", rgMeta.NumColumns())
		fmt.Println("  Columns info:")
		fmt.Printf("\t %5s | %20s | %10s | %8s | %8s |  %8s | %8s | %8s | %12s | %15s | %8s | %8s\n",
			"Col", "Path", "Type", "Values", "Min", "Max", "Nullcount", "DistinctCount", "Comp", "Encodings", "CSize", "USize")
		fmt.Printf("\t %s\n", strings.Repeat("-", 103))
		for idx, c := range fileMetadata.Schema.Columns() {
			//l.Debug("inspecting column %d: %s (%d)", idx, c.LogicalType(), c.TypeLength())
			cm, err := rgMeta.ColumnChunk(idx)
			if err != nil {
				l.Fatal("error getting column %d, %s, metadata: %v", idx, c.Name(), err)
			}
			stats, err := cm.Statistics()
			if err != nil {
				l.Fatal("error statistics for column %d, %s, metadata: %v", idx, c.Name(), err)
			}
			/*
				if stats.HasMinMax() {
					fmt.Printf(", Min: %v, Max: %v",
						metadata.GetStatValue(stats.Type(), stats.EncodeMin()),
						metadata.GetStatValue(stats.Type(), stats.EncodeMax()))
				}
			*/
			NullCount := int64(0)
			if stats.HasNullCount() {
				//fmt.Printf(", Null Values: %d", stats.NullCount())
				NullCount = stats.NullCount()
			}
			DistinctCount := int64(0)
			if stats.HasDistinctCount() {
				//fmt.Printf(", Distinct Values: %d", stats.DistinctCount())
				DistinctCount = stats.DistinctCount()
			}

			path := strings.Join(cm.PathInSchema(), ".")
			typ := cm.Type()
			numValues := cm.NumValues()
			comp := cm.Compression().String()
			enc := fmt.Sprintf("%v", cm.Encodings())
			cSize := cm.TotalCompressedSize()
			uSize := cm.TotalUncompressedSize()
			if stats.HasMinMax() {
				fmt.Printf("\t %5d | %20s | %10s | %8d | %8v | %8v | %8d | %8d | %12s | %15s | %8d | %8d\n",
					idx+1, path, typ, numValues,
					metadata.GetStatValue(stats.Type(), stats.EncodeMin()),
					metadata.GetStatValue(stats.Type(), stats.EncodeMax()),
					NullCount, DistinctCount,
					comp, enc, cSize, uSize)
			} else {
				fmt.Printf("\t %5d | %20s | %10s | %8d | %8v | %8v | %8d | %8d | %12s | %15s | %8d | %8d\n",
					idx+1, path, typ, numValues, nil, nil, NullCount, DistinctCount, comp, enc, cSize, uSize)
			}
		}
	}
}
