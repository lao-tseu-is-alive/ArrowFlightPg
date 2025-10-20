package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/lao-tseu-is-alive/ArrowFlightPg/pkg/version"
	"github.com/lao-tseu-is-alive/go-cloud-k8s-common-libs/pkg/golog"
)

const (
	APP              = "parquetInfo"
	maxStringDisplay = 18
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
		fmt.Printf("%5s | %20s | %10s | %8s | %18s | %18s | %8s| %8s | %12s | %16s | %8s | %8s\n",
			"Col", "Path", "Type", "Values", "Min", "Max", "NullCount", "Distinct", "Comp", "Encodings", "CSize", "USize")
		fmt.Printf("%s\n", strings.Repeat("-", 172))
		for idx, c := range fileMetadata.Schema.Columns() {
			//l.Debug("inspecting column %d: %s (%d)", idx, c.LogicalType(), c.TypeLength())
			cm, err := rgMeta.ColumnChunk(idx)
			if err != nil {
				l.Fatal("error getting column %d, %s, metadata: %v", idx, c.Name(), err)
			}

			path := strings.Join(cm.PathInSchema(), ".")
			typ := cm.Type()
			typString := typ.String()
			if typ == parquet.Types.ByteArray {
				if c.LogicalType().String() == "String" {
					typString = "VARCHAR"
				}
			}
			numValues := cm.NumValues()
			comp := cm.Compression().String()
			enc := fmt.Sprintf("%v", cm.Encodings())
			switch enc {
			case "[PLAIN RLE_DICTIONARY]":
				enc = "[PLAIN RLE_DICT]"
			case "[PLAIN_DICTIONARY PLAIN RLE]":
				enc = "[PLAIN_DICT RLE]"
			default:

			}

			cSize := cm.TotalCompressedSize()
			uSize := cm.TotalUncompressedSize()
			stats, err := cm.Statistics()
			if err != nil {
				l.Fatal("error statistics for column %d, %s, metadata: %v", idx, c.Name(), err)
			}
			NullCount := int64(0)
			DistinctCount := int64(0)
			var minVal, maxVal interface{}
			minVal = nil // Or use a placeholder like "N/A" if you prefer
			maxVal = nil // Or use a placeholder like "N/A"
			if stats != nil {
				if stats.HasNullCount() {
					//fmt.Printf(", Null Values: %d", stats.NullCount())
					NullCount = stats.NullCount()
				}
				if stats.HasDistinctCount() {
					//fmt.Printf(", Distinct Values: %d", stats.DistinctCount())
					DistinctCount = stats.DistinctCount()
				}
				if stats.HasMinMax() {
					minRaw := metadata.GetStatValue(stats.Type(), stats.EncodeMin())
					maxRaw := metadata.GetStatValue(stats.Type(), stats.EncodeMax())

					// Check if the type is BYTE_ARRAY and convert to string for printing
					if typ == parquet.Types.ByteArray {
						if minBytes, ok := minRaw.([]byte); ok {
							// Convert byte slice to string
							if len(string(minBytes)) > maxStringDisplay {
								minVal = string(minBytes)[0:maxStringDisplay-1] + "…"
							} else {
								minVal = string(minBytes)
							}
						} else {
							minVal = minRaw // Fallback in case of unexpected type
						}
						if maxBytes, ok := maxRaw.([]byte); ok {
							// Convert byte slice to string
							if len(string(maxBytes)) > maxStringDisplay {
								maxVal = string(maxBytes)[0:maxStringDisplay-1] + "…"
							} else {
								maxVal = string(maxBytes)
							}
						} else {
							maxVal = maxRaw // Fallback
						}
					} else {
						// Not a BYTE_ARRAY, use the raw value
						minVal = minRaw
						maxVal = maxRaw
					}
				}
			}
			fmt.Printf("%5d | %20s | %10s | %8d | %18v | %18v | %8d | %8d | %12s | %16s | %8d | %8d\n",
				idx+1, path, typString, numValues, minVal, maxVal, NullCount, DistinctCount, comp, enc, cSize, uSize)
		}
	}
}
