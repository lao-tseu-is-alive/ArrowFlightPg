package db2parquet

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lao-tseu-is-alive/ArrowFlightPg/pkg/db"
	"github.com/lao-tseu-is-alive/ArrowFlightPg/pkg/db2arrow"
	"github.com/lao-tseu-is-alive/go-cloud-k8s-common-libs/pkg/golog"
)

// CreateParquetFileFromDbTable create a parquet file from a db schema and table
func CreateParquetFileFromDbTable(
	ctx context.Context,
	dbConn *pgxpool.Pool,
	schemaName string,
	tableName string,
	tableColumns []db.ColumnInfo,
	parquetFilePath string,
	batchSize int,
	log golog.MyLogger) error {
	// Step 2: Map to Arrow schema
	schema, err := db2arrow.MapToArrowSchema(tableColumns)
	if err != nil {
		return fmt.Errorf("error doing db2arrow.MapToArrowSchema() : %v", err)
	}
	log.Info("Arrow schema created for table %s.%s", schemaName, tableName)
	// Step 3: Set up Parquet file writer
	file, err := os.Create(parquetFilePath)
	if err != nil {
		return fmt.Errorf("failed to create Parquet file %s: %w", parquetFilePath, err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Error("failed to close Parquet file: %v", err)
		}
	}(file)

	props := parquet.NewWriterProperties()
	arrowProps := pqarrow.DefaultWriterProps()
	writer, err := pqarrow.NewFileWriter(schema, file, props, arrowProps)
	if err != nil {
		return fmt.Errorf("failed to create Parquet writer: %w", err)
	}
	defer func(writer *pqarrow.FileWriter) {
		err := writer.Close()
		if err != nil {
			log.Error("failed to close Parquet writer: %v", err)
		}
	}(writer)

	log.Info("Parquet writer created for table %s.%s", schemaName, tableName)

	// Step 4: Start a read-only transaction and declare a cursor
	tx, err := dbConn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer func(tx pgx.Tx, ctx context.Context) {
		err := tx.Rollback(ctx)
		if err != nil {
			log.Error("failed to rollback transaction: %v", err)
		}
	}(tx, ctx) // Rollback if not committed

	cursorName := "convert_cursor"
	_, err = tx.Exec(ctx, fmt.Sprintf("DECLARE %s CURSOR FOR SELECT * FROM %s", cursorName, tableName))
	if err != nil {
		return fmt.Errorf("failed to declare cursor: %w", err)
	}
	log.Info("Cursor declared for table %s.%s", schemaName, tableName)

	// Step 5: Initialize Arrow builders
	mem := memory.NewGoAllocator()
	builders := make([]array.Builder, len(schema.Fields()))
	for i, field := range schema.Fields() {
		builders[i] = array.NewBuilder(mem, field.Type)
		//defer builders[i].Release()
	}

	// Step 6: Fetch and process data in batches
	batchNumber := 0
	for {
		// Fetch a batch of rows
		batchNumber++
		rows, err := tx.Query(ctx, fmt.Sprintf("FETCH %d FROM %s", batchSize, cursorName))
		if err != nil {
			return fmt.Errorf("failed to fetch from cursor: %w", err)
		}
		log.Info("Fetched batch %d of %d rows for table %s.%s", batchNumber, batchSize, schemaName, tableName)

		// Prepare builders for the batch
		for _, builder := range builders {
			builder.Resize(0)
			builder.Reserve(batchSize)
		}

		// Process each row in the batch
		rowCount := 0
		for rows.Next() {
			rowCount++
			if rowCount%10 == 0 {
				log.Info("Processing row %d", rowCount)
			}
			values, err := rows.Values()
			if err != nil {
				rows.Close()
				return fmt.Errorf("failed to get row values: %w", err)
			}

			for i, val := range values {
				if val == nil {
					builders[i].AppendNull()
					continue
				}
				switch b := builders[i].(type) {
				case *array.Int16Builder:
					v, ok := val.(int16)
					if !ok {
						return fmt.Errorf("type mismatch for %s: expected int16", schema.Field(i).Name)
					}
					b.Append(v)
				case *array.Int32Builder:
					v, ok := val.(int32)
					if !ok {
						return fmt.Errorf("type mismatch for %s: expected int32", schema.Field(i).Name)
					}
					b.Append(v)
				case *array.Int64Builder:
					v, ok := val.(int64)
					if !ok {
						return fmt.Errorf("type mismatch for %s: expected int64", schema.Field(i).Name)
					}
					b.Append(v)
				case *array.Float32Builder:
					v, ok := val.(float32)
					if !ok {
						return fmt.Errorf("type mismatch for %s: expected float32", schema.Field(i).Name)
					}
					b.Append(v)
				case *array.Float64Builder:
					v, ok := val.(float64)
					if !ok {
						return fmt.Errorf("type mismatch for %s: expected float64", schema.Field(i).Name)
					}
					b.Append(v)
				case *array.StringBuilder:
					v, ok := val.(string)
					if !ok {
						return fmt.Errorf("type mismatch for %s: expected string", schema.Field(i).Name)
					}
					b.Append(v)
				case *array.BinaryBuilder:
					v, ok := val.([]byte)
					if !ok {
						return fmt.Errorf("type mismatch for %s: expected []byte", schema.Field(i).Name)
					}
					b.Append(v)
				case *array.BooleanBuilder:
					v, ok := val.(bool)
					if !ok {
						return fmt.Errorf("type mismatch for %s: expected bool", schema.Field(i).Name)
					}
					b.Append(v)
				case *array.Date32Builder:
					v, ok := val.(time.Time)
					if !ok {
						return fmt.Errorf("type mismatch for %s: expected time.Time", schema.Field(i).Name)
					}
					days := int32(v.Sub(time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)).Hours() / 24)
					b.Append(arrow.Date32(days))
				case *array.TimestampBuilder:
					v, ok := val.(time.Time)
					if !ok {
						return fmt.Errorf("type mismatch for %s: expected time.Time", schema.Field(i).Name)
					}
					b.Append(arrow.Timestamp(v.UnixNano() / 1000)) // Microseconds
				default:
					return fmt.Errorf("unsupported type for column %s", schema.Field(i).Name)
				}
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return fmt.Errorf("error processing rows: %w", err)
		}

		// Exit if no rows were fetched (end of data)
		if rowCount == 0 {
			break
		}
		// Step 7: Create and write Arrow RecordBatch
		arrays := make([]arrow.Array, len(builders))
		for i, builder := range builders {
			arrays[i] = builder.NewArray()
			//defer arrays[i].Release()
		}
		record := array.NewRecord(schema, arrays, int64(rowCount))
		//defer record.Release()

		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write RecordBatch: %w", err)
		}
	}
	log.Info("All rows processed for table %s.%s", schemaName, tableName)
	// Step 8: Clean up cursor and commit transaction
	if _, err := tx.Exec(ctx, fmt.Sprintf("CLOSE %s", cursorName)); err != nil {
		return fmt.Errorf("failed to close cursor: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	log.Info("Transaction committed for table %s.%s", schemaName, tableName)

	// Step 9: Finalize Parquet file
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close Parquet writer: %w", err)
	}
	log.Info("Parquet writer closed for table %s.%s", schemaName, tableName)

	return nil
}
