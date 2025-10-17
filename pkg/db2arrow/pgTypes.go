package db2arrow

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/lao-tseu-is-alive/ArrowFlightPg/pkg/db"
)

// MapDataType converts PostgresSQL data types to Apache Arrow data types.
func MapDataType(pgType string) (arrow.DataType, error) {
	switch pgType {
	case "smallint":
		return arrow.PrimitiveTypes.Int16, nil
	case "integer":
		return arrow.PrimitiveTypes.Int32, nil
	case "bigint":
		return arrow.PrimitiveTypes.Int64, nil
	case "real":
		return arrow.PrimitiveTypes.Float32, nil
	case "double precision":
		return arrow.PrimitiveTypes.Float64, nil
	case "numeric", "decimal":
		// Simplified to Float64; use Decimal128 for precision if needed
		return arrow.PrimitiveTypes.Float64, nil
	case "text", "character varying", "character":
		return arrow.BinaryTypes.String, nil
	case "bytea":
		return arrow.BinaryTypes.Binary, nil
	case "boolean":
		return arrow.FixedWidthTypes.Boolean, nil
	case "date":
		return arrow.FixedWidthTypes.Date32, nil
	case "timestamp without time zone", "timestamp with time zone":
		// Uses microsecond precision; timezone ignored for simplicity
		return &arrow.TimestampType{Unit: arrow.Microsecond}, nil
	default:
		return nil, fmt.Errorf("unsupported PostgreSQL data type: %s", pgType)
	}
}

// MapToArrowSchema creates an Arrow schema from PostgresSQL column metadata.
func MapToArrowSchema(columns []db.ColumnInfo) (*arrow.Schema, error) {
	fields := make([]arrow.Field, len(columns))
	for i, col := range columns {
		// discard fields with unsupported data types
		if col.DataType != "tsvector" && col.DataType != "USER-DEFINED" {
			dt, err := MapDataType(col.DataType)
			if err != nil {
				return nil, fmt.Errorf("failed to map column %s: %w", col.Name, err)
			}
			fields[i] = arrow.Field{Name: col.Name, Type: dt, Nullable: col.Nullable}
		}
	}
	return arrow.NewSchema(fields, nil), nil
}
