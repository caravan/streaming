package table

type (
	// Table is an interface that associates a Key with multiple named Columns.
	// The Key and Columns are selected using an Updater. Multiple Updaters are
	// able to act on a single Table
	Table[Key comparable, Value any] interface {
		// Columns will return the Column names defined for this Table
		Columns() []ColumnName

		// Getter creates a Getter based on the specified ColumnNames.
		Getter(...ColumnName) (Getter[Key, Value], error)

		// Setter creates a Setter based on the specified ColumnNames.
		Setter(...ColumnName) (Setter[Key, Value], error)
	}

	// ColumnName is exactly what you think it is
	ColumnName string

	// Getter is a function that is capable of retrieving a pre-defined set of
	// column Values from a Table based on the provided Key
	Getter[Key comparable, Value any] func(Key) ([]Value, error)

	// Setter is a function that is capable of updating a pre-defined set of
	// column Values in a Table based on the provided Key
	Setter[Key comparable, Value any] func(Key, ...Value) error
)

// Error messages
const (
	ErrKeyNotFound         = "key not found in table: %v"
	ErrColumnNotFound      = "column not found in table: %s"
	ErrDuplicateColumnName = "column name duplicated in table: %s"
	ErrValueCountRequired  = "%d values are required, you provided %d"
)
