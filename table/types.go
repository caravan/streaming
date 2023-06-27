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

	Updater[Msg any, Key comparable, Value any] interface {
		// Key returns the key selector for this Table. This selector can be
		// used to retrieve the Key from a message for this Updater independent
		// of calls to Update
		Key() KeySelector[Msg, Key]

		// Columns will return the column selector definitions that are defined
		// as part of this Updater. These selectors can be used to retrieve
		// Values from a message for this Updater independent of calls to
		// Update
		Columns() []ColumnSelector[Msg, Value]

		// Update extracts a Key and Column Values from a message and updates
		// the associated Table
		Update(Msg) error
	}

	// KeySelector is used to extract the Key from a message
	KeySelector[Msg any, Key comparable] func(Msg) (Key, error)

	// ColumnSelector describes a column to be extracted from a message. The
	// description includes the column's name and a ValueSelector for
	// retrieving the column's value from a message
	ColumnSelector[Msg, Value any] interface {
		Name() ColumnName
		Select(Msg) (Value, error)
	}

	// ValueSelector is used to extract a Value from a message
	ValueSelector[Msg, Value any] func(Msg) (Value, error)
)
