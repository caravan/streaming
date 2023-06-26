package table

import "github.com/caravan/essentials/id"

type (
	// Key is used to uniquely identify a message in the Table
	Key = id.ID

	// KeySelector is used to extract the Key from a message
	KeySelector[Msg any] func(Msg) (Key, error)

	// Selector is used to extract a Value from a message
	Selector[Msg, Value any] func(Msg) (Value, error)

	// ColumnName is exactly what you think it is
	ColumnName string

	// Column describes a column to be selected from a Table. The
	// description includes the column's name and a Selector for
	// retrieving the column's value from a message
	Column[Msg, Value any] interface {
		Name() ColumnName
		Selector() Selector[Msg, Value]
	}

	// Relation describes a set of associated Values
	Relation[Value any] []Value

	// ColumnSelector is a function that is capable of retrieving a
	// pre-defined set of Column values from a Table
	ColumnSelector[Value any] func(Key) (Relation[Value], error)

	// Table is an interface that associates a Key with a message. The Key
	// is selected from the message using the Table's KeySelector
	Table[Msg, Value any] interface {
		// KeySelector returns the key selector for this Table. This
		// selector can be used to retrieve the Key from a message for this
		// Table independent of calls to Update or Selector
		KeySelector() KeySelector[Msg]

		// Columns will return the column definitions that are defined as part
		// of this Table
		Columns() []Column[Msg, Value]

		// Update stores a message in the Table, associating it with the
		// Key retrieved using the KeySelector. If a message was already
		// associated with the Key, it is overwritten. The materialized
		// Relation is returned.
		Update(Msg) (Relation[Value], error)

		// Selector creates a ColumnSelector based on the specified
		// ColumnNames. The ColumnSelector can then be used to retrieve
		// the specified column values based on a provided Key
		Selector(...ColumnName) (ColumnSelector[Value], error)
	}
)
