package table

import (
	"github.com/caravan/essentials/event"
	"github.com/caravan/essentials/id"
)

type (
	// Key is used to uniquely identify an Event in the Table
	Key = id.ID

	// Value is a placeholder for what will eventually be a generic
	Value interface{}

	// KeySelector is used to extract the Key from an Event
	KeySelector func(event.Event) (Key, error)

	// Selector is used to extract a Value from an Event
	Selector func(event.Event) (Value, error)

	// ColumnName is exactly what you think it is
	ColumnName string

	// Column describes a column to be selected from a Table. The
	// description includes the column's name and a Selector for
	// retrieving the column's value from an Event
	Column interface {
		Name() ColumnName
		Selector() Selector
	}

	// Relation describes a set of associated Values
	Relation []Value

	// ColumnSelector is a function that is capable of retrieving a
	// pre-defined set of Column values from a Table
	ColumnSelector func(Key) (Relation, error)

	// Table is an interface that associates a Key with an Event. The Key
	// is selected from the Event using the Table's KeySelector
	Table interface {
		// KeySelector returns the key selector for this Table. This
		// selector can be used to retrieve the Key from an Event for this
		// Table independent of calls to Update or Selector
		KeySelector() KeySelector

		// Columns returns the column definitions that are defined as part
		// of this Table
		Columns() []Column

		// Update stores an Event in the Table, associating it with the
		// Key retrieved using the KeySelector. If an Event was already
		// associated with the Key, it is overwritten. The materialized
		// Relation is returned.
		Update(event.Event) (Relation, error)

		// Selector creates a ColumnSelector based on the specified
		// ColumnNames. The ColumnSelector can then be used to retrieve
		// the specified column values based on a provided Key
		Selector(...ColumnName) (ColumnSelector, error)
	}
)
