package table

import (
	"fmt"
	"sync"

	"github.com/caravan/essentials/message"
	"github.com/caravan/streaming/table"
)

type (
	// Table is the internal implementation of a Table
	Table struct {
		sync.RWMutex
		key       table.KeySelector
		cols      []table.Column
		selectors []table.Selector
		lookups   columnIndexes
		rows      tableRows
	}

	columnIndexes map[table.ColumnName]int
	tableRows     map[table.Key]table.Relation
)

// Error messages
const (
	ErrKeyNotFound    = "key not found in table: %s"
	ErrColumnNotFound = "column not found in table: %s"
)

// Make instantiates a new internal Table instance
func Make(key table.KeySelector, cols ...table.Column) *Table {
	return &Table{
		key:       key,
		cols:      cols,
		selectors: makeSelectors(cols),
		lookups:   makeLookups(cols),
		rows:      tableRows{},
	}
}

// KeySelector returns the KeySelector for this Table
func (t *Table) KeySelector() table.KeySelector {
	return t.key
}

// Columns returns the defined Columns for this Table
func (t *Table) Columns() []table.Column {
	return t.cols
}

// Update adds or overwrites an Event in the Table. The Event is
// associated with a Key that is selected from the Event using the
// Table's KeySelector
func (t *Table) Update(e message.Event) (table.Relation, error) {
	t.Lock()
	defer t.Unlock()
	k, err := t.key(e)
	if err != nil {
		return nil, err
	}
	row := make(table.Relation, len(t.lookups))
	for i, s := range t.selectors {
		v, err := s(e)
		if err != nil {
			return nil, err
		}
		row[i] = v
	}
	t.rows[k] = row
	return row, nil
}

// Selector constructs a ColumnSelector for this Table, the results of
// which match the provided column names
func (t *Table) Selector(c ...table.ColumnName) (table.ColumnSelector, error) {
	sel, err := t.columnSelect(c)
	if err != nil {
		return nil, err
	}
	return t.tableSelector(sel)
}

func (t *Table) columnSelect(c []table.ColumnName) ([]int, error) {
	sel := make([]int, len(c))
	for i, name := range c {
		s, ok := t.lookups[name]
		if !ok {
			return nil, fmt.Errorf(ErrColumnNotFound, name)
		}
		sel[i] = s
	}
	return sel, nil
}

func (t *Table) tableSelector(indexes []int) (table.ColumnSelector, error) {
	return func(key table.Key) (table.Relation, error) {
		if e, ok := t.get(key); ok {
			res := make(table.Relation, len(indexes))
			for out, in := range indexes {
				res[out] = e[in]
			}
			return res, nil
		}
		return nil, fmt.Errorf(ErrKeyNotFound, key)
	}, nil
}

func (t *Table) get(k table.Key) (table.Relation, bool) {
	t.RLock()
	defer t.RUnlock()
	res, ok := t.rows[k]
	return res, ok
}

func makeSelectors(cols []table.Column) []table.Selector {
	res := make([]table.Selector, len(cols))
	for i, c := range cols {
		res[i] = c.Selector()
	}
	return res
}

func makeLookups(cols []table.Column) columnIndexes {
	res := make(columnIndexes, len(cols))
	for i, c := range cols {
		res[c.Name()] = i
	}
	return res
}
