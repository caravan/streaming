package table

import (
	"fmt"
	"sync"
)

type (
	// Table is the internal implementation of a Table
	table[Msg, Value any] struct {
		sync.RWMutex
		key       KeySelector[Msg]
		cols      []Column[Msg, Value]
		selectors []Selector[Msg, Value]
		lookups   columnIndexes
		rows      tableRows[Value]
	}

	columnIndexes        map[ColumnName]int
	tableRows[Value any] map[Key]Relation[Value]
)

// Error messages
const (
	ErrKeyNotFound    = "key not found in table: %s"
	ErrColumnNotFound = "column not found in table: %s"
)

// Make instantiates a new internal Table instance
func Make[Msg, Value any](
	key KeySelector[Msg], cols ...Column[Msg, Value],
) Table[Msg, Value] {
	return &table[Msg, Value]{
		key:       key,
		cols:      cols,
		selectors: makeSelectors(cols),
		lookups:   makeLookups(cols),
		rows:      tableRows[Value]{},
	}
}

// KeySelector returns the KeySelector for this Table
func (t *table[Msg, _]) KeySelector() KeySelector[Msg] {
	return t.key
}

// Columns returns the defined Columns for this Table
func (t *table[Msg, Value]) Columns() []Column[Msg, Value] {
	return t.cols
}

// Update adds or overwrites an message in the Table. The message is associated
// with a Key that is selected from the message using the Table's KeySelector
func (t *table[Msg, Value]) Update(msg Msg) (Relation[Value], error) {
	t.Lock()
	defer t.Unlock()
	k, err := t.key(msg)
	if err != nil {
		return nil, err
	}
	row := make(Relation[Value], len(t.lookups))
	for i, s := range t.selectors {
		v, err := s(msg)
		if err != nil {
			return nil, err
		}
		row[i] = v
	}
	t.rows[k] = row
	return row, nil
}

// Selector constructs a ColumnSelector for this Table, the results of which
// match the provided column names
func (t *table[Msg, Value]) Selector(
	c ...ColumnName,
) (ColumnSelector[Value], error) {
	sel, err := t.columnSelect(c)
	if err != nil {
		return nil, err
	}
	return t.tableSelector(sel)
}

func (t *table[_, _]) columnSelect(c []ColumnName) ([]int, error) {
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

func (t *table[_, Value]) tableSelector(
	indexes []int,
) (ColumnSelector[Value], error) {
	return func(key Key) (Relation[Value], error) {
		if e, ok := t.get(key); ok {
			res := make(Relation[Value], len(indexes))
			for out, in := range indexes {
				res[out] = e[in]
			}
			return res, nil
		}
		return nil, fmt.Errorf(ErrKeyNotFound, key)
	}, nil
}

func (t *table[_, Value]) get(k Key) (Relation[Value], bool) {
	t.RLock()
	defer t.RUnlock()
	res, ok := t.rows[k]
	return res, ok
}

func makeSelectors[Msg, Value any](
	cols []Column[Msg, Value],
) []Selector[Msg, Value] {
	res := make([]Selector[Msg, Value], len(cols))
	for i, c := range cols {
		res[i] = c.Selector()
	}
	return res
}

func makeLookups[Msg, Value any](cols []Column[Msg, Value]) columnIndexes {
	res := make(columnIndexes, len(cols))
	for i, c := range cols {
		res[c.Name()] = i
	}
	return res
}
