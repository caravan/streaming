package table

import (
	"fmt"
	"sync"

	"github.com/caravan/streaming/table"
)

// Table is the internal implementation of a table.Table
type Table[Key comparable, Value any] struct {
	sync.RWMutex
	names   []table.ColumnName
	indexes map[table.ColumnName]int
	rows    map[Key][]Value
}

func Make[Key comparable, Value any](
	c ...table.ColumnName,
) (table.Table[Key, Value], error) {
	if err := checkColumnDuplicates(c); err != nil {
		return nil, err
	}
	indexes := map[table.ColumnName]int{}
	for i, n := range c {
		indexes[n] = i
	}
	return &Table[Key, Value]{
		names:   c,
		indexes: indexes,
		rows:    map[Key][]Value{},
	}, nil
}

func (t *Table[_, _]) Columns() []table.ColumnName {
	return t.names[:]
}

func (t *Table[Key, Value]) Getter(
	c ...table.ColumnName,
) (table.Getter[Key, Value], error) {
	indexes, err := t.columnIndexes(c)
	if err != nil {
		return nil, err
	}
	return func(k Key) ([]Value, error) {
		t.RLock()
		defer t.RUnlock()

		if e, ok := t.rows[k]; ok {
			res := make([]Value, len(indexes))
			for out, in := range indexes {
				res[out] = e[in]
			}
			return res, nil
		}
		return nil, fmt.Errorf(table.ErrKeyNotFound, k)
	}, nil
}

func (t *Table[Key, Value]) Setter(
	c ...table.ColumnName,
) (table.Setter[Key, Value], error) {
	indexes, err := t.columnIndexes(c)
	if err != nil {
		return nil, err
	}
	if err := checkColumnDuplicates(c); err != nil {
		return nil, err
	}

	return func(k Key, v ...Value) error {
		t.Lock()
		defer t.Unlock()

		if len(v) != len(indexes) {
			return fmt.Errorf(
				table.ErrValueCountRequired, len(indexes), len(v),
			)
		}
		e, ok := t.rows[k]
		if !ok {
			e = make([]Value, len(t.names))
		}
		for in, out := range indexes {
			e[out] = v[in]
		}
		t.rows[k] = e
		return nil
	}, nil
}

func (t *Table[_, _]) columnIndexes(c []table.ColumnName) ([]int, error) {
	sel := make([]int, len(c))
	for i, name := range c {
		s, ok := t.indexes[name]
		if !ok {
			return nil, fmt.Errorf(table.ErrColumnNotFound, name)
		}
		sel[i] = s
	}
	return sel, nil
}

func checkColumnDuplicates(c []table.ColumnName) error {
	names := map[table.ColumnName]bool{}
	for _, n := range c {
		if _, ok := names[n]; ok {
			return fmt.Errorf(table.ErrDuplicateColumnName, n)
		}
		names[n] = true
	}
	return nil
}
