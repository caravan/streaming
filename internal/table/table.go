package table

import (
	"fmt"
	"sync"

	_table "github.com/caravan/streaming/table"
)

// the internal implementation of a Table
type table[Key comparable, Value any] struct {
	sync.RWMutex
	names   []_table.ColumnName
	indexes map[_table.ColumnName]int
	rows    map[Key][]Value
}

func Make[Key comparable, Value any](
	c ..._table.ColumnName,
) (_table.Table[Key, Value], error) {
	if err := checkColumnDuplicates(c); err != nil {
		return nil, err
	}
	indexes := map[_table.ColumnName]int{}
	for i, n := range c {
		indexes[n] = i
	}
	return &table[Key, Value]{
		names:   c,
		indexes: indexes,
		rows:    map[Key][]Value{},
	}, nil
}

func (t *table[_, _]) Columns() []_table.ColumnName {
	return t.names[:]
}

func (t *table[Key, Value]) Getter(
	c ..._table.ColumnName,
) (_table.Getter[Key, Value], error) {
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
		return nil, fmt.Errorf(_table.ErrKeyNotFound, k)
	}, nil
}

func (t *table[Key, Value]) Setter(
	c ..._table.ColumnName,
) (_table.Setter[Key, Value], error) {
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
				_table.ErrValueCountRequired, len(indexes), len(v),
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

func (t *table[_, _]) columnIndexes(c []_table.ColumnName) ([]int, error) {
	sel := make([]int, len(c))
	for i, name := range c {
		s, ok := t.indexes[name]
		if !ok {
			return nil, fmt.Errorf(_table.ErrColumnNotFound, name)
		}
		sel[i] = s
	}
	return sel, nil
}

func checkColumnDuplicates(c []_table.ColumnName) error {
	names := map[_table.ColumnName]bool{}
	for _, n := range c {
		if _, ok := names[n]; ok {
			return fmt.Errorf(_table.ErrDuplicateColumnName, n)
		}
		names[n] = true
	}
	return nil
}
