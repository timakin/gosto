package gosto

import (
	"fmt"
	"reflect"

	"cloud.google.com/go/datastore"
)

// Count returns the number of results for the query.
func (g *Gosto) Count(q *datastore.Query) (int, error) {
	return g.DSClient.Count(g.Context, q)
}

// GetAll runs the query and returns all the keys that match the query, as well
// as appending the values to dst, setting the Gosto key fields of dst, and
// caching the returned data in local memory.
//
// For "keys-only" queries dst can be nil, however if it is not, then GetAll
// appends zero value structs to dst, only setting the Gosto key fields.
// No data is cached with "keys-only" queries.
//
// See: https://developers.google.com/appengine/docs/go/datastore/reference#Query.GetAll
func (g *Gosto) GetAll(q *datastore.Query, dst interface{}) ([]*datastore.Key, error) {
	v := reflect.ValueOf(dst)
	vLenBefore := 0

	if dst != nil {
		if v.Kind() != reflect.Ptr {
			return nil, fmt.Errorf("Gosto: Expected dst to be a pointer to a slice or nil, got instead: %v", v.Kind())
		}

		v = v.Elem()
		if v.Kind() != reflect.Slice {
			return nil, fmt.Errorf("Gosto: Expected dst to be a pointer to a slice or nil, got instead: %v", v.Kind())
		}

		vLenBefore = v.Len()
	}

	keys, err := g.DSClient.GetAll(g.Context, q, dst)
	if err != nil {
		if errFieldMismatch(err) {
			if IgnoreFieldMismatch {
				err = nil
			}
		} else {
			return keys, err
		}
	}
	if dst == nil || len(keys) == 0 {
		return keys, err
	}

	elemType := v.Type().Elem()
	ptr := false
	if elemType.Kind() == reflect.Ptr {
		elemType = elemType.Elem()
		ptr = true
	}

	if elemType.Kind() != reflect.Struct {
		return keys, fmt.Errorf("Gosto: Expected struct, got instead: %v", elemType.Kind())
	}

	for i := 0; i < len(keys); i++ {
		ev := reflect.New(elemType)
		if !ptr {
			ev = ev.Elem()
		}

		v.Set(reflect.Append(v, ev))
	}

	for i, k := range keys {
		var e interface{}
		vi := v.Index(vLenBefore + i)
		if vi.Kind() == reflect.Ptr {
			e = vi.Interface()
		} else {
			e = vi.Addr().Interface()
		}

		if err := g.setStructKey(e, k); err != nil {
			return nil, err
		}
	}

	return keys, err
}

// Run runs the query.
func (g *Gosto) Run(q *datastore.Query) *Iterator {
	return &Iterator{
		g: g,
		i: g.DSClient.Run(g.Context, q),
	}
}

type Iterator struct {
	g *Gosto
	i *datastore.Iterator
}

func (t *Iterator) Cursor() (datastore.Cursor, error) {
	return t.i.Cursor()
}

func (t *Iterator) Next(dst interface{}) (*datastore.Key, error) {
	k, err := t.i.Next(dst)
	if err != nil && (!IgnoreFieldMismatch || !errFieldMismatch(err)) {
		return k, err
	}

	if dst != nil {
		// Update the struct to have correct key info
		t.g.setStructKey(dst, k)
	}

	return k, nil
}
