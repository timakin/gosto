package gosto

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/pkg/errors"

	"cloud.google.com/go/datastore"
)

var (
	// IgnoreFieldMismatch decides whether *datastore.ErrFieldMismatch errors
	// should be silently ignored. This allows you to easily remove fields from structs.
	IgnoreFieldMismatch = true
)

// Gosto holds the app engine context and the request memory cache.
type Gosto struct {
	Context       context.Context
	DSClient      *datastore.Client
	inTransaction bool
	// KindNameResolver is used to determine what Kind to give an Entity.
	// Defaults to DefaultKindName
	KindNameResolver KindNameResolver
}

// NewGosto creates a new Gosto object from the given request.
func NewGosto(ctx context.Context, projectID string) (*Gosto, error) {
	client, err := datastore.NewClient(ctx, projectID)
	if err != nil {
		return nil, errors.Wrap(err, "Gosto: failed to initialize a datastore client.")
	}

	return &Gosto{
		Context:          ctx,
		DSClient:         client,
		KindNameResolver: DefaultKindName,
	}, nil
}

func (g *Gosto) extractKeys(src interface{}, putRequest bool) ([]*datastore.Key, error) {
	v := reflect.Indirect(reflect.ValueOf(src))
	if v.Kind() != reflect.Slice {
		return nil, fmt.Errorf("Gosto: value must be a slice or pointer-to-slice")
	}
	l := v.Len()

	keys := make([]*datastore.Key, l)
	for i := 0; i < l; i++ {
		vi := v.Index(i)
		key, hasStringID, err := g.getStructKey(vi.Interface())
		if err != nil {
			return nil, err
		}
		if !putRequest && key.Incomplete() {
			return nil, fmt.Errorf("Gosto: cannot find a key for struct - %v", vi.Interface())
		} else if putRequest && key.Incomplete() && hasStringID {
			return nil, fmt.Errorf("Gosto: empty string id on put")
		}
		keys[i] = key
	}
	return keys, nil
}

// Key is the same as KeyError, except nil is returned on error or if the key
// is incomplete.
func (g *Gosto) Key(src interface{}) *datastore.Key {
	if k, err := g.KeyError(src); err == nil {
		return k
	}
	return nil
}

// Kind returns src's datastore Kind or "" on error.
func (g *Gosto) Kind(src interface{}) string {
	if k, err := g.KeyError(src); err == nil {
		return k.Kind
	}
	return ""
}

// KeyError returns the key of src based on its properties.
func (g *Gosto) KeyError(src interface{}) (*datastore.Key, error) {
	key, _, err := g.getStructKey(src)
	return key, err
}

// RunInTransaction runs f in a transaction. It calls f with a transaction
// context tg that f should use for all App Engine operations.
//
// Otherwise similar to appengine/datastore.RunInTransaction:
// https://developers.google.com/appengine/docs/go/datastore/reference#RunInTransaction
func (g *Gosto) RunInTransaction(f func(tx *datastore.Transaction) error, opts ...datastore.TransactionOption) error {
	_, err := g.DSClient.RunInTransaction(g.Context, func(tx *datastore.Transaction) error {
		return f(tx)
	}, opts...)
	if err != nil {
		return err
	}
	return nil
}

// Put saves the entity src into the datastore based on src's key k. If k
// is an incomplete key, the returned key will be a unique key generated by
// the datastore.
func (g *Gosto) Put(src interface{}) (*datastore.Key, error) {
	ks, err := g.PutMulti([]interface{}{src})
	if err != nil {
		if me, ok := err.(datastore.MultiError); ok {
			return nil, me[0]
		}
		return nil, err
	}
	return ks[0], nil
}

const putMultiLimit = 500

// PutMulti is a batch version of Put.
//
// src must be a *[]S, *[]*S, *[]I, []S, []*S, or []I, for some struct type S,
// or some interface type I. If *[]I or []I, each element must be a struct pointer.
func (g *Gosto) PutMulti(src interface{}) ([]*datastore.Key, error) {
	keys, err := g.extractKeys(src, true) // allow incomplete keys on a Put request
	if err != nil {
		return nil, err
	}

	v := reflect.Indirect(reflect.ValueOf(src))
	mu := new(sync.Mutex)
	multiErr, any := make(datastore.MultiError, len(keys)), false
	goroutines := (len(keys)-1)/putMultiLimit + 1
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(i int) {
			defer wg.Done()
			lo := i * putMultiLimit
			hi := (i + 1) * putMultiLimit
			if hi > len(keys) {
				hi = len(keys)
			}
			rkeys, pmerr := datastore.PutMulti(keys[lo:hi], v.Slice(lo, hi).Interface())
			if pmerr != nil {
				mu.Lock()
				any = true // this flag tells PutMulti to return multiErr later
				mu.Unlock()
				merr, ok := pmerr.(datastore.MultiError)
				if !ok {
					for j := lo; j < hi; j++ {
						multiErr[j] = pmerr
					}
					return
				}
				copy(multiErr[lo:hi], merr)
			}
		}(i)
	}
	wg.Wait()

	if any {
		return keys, realError(multiErr)
	}
	return keys, nil
}

// Get loads the entity based on dst's key into dst
// If there is no such entity for the key, Get returns
// datastore.ErrNoSuchEntity.
func (g *Gosto) Get(dst interface{}) error {
	set := reflect.ValueOf(dst)
	if set.Kind() != reflect.Ptr {
		return fmt.Errorf("Gosto: expected pointer to a struct, got %#v", dst)
	}
	if !set.CanSet() {
		set = set.Elem()
	}
	dsts := []interface{}{dst}
	if err := g.GetMulti(dsts); err != nil {
		// Look for an embedded error if it's multi
		if me, ok := err.(datastore.MultiError); ok {
			return me[0]
		}
		// Not multi, normal error
		return err
	}
	set.Set(reflect.Indirect(reflect.ValueOf(dsts[0])))
	return nil
}

const getMultiLimit = 1000

// GetMulti is a batch version of Get.
//
// dst must be a *[]S, *[]*S, *[]I, []S, []*S, or []I, for some struct type S,
// or some interface type I. If *[]I or []I, each element must be a struct pointer.
func (g *Gosto) GetMulti(dst interface{}) error {
	keys, err := g.extractKeys(dst, false) // don't allow incomplete keys on a Get request
	if err != nil {
		return err
	}

	v := reflect.Indirect(reflect.ValueOf(dst))

	multiErr, anyErr := make(datastore.MultiError, len(keys)), false

	if g.inTransaction {
		// todo: support getMultiLimit in transactions
		if err := datastore.GetMulti(g.Context, keys, v.Interface()); err != nil {
			if merr, ok := err.(datastore.MultiError); ok {
				for i := 0; i < len(keys); i++ {
					if merr[i] != nil && (!IgnoreFieldMismatch || !errFieldMismatch(merr[i])) {
						anyErr = true // this flag tells GetMulti to return multiErr later
						multiErr[i] = merr[i]
					}
				}
			} else {
				anyErr = true // this flag tells GetMulti to return multiErr later
				for i := 0; i < len(keys); i++ {
					multiErr[i] = err
				}
			}
			if anyErr {
				return realError(multiErr)
			}
		}
		return nil
	}

	var dskeys []*datastore.Key
	var dsdst []interface{}
	var dixs []int

	var mixs []int

	mu := new(sync.Mutex)
	goroutines := (len(dskeys)-1)/getMultiLimit + 1
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(i int) {
			defer wg.Done()
			var toCache []interface{}
			var exists []byte
			lo := i * getMultiLimit
			hi := (i + 1) * getMultiLimit
			if hi > len(dskeys) {
				hi = len(dskeys)
			}
			gmerr := datastore.GetMulti(g.Context, dskeys[lo:hi], dsdst[lo:hi])
			if gmerr != nil {
				mu.Lock()
				anyErr = true // this flag tells GetMulti to return multiErr later
				mu.Unlock()
				merr, ok := gmerr.(datastore.MultiError)
				if !ok {
					for j := lo; j < hi; j++ {
						multiErr[j] = gmerr
					}
					return
				}
				for i, idx := range dixs[lo:hi] {
					if merr[i] == nil || (IgnoreFieldMismatch && errFieldMismatch(merr[i])) {
						exists = append(exists, 1)
					} else {
						if merr[i] == datastore.ErrNoSuchEntity {
							exists = append(exists, 0)
						}
						multiErr[idx] = merr[i]
					}
				}
			} else {
				exists = append(exists, bytes.Repeat([]byte{1}, hi-lo)...)
			}
		}(i)
	}
	wg.Wait()
	if anyErr {
		return realError(multiErr)
	}
	return nil
}

// Delete deletes the entity for the given key.
func (g *Gosto) Delete(key *datastore.Key) error {
	keys := []*datastore.Key{key}
	err := g.DeleteMulti(keys)
	if me, ok := err.(datastore.MultiError); ok {
		return me[0]
	}
	return err
}

const deleteMultiLimit = 500

// Returns a single error if each error in MultiError is the same
// otherwise, returns multiError or nil (if multiError is empty)
func realError(multiError datastore.MultiError) error {
	if len(multiError) == 0 {
		return nil
	}
	init := multiError[0]
	for i := 1; i < len(multiError); i++ {
		// since type error could hold structs, pointers, etc,
		// the only way to compare non-nil errors is by their string output
		if init == nil || multiError[i] == nil {
			if init != multiError[i] {
				return multiError
			}
		} else if init.Error() != multiError[i].Error() {
			return multiError
		}
	}
	// all errors are the same
	// some errors are *always* returned in MultiError form from the datastore
	if _, ok := init.(*datastore.ErrFieldMismatch); ok { // returned in GetMulti
		return multiError
	}
	if init == datastore.ErrInvalidEntityType || // returned in GetMulti
		init == datastore.ErrNoSuchEntity { // returned in GetMulti
		return multiError
	}
	// datastore.ErrInvalidKey is returned as a single error in PutMulti
	return init
}

// DeleteMulti is a batch version of Delete.
func (g *Gosto) DeleteMulti(keys []*datastore.Key) error {
	if len(keys) == 0 {
		return nil
	}

	mu := new(sync.Mutex)
	multiErr, any := make(datastore.MultiError, len(keys)), false
	goroutines := (len(keys)-1)/deleteMultiLimit + 1
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(i int) {
			defer wg.Done()
			lo := i * deleteMultiLimit
			hi := (i + 1) * deleteMultiLimit
			if hi > len(keys) {
				hi = len(keys)
			}
			dmerr := datastore.DeleteMulti(g.Context, keys[lo:hi])
			if dmerr != nil {
				mu.Lock()
				any = true // this flag tells DeleteMulti to return multiErr later
				mu.Unlock()
				merr, ok := dmerr.(datastore.MultiError)
				if !ok {
					for j := lo; j < hi; j++ {
						multiErr[j] = dmerr
					}
					return
				}
				copy(multiErr[lo:hi], merr)
			}
		}(i)
	}
	wg.Wait()
	if any {
		return realError(multiErr)
	}
	return nil
}

// NotFound returns true if err is an datastore.MultiError and err[idx] is a datastore.ErrNoSuchEntity.
func NotFound(err error, idx int) bool {
	if merr, ok := err.(datastore.MultiError); ok {
		return idx < len(merr) && merr[idx] == datastore.ErrNoSuchEntity
	}
	return false
}

// errFieldMismatch returns true if err is *datastore.ErrFieldMismatch
func errFieldMismatch(err error) bool {
	_, ok := err.(*datastore.ErrFieldMismatch)
	return ok
}
