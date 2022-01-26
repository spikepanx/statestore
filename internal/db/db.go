package db

import (
	"fmt"

	"github.com/boltdb/bolt"
)

type Entity interface {
	Bucket([]byte) []byte
	Key([]byte) []byte
	Value([]byte) []byte
	Error(error) error
}

type EntityImpl struct {
	Bucketname []byte
	K, V       []byte
	Err        error
}

func (ev EntityImpl) Bucket(b []byte) []byte {
	if b != nil {
		ev.Bucketname = make([]byte, len(b))
		copy(ev.Bucketname, b)
	}
	return ev.Bucketname
}
func (ev EntityImpl) Key(k []byte) []byte {
	if k != nil {
		ev.K = make([]byte, len(k))
		copy(ev.K, k)
	}
	return ev.K
}
func (ev EntityImpl) Value(v []byte) []byte {
	if v != nil {
		ev.V = make([]byte, len(v))
		copy(ev.V, v)
	}
	return ev.V
}
func (ev EntityImpl) Error(err error) error {
	if err != nil {
		ev.Err = err
	}
	return ev.Err
}

type Database interface {
	// Put adds an Entity to the underlying database
	Put(Entity) error
	Get(key []byte) (Entity, error)
}

type BoltDatabaseImpl struct {
	db *bolt.DB
}

type KVEntryImpl struct {
	Bucket, K, V []byte
	Error        error
}

// Put adds Entity to bold db. If bucket given by entity does not exists bucket
// is created
func (boltDB BoltDatabaseImpl) Put(e Entity) (err error) {
	if err = boltDB.db.Update(func(tx *bolt.Tx) error {
		var b *bolt.Bucket
		if b = tx.Bucket(e.Bucket(nil)); b == nil { // if bucket not exists, create
			if b, err = tx.CreateBucket([]byte(e.Bucket(nil))); err != nil {
				return fmt.Errorf("BoltDatabaseImpl::Put:create bucket: %s", err)
			}
		}
		return b.Put(e.Key(nil), e.Value(nil))
	}); err != nil {
		e.Error(fmt.Errorf("BoltDatabaseImpl::Put: %w", err))
	}
	return
}

// Get fetches value referenced by bucket and key from given db. Any error occured is added
// to the returned instance
func (boltDB BoltDatabaseImpl) Get(bucket, key []byte) (kv *KVEntryImpl) {
	kv = &KVEntryImpl{bucket, key, nil, nil}

	if err := boltDB.db.View(func(tx *bolt.Tx) error {
		var b *bolt.Bucket
		if b = tx.Bucket(bucket); b == nil {
			return fmt.Errorf("bucket %s does not exist", bucket)
		}
		var v []byte
		if v = b.Get(key); v == nil {
			return fmt.Errorf("key %s does not exist or key is a nested bucket", key)
		}
		kv.V = make([]byte, len(v))
		copy(kv.V, v)
		return nil
	}); err != nil {
		kv.Error = fmt.Errorf("BoltDatabaseImpl::Get: %w", err)
	}
	return kv
}

// func UpsertBucketStream(ctx context.Context, db Database, in chan Entity, bufSize uint) (out chan Entity) {
// 	out = make(chan Entity, bufSize)

// 	go func() {
// 		defer close(out)
// 		for run := true; run; {
// 			select {
// 			case <-ctx.Done():
// 				run = false
// 			case ev, ok := <-in:
// 				if !ok {
// 					run = false
// 					continue
// 				}
// 				if err := db.Put(ev); err != nil {
// 					ev.Error(fmt.Errorf("db::UpsertStream: %w", err))
// 				}
// 				out <- ev
// 			}
// 		}
// 	}()
// 	return
// }
