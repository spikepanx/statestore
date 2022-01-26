package db

import (
	"fmt"
	"testing"
	"time"

	"github.com/boltdb/bolt"
	"github.com/golang/glog"
	"github.com/stretchr/testify/assert"
)

var (
	db = getDBOrDie("utest2.db")
)

// getDBOrDie is a helper to create a test db. If that fails exists process with a return value of 255
func getDBOrDie(fname string) (db *bolt.DB) {
	var err error
	if db, err = bolt.Open(fname, 0600, &bolt.Options{
		Timeout:         1 * time.Second,
		NoGrowSync:      false,
		ReadOnly:        false,
		MmapFlags:       0,
		InitialMmapSize: 0,
	}); err != nil {
		err = fmt.Errorf("getDBOrDie: %w", err)
		glog.Fatalf("open DB <%s> failed: %s", fname, err)
	}
	return
}

func TestBoltDatabaseImpl_Put(t *testing.T) {
	type fields struct {
		db *bolt.DB
	}
	type args struct {
		e Entity
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "create db, bucket and first key",
			fields: fields{
				db: db,
			},
			args: args{
				e: EntityImpl{
					Bucketname: []byte("tstbucket"),
					K:          []byte("foo"),
					V:          []byte("bar"),
					Err:        nil,
				},
			},
			wantErr: false,
		},
		{
			name: "update key",
			fields: fields{
				db: db,
			},
			args: args{
				e: EntityImpl{
					Bucketname: []byte("tstbucket"),
					K:          []byte("foo"),
					V:          []byte("barbazz"),
					Err:        nil,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			boltDB := BoltDatabaseImpl{
				db: tt.fields.db,
			}
			if err := boltDB.Put(tt.args.e); (err != nil) != tt.wantErr {
				t.Errorf("BoltDatabaseImpl.Put() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBoltDatabaseImpl_Get(t *testing.T) {
	var (
		bucket = "TestBoltDatabaseImpl_Get"
		key    = []byte("myKey")
		value  = []byte("myValue is here")
	)

	type fields struct {
		db *bolt.DB
	}
	type args struct {
		bucket []byte
		key    []byte
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		wantKv *KVEntryImpl
	}{
		{
			name: "success",
			fields: fields{
				db: db,
			},
			args: args{
				bucket: []byte(bucket),
				key:    key,
			},
			wantKv: &KVEntryImpl{
				Bucket: []byte(bucket),
				K:      key,
				V:      value,
				Error:  nil,
			},
		},
		{
			name: "key not found",
			fields: fields{
				db: db,
			},
			args: args{
				bucket: []byte(bucket),
				key:    []byte("unknown key"),
			},
			wantKv: &KVEntryImpl{
				Bucket: []byte(bucket),
				K:      []byte("unknown key"),
				V:      nil,
				Error:  assert.AnError,
			},
		},
		{
			name: "bucket not found",
			fields: fields{
				db: db,
			},
			args: args{
				bucket: []byte("unknown bucket"),
				key:    key,
			},
			wantKv: &KVEntryImpl{
				Bucket: []byte("unknown bucket"),
				K:      key,
				V:      nil,
				Error:  assert.AnError,
			},
		},
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		var b *bolt.Bucket
		if b = tx.Bucket([]byte(bucket)); b == nil {
			var err error
			if b, err = tx.CreateBucket([]byte(bucket)); err != nil {
				return fmt.Errorf("CreateBucket: %w", err)
			}
		}
		return b.Put(key, value)
	}); err != nil {
		t.Fatalf("TestBoltDatabaseImpl_Get: failed to prepare database: %s", err)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			boltDB := BoltDatabaseImpl{
				db: tt.fields.db,
			}
			gotKv := boltDB.Get(tt.args.bucket, tt.args.key)
			if tt.wantKv.Error == nil {
				assert.NoError(t, gotKv.Error)
			} else {
				assert.Error(t, gotKv.Error)
			}
			assert.Equal(t, tt.wantKv.Bucket, gotKv.Bucket)
			assert.Equal(t, tt.wantKv.K, gotKv.K)
			assert.Equal(t, tt.wantKv.V, gotKv.V)
		})
	}
}

func TestStats(t *testing.T) {
	stats := db.Stats()
	t.Logf("%#v", stats)
}
