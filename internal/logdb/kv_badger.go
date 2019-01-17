// Copyright 2017-2019 Lei Ni (nilei81@gmail.com)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logdb

import (
	"bytes"

	"github.com/dgraph-io/badger"

	"github.com/lni/dragonboat/raftio"
)

// WARNING: this is an experimental implementation for badger support. it is
// known to be unstable. DO NOT use it in any production environment.
type badgerKV struct {
	db *badger.DB
}

func openBadgerDB(dir string, wal string) (*badgerKV, error) {
	opts := badger.DefaultOptions
	opts.SyncWrites = true
	// TODO:
	// WAL is ignored here
	// the WAL definition in dragonboat is a rocksdb WAL style log that will be
	// continously shrinked. such a WAL can thus be fitted on a smaller but much
	// faster storage such as an Intel Optane SSD like P4801X. the vlog in badger
	// probably doesn't has such behaviour. double check on this.
	opts.Dir = dir
	opts.ValueDir = dir
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &badgerKV{db: db}, nil
}

func (b *badgerKV) Close() error {
	return b.db.Close()
}

func (b *badgerKV) SaveValue(key []byte, value []byte) error {
	return b.db.Update(func(tx *badger.Txn) error {
		return tx.Set(key, value)
	})
}

func (b *badgerKV) GetValue(key []byte, op func([]byte) error) error {
	return b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return op(nil)
			}
			return err
		}
		return item.Value(func(val []byte) error {
			return op(val)
		})
	})
}

func (b *badgerKV) DeleteValue(key []byte) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

func (b *badgerKV) IterateValue(fk []byte, lk []byte, inc bool,
	op func(key []byte, data []byte) (bool, error)) {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 10
	err := b.db.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(opts)
		defer iter.Close()
		for iter.Seek(fk); iter.Valid(); iter.Next() {
			item := iter.Item()
			key := item.Key()
			if inc {
				if bytes.Compare(key, lk) > 0 {
					return nil
				}
			} else {
				if bytes.Compare(key, lk) >= 0 {
					return nil
				}
			}
			var cont bool
			err := item.Value(func(val []byte) error {
				ct, err := op(key, val)
				if err != nil {
					return err
				}
				cont = ct
				return nil
			})
			if err != nil {
				panic(err)
			}
			if !cont {
				return nil
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
}

func (b *badgerKV) RemoveEntries(fk []byte, lk []byte) error {
	opt := badger.DefaultIteratorOptions
	opt.PrefetchValues = false
	keys := make([][]byte, 0)
	err := b.db.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(opt)
		defer iter.Close()
		for iter.Seek(fk); iter.Valid(); iter.Next() {
			key := iter.Item().Key()
			if bytes.Compare(key, lk) >= 0 {
				break
			}
			keys = append(keys, key)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	wb := b.db.NewWriteBatch()
	defer wb.Cancel()
	for _, key := range keys {
		if err := wb.Delete(key); err != nil {
			return nil
		}
	}
	return wb.Flush()
}

func (b *badgerKV) Compaction(fk []byte, lk []byte) error {
	// FIXME:
	// handle err returned by RunValueLogGC
	for {
		if err := b.db.RunValueLogGC(0.5); err != nil {
			return nil
		}
	}
	return nil
}

func (b *badgerKV) GetWriteBatch(ctx raftio.IContext) IWriteBatch {
	if ctx != nil {
		wb := ctx.GetWriteBatch()
		if wb != nil {
			return wb.(*simpleWriteBatch)
		}
	}
	return newSimpleWriteBatch()
}

func (b *badgerKV) CommitWriteBatch(wb IWriteBatch) error {
	swb, ok := wb.(*simpleWriteBatch)
	if !ok {
		panic("unknown type")
	}
	return b.db.Update(func(txn *badger.Txn) error {
		for _, pair := range swb.vals {
			err := txn.Set(pair.key, pair.val)
			if err != nil {
				return err
			}
		}
		return nil
	})
}
