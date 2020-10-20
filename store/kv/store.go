// Copyright 2020 dfuse Platform Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/dfuse-io/dtracing"
	"github.com/dfuse-io/fluxdb/store"
	kv "github.com/dfuse-io/kvdb/store"
	"github.com/dfuse-io/logging"
	"go.uber.org/zap"
)

var TblPrefixName = map[byte]string{
	TblPrefixRows:           "rows",
	TblPrefixLastCheckpoint: "checkpoint",
}

const (
	TblPrefixRows           = 0x00
	TblPrefixLastCheckpoint = 0x01
)

var TableMapper = map[byte]string{}

type KVStore struct {
	db kv.KVStore
}

func NewStore(dsnString string) (*KVStore, error) {
	store, err := kv.New(dsnString, kv.WithEmptyValue())
	if err != nil {
		return nil, fmt.Errorf("cannot create new kv store: %w", err)
	}

	return &KVStore{
		db: store,
	}, nil

}

func (s *KVStore) Close() error {
	if closer, ok := s.db.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func (s *KVStore) NewBatch(logger *zap.Logger) store.Batch {
	return newBatch(s, logger)
}

func (s *KVStore) FetchSingletEntry(ctx context.Context, keyStart, keyEnd []byte) (key []byte, value []byte, err error) {
	err = s.scanRange(ctx, TblPrefixRows, keyStart, keyEnd, 1, func(rowKey []byte, rowValue []byte) error {
		key = rowKey
		value = rowValue

		// We only ever check a single row
		return store.BreakScan
	})

	if err != nil && err != store.BreakScan {
		return nil, nil, fmt.Errorf("unable to fetch single tablet row (range [%s, %s[): %w", Key(keyStart), Key(keyEnd), err)
	}

	return key, value, nil
}

func (s *KVStore) HasTabletRow(ctx context.Context, keyStart, keyEnd []byte) (exists bool, err error) {
	err = s.scanRange(ctx, TblPrefixRows, keyStart, keyEnd, 1, func(_ []byte, _ []byte) error {
		exists = true
		return store.BreakScan
	})

	if err != nil && err != store.BreakScan {
		return false, fmt.Errorf("scan has tablet row [%q, %q[: %w", keyStart, keyEnd, err)
	}

	return exists, nil
}

func (s *KVStore) FetchTabletRow(ctx context.Context, key []byte) (value []byte, err error) {
	return s.fetchKey(ctx, TblPrefixRows, key)
}

func (s *KVStore) FetchTabletRows(ctx context.Context, keys [][]byte, onKeyValue store.OnKeyValue) error {
	return s.fetchKeys(ctx, TblPrefixRows, keys, onKeyValue)
}

func (s *KVStore) ScanTabletRows(ctx context.Context, keyStart, keyEnd []byte, onKeyValue store.OnKeyValue) error {
	err := s.scanRange(ctx, TblPrefixRows, keyStart, keyEnd, kv.Unlimited, func(key []byte, value []byte) error {
		err := onKeyValue(key, value)
		if err == store.BreakScan {
			return store.BreakScan
		}

		if err != nil {
			return fmt.Errorf("on tablet row for key %q failed: %w", Key(key), err)
		}

		return nil
	})

	if err != nil && err != store.BreakScan {
		return fmt.Errorf("unable to scan tablet rows [%q, %q[: %w", Key(keyStart), Key(keyEnd), err)
	}

	return nil
}

func (s *KVStore) ScanIndexKeys(ctx context.Context, prefix []byte, onKey store.OnKey) error {
	err := s.scanPrefix(ctx, TblPrefixRows, prefix, kv.Unlimited, true, func(key []byte, _ []byte) error {
		err := onKey(key)
		if err == store.BreakScan {
			return store.BreakScan
		}

		if err != nil {
			return fmt.Errorf("on scan index %s: %w", Key(key), err)
		}

		return nil
	})

	if err != nil && err != store.BreakScan {
		return fmt.Errorf("scan indexes: %w", err)
	}

	return nil
}

func (s *KVStore) FetchLastWrittenCheckpoint(ctx context.Context, key []byte) (out []byte, err error) {
	logging.Logger(ctx, zlog).Debug("fetching last written block", zap.Stringer("key", Key(key)))
	value, err := s.fetchKey(ctx, TblPrefixLastCheckpoint, key)
	if err != nil {
		return nil, err
	}

	return value, nil
}

func (s *KVStore) ScanLastShardsWrittenCheckpoint(ctx context.Context, keyPrefix []byte, onKeyValue store.OnKeyValue) error {
	err := s.scanPrefix(ctx, TblPrefixLastCheckpoint, keyPrefix, kv.Unlimited, false, func(key []byte, value []byte) error {
		err := onKeyValue(key, value)
		if err == store.BreakScan {
			return store.BreakScan
		}

		if err != nil {
			return fmt.Errorf("on block ref key %q scan last shards checkpoint: %w", Key(key), err)
		}

		return nil
	})

	if err != nil && err != store.BreakScan {
		return fmt.Errorf("unable to determine if table %q has key prefix %q: %w", TblPrefixLastCheckpoint, keyPrefix, err)
	}

	return nil
}

func (s *KVStore) fetchKey(ctx context.Context, table byte, key []byte) (out []byte, err error) {
	kvKey := packKey(table, key)

	out, err = s.db.Get(ctx, kvKey)
	if errors.Is(err, kv.ErrNotFound) {
		return nil, store.ErrNotFound
	}

	if err != nil {
		return nil, fmt.Errorf("unable to fetch table %q key %q: %w", TblPrefixName[table], Key(key), err)
	}

	return out, nil
}

func (s *KVStore) fetchKeys(batchCtx context.Context, table byte, keys [][]byte, onKeyValue store.OnKeyValue) error {
	batchCtx, cancelBatch := context.WithCancel(batchCtx)
	defer cancelBatch()

	kvKeys := make([][]byte, len(keys))
	for i, key := range keys {
		kvKeys[i] = packKey(table, key)
	}

	itr := s.db.BatchGet(batchCtx, kvKeys)

	for itr.Next() {
		value := itr.Item().Value
		// We must be prudent here, a `nil` value indicate a key not found, a `[]byte{}` indicates a found key without a value!
		if value == nil {
			continue
		}

		_, key := unpackKey(itr.Item().Key)
		err := onKeyValue(key, value)
		if err == store.BreakScan {
			return nil
		}

		if err != nil {
			return fmt.Errorf("on tablet row for key %q failed: %w", key, err)
		}
	}
	if err := itr.Err(); err != nil {
		return fmt.Errorf("unable to fetch table %q keys (%d): %w", TblPrefixName[table], len(keys), err)
	}

	return nil
}

func (s *KVStore) scanPrefix(ctx context.Context, table byte, prefixKey []byte, limit int, keyOnly bool, onRow func(key []byte, value []byte) error) error {
	kvPrefix := packKey(table, prefixKey)

	itrCtx, cancelIterator := context.WithCancel(ctx)
	defer cancelIterator()

	var readOptions []kv.ReadOption
	if keyOnly {
		readOptions = []kv.ReadOption{kv.KeyOnly()}
	}

	itr := s.db.Prefix(itrCtx, kvPrefix, limit, readOptions...)
	for itr.Next() {
		item := itr.Item()
		t, key := unpackKey(item.Key)
		err := onRow(key, item.Value)

		if err == store.BreakScan {
			return nil
		}

		if err != nil {
			return fmt.Errorf("scan prefix: unable to process for table %q with key %q: %w", TblPrefixName[t], key, err)
		}
	}
	if err := itr.Err(); err != nil {
		return fmt.Errorf("unable to scan table %q keys with prefix %q: %w", TblPrefixName[table], prefixKey, err)
	}

	return nil
}

func (s *KVStore) scanRange(ctx context.Context, table byte, keyStart, keyEnd []byte, limit int, onRow func(key []byte, value []byte) error) error {
	logging.Logger(ctx, zlog).Debug("scanning range", zap.Stringer("start", Key(keyStart)), zap.Stringer("end", Key(keyEnd)))

	startKey := packKey(table, keyStart)
	var endKey []byte

	if len(keyEnd) > 0 {
		endKey = packKey(table, keyEnd)
	} else {
		// there is no key end key specified we go till the end of the table (1 byte more then the table prefix)
		endKey = []byte{table + 1}
	}

	scanCtx, cancelScan := context.WithCancel(ctx)
	defer cancelScan()

	itr := s.db.Scan(scanCtx, startKey, endKey, limit)

	for itr.Next() {
		item := itr.Item()
		t, key := unpackKey(item.Key)
		err := onRow(key, item.Value)
		if err == store.BreakScan {
			return nil
		}

		if err != nil {
			return fmt.Errorf("scan range: unable to process for table %q with key %q: %w", TblPrefixName[t], key, err)
		}
	}

	if err := itr.Err(); err != nil {
		return fmt.Errorf("unable to scan table %q keys with start key %q and end key %q: %w", TblPrefixName[table], keyStart, keyEnd, err)
	}

	return nil
}

func (s *KVStore) scanInfiniteRange(ctx context.Context, table byte, keyStart []byte, limit int, onRow func(key []byte, value []byte) error) error {
	return s.scanRange(ctx, table, keyStart, nil, limit, onRow)
}

type batch struct {
	store          *KVStore
	count          int
	tableMutations map[byte]*keyToValueMap

	zlog *zap.Logger
}

func newBatch(store *KVStore, logger *zap.Logger) *batch {
	batchSet := &batch{store: store, zlog: logger}
	batchSet.Reset()

	return batchSet
}

func (b *batch) Reset() {
	b.count = 0
	b.tableMutations = map[byte]*keyToValueMap{
		TblPrefixRows:           {mappings: map[string][]byte{}},
		TblPrefixLastCheckpoint: {mappings: map[string][]byte{}},
	}
}

var maxMutationCount = 100

// FIXME: Instead of re-adding our custom logic of 100 max mutation count in there, we should
//        instead rely on `kvdb.Batch` heuristics to determine if full or not. Only thing to consider
//        when doing this refactoring (i.e. removing a flush on 100 rows written) is to make "100%"
//        sure that last checkpoint mutations are always ever written last!
func (b *batch) FlushIfFull(ctx context.Context) error {
	if b.count <= maxMutationCount {
		// We are not there yet
		return nil
	}

	b.zlog.Debug("flushing a full batch set", zap.Int("count", b.count))
	if err := b.Flush(ctx); err != nil {
		return fmt.Errorf("flushing batch set: %w", err)
	}

	return nil
}

func (b *batch) Flush(ctx context.Context) error {
	ctx, span := dtracing.StartSpan(ctx, "flush batch set")
	defer span.End()

	b.zlog.Debug("flushing batch set")

	tableNames := []byte{
		TblPrefixRows,

		// The table name `last` must always be the last table in this list!
		TblPrefixLastCheckpoint,
	}

	// TODO: We could eventually parallelize this, but remember, last would need to be processed last, after all others!
	for _, tblName := range tableNames {
		muts := b.tableMutations[tblName]

		if muts.len() <= 0 {
			continue
		}

		b.zlog.Debug("applying bulk update", zap.String("table_name", TblPrefixName[tblName]), zap.Int("mutation_count", muts.len()))
		ctx, span := dtracing.StartSpan(ctx, "apply bulk updates", "table", tblName, "mutation_count", muts.len())

		for key, value := range muts.mappings {
			err := b.store.db.Put(ctx, packKey(tblName, []byte(key)), value)
			if err != nil {
				return fmt.Errorf("unable to add table %q key %q to tx: %w", tblName, key, err)
			}
		}
		span.End()
	}

	err := b.store.db.FlushPuts(ctx)
	if err != nil {
		return fmt.Errorf("apply bulk: %w", err)
	}

	b.Reset()

	return nil
}

func (b *batch) setTable(table byte, key []byte, value []byte) {
	b.tableMutations[table].put(key, value)
	b.count++
}

func (b *batch) SetRow(key []byte, value []byte) {
	b.setTable(TblPrefixRows, key, value)
}

func (b *batch) SetLastCheckpoint(key []byte, value []byte) {
	b.setTable(TblPrefixLastCheckpoint, key, value)
}

func packKey(table byte, key []byte) []byte {
	return append([]byte{table}, []byte(key)...)
}

func unpackKey(packedKey []byte) (table byte, key []byte) {
	if len(packedKey) < 1 {
		return
	}

	return packedKey[0], packedKey[1:]
}

type Key = store.Key

type keyToValueMap struct {
	mappings map[string][]byte
}

func (m *keyToValueMap) put(key []byte, value []byte) {
	m.mappings[string(key)] = value
}

func (m *keyToValueMap) get(key []byte) (value []byte, found bool) {
	value, found = m.mappings[string(key)]
	return
}

func (m *keyToValueMap) has(key []byte) bool {
	_, found := m.mappings[string(key)]
	return found
}

func (m *keyToValueMap) delete(key []byte) {
	delete(m.mappings, string(key))
}

func (m *keyToValueMap) len() int {
	return len(m.mappings)
}
