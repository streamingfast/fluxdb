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

package fluxdb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/dfuse-io/dtracing"
	"github.com/dfuse-io/fluxdb/store"
	"github.com/dfuse-io/logging"
	pbfluxdb "github.com/dfuse-io/pbgo/dfuse/fluxdb/v1"
	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
)

var bigEndian = binary.BigEndian

func (fdb *FluxDB) IndexTables(ctx context.Context) error {
	ctx, span := dtracing.StartSpan(ctx, "index tables")
	defer span.End()

	zlog := logging.Logger(ctx, zlog)
	zlog.Debug("indexing tables")

	batch := fdb.store.NewBatch(zlog)

	for tablet, height := range fdb.idxCache.scheduleIndexing {
		zlog.Debug("indexing table", zap.Stringer("tablet", tablet), zap.Uint64("height", height))

		if err := batch.FlushIfFull(ctx); err != nil {
			return fmt.Errorf("flush if full: %w", err)
		}

		indexSinglet := newIndexSinglet(tablet)

		zlog.Debug("checking if index already exist in cache")
		index := fdb.idxCache.GetIndex(tablet)
		if index == nil {
			zlog.Debug("index not in cache")

			indexEntry, err := fdb.ReadSingletEntryAt(ctx, indexSinglet, math.MaxUint64, nil)
			if errors.Is(err, store.ErrNotFound) {
				zlog.Debug("index does not exist yet, creating empty one")
				index = NewTabletIndex()
			} else if err != nil {
				return fmt.Errorf("get index %s (%d): %w", tablet, height, err)
			} else {
				index = indexEntry.(indexSingletEntry).index
			}
		}

		startKey := KeyForTabletAt(tablet, index.AtHeight+1)
		endKey := KeyForTabletAt(tablet, height+1)

		zlog.Debug("reading table rows for indexation", zap.Stringer("first_row_key", startKey), zap.Stringer("last_row_key", endKey))

		count := 0
		err := fdb.store.ScanTabletRows(ctx, startKey, endKey, func(key []byte, value []byte) error {
			// We are really only interested by the row's key here, so we don't give it any value, just like if it would be a deleted row
			row, err := NewTabletRow(tablet, key, nil)
			if err != nil {
				return fmt.Errorf("couldn't parse row key %q: %w", key, err)
			}

			count++

			if len(value) == 0 {
				index.PrimaryKeyToHeight.delete(row.PrimaryKey())
			} else {
				index.PrimaryKeyToHeight.put(row.PrimaryKey(), row.Height())
			}

			return nil
		})

		if err != nil {
			return fmt.Errorf("read rows: %w", err)
		}

		index.AtHeight = height
		index.SquelchCount = uint64(count)

		zlog.Debug("about to marshal index to binary",
			zap.Stringer("tablet", tablet),
			zap.Uint64("at_height", index.AtHeight),
			zap.Uint64("squelched_count", index.SquelchCount),
			zap.Int("row_count", index.PrimaryKeyToHeight.len()),
		)

		indexEntry := newIndexSingletEntry(indexSinglet, index)
		value, err := indexEntry.MarshalValue()
		if err != nil {
			return fmt.Errorf("singlet to proto: %w", err)
		}

		if len(value) > 25000000 {
			zlog.Warn("index singlet pretty heavy", zap.Stringer("index_entry", indexEntry), zap.Int("byte_count", len(value)))
		}

		batch.SetRow(KeyForSingletEntry(indexEntry), value)

		zlog.Debug("caching index in index cache", zap.Stringer("index_entry", indexEntry), zap.Stringer("tablet", tablet))
		fdb.idxCache.CacheIndex(tablet, index)
		fdb.idxCache.ResetCounter(tablet)
		delete(fdb.idxCache.scheduleIndexing, tablet)
	}

	if err := batch.Flush(ctx); err != nil {
		return fmt.Errorf("final flush: %w", err)
	}

	return nil
}

// getIndex returns the latest active index at the provided height. If there is
// index available at this height, this method returns `nil` as the index value.
func (fdb *FluxDB) getIndex(ctx context.Context, tablet Tablet, height uint64) (*TabletIndex, error) {
	ctx, span := dtracing.StartSpan(ctx, "get index")
	defer span.End()

	zlog := logging.Logger(ctx, zlog)
	zlog.Debug("fetching tablet index from database", zap.Stringer("tablet", tablet), zap.Uint64("height", height))

	indexEntry, err := fdb.ReadSingletEntryAt(ctx, newIndexSinglet(tablet), height, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to read entry: %w", err)
	}

	if indexEntry != nil {
		return indexEntry.(indexSingletEntry).index, nil
	}

	return nil, nil
}

type indexCache struct {
	lastIndexes      map[Tablet]*TabletIndex
	lastCounters     map[Tablet]int
	scheduleIndexing map[Tablet]uint64
}

func newIndexCache() *indexCache {
	return &indexCache{
		lastIndexes:      make(map[Tablet]*TabletIndex),
		lastCounters:     make(map[Tablet]int),
		scheduleIndexing: make(map[Tablet]uint64),
	}
}

func (t *indexCache) GetIndex(tablet Tablet) *TabletIndex {
	return t.lastIndexes[tablet]
}

func (t *indexCache) CacheIndex(tablet Tablet, tableIndex *TabletIndex) {
	t.lastIndexes[tablet] = tableIndex
}

func (t *indexCache) GetCount(tablet Tablet) int {
	return t.lastCounters[tablet]
}

func (t *indexCache) IncCount(tablet Tablet) {
	t.lastCounters[tablet]++
}

func (t *indexCache) ResetCounter(tablet Tablet) {
	t.lastCounters[tablet] = 0
}

// This algorithm determines the space between the indexes
func (t *indexCache) shouldTriggerIndexing(tablet Tablet) bool {
	mutatedRowsCount := t.lastCounters[tablet]
	if mutatedRowsCount < 1000 {
		return false
	}

	lastIndex := t.lastIndexes[tablet]
	if lastIndex == nil {
		return true
	}

	if lastIndex.RowCount() > 50000 && mutatedRowsCount < 5000 {
		return false
	}

	if lastIndex.RowCount() > 100000 && mutatedRowsCount < 10000 {
		return false
	}

	return true
}

func (t *indexCache) ScheduleIndex(tablet Tablet, height uint64) {
	t.scheduleIndexing[tablet] = height
}

func (t *indexCache) IndexingSchedule() map[Tablet]uint64 {
	return t.scheduleIndexing
}

var indexSingletCollection uint16 = 0xFFFF
var indexSingletCollectionName string = "idx"

func init() {
	registerSingletFactory(indexSingletCollection, indexSingletCollectionName, func(identifier []byte) (Singlet, error) {
		// Our identifier is the full `TabletKey` which contains the collection and the tablet identifier
		tablet, err := NewTablet(identifier)
		if err != nil {
			return nil, fmt.Errorf("index tablet: %w", err)
		}

		return newIndexSinglet(tablet), nil
	})
}

type indexSinglet struct {
	tabletKey TabletKey
}

func newIndexSinglet(forTablet Tablet) indexSinglet {
	return indexSinglet{tabletKey: KeyForTablet(forTablet)}
}

func (s indexSinglet) Collection() uint16 {
	return indexSingletCollection
}

func (s indexSinglet) Identifier() []byte {
	// Our singlet identifier is the actual full TabletKey (including its collection bytes)
	return s.tabletKey
}

func (s indexSinglet) Entry(height uint64, value []byte) (SingletEntry, error) {
	indexProto := pbfluxdb.TabletIndex{}
	if err := proto.Unmarshal(value, &indexProto); err != nil {
		return nil, fmt.Errorf("unmarshal index: %w", err)
	}

	index := &TabletIndex{
		AtHeight:     height,
		SquelchCount: indexProto.SquelchedCount,
	}

	entryCount := len(indexProto.Entries)
	if entryCount > 0 {
		index.PrimaryKeyToHeight = newPrimaryKeyToHeightMap(entryCount)
		for _, entry := range indexProto.Entries {
			index.PrimaryKeyToHeight.put(entry.PrimaryKey, entry.Height)
		}
	} else {
		index.PrimaryKeyToHeight = &primaryKeyToHeightMap{bytesMap: &bytesMap{mappings: nil}}
	}

	return newIndexSingletEntry(s, index), nil
}

func (s indexSinglet) String() string {
	return indexSingletCollectionName + ":" + s.tabletKey.String()
}

type indexSingletEntry struct {
	BaseSingletEntry
	index *TabletIndex
}

func newIndexSingletEntry(singlet indexSinglet, index *TabletIndex) indexSingletEntry {
	return indexSingletEntry{
		BaseSingletEntry: NewBaseSingletEntry(singlet, index.AtHeight, false),
		index:            index,
	}
}

func (s indexSingletEntry) MarshalValue() ([]byte, error) {
	out := &pbfluxdb.TabletIndex{
		SquelchedCount: s.index.SquelchCount,
		Entries:        make([]*pbfluxdb.TabletIndexEntry, s.index.PrimaryKeyToHeight.len()),
	}

	i := 0
	for primaryKey, height := range s.index.PrimaryKeyToHeight.mappings {
		out.Entries[i] = &pbfluxdb.TabletIndexEntry{PrimaryKey: []byte(primaryKey), Height: height.(uint64)}
		i++
	}

	value, err := proto.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("marshal index: %w", err)
	}

	return value, nil
}

type primaryKeyToHeightMap struct {
	*bytesMap
}

func newPrimaryKeyToHeightMap(length int) *primaryKeyToHeightMap {
	return &primaryKeyToHeightMap{
		bytesMap: &bytesMap{
			mappings: make(map[string]interface{}, length),
		},
	}
}

func (m *primaryKeyToHeightMap) put(k []byte, v uint64) { m._put(k, v) }
func (m *primaryKeyToHeightMap) get(k []byte) (uint64, bool) {
	v, f := m._get(k)
	return v.(uint64), f
}

func (m *primaryKeyToHeightMap) delete(k []byte) {
	m.bytesMap.delete(k)
}

func (m *primaryKeyToHeightMap) rowKeys(tablet Tablet, height uint64) (keys [][]byte) {
	keys = make([][]byte, m.len())

	i := 0
	for primaryKey, height := range m.mappings {
		keys[i] = KeyForTabletRowParts(tablet, height.(uint64), []byte(primaryKey))
		i++
	}

	return
}
