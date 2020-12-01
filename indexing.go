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
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"

	"github.com/dfuse-io/dtracing"
	"github.com/dfuse-io/logging"
	pbfluxdb "github.com/dfuse-io/pbgo/dfuse/fluxdb/v1"
	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
)

func (fdb *FluxDB) IndexTables(ctx context.Context) error {
	if fdb.disableIndexing {
		zlog.Debug("indexing is disabled, nothing to do")
		return nil
	}

	ctx, span := dtracing.StartSpan(ctx, "index tables")
	defer span.End()

	zlog := logging.Logger(ctx, zlog)
	zlog.Debug("indexing tables")

	batch := fdb.store.NewBatch(zlog)

	for key, height := range fdb.idxCache.scheduleIndexing {
		tabletKey := TabletKey(key)
		tablet, err := NewTablet(tabletKey)
		if err != nil {
			return fmt.Errorf("unable to obtain tablet from its key: %w", err)
		}

		zlog.Debug("indexing table", zap.Stringer("tablet", tablet), zap.Uint64("height", height))
		if err := batch.FlushIfFull(ctx); err != nil {
			return fmt.Errorf("flush if full: %w", err)
		}

		index, err := fdb.indexTablet(ctx, height, tablet, false, false)
		if err != nil {
			return fmt.Errorf("index tablet %s: %w", tablet, err)
		}

		zlog.Debug("about to marshal index to binary",
			zap.Stringer("tablet", tablet),
			zap.Uint64("at_height", index.AtHeight),
			zap.Uint64("squelched_count", index.SquelchCount),
			zap.Int("row_count", index.PrimaryKeyToHeight.len()),
		)

		indexSinglet := newIndexSingletFromKey(tabletKey)
		indexEntry := newIndexSingletEntry(indexSinglet, index)
		value, err := indexEntry.MarshalValue()
		if err != nil {
			return fmt.Errorf("index entry to proto: %w", err)
		}

		// When above 25MB, flag as being a big index
		if len(value) > 25*1000*1000 {
			zlog.Warn("index singlet pretty heavy", zap.Stringer("index_entry", indexEntry), zap.Int("byte_count", len(value)))
		}

		batch.SetRow(KeyForSingletEntry(indexEntry), value)

		zlog.Debug("caching index in index cache", zap.Stringer("index_entry", indexEntry))
		fdb.idxCache.CacheIndex(tabletKey, index)
		fdb.idxCache.ResetCounter(tabletKey)
		delete(fdb.idxCache.scheduleIndexing, string(tabletKey))
	}

	if err := batch.Flush(ctx); err != nil {
		return fmt.Errorf("final flush: %w", err)
	}

	return nil
}

func (fdb *FluxDB) ReindexTablets(ctx context.Context, height uint64, lowerBound Tablet, dryRun bool) (tabletCount int, indexCount int, err error) {
	prefix := make([]byte, 2)
	bigEndian.PutUint16(prefix, indexSingletCollection)

	lowerBoundKey := []byte(nil)
	if lowerBound != nil {
		lowerBoundKey = KeyForTablet(lowerBound)
	}

	indexKeysPerTablet := map[string][]indexSingletEntry{}
	err = fdb.store.ScanIndexKeys(ctx, prefix, func(key []byte) error {
		entry, err := NewSingletEntryFromStorage(key, nil)
		if err != nil {
			return fmt.Errorf("invalid singlet key %x: %w", key, err)
		}

		if height != 0 && entry.Height() > height {
			return nil
		}

		indexEntry := entry.(indexSingletEntry)
		tabletKey := indexEntry.singlet.(indexSinglet).tabletKey

		if bytes.Compare(tabletKey, lowerBoundKey) < 0 {
			return nil
		}

		indexCount++
		indexKeysPerTablet[string(tabletKey)] = append(indexKeysPerTablet[string(tabletKey)], indexEntry)
		return nil
	})

	if err != nil {
		return 0, 0, fmt.Errorf("scan: %w", err)
	}

	orderedIndexTablets := orderedIndexTabletKeys(indexKeysPerTablet)

	zlog.Debug("re-indexing tablets", zap.Int("tablet_count", len(indexKeysPerTablet)), zap.Int("index_count", indexCount))
	if dryRun {
		if traceEnabled {
			for _, tabletKey := range orderedIndexTablets {
				for _, entry := range indexKeysPerTablet[tabletKey] {
					zlog.Debug("would re-index tablet index", zap.Stringer("id", entry))
				}
			}
		}

		return len(indexKeysPerTablet), indexCount, nil
	}

	batch := fdb.store.NewBatch(zlog)
	for _, key := range orderedIndexTablets {
		entries := indexKeysPerTablet[key]
		tablet, err := NewTablet([]byte(key))
		if err != nil {
			return 0, 0, fmt.Errorf("new tablet for key %x: %w", []byte(key), err)
		}

		sort.Slice(entries, func(i, j int) bool {
			return entries[i].height < entries[j].height
		})

		indexSinglet := newIndexSingletFromKey(TabletKey(key))
		for _, entry := range entries {
			index, err := fdb.indexTablet(ctx, entry.height, tablet, false, true)
			if err != nil {
				return 0, 0, fmt.Errorf("tablet index: %w", err)
			}

			value, err := newIndexSingletEntry(indexSinglet, index).MarshalValue()
			if err != nil {
				return 0, 0, fmt.Errorf("index entry to proto: %w", err)
			}

			// When above 25MB, flag as being a big index
			if len(value) > 25*1000*1000 {
				zlog.Warn("index singlet pretty heavy", zap.Stringer("index_entry", entry), zap.Int("byte_count", len(value)))
			}

			zlog.Debug("re-indexed tablet, adding it to batch", zap.Stringer("index", entry))
			batch.SetRow(KeyForSingletEntry(entry), value)
			err = batch.FlushIfFull(ctx)
			if err != nil {
				return 0, 0, fmt.Errorf("write indexes: %w", err)
			}

			fdb.idxCache.CacheIndex(TabletKey(key), index)
		}

		fdb.idxCache.Reset()
	}

	err = batch.Flush(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("write indexes: %w", err)
	}

	return len(indexKeysPerTablet), indexCount, nil
}

func (fdb *FluxDB) ReindexTablet(ctx context.Context, height uint64, tablet Tablet, write bool) (*TabletIndex, bool, error) {
	zlog.Debug("re-indexing tablet", zap.Stringer("tablet", tablet), zap.Uint64("height", height))
	maxHeight := uint64(math.MaxUint64)
	if height != 0 {
		maxHeight = height
	}

	tabletKey := KeyForTablet(tablet)
	indexSinglet := newIndexSingletFromKey(tabletKey)

	zlog.Debug("looking for existing index", zap.Uint64("max_height", maxHeight))
	// FIXME: Ideally, we should actually just read the key, not the value, we don't care about the old value when re-indexing
	indexEntry, err := fdb.ReadSingletEntryAt(ctx, indexSinglet, maxHeight, nil)
	if err != nil {
		return nil, false, fmt.Errorf("get index %s at height %d: %w", tablet, height, err)
	}

	if indexEntry == nil {
		zlog.Debug("re-index not required since no index existed at this block height", zap.Uint64("height", height))
		return nil, false, nil
	}

	zlog.Debug("found existing index for height, re-computing it", zap.Stringer("index_entry", indexEntry))

	reindex, err := fdb.indexTablet(ctx, indexEntry.Height(), tablet, false, true)
	if err != nil {
		return nil, false, fmt.Errorf("index tablet: %w", err)
	}

	value, err := newIndexSingletEntry(indexSinglet, reindex).MarshalValue()
	if err != nil {
		return nil, false, fmt.Errorf("index entry to proto: %w", err)
	}

	// When above 25MB, flag as being a big index
	if len(value) > 25*1000*1000 {
		zlog.Warn("index singlet pretty heavy", zap.Stringer("index_entry", indexEntry), zap.Int("byte_count", len(value)))
	}

	batch := fdb.store.NewBatch(zlog)
	batch.SetRow(KeyForSingletEntry(indexEntry), value)

	if !write {
		zlog.Debug("not writing back index to storage engine", zap.Stringer("index_entry", indexEntry))
		return reindex, false, nil
	}

	err = batch.Flush(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("write index: %w", err)
	}

	return reindex, true, nil
}

func (fdb *FluxDB) indexTablet(ctx context.Context, height uint64, tablet Tablet, skipFromCache bool, skipFromStore bool) (*TabletIndex, error) {
	tabletKey := KeyForTablet(tablet)

	var index *TabletIndex
	if !skipFromCache {
		zlog.Debug("checking if index already exist in cache")
		index = fdb.idxCache.GetIndex(tabletKey)
		if index != nil {
			zlog.Debug("found tablet index in cache", zap.Uint64("at_height", index.AtHeight))
		}
	}

	if index == nil && !skipFromStore {
		zlog.Debug("index not in cache, checking in storage")
		indexSinglet := newIndexSingletFromKey(tabletKey)

		var err error
		index, err = fdb.fetchIndex(ctx, indexSinglet, height-1)
		if err != nil {
			return nil, fmt.Errorf("get index %s at height %d: %w", tablet, height, err)
		}

		if index != nil {
			zlog.Debug("found tablet index in store", zap.Uint64("at_height", index.AtHeight))
		}
	}

	if index == nil {
		zlog.Debug("index does not exist yet, creating empty one")
		index = NewTabletIndex()
	}

	startHeight := uint64(0)
	if index.AtHeight != 0 {
		startHeight = index.AtHeight + 1
	}

	startKey := KeyForTabletAt(tablet, startHeight)
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
		return nil, fmt.Errorf("read rows: %w", err)
	}

	index.AtHeight = height
	index.SquelchCount = uint64(count)

	return index, nil
}

func (fdb *FluxDB) fetchIndex(ctx context.Context, singlet indexSinglet, height uint64) (*TabletIndex, error) {
	actualHeight := height
	if fdb.isInIgnoreIndexRange(actualHeight) {
		// We ignore between 150M - 155M but the current height is between this, move to 150M so we pick an index that happened before this range
		actualHeight = fdb.ignoreIndexRangeStart
	}

	indexEntry, err := fdb.ReadSingletEntryAt(ctx, singlet, actualHeight, nil)
	if err != nil {
		return nil, err
	}

	if indexEntry == nil {
		return nil, nil
	}

	index := indexEntry.(indexSingletEntry).index
	if fdb.isInIgnoreIndexRange(index.AtHeight) {
		// We ignore between 150M - 155M but the current index is in it, re-fetch with 150M so we pick an index that happened before this range
		return fdb.fetchIndex(ctx, singlet, fdb.ignoreIndexRangeStart)
	}

	return index, nil
}

func (fdb *FluxDB) isInIgnoreIndexRange(height uint64) bool {
	// The range must be defined at both end
	if fdb.ignoreIndexRangeStart == 0 || fdb.ignoreIndexRangeStop == 0 {
		return false
	}

	if fdb.ignoreIndexRangeStart >= fdb.ignoreIndexRangeStop {
		zlog.Warn("index ignore range must be be valid, got start after stop", zap.Uint64("start", fdb.ignoreIndexRangeStart), zap.Uint64("stop", fdb.ignoreIndexRangeStop))
		return false
	}

	return height > fdb.ignoreIndexRangeStart && height <= fdb.ignoreIndexRangeStop
}

// ReadTabletIndexAt returns the latest active index at the provided height. If there is
// index available at this height, this method returns `nil` as the index value.
func (fdb *FluxDB) ReadTabletIndexAt(ctx context.Context, tablet Tablet, height uint64) (*TabletIndex, error) {
	ctx, span := dtracing.StartSpan(ctx, "read tablet index")
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
	lastIndexes      map[string]*TabletIndex
	lastCounters     map[string]int
	scheduleIndexing map[string]uint64
}

func newIndexCache() *indexCache {
	return &indexCache{
		lastIndexes:      make(map[string]*TabletIndex),
		lastCounters:     make(map[string]int),
		scheduleIndexing: make(map[string]uint64),
	}
}

func (t *indexCache) GetIndex(key TabletKey) *TabletIndex {
	return t.lastIndexes[string(key)]
}

func (t *indexCache) CacheIndex(key TabletKey, tableIndex *TabletIndex) {
	t.lastIndexes[string(key)] = tableIndex
}

func (t *indexCache) GetCount(key TabletKey) int {
	return t.lastCounters[string(key)]
}

func (t *indexCache) IncCount(key TabletKey) {
	t.lastCounters[string(key)]++
}

func (t *indexCache) ResetCounter(key TabletKey) {
	t.lastCounters[string(key)] = 0
}

func (t *indexCache) Reset() {
	t.lastIndexes = make(map[string]*TabletIndex)
	t.lastCounters = make(map[string]int)
	t.scheduleIndexing = make(map[string]uint64)
}

// This algorithm determines the space between the indexes
func (t *indexCache) shouldTriggerIndexing(key TabletKey) bool {
	mutatedRowsCount := t.lastCounters[string(key)]
	if mutatedRowsCount < 1000 {
		return false
	}

	lastIndex := t.lastIndexes[string(key)]
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

func (t *indexCache) ScheduleIndex(key TabletKey, height uint64) {
	t.scheduleIndexing[string(key)] = height
}

func (t *indexCache) IndexingSchedule() map[string]uint64 {
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
	return newIndexSingletFromKey(KeyForTablet(forTablet))
}

func newIndexSingletFromKey(tabletKey TabletKey) indexSinglet {
	return indexSinglet{tabletKey: tabletKey}
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
		index.PrimaryKeyToHeight = newPrimaryKeyToHeightMap(8) // 8 is our reference small size
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
		BaseSingletEntry: NewBaseSingletEntry(singlet, index.AtHeight, nil),
		index:            index,
	}
}

func (s indexSingletEntry) IsDeletion() bool {
	return false
}

func (s indexSingletEntry) MarshalValue() ([]byte, error) {
	return s.index.MarshalValue()
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
	if !f {
		return 0, f
	}
	return v.(uint64), f
}

func (m *primaryKeyToHeightMap) delete(k []byte) {
	m.bytesMap.delete(k)
}

func (m *primaryKeyToHeightMap) rowKeys(tablet Tablet, height uint64) (keys [][]byte) {
	keys = make([][]byte, m.len())

	i := 0
	for primaryKey, height := range m.mappings {
		keys[i] = KeyForTabletRowFromParts(tablet, height.(uint64), []byte(primaryKey))
		i++
	}

	return
}

func orderedIndexTabletKeys(mappings map[string][]indexSingletEntry) (out []string) {
	out = make([]string, len(mappings))

	i := 0
	for key := range mappings {
		out[i] = key
		i++
	}

	sort.Slice(out, func(i, j int) bool {
		return bytes.Compare([]byte(out[i]), []byte(out[j])) < 0
	})
	return
}
