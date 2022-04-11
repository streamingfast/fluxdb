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

	"github.com/streamingfast/dtracing"
	"github.com/streamingfast/fluxdb/store"
	"github.com/streamingfast/logging"
	pbfluxdb "github.com/streamingfast/pbgo/sf/fluxdb/v1"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
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
		if _, err := batch.FlushIfFull(ctx); err != nil {
			return fmt.Errorf("flush if full: %w", err)
		}

		index, skipped, err := fdb.indexTablet(ctx, height, tablet, false, false, false)
		if err != nil {
			return fmt.Errorf("index tablet %q: %w", tablet, err)
		}

		if skipped {
			// After fetching the last index, we determined this index should not be indexed, cache the last index
			// and delete schedule indexing for it. We do not reset counter however, since we want to actually
			// index it later when it reached a different threshold of mutation count.
			if index != nil {
				fdb.idxCache.CacheIndex(tabletKey, index)
			}
			delete(fdb.idxCache.scheduleIndexing, string(tabletKey))

			continue
		}

		zlog.Debug("about to write index to store",
			zap.Stringer("tablet", tablet),
			zap.Uint64("at_height", index.AtHeight),
			zap.Uint64("squelched_count", index.SquelchCount),
			zap.Int("row_count", index.PrimaryKeyToHeight.len()),
		)

		indexSinglet := newIndexSingletFromKey(tabletKey)
		if err := fdb.writeIndex(ctx, batch, index, indexSinglet); err != nil {
			return fmt.Errorf("write index %q: %w", indexSinglet, err)
		}

		zlog.Debug("caching index in index cache", zap.Stringer("index_singlet", indexSinglet))
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
	indexKeysPerTablet, indexCount, err := fdb.fetchTabletIndexes(ctx, height, lowerBound)
	if err != nil {
		return 0, 0, fmt.Errorf("scan: %w", err)
	}

	orderedIndexTablets := orderedIndexTabletKeys(indexKeysPerTablet)
	zlog.Debug("re-indexing tablets", zap.Int("tablet_count", len(indexKeysPerTablet)), zap.Int("index_count", indexCount))
	if dryRun {
		if tracer.Enabled() {
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
			index, _, err := fdb.indexTablet(ctx, entry.height, tablet, true, false, true)
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
			_, err = batch.FlushIfFull(ctx)
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

	reindex, _, err := fdb.indexTablet(ctx, indexEntry.Height(), tablet, true, false, true)
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

func (fdb *FluxDB) indexTablet(ctx context.Context, height uint64, tablet Tablet, forceIndex bool, skipFromCache bool, skipFromStore bool) (index *TabletIndex, skipped bool, err error) {
	tabletKey := KeyForTablet(tablet)

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
			return nil, false, fmt.Errorf("get index %s at height %d: %w", tablet, height, err)
		}

		if index != nil {
			zlog.Debug("found tablet index in store", zap.Uint64("at_height", index.AtHeight))
		}
	}

	// If we are not forcing indexing and a check to `shouldIndex` returns false, skip the indexing. This
	// condition can happen on a service restart where the index cache is cleared. In this situation, you must
	// wait until here to fetch the last index and check its row count to determine if an index should be created
	// or not.
	if !forceIndex && !fdb.idxCache.shouldIndex(tabletKey, index) {
		zlog.Debug("determined that index should not be created, skipping indexing for tablet", zap.Stringer("tablet", tabletKey))
		return index, true, nil
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
	err = fdb.store.ScanTabletRows(ctx, startKey, endKey, func(key []byte, value []byte) error {
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
		return nil, false, fmt.Errorf("read rows: %w", err)
	}

	index.AtHeight = height
	index.SquelchCount = uint64(count)

	return index, false, nil
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

func (fdb *FluxDB) PruneTabletIndexes(ctx context.Context, pruneFrequency int, height uint64, lowerBound Tablet, dryRun bool) (tabletCount int, indexCount int, deletedIndexCount int, err error) {
	if pruneFrequency <= 1 {
		return 0, 0, 0, fmt.Errorf("prune frequency must be greater than 1, got %d", pruneFrequency)
	}

	indexKeysPerTablet, indexCount, err := fdb.fetchTabletIndexes(ctx, height, lowerBound)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("scan: %w", err)
	}

	orderedIndexTablets := orderedIndexTabletKeys(indexKeysPerTablet)
	zlog.Debug("pruning tablet indexes",
		zap.Bool("dry_run", dryRun),
		zap.Int("frequency", pruneFrequency),
		zap.Int("tablet_count", len(indexKeysPerTablet)),
		zap.Int("index_count", indexCount),
	)

	batch := fdb.store.NewBatch(zlog)
	for _, tabletKey := range orderedIndexTablets {
		indexes := indexKeysPerTablet[tabletKey]

		// If there is less index than the prune frequency requested, there is nothing to do on this tablet
		// We add + 2 to prune frequency on the check because we always keep most current and least current
		// indexes.
		if len(indexes) <= pruneFrequency+2 {
			continue
		}

		tablet, err := NewTablet([]byte(tabletKey))
		if err != nil {
			return 0, 0, 0, fmt.Errorf("new tablet for key %x: %w", []byte(tabletKey), err)
		}

		// We remove first and last elements (sure to be present) and sort the remaining from highest height to lowest height
		indexes = indexes[1 : len(indexes)-1]
		sort.Slice(indexes, func(i, j int) bool {
			return indexes[i].height >= indexes[j].height
		})

		tabletCount++
		for i, entry := range indexes {
			if (i+1)%pruneFrequency == 0 {
				deletedIndexCount++
				if dryRun {
					zlog.Debug("would prune tablet index", zap.Stringer("id", entry))
					continue
				}

				batch.PurgeRow(KeyForSingletEntry(entry))
			}
		}

		flushed, err := batch.FlushIfFull(ctx)
		if err != nil {
			return 0, 0, 0, fmt.Errorf("write indexes: %w", err)
		}

		if flushed {
			zlog.Info("flushed tablet indexes up to tablet", zap.Stringer("lower_bound", tablet))
		}
	}

	err = batch.Flush(ctx)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("write indexes: %w", err)
	}

	return tabletCount, indexCount, deletedIndexCount, nil
}

func (fdb *FluxDB) fetchTabletIndexes(ctx context.Context, height uint64, lowerBound Tablet) (indexKeysPerTablet map[string][]indexSingletEntry, indexCount int, err error) {
	prefix := make([]byte, 2)
	bigEndian.PutUint16(prefix, indexSingletCollection)

	lowerBoundKey := []byte(nil)
	if lowerBound != nil {
		lowerBoundKey = KeyForTablet(lowerBound)
	}

	indexKeysPerTablet = map[string][]indexSingletEntry{}
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

	return
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

func (fdb *FluxDB) writeIndex(ctx context.Context, batch store.Batch, index *TabletIndex, singlet indexSinglet) error {
	indexEntry := newIndexSingletEntry(singlet, index)
	value, err := indexEntry.MarshalValue()
	if err != nil {
		return fmt.Errorf("index entry to proto: %w", err)
	}

	// When above 25MB, flag as being a big index
	if len(value) > 25*1000*1000 {
		zlog.Warn("index singlet pretty heavy", zap.Stringer("index_entry", indexEntry), zap.Int("byte_count", len(value)))
	}

	batch.SetRow(KeyForSingletEntry(indexEntry), value)
	return nil
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
	return t.shouldIndex(key, t.lastIndexes[string(key)])
}

// shouldIndex determines if the following tablet and its previous tablet index (could be nil)
// should be indexed again.
//
// The algorithm is as follow:
//  If there is less than 25K mutations, skip
//  If there is greater or equal than 25K mutations =>
//    If there is no previous index, index
//    If there is a previous index =>
//      If the previous index has less or equal than 50K rows, index
//      If the previous index has more than 50K rows but less or equal than 200K rows =>
//         If there is greater than "previous index row count / 2" mutations, index
//         Otherwise, skip
//      If the previous index has more than 200K rows =>
//         If there is greater than 100K mutations, index
//         Otherwise, skip
func (t *indexCache) shouldIndex(key TabletKey, previousIndex *TabletIndex) bool {
	mutatedRowsCount := t.lastCounters[string(key)]

	// If there is less than 25K mutations, wheter or not a previous index existed, we are not ready to index this tablet
	if mutatedRowsCount < 25000 {
		return false
	}

	// There is >= 25K mutations and there is no known previous index, we are ready to index this tablet
	if previousIndex == nil {
		return true
	}

	// Special handling for big table, i.e. previous index had > 50K rows, to limit the number of index we create at the expense on longer read time
	if previousIndex.RowCount() > 50000 {
		// First, let's compute half of the row count, it will be necessarly > 25K rows here
		halfRow := previousIndex.RowCount() / 2
		switch {
		// If the tablet has 200K rows or less (halfRow <= 100K) and there is >= Half Row mutations, we index
		case halfRow <= 100000:
			return mutatedRowsCount > int(halfRow)

		// If the tablet has > 200K rows, cap the mutations to 100K rows
		default:
			return mutatedRowsCount >= 100000
		}
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
