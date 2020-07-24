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

	"github.com/dfuse-io/dtracing"
	"github.com/dfuse-io/fluxdb/store"
	"github.com/dfuse-io/logging"
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

		zlog.Debug("checking if index already exist in cache")
		index := fdb.idxCache.GetIndex(tablet)
		if index == nil {
			zlog.Debug("index not in cache")

			var err error
			index, err = fdb.getIndex(ctx, height, tablet)
			if err != nil {
				return fmt.Errorf("get index %s (%d): %w", tablet, height, err)
			}

			if index == nil {
				zlog.Debug("index does not exist yet, creating empty one")
				index = NewTabletIndex()
			}
		}

		startKey := tablet.KeyAt(index.AtHeight + 1)
		endKey := tablet.KeyAt(height + 1)

		zlog.Debug("reading table rows for indexation", zap.String("first_row_key", startKey), zap.String("last_row_key", endKey))

		count := 0
		err := fdb.store.ScanTabletRows(ctx, startKey, endKey, func(key string, value []byte) error {
			row, err := tablet.NewRowFromKV(key, value)
			if err != nil {
				return fmt.Errorf("couldn't parse row key %q: %w", key, err)
			}

			count++

			if len(value) == 0 {
				delete(index.PrimaryKeyToHeight, row.PrimaryKey())
			} else {
				index.PrimaryKeyToHeight[row.PrimaryKey()] = row.Height()
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
			zap.Int("row_count", len(index.PrimaryKeyToHeight)),
		)

		snapshot, err := tablet.IndexMapper().EncodeIndex(index.SquelchCount, index.PrimaryKeyToHeight)
		if err != nil {
			return fmt.Errorf("unable to marshal tablet index to binary: %w", err)
		}

		indexKey := tablet.Key() + "/" + HexRevHeight(index.AtHeight)

		byteCount := len(snapshot)
		if byteCount > 25000000 {
			zlog.Warn("table index pretty heavy", zap.String("index_key", indexKey), zap.Int("byte_count", byteCount))
		}

		batch.SetIndex(indexKey, snapshot)

		zlog.Debug("caching index in index cache", zap.String("index_key", indexKey), zap.Stringer("tablet", tablet))
		fdb.idxCache.CacheIndex(tablet, index)
		fdb.idxCache.ResetCounter(tablet)
		delete(fdb.idxCache.scheduleIndexing, tablet)
	}

	if err := batch.Flush(ctx); err != nil {
		return fmt.Errorf("final flush: %w", err)
	}

	return nil
}

func (fdb *FluxDB) getIndex(ctx context.Context, height uint64, tablet Tablet) (index *TabletIndex, err error) {
	ctx, span := dtracing.StartSpan(ctx, "get index")
	defer span.End()

	zlog := logging.Logger(ctx, zlog)
	zlog.Debug("fetching table index from database", zap.Stringer("tablet", tablet), zap.Uint64("height", height))

	tabletKey := string(tablet.Key())
	prefixKey := tabletKey + "/"
	startIndexKey := prefixKey + HexRevHeight(height)

	zlog.Debug("reading table index row", zap.String("start_index_key", startIndexKey))
	rowKey, rawIndex, err := fdb.store.FetchIndex(ctx, tabletKey, prefixKey, startIndexKey)
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	indexHeight, err := chunkKeyRevHeight(rowKey, prefixKey)
	if err != nil {
		return nil, fmt.Errorf("couldn't infer block num in table index's row key: %w", err)
	}

	index = &TabletIndex{
		AtHeight: indexHeight,
	}

	index.SquelchCount, index.PrimaryKeyToHeight, err = tablet.IndexMapper().DecodeIndex(rawIndex)
	if err != nil {
		return nil, fmt.Errorf("couldn't decode tablet index: %w", err)
	}

	return index, nil
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

	indexRowCount := len(lastIndex.PrimaryKeyToHeight)

	if indexRowCount > 50000 && mutatedRowsCount < 5000 {
		return false
	}

	if indexRowCount > 100000 && mutatedRowsCount < 10000 {
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

// type TableIndex struct {
// 	AtHeight  uint64
// 	Squelched uint64
// 	Map       map[string]uint64 // Map[primaryKey] => height
// }

// func NewTableIndex() *TableIndex {
// 	return &TableIndex{Map: make(map[string]uint64)}
// }

// func (index *TableIndex) RowCount() int {
// 	if index == nil {
// 		return 0
// 	}

// 	return len(index.Map)
// }

// func NewTableIndexFromBinary(ctx context.Context, tablet Tablet, atHeight uint64, buffer []byte) (*TableIndex, error) {
// 	ctx, span := dtracing.StartSpan(ctx, "new table index from binary", "tablet", tablet, "height", atHeight)
// 	defer span.End()

// 	// Byte count for primary key + 8 bytes for block num value
// 	primaryKeyByteCount := tablet.PrimaryKeyByteCount()
// 	entryByteCount := primaryKeyByteCount + 8

// 	// First 16 bytes are reserved to keep stats in there..
// 	byteCount := len(buffer)
// 	if (byteCount-16) < 0 || (byteCount-16)%entryByteCount != 0 {
// 		return nil, fmt.Errorf("unable to unmarshal table index: %d bytes alignment + 16 bytes metadata is off (has %d bytes)", entryByteCount, byteCount)
// 	}

// 	mapping := map[string]uint64{}
// 	for pos := 16; pos < byteCount; pos += entryByteCount {
// 		primaryKey, err := tablet.DecodePrimaryKey(buffer[pos:])
// 		if err != nil {
// 			return nil, fmt.Errorf("unable to read primary key for tablet %q: %w", tablet, err)
// 		}

// 		blockNumPtr := bigEndian.Uint64(buffer[pos+primaryKeyByteCount:])
// 		mapping[primaryKey] = blockNumPtr
// 	}

// 	return &TableIndex{
// 		AtHeight:  atHeight,
// 		Squelched: bigEndian.Uint64(buffer[:8]),
// 		Map:       mapping,
// 	}, nil
// }

// func (index *TableIndex) MarshalBinary(ctx context.Context, tablet Tablet) ([]byte, error) {
// 	ctx, span := dtracing.StartSpan(ctx, "marshal table index to binary", "tablet", tablet)
// 	defer span.End()

// 	primaryKeyByteCount := tablet.PrimaryKeyByteCount()
// 	entryByteCount := primaryKeyByteCount + 8 // Byte count for primary key + 8 bytes for block num value

// 	snapshot := make([]byte, entryByteCount*len(index.Map)+16)
// 	bigEndian.PutUint64(snapshot, index.Squelched)

// 	pos := 16
// 	for primaryKey, height := range index.Map {
// 		err := tablet.EncodePrimaryKey(snapshot[pos:], primaryKey)
// 		if err != nil {
// 			return nil, fmt.Errorf("unable to read primary key for tablet %q: %w", tablet, err)
// 		}

// 		bigEndian.PutUint64(snapshot[pos+primaryKeyByteCount:], height)
// 		pos += entryByteCount
// 	}

// 	return snapshot, nil
// }

// func (index *TableIndex) String() string {
// 	builder := &strings.Builder{}
// 	fmt.Fprintln(builder, "INDEX:")

// 	fmt.Fprintln(builder, "  * At block num:", index.AtHeight)
// 	fmt.Fprintln(builder, "  * Squelches:", index.Squelched)
// 	var keys []string
// 	for primKey := range index.Map {
// 		keys = append(keys, primKey)
// 	}

// 	sort.Strings(keys)

// 	fmt.Fprintln(builder, "Snapshot (primkey -> height)")
// 	for _, k := range keys {
// 		fmt.Fprintf(builder, "  %s -> %d\n", k, index.Map[k])
// 	}

// 	return builder.String()
// }

// type indexPrimaryKeyReader = func(buffer []byte) (string, error)
// type indexPrimaryKeyWriter = func(primaryKey string, buffer []byte) error

// func twoUint64PrimaryKeyReaderFactory(tag string) indexPrimaryKeyReader {
// 	return func(buffer []byte) (string, error) {
// 		if len(buffer) < 16 {
// 			return "", fmt.Errorf("%s primary key reader: not enough bytes to read, %d bytes left, wants %d", tag, len(buffer), 16)
// 		}

// 		chunk1, err := readOneUint64(buffer)
// 		if err != nil {
// 			return "", fmt.Errorf("%s primary key reader, chunk #1: %w", tag, err)
// 		}

// 		chunk2, err := readOneUint64(buffer[8:])
// 		if err != nil {
// 			return "", fmt.Errorf("%s primary key reader, chunk #2: %w", tag, err)
// 		}

// 		return strings.Join([]string{chunk1, chunk2}, ":"), nil
// 	}
// }

// func readOneUint64(buffer []byte) (string, error) {
// 	if len(buffer) < 8 {
// 		return "", fmt.Errorf("not enough bytes to read uint64, %d bytes left, wants %d", len(buffer), 8)
// 	}

// 	return fmt.Sprintf("%016x", bigEndian.Uint64(buffer)), nil
// }

// func twoUint64PrimaryKeyWriterFactory(tag string) indexPrimaryKeyWriter {
// 	return func(primaryKey string, buffer []byte) error {

// 		chunks := strings.Split(primaryKey, ":")
// 		if len(chunks) != 2 {
// 			return fmt.Errorf("%s primary key should have 2 chunks, got %d", tag, len(chunks))
// 		}

// 		err := writeOneUint64(chunks[0], buffer)
// 		if err != nil {
// 			return fmt.Errorf("%s primary key writer, chunk #1: %w", tag, err)
// 		}

// 		err = writeOneUint64(chunks[1], buffer[8:])
// 		if err != nil {
// 			return fmt.Errorf("%s primary key writer, chunk #2: %w", tag, err)
// 		}

// 		return nil
// 	}
// }

// func writeOneUint64(primaryKey string, buffer []byte) error {
// 	value, err := strconv.ParseUint(primaryKey, 16, 64)
// 	if err != nil {
// 		return fmt.Errorf("unable to transform primary key to uint64: %w", err)
// 	}

// 	bigEndian.PutUint64(buffer, value)
// 	return nil
// }
