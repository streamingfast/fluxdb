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
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/dfuse-io/bstream"
	"github.com/streamingfast/dtracing"
	"github.com/streamingfast/fluxdb/store"
	"github.com/dfuse-io/logging"
	pbfluxdb "github.com/dfuse-io/pbgo/dfuse/fluxdb/v1"
	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
)

func (fdb *FluxDB) ReadTabletAt(
	ctx context.Context,
	height uint64,
	tablet Tablet,
	speculativeWrites []*WriteRequest,
) ([]TabletRow, error) {
	ctx, span := dtracing.StartSpan(ctx, "read tablet", "tablet", tablet, "height", height)
	defer span.End()

	zlogger := logging.Logger(ctx, zlog)
	zlogger.Debug("reading tablet", zap.Stringer("tablet", tablet), zap.Uint64("height", height))

	idx, err := fdb.ReadTabletIndexAt(ctx, tablet, height)
	if err != nil {
		return nil, fmt.Errorf("fetch tablet index: %w", err)
	}

	startKey := KeyForTabletAt(tablet, 0)
	endKey := KeyForTabletAt(tablet, height+1)

	var rowByPrimaryKey *primaryKeyToTabletRowMap
	if idx != nil {
		idxRowCount := idx.RowCount()
		zlogger.Debug("tablet index exists, reconciling it", zap.Uint64("height", idx.AtHeight), zap.Uint64("row_count", idxRowCount))
		startKey = KeyForTabletAt(tablet, idx.AtHeight+1)

		// Let's pre-allocated `rowByPrimaryKey`, it's likely to need at least as much rows as in the index itself
		rowByPrimaryKey = newPrimaryKeyToTabletRowMap(int(idxRowCount))
		keys := idx.PrimaryKeyToHeight.rowKeys(tablet, height)

		// Fetch all rows in the index.. could be millions
		// We need to batch so that the RowList, when serialized, doesn't blow up 1MB
		// We should batch in 10,000 key reads, we can parallelize those...
		chunkSize := 5000
		chunks := int(math.Ceil(float64(len(keys)) / float64(chunkSize)))

		zlogger.Debug("reading index rows chunks", zap.Int("chunk_count", chunks))
		for i := 0; i < chunks; i++ {
			chunkStart := i * chunkSize
			chunkEnd := (i + 1) * chunkSize
			max := len(keys)
			if max < chunkEnd {
				chunkEnd = max
			}

			keysChunk := keys[chunkStart:chunkEnd]
			zlogger.Debug("reading tablet index rows chunk", zap.Int("chunk_index", i), zap.Int("key_count", len(keysChunk)))

			keyRead := false
			err := fdb.store.FetchTabletRows(ctx, keysChunk, func(key []byte, value []byte) error {
				if len(value) == 0 {
					return fmt.Errorf("indexes mappings should not contain empty data, empty rows don't make sense in a tablet index, row %q", Key(key))
				}

				row, err := NewTabletRow(tablet, key, value)
				if err != nil {
					return fmt.Errorf("tablet index new row %q: %w", Key(key), err)
				}

				rowByPrimaryKey.put(row.PrimaryKey(), row)

				keyRead = true
				return nil
			})

			if err != nil {
				return nil, fmt.Errorf("reading tablet index rows chunk %d: %w", i, err)
			}

			if !keyRead {
				return nil, fmt.Errorf("reading a tablet index yielded no row, had %d keys in chunk", len(keysChunk))
			}
		}

		zlogger.Debug("finished reconciling index")
	} else {
		// We need to create a new map with a default length
		rowByPrimaryKey = newPrimaryKeyToTabletRowMap(8)
	}

	zlogger.Debug("reading tablet rows from database",
		zap.Bool("index_found", idx != nil),
		zap.Uint64("index_row_count", idx.RowCount()),
		zap.Stringer("start_key", startKey),
		zap.Stringer("end_key", endKey),
	)

	deletedCount := 0
	updatedCount := 0

	err = fdb.store.ScanTabletRows(ctx, startKey, endKey, func(key []byte, value []byte) error {
		row, err := NewTabletRow(tablet, key, value)
		if err != nil {
			return fmt.Errorf("tablet new row %q: %w", Key(key), err)
		}

		if row.IsDeletion() {
			deletedCount++
			rowByPrimaryKey.delete(row.PrimaryKey())

			return nil
		}

		updatedCount++
		rowByPrimaryKey.put(row.PrimaryKey(), row)

		return nil
	})

	if err != nil {
		return nil, err
	}

	zlogger.Debug("reading tablet rows from speculative writes",
		zap.Int("accumulated_row_count", rowByPrimaryKey.len()),
		zap.Int("deleted_count", deletedCount),
		zap.Int("updated_count", updatedCount),
		zap.Int("speculative_write_count", len(speculativeWrites)),
	)

	for _, speculativeWrite := range speculativeWrites {
		for _, speculativeRow := range speculativeWrite.TabletRows {
			if !TabletEqual(tablet, speculativeRow.Tablet()) {
				continue
			}

			if speculativeRow.IsDeletion() {
				deletedCount++
				rowByPrimaryKey.delete(speculativeRow.PrimaryKey())
			} else {
				updatedCount++
				rowByPrimaryKey.put(speculativeRow.PrimaryKey(), speculativeRow)
			}
		}
	}

	zlogger.Debug("post-processing tablet rows", zap.Int("row_count", rowByPrimaryKey.len()))

	rows := rowByPrimaryKey.values()
	sort.Slice(rows, func(i, j int) bool { return bytes.Compare(rows[i].PrimaryKey(), rows[j].PrimaryKey()) < 0 })

	zlogger.Debug("finished reading tablet rows", zap.Int("deleted_count", deletedCount), zap.Int("updated_count", updatedCount))
	return rows, nil
}

func (fdb *FluxDB) ReadTabletRowAt(
	ctx context.Context,
	height uint64,
	tablet Tablet,
	primaryKey TabletRowPrimaryKey,
	speculativeWrites []*WriteRequest,
) (TabletRow, error) {
	ctx, span := dtracing.StartSpan(ctx, "read tablet row", "tablet", tablet, "height", height, "primaryKey", primaryKey)
	defer span.End()

	zlogger := logging.Logger(ctx, zlog)
	zlogger.Debug("reading tablet row", zap.Stringer("tablet", tablet), zap.Uint64("height", height), zap.Stringer("primary_key", primaryKey))

	idx, err := fdb.ReadTabletIndexAt(ctx, tablet, height)
	if err != nil {
		return nil, fmt.Errorf("fetch tablet index: %w", err)
	}

	startKey := KeyForTabletAt(tablet, 0)
	endKey := KeyForTabletAt(tablet, height+1)

	primaryKeyBytes := primaryKey.Bytes()
	var row TabletRow
	if idx != nil {
		idxRowCount := idx.RowCount()
		zlogger.Debug("tablet index exists, reconciling it", zap.Uint64("height", idx.AtHeight), zap.Uint64("row_count", idxRowCount))
		startKey = KeyForTabletAt(tablet, idx.AtHeight+1)

		if height, ok := idx.PrimaryKeyToHeight.get(primaryKeyBytes); ok {
			rowKey := KeyForTabletRowFromParts(tablet, height, primaryKeyBytes)
			zlogger.Debug("reading index row", zap.Stringer("row_key", rowKey))

			value, err := fdb.store.FetchTabletRow(ctx, rowKey)
			if errors.Is(err, store.ErrNotFound) {
				return nil, fmt.Errorf("indexes mappings should not contain empty data, empty rows don't make sense in an index, row %q", rowKey)
			}
			if err != nil {
				return nil, fmt.Errorf("reading tablet index row %q: %w", rowKey, err)
			}
			if len(value) > 0 {
				row, err = tablet.Row(height, primaryKeyBytes, value)
				if err != nil {
					return nil, fmt.Errorf("could not create table from key value with row key %q: %w", rowKey, err)
				}
			}

		}
		zlogger.Debug("finished reconciling index", zap.Bool("row_exist", row != nil))
	}

	zlogger.Debug("reading tablet row from database",
		zap.Bool("row_exist", row != nil),
		zap.Bool("index_found", idx != nil),
		zap.Stringer("start_key", startKey),
		zap.Stringer("end_key", endKey),
	)

	deletedCount := 0
	updatedCount := 0

	err = fdb.store.ScanTabletRows(ctx, startKey, endKey, func(key []byte, value []byte) error {
		candidateRow, err := NewTabletRow(tablet, key, value)
		if err != nil {
			return fmt.Errorf("tablet new row %q: %w", Key(key), err)
		}

		if !bytes.Equal(primaryKeyBytes, candidateRow.PrimaryKey()) {
			return nil
		}

		if candidateRow.IsDeletion() {
			row = nil
			deletedCount++

			return nil
		}

		updatedCount++
		row = candidateRow
		return nil
	})
	if err != nil {
		return nil, err
	}

	zlogger.Debug("reading tablet row from speculative writes",
		zap.Int("deleted_count", deletedCount),
		zap.Int("updated_count", updatedCount),
		zap.Int("speculative_write_count", len(speculativeWrites)),
	)

	for _, speculativeWrite := range speculativeWrites {
		for _, speculativeRow := range speculativeWrite.TabletRows {
			if !TabletEqual(tablet, speculativeRow.Tablet()) {
				continue
			}

			if !bytes.Equal(primaryKeyBytes, speculativeRow.PrimaryKey()) {
				continue
			}

			if speculativeRow.IsDeletion() {
				deletedCount++
				row = nil
			} else {
				updatedCount++
				row = speculativeRow
			}
		}
	}

	zlogger.Debug("finished reading tablet row", zap.Int("deleted_count", deletedCount), zap.Int("updated_count", updatedCount))
	return row, nil
}

// ReadSingletEntryAt query the storage engine returning the active singlet entry
// value at specified height.
//
// Returns `<Entry>, nil` when an entry has been found, `nil, nil` when no entry was found
// and finally, `nil, <error>` if an error was encountered while fetching the singlet entry.
func (fdb *FluxDB) ReadSingletEntryAt(
	ctx context.Context,
	singlet Singlet,
	height uint64,
	speculativeWrites []*WriteRequest,
) (SingletEntry, error) {
	ctx, span := dtracing.StartSpan(ctx, "read singlet entry", "singlet", singlet, "height", height)
	defer span.End()

	// We are using inverted block num, so we are scanning from highest block num (request block num) to lowest block (0)
	startKey := KeyForSingletAt(singlet, height)
	endKey := KeyForSingletAt(singlet, 0)

	zlog := logging.Logger(ctx, zlog)
	zlog.Debug("reading singlet entry from database", zap.Stringer("singlet", singlet), zap.Uint64("height", height), zap.Stringer("start_key", startKey), zap.Stringer("end_key", endKey))

	var entry SingletEntry
	key, value, err := fdb.store.FetchSingletEntry(ctx, startKey, endKey)
	if err != nil {
		return nil, fmt.Errorf("db fetch single entry: %w", err)
	}

	// If there is a key set (record found) and the value is non-nil (it's NOT a deleted entry), then populated it
	if len(key) > 0 && len(value) > 0 {
		entry, err = NewSingletEntry(singlet, key, value)
		if err != nil {
			return nil, fmt.Errorf("failed to create single tablet row %q: %w", Key(key), err)
		}
	}

	zlog.Debug("reading singlet entry from speculative writes", zap.Bool("db_exist", entry != nil), zap.Int("speculative_write_count", len(speculativeWrites)))
	for _, writeRequest := range speculativeWrites {
		for _, speculativeEntry := range writeRequest.SingletEntries {
			if !SingletEqual(singlet, speculativeEntry.Singlet()) {
				continue
			}

			if speculativeEntry.IsDeletion() {
				entry = nil
			} else {
				entry = speculativeEntry
			}
		}
	}

	zlog.Debug("finished reading singlet entry", zap.Bool("entry_exist", entry != nil))
	return entry, nil
}

func (fdb *FluxDB) HasSeenAnyRowForTablet(ctx context.Context, tablet Tablet) (exists bool, err error) {
	ctx, span := dtracing.StartSpan(ctx, "has seen tablet row", "tablet", tablet.String())
	defer span.End()

	return fdb.store.HasTabletRow(ctx, KeyForTabletAt(tablet, 0), KeyForTabletAt(tablet, math.MaxUint64))
}

func (fdb *FluxDB) FetchLastWrittenCheckpoint(ctx context.Context) (height uint64, block bstream.BlockRef, err error) {
	zlogger := logging.Logger(ctx, zlog)

	value, err := fdb.store.FetchLastWrittenCheckpoint(ctx, fdb.lastCheckpointKey())
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			zlogger.Info("last written block empty, returning empty checkpoint values")
			return 0, bstream.BlockRefEmpty, nil
		}

		return 0, nil, fmt.Errorf("kv store: %w", err)
	}

	height, block, err = unmarshalCheckpoint(value)
	if err != nil {
		return 0, nil, fmt.Errorf("unable to unmarshal checkpoint: %w", err)
	}

	zlogger.Debug("last written checkpoint", zap.Uint64("height", height), zap.Stringer("block", block))
	return
}

func (fdb *FluxDB) CheckCleanDBForSharding() error {
	_, err := fdb.store.FetchLastWrittenCheckpoint(context.Background(), lastCheckpointRowKey)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// When there is nothing, it's what we expect, so there is no error
			return nil
		}

		return err
	}

	// At this point, the fetch return something viable, this is not correct for sharding reprocessing
	return errors.New("live injector's marker of last written block present, expected no element to exist")
}

func (fdb *FluxDB) finalCheckpointKey() []byte {
	return lastCheckpointRowKey
}

func (fdb *FluxDB) lastCheckpointKey() []byte {
	if fdb.IsSharding() {
		return []byte(fmt.Sprintf("shard-%03d", fdb.shardIndex))
	}

	return lastCheckpointRowKey
}

func unmarshalCheckpoint(value []byte) (height uint64, block bstream.BlockRef, err error) {
	var checkpoint pbfluxdb.Checkpoint
	err = proto.Unmarshal(value, &checkpoint)
	if err != nil {
		return 0, nil, err
	}

	height = checkpoint.Height
	block = bstream.NewBlockRef(checkpoint.Block.Id, checkpoint.Block.Num)
	return
}
