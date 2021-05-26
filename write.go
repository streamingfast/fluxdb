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
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/dtracing"
	"github.com/dfuse-io/fluxdb/store"
	"github.com/dfuse-io/logging"
	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	pbfluxdb "github.com/dfuse-io/pbgo/dfuse/fluxdb/v1"
	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
)

var printTabletStats = os.Getenv("STATEDB_SIZE_STATS") != ""

func (fdb *FluxDB) WriteBatch(ctx context.Context, w []*WriteRequest) error {
	ctx, span := dtracing.StartSpan(ctx, "write batch", "write_request_count", len(w))
	defer span.End()

	if err := fdb.isNextBlock(ctx, w[0].Height); err != nil {
		return fmt.Errorf("next block check: %w", err)
	}

	batch := fdb.store.NewBatch(zlog)

	for _, req := range w {
		if err := fdb.writeBlock(ctx, batch, req); err != nil {
			return fmt.Errorf("write block: %w", err)
		}

		if _, err := batch.FlushIfFull(ctx); err != nil {
			return fmt.Errorf("flushing if full: %w", err)
		}
	}

	if err := batch.Flush(ctx); err != nil {
		return fmt.Errorf("flush: %w", err)
	}

	if sched := fdb.idxCache.IndexingSchedule(); len(sched) != 0 {
		err := fdb.IndexTables(ctx)
		if err != nil {
			return fmt.Errorf("index tables: %w", err)
		}
	}

	return nil
}

type ShardProgressStats struct {
	HighestHeight     uint64
	BlockRefByShard   map[int]bstream.BlockRef
	ReferenceBlockRef bstream.BlockRef
	FaultyShards      []int
	MissingShards     []int
}

func (fdb *FluxDB) VerifyAllShardsWritten(ctx context.Context) (*ShardProgressStats, error) {
	stats, err := fdb.fetchAllShardProgressStats(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetch shard progress: %w", err)
	}

	if len(stats.MissingShards) > 0 {
		err = fmt.Errorf("missing shards: %v", stats.MissingShards)
	}

	if len(stats.FaultyShards) > 0 {
		err = fmt.Errorf("shards not matching reference block %s (shards %v): %w", stats.ReferenceBlockRef, stats.FaultyShards, err)
	}

	return stats, err
}

func (fdb *FluxDB) fetchAllShardProgressStats(ctx context.Context) (*ShardProgressStats, error) {
	stats := &ShardProgressStats{
		BlockRefByShard:   map[int]bstream.BlockRef{},
		ReferenceBlockRef: bstream.BlockRefEmpty,
	}

	seen := make(map[int]bstream.BlockRef)
	err := fdb.store.ScanLastShardsWrittenCheckpoint(ctx, []byte("shard-"), func(key []byte, value []byte) error {
		height, block, err := unmarshalCheckpoint(value)
		if err != nil {
			return fmt.Errorf("unable to unmarshal checkpoint: %w", err)
		}

		shardIndexRaw := string(bytes.TrimPrefix(key, []byte("shard-")))
		shardIndex, err := strconv.Atoi(shardIndexRaw)
		if err != nil {
			return fmt.Errorf("invalid shard index %q: %w", shardIndexRaw, err)
		}

		seen[shardIndex] = block
		if height > stats.HighestHeight {
			stats.HighestHeight = height
		}

		if stats.ReferenceBlockRef == bstream.BlockRefEmpty {
			stats.ReferenceBlockRef = block

			if traceEnabled {
				zlog.Debug("shard progression updating reference block", zap.Stringer("reference_block", stats.ReferenceBlockRef))
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	if traceEnabled {
		zlog.Debug("shard progression initial fetching done",
			zap.Int("seen_count", len(seen)),
			zap.Uint64("highest_height", stats.HighestHeight),
			zap.Stringer("reference_block", stats.ReferenceBlockRef),
		)
	}

	emptyBlockRefCount := 0
	for i := 0; i < fdb.shardCount; i++ {
		seenBlock, found := seen[i]
		if !found {
			emptyBlockRefCount++
			seenBlock = bstream.BlockRefEmpty
		}

		stats.BlockRefByShard[i] = seenBlock
	}

	for shardIndex, shardBlock := range stats.BlockRefByShard {
		if bstream.EqualsBlockRefs(shardBlock, bstream.BlockRefEmpty) {
			stats.MissingShards = append(stats.MissingShards, shardIndex)
		}
		if !bstream.EqualsBlockRefs(shardBlock, stats.ReferenceBlockRef) {
			stats.FaultyShards = append(stats.FaultyShards, shardIndex)
		}
	}

	if traceEnabled {
		zlog.Debug("shard progression fetching done",
			zap.Int("empty_shard_count", emptyBlockRefCount),
			zap.Int("missing_shard_count", len(stats.MissingShards)),
			zap.Int("faulyt_shard_count", len(stats.FaultyShards)),
		)
	}

	return stats, nil
}

func (fdb *FluxDB) WriteShardingFinalCheckpoint(ctx context.Context, height uint64, block bstream.BlockRef) error {
	batch := fdb.store.NewBatch(zlog)
	if err := fdb.setFinalCheckpoint(batch, height, block); err != nil {
		return fmt.Errorf("set last checkpoint: %w", err)
	}

	if err := batch.Flush(ctx); err != nil {
		return fmt.Errorf("flushing last block marker: %w", err)
	}

	return nil
}

func (fdb *FluxDB) DeleteAllShardCheckpoints(ctx context.Context) error {
	return fdb.store.DeleteShardsCheckpoint(ctx, []byte("shard-"))
}

func (fdb *FluxDB) writeBlock(ctx context.Context, batch store.Batch, w *WriteRequest) (err error) {

	totalSize := 0
	indexCount := 0
	tabletSizes := make(map[string]int)

	for _, entry := range w.SingletEntries {
		var value []byte
		if !entry.IsDeletion() {
			value, err = entry.MarshalValue()
			if err != nil {
				return fmt.Errorf("singlet to proto: %w", err)
			}
			if printTabletStats {
				totalSize += len(value)
			}
		}

		batch.SetRow(KeyForSingletEntry(entry), value)
	}

	for _, row := range w.TabletRows {
		var value []byte
		if !row.IsDeletion() {
			value, err = row.MarshalValue()
			if err != nil {
				return fmt.Errorf("tablet to proto: %w", err)
			}
		}

		batch.SetRow(KeyForTabletRow(row), value)

		if printTabletStats {
			shortName := row.String()
			nameParts := strings.Split(shortName, ":")
			if len(nameParts) > 3 {
				shortName = strings.Join(nameParts[0:3], ":")
			}
			totalSize += len(value)
			if size, ok := tabletSizes[shortName]; ok {
				size += len(value)
			} else {
				tabletSizes[shortName] = len(value)
			}
		}

		if !fdb.disableIndexing {
			// We could group `w.TabletRows` by tablet here greatly reducing the number of time
			// we need to compute the tablet key, reducing memory allocation an GC at the same time.
			tabletKey := KeyForTablet(row.Tablet())
			fdb.idxCache.IncCount(tabletKey)
			if fdb.idxCache.shouldTriggerIndexing(tabletKey) {
				fdb.idxCache.ScheduleIndex(tabletKey, w.Height)
				indexCount += 1
			}
		}
	}

	if printTabletStats && len(tabletSizes) > 0 {
		keys := make([]string, 0, len(tabletSizes))
		for key := range tabletSizes {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool { return tabletSizes[keys[i]] > tabletSizes[keys[j]] })

		bigTablets := []string{}
		for i, key := range keys {
			if i > 4 {
				break
			}
			bigTablets = append(bigTablets, fmt.Sprintf("%s, %d\n", key, tabletSizes[key]))
		}

		zlog.Info("statedb size stats", zap.Strings("biggest_tablets", bigTablets), zap.Int("total_size", totalSize), zap.Uint64("block_num", w.BlockRef.Num()), zap.Int("indexes_created", indexCount))
	}

	return fdb.setLastCheckpoint(batch, w.Height, w.BlockRef)
}

func (fdb *FluxDB) isNextBlock(ctx context.Context, writeHeight uint64) error {
	zlogger := logging.Logger(ctx, zlog)
	zlogger.Debug("checking if is next block", zap.Uint64("height", writeHeight))

	_, lastBlock, err := fdb.FetchLastWrittenCheckpoint(ctx)
	if err != nil {
		return err
	}

	// FIXME (height): This works only for block num, if we move to a "height" structure, we should just check if linear probably
	lastHeight := lastBlock.Num()
	if lastHeight != writeHeight-1 && lastHeight != 0 && lastHeight != 1 {
		return fmt.Errorf("block %d does not follow last block %d in db", writeHeight, lastHeight)
	}

	return nil
}

func (fdb *FluxDB) setLastCheckpoint(batch store.Batch, height uint64, lastBlock bstream.BlockRef) error {
	return fdb.setCheckpoint(batch, fdb.lastCheckpointKey(), height, lastBlock)
}

func (fdb *FluxDB) setFinalCheckpoint(batch store.Batch, height uint64, lastBlock bstream.BlockRef) error {
	return fdb.setCheckpoint(batch, fdb.finalCheckpointKey(), height, lastBlock)
}

func (fdb *FluxDB) setCheckpoint(batch store.Batch, key []byte, height uint64, lastBlock bstream.BlockRef) error {
	cellData, err := proto.Marshal(&pbfluxdb.Checkpoint{
		Height: height,
		Block:  &pbbstream.BlockRef{Id: lastBlock.ID(), Num: lastBlock.Num()},
	})
	if err != nil {
		return fmt.Errorf("unable to marshal checkpoint: %w", err)
	}

	batch.SetLastCheckpoint(key, cellData)
	return nil
}
