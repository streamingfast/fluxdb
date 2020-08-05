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

	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/dtracing"
	"github.com/dfuse-io/fluxdb/store"
	"github.com/dfuse-io/logging"
	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	pbfluxdb "github.com/dfuse-io/pbgo/dfuse/fluxdb/v1"
	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
)

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

		if err := batch.FlushIfFull(ctx); err != nil {
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

func (fdb *FluxDB) VerifyAllShardsWritten(ctx context.Context) (uint64, bstream.BlockRef, error) {
	seen := make(map[string]bstream.BlockRef)
	highestHeight := uint64(0)

	if err := fdb.store.ScanLastShardsWrittenCheckpoint(ctx, []byte("shard-"), func(key []byte, value []byte) error {
		height, block, err := unmarshalCheckpoint(value)
		if err != nil {
			return fmt.Errorf("unable to unmarshal checkpoint: %w", err)
		}

		seen[string(bytes.TrimPrefix(key, []byte("shard-")))] = block
		if height > highestHeight {
			highestHeight = height
		}

		return nil
	}); err != nil {
		return 0, bstream.BlockRefEmpty, err
	}

	shardToBlock := make(map[string]bstream.BlockRef)

	var referenceBlock bstream.BlockRef
	for i := 0; i < fdb.shardCount; i++ {
		key := fmt.Sprintf("%03d", i)
		seenBlock, found := seen[key]
		if !found {
			seenBlock = bstream.BlockRefEmpty
		}

		shardToBlock[key] = seenBlock
		if i == 0 {
			referenceBlock = seenBlock
		}
	}

	var faultyShards []string
	var missingShards []string
	for key, seenBlock := range shardToBlock {
		if bstream.EqualsBlockRefs(seenBlock, bstream.BlockRefEmpty) {
			missingShards = append(missingShards, key)
		}
		if !bstream.EqualsBlockRefs(seenBlock, referenceBlock) {
			faultyShards = append(faultyShards, key)
		}
	}

	var err error
	if missingShards != nil {
		err = fmt.Errorf("missing shards: %v", missingShards)
	}

	if faultyShards != nil {
		err = fmt.Errorf("shards not matching reference block %s (shards %v): %w", referenceBlock, faultyShards, err)
	}

	return highestHeight, referenceBlock, err
}

func (fdb *FluxDB) WriteShardingFinalCheckpoint(ctx context.Context, height uint64, block bstream.BlockRef) error {
	batch := fdb.store.NewBatch(zlog)
	if err := fdb.setLastCheckpoint(batch, height, block); err != nil {
		return fmt.Errorf("set last checkpoint: %w", err)
	}

	if err := batch.Flush(ctx); err != nil {
		return fmt.Errorf("flushing last block marker: %w", err)
	}

	return nil
}

func (fdb *FluxDB) writeBlock(ctx context.Context, batch store.Batch, w *WriteRequest) (err error) {
	for _, entry := range w.SingletEntries {
		var value []byte
		if !entry.IsDeletion() {
			value, err = entry.MarshalValue()
			if err != nil {
				return fmt.Errorf("singlet to proto: %w", err)
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

		// We could group `w.TabletRows` by tablet here greatly reducing the number of time
		// we need to compute the tablet key, reducing memory allocation an GC at the same time.
		tabletKey := KeyForTablet(row.Tablet())
		fdb.idxCache.IncCount(tabletKey)
		if fdb.idxCache.shouldTriggerIndexing(tabletKey) {
			fdb.idxCache.ScheduleIndex(tabletKey, w.Height)
		}
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
	cellData, err := proto.Marshal(&pbfluxdb.Checkpoint{
		Height: height,
		Block:  &pbbstream.BlockRef{Id: lastBlock.ID(), Num: lastBlock.Num()},
	})
	if err != nil {
		return fmt.Errorf("unable to marshal checkpoint: %w", err)
	}

	batch.SetLastCheckpoint(fdb.lastCheckpointKey(), cellData)
	return nil
}
